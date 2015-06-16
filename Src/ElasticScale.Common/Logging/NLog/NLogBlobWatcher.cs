#region "Usings"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Xml;
using System.IO;
using System.Diagnostics;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Blob;
#endregion


namespace Microsoft.Azure.SqlDatabase.ElasticScale.Common
{
    /// <summary>
    /// NLog extension class for checking for configuration updates
    /// in a shared blob container
    /// </summary>
    public class NLogBlobWatcher : MethodRunner, ILogConfigWatcher, IDisposable
    {
        public NLogBlobWatcher()
            : this(TimeSpan.FromMinutes(1))
        { }

        public NLogBlobWatcher(TimeSpan refreshInterval, string container = "telemetry-config")
            : base("BlobWatcher", "Logging")
        {
            _container = container;

            // Poll the container for updates
            // TODO : Use a system.timer here instead of observable
            //_timer = Observable
            //    .Timer(TimeSpan.Zero, refreshInterval)
            //    .Subscribe(e => CheckForUpdates());
        }

        #region "Private Variables"

        private object lockObj = new object();

        private string _container;

        private DateTime _lastUpdatedUtc = DateTime.MinValue;

        private IDisposable _timer;

        public event EventHandler OnConfigChange;

        #endregion

        private void InitializeContainer()
        {
            // Get the blob storage account (hard coded to use the same as the Windows Azure Diagnostics account)
            var storageAccount = ConfigurationHelper.GetDiagnosticsStorage();
            var blobClient = storageAccount.CreateCloudBlobClient();
            blobClient.ParallelOperationThreadCount = 1;
            blobClient.RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 3);

            // Create the container reference (create if it doesn't exist)
            var container = blobClient.GetContainerReference(_container);

            // Only perform the initialization once per instance
            if (container.CreateIfNotExists())
            {
                container.SetPermissions(new BlobContainerPermissions()
                {
                    PublicAccess = BlobContainerPublicAccessType.Off
                });
            }
        }

        private void CheckForUpdates()
        {
            // Use a mutex to prevent overlapping checks (most common whilst 
            // debugging)
            if (Monitor.TryEnter(lockObj, 1))
            {
                try
                {
                    // Get the blob storage account (hard coded to use the same as the Windows Azure Diagnostics account)
                    var storageAccount = ConfigurationHelper.GetDiagnosticsStorage();

                    // Get the blob and container client proxies
                    var blobClient = storageAccount.CreateCloudBlobClient();
                    blobClient.ParallelOperationThreadCount = 1;
                    blobClient.RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 3);                    
                    var container = blobClient.GetContainerReference(_container);

                    // Check to see if any of the configuration files exist:
                    // NLog.config
                    // NLog.[role name].config
                    var fileNames = new string[] 
                    {
                        "NLog.config",
                        String.Format("NLog.{0}.config", ConfigurationHelper.RoleName)
                    };

                    // Iterate through looking for a valid updated file
                    foreach (var f in fileNames)
                    {
                        var blob = container.GetBlockBlobReference(f);
                        var lastUpdated = DateTime.MinValue;

                        try
                        {
                            blob.FetchAttributes();
                            lastUpdated = blob.Properties.LastModified.Value.DateTime;
                        }
                        catch (Exception) { }

                        if (lastUpdated > _lastUpdatedUtc)
                        {
                            Trace.WriteLine("Updated config file {0} available");
                            UpdateConfiguration(blob);
                            _lastUpdatedUtc = lastUpdated;

                            if (OnConfigChange != null)
                                OnConfigChange(this, EventArgs.Empty);

                            return;
                        }
                    }
                }
                catch (Exception ex0)
                {
                    Logger.Warning(ex0, "Could not check for diagnostics configuration update");
                }
                finally
                {
                    Monitor.Exit(lockObj);
                }
            }
        }

        /// <summary>
        /// Given a new blob, update the local configuration
        /// </summary>
        /// <param name="blob"></param>
        private void UpdateConfiguration(CloudBlockBlob blob)
        {
            // Download to a local temporary file
            var tempFileName = System.IO.Path.GetTempFileName();

            using (var tempFile = new FileStream(tempFileName, FileMode.CreateNew, FileAccess.Write))
            {
                blob.DownloadToStream(tempFile);
            }

            var config = new global::NLog.Config.XmlLoggingConfiguration(tempFileName);

            Trace.WriteLine("Resetting NLog configuration");
            global::NLog.LogManager.Configuration = config;

            NLoggerFactory factory = new NLoggerFactory();
            factory.InitializeForCloud();                                  
        }

     
        public void Dispose()
        {
            if (_timer != null)
            {
                _timer.Dispose();
                _timer = null;
            }
        }
    }
}
