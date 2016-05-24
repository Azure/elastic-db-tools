// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Shard management performance counter names.
    /// </summary>
    internal enum PerformanceCounterName
    {
        // counters to track cached mappings
        MappingsCount,
        MappingsAddOrUpdatePerSec,
        MappingsRemovePerSec,
        MappingsLookupSucceededPerSec,
        MappingsLookupFailedPerSec,
        // counters to track ShardManagement operations
        DdrOperationsPerSec,
    };

    /// <summary>
    /// Structure holding performance counter creation information
    /// </summary>
    internal struct PerfCounterCreationData
    {
        private PerformanceCounterName counterName;
        private PerformanceCounterType counterType;
        private string counterDisplayName;
        private string counterHelpText;

        public PerfCounterCreationData(PerformanceCounterName name, PerformanceCounterType type, string displayName, string helpText)
        {
            counterName = name;
            counterType = type;
            counterDisplayName = displayName;
            counterHelpText = helpText;
        }

        public PerformanceCounterName CounterName
        {
            get { return counterName; }
        }

        public PerformanceCounterType CounterType
        {
            get { return counterType; }
        }

        public string CounterDisplayName
        {
            get { return counterDisplayName; }
        }

        public string CounterHelpText
        {
            get { return counterHelpText; }
        }
    }

    /// <summary>
    /// Class represenging single instance of a all performance counters in shard management catagory
    /// </summary>
    internal class PerfCounterInstance : IDisposable
    {
        private static object _lockObject = new object();

        private static ILogger Tracer
        {
            get
            {
                return TraceHelper.Tracer;
            }
        }

        internal static readonly List<PerfCounterCreationData> counterList = new List<PerfCounterCreationData>()
        {
            new PerfCounterCreationData(PerformanceCounterName.MappingsCount, PerformanceCounterType.NumberOfItems64, PerformanceCounters.MappingsCountDisplayName, PerformanceCounters.MappingsCountHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsAddOrUpdatePerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsAddOrUpdatePerSecDisplayName, PerformanceCounters.MappingsAddOrUpdatePerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsRemovePerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsRemovePerSecDisplayName, PerformanceCounters.MappingsRemovePerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsLookupSucceededPerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsLookupSucceededPerSecDisplayName, PerformanceCounters.MappingsLookupSucceededPerSecHelpText),
            new PerfCounterCreationData(PerformanceCounterName.MappingsLookupFailedPerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.MappingsLookupFailedPerSecDisplayName, PerformanceCounters.MappingsLookupFailedPerSecHelpText),

            new PerfCounterCreationData(PerformanceCounterName.DdrOperationsPerSec, PerformanceCounterType.RateOfCountsPerSecond64, PerformanceCounters.DdrOperationsPerSecDisplayName, PerformanceCounters.DdrOperationsPerSecHelpText),
        };

        private Dictionary<PerformanceCounterName, PerformanceCounterWrapper> _counters;

        private bool _initialized;

        private string _instanceName;

        /// <summary>
        /// Initialize perf counter instance based on shard map name
        /// </summary>
        /// <param name="shardMapName"></param>
        public PerfCounterInstance(string shardMapName)
        {
            _initialized = false;

            _instanceName = string.Concat(Process.GetCurrentProcess().Id.ToString(), "-", shardMapName);

            try
            {
                // check if caller has permissions to create performance counters.
                if (!PerfCounterInstance.HasCreatePerformanceCounterPermissions())
                {
                    // Trace out warning and continue
                    Tracer.TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter,
                        "create",
                        "User does not have permissions to create performance counters, no performance data will be collected.");
                }
                else
                {
                    // check if PerformanceCounterCategory exists

                    if (!PerformanceCounterCategory.Exists(PerformanceCounters.ShardManagementPerformanceCounterCategory))
                    {
                        // We are not creating performance counter category here as per recommendation in documentation, copying note from
                        // https://msdn.microsoft.com/en-us/library/sb32hxtc(v=vs.110).aspx
                        // It is strongly recommended that new performance counter categories be created 
                        // during the installation of the application, not during the execution of the application.
                        // This allows time for the operating system to refresh its list of registered performance counter categories.
                        // If the list has not been refreshed, the attempt to use the category will fail.

                        // Trace out warning and continue
                        Tracer.TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter,
                            "create",
                            "Performance counter category {0} does not exist, no performance data will be collected.",
                            PerformanceCounters.ShardManagementPerformanceCounterCategory);
                    }
                    else
                    {
                        // Check if specific instance exists
                        if (PerformanceCounterCategory.InstanceExists(_instanceName,
                            PerformanceCounters.ShardManagementPerformanceCounterCategory))
                        {
                            // As performance counters are created with Process lifetime and instance name is unique (PID + shard map name),
                            // this should never happen. Trace out error and silently continue.
                            Tracer.TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter,
                                "create",
                                "Performance counter instance {0} already exists, no performance data will be collected.",
                                _instanceName);
                        }
                        else
                        {
                            // now initialize all counters for this instance
                            _counters = new Dictionary<PerformanceCounterName, PerformanceCounterWrapper>();

                            foreach (PerfCounterCreationData d in PerfCounterInstance.counterList)
                            {
                                _counters.Add(d.CounterName,
                                    new PerformanceCounterWrapper(
                                        PerformanceCounters.ShardManagementPerformanceCounterCategory, _instanceName,
                                        d.CounterDisplayName));
                            }

                            // check that atleast one performance counter was created, so that we can remove instance as part of Dispose()
                            _initialized = _counters.Any(c => c.Value._isValid = true);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // Note: If any of the initialization calls throws, log the exception and silently continue.
                // No perf data will be collected in this case.
                // All other non-static code paths access PerformanceCounter and PerformanceCounterCategory
                // objects only if _initialized is set to true.

                Tracer.TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter,
                    "PerfCounterInstance..ctor",
                    "Exception caught while creating performance counter instance, no performance data will be collected. Exception: {0}",
                    e.ToString());
            }
        }

        /// <summary>
        /// Try to increment specified performance counter by 1 for current instance.
        /// </summary>
        /// <param name="counterName">Counter to increment.</param>
        internal void IncrementCounter(PerformanceCounterName counterName)
        {
            if (_initialized)
            {
                PerformanceCounterWrapper pc;
                if (_counters.TryGetValue(counterName, out pc))
                {
                    pc.Increment();
                }
            }
        }

        /// <summary>
        ///  Try to update performance counter with speficied value.
        /// </summary>
        /// <param name="counterName">Counter to update.</param>
        /// <param name="value">New value.</param>
        internal void SetCounter(PerformanceCounterName counterName, long value)
        {
            if (_initialized)
            {
                PerformanceCounterWrapper pc;
                if (_counters.TryGetValue(counterName, out pc))
                {
                    pc.SetRawValue(value);
                }
            }
        }
        /// <summary>
        /// Static method to recreate Shard Management performance counter catagory with given counter list.
        /// </summary>
        internal static void CreatePerformanceCategoryAndCounters()
        {
            // Creation of performance counters need Administrator privilege
            if (HasCreatePerformanceCategoryPermissions())
            {
                // Delete performance counter category, if exists.
                if (PerformanceCounterCategory.Exists(PerformanceCounters.ShardManagementPerformanceCounterCategory))
                {
                    PerformanceCounterCategory.Delete(PerformanceCounters.ShardManagementPerformanceCounterCategory);
                }

                CounterCreationDataCollection smmCounters = new CounterCreationDataCollection();

                foreach (PerfCounterCreationData d in PerfCounterInstance.counterList)
                {
                    smmCounters.Add(new CounterCreationData(d.CounterDisplayName, d.CounterHelpText, d.CounterType));
                }

                PerformanceCounterCategory.Create(
                    PerformanceCounters.ShardManagementPerformanceCounterCategory,
                    PerformanceCounters.ShardManagementPerformanceCounterCategoryHelp,
                    PerformanceCounterCategoryType.MultiInstance,
                    smmCounters);
            }
            else
            {
                // Trace out warning and continue
                Tracer.TraceWarning(TraceSourceConstants.ComponentNames.PerfCounter,
                    "createCategory",
                    "User does not have permissions to create performance counter category");
            }
        }

        /// <summary>
        /// Check if caller has permissions to create performance counter catagory.
        /// </summary>
        /// <returns>If caller can create performance counter catagory</returns>
        internal static bool HasCreatePerformanceCategoryPermissions()
        {
            // PerformanceCounterCategory creation requires user to be part of Administrators group.

            WindowsPrincipal wp = new WindowsPrincipal(WindowsIdentity.GetCurrent());
            return wp.IsInRole(WindowsBuiltInRole.Administrator);
        }

        /// <summary>
        /// Check if caller has permissions to create performance counter instance
        /// </summary>
        /// <returns>If caller can create performance counter instance.</returns>
        internal static bool HasCreatePerformanceCounterPermissions()
        {
            // PerformanceCounter creation requires user to be part of Administrators or 'Performance Monitor Users' local group.
            WindowsPrincipal wp = new WindowsPrincipal(WindowsIdentity.GetCurrent());
            return wp.IsInRole(WindowsBuiltInRole.Administrator) || wp.IsInRole(PerformanceCounters.PerformanceMonitorUsersGroupName);
        }

        /// <summary>
        /// Dispose performance counter instance
        /// </summary>
        public void Dispose()
        {
            if (_initialized)
            {
                lock (_lockObject)
                {
                    // If performance counter instance exists, remove it here.
                    if (_initialized)
                    {
                        // We can assume here that performance counter catagory, instance and first counter in the cointerList exist as _initialized is set to true.
                        using (PerformanceCounter pcRemove = new PerformanceCounter())
                        {
                            pcRemove.CategoryName = PerformanceCounters.ShardManagementPerformanceCounterCategory;
                            pcRemove.CounterName = counterList.First().CounterDisplayName;
                            pcRemove.InstanceName = _instanceName;
                            pcRemove.InstanceLifetime = PerformanceCounterInstanceLifetime.Process;
                            pcRemove.ReadOnly = false;
                            // Removing instance using a single counter removes all counters for that instance.
                            pcRemove.RemoveInstance();
                        }
                    }
                    _initialized = false;
                }
            }
        }
    }
}