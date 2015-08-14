using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.Common.TransientFaultHandling.Implementation;
using System.Xml;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// SQL based data store that persists ShardMapManager data structures.
    /// </summary>
    internal class SqlStore : IStore
    {
        /// <summary>
        /// GSM version of store supported by this library.
        /// </summary>
        internal const int SqlStoreGsmVersion = 1;

        /// <summary>
        /// LSM Minor version of store supported by this library.
        /// </summary>
        internal const int SqlStoreLsmVersion = 1;

        /// <summary>
        /// Regular expression for go tokens.
        /// </summary>
        private static readonly Regex GoTokenRegularExpression = new Regex(
                @"^\s*go\s*$", 
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        /// <summary>
        /// Regular expression for comment lines.
        /// </summary>
        private static readonly Regex CommentLineRegularExpression = new Regex(
                @"^\s*--", 
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        /// <summary>
        /// Parsed representation of GSM existence check script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> CheckIfExistsGlobalScript = SqlStore.SplitScriptCommands(Scripts.CheckShardMapManagerGlobal);

        /// <summary>
        /// Parsed representation of GSM creation script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> CreateGlobalScript = SqlStore.SplitScriptCommands(Scripts.CreateShardMapManagerGlobal);

        /// <summary>
        /// Parsed representation of GSM drop script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> DropGlobalScript = SqlStore.SplitScriptCommands(Scripts.DropShardMapManagerGlobal);

        /// <summary>
        /// Parsed represenation of Schema Info Collection script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> CreateSchemaInfoScript = SqlStore.SplitScriptCommands(Scripts.CreateSchemaInfoCollectionGlobal);

        /// <summary>
        /// Parsed represenation of Schema Info Collection script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> DropSchemaInfoScript = SqlStore.SplitScriptCommands(Scripts.DropSchemaInfoCollectionGlobal);

        /// <summary>
        /// Parsed representation of LSM existence check script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> CheckIfExistsLocalScript = SqlStore.SplitScriptCommands(Scripts.CheckShardMapManagerLocal);

        /// <summary>
        /// Parsed representation of LSM creation script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> CreateLocalScript = SqlStore.SplitScriptCommands(Scripts.CreateShardMapManagerLocal);

        /// <summary>
        /// Parsed representation of LSM drop script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> DropLocalScript = SqlStore.SplitScriptCommands(Scripts.DropShardMapManagerLocal);

        /// <summary>
        /// SQL transient fault detection strategy.
        /// </summary>
        private static SqlDatabaseTransientErrorDetectionStrategy sqlTransientErrorDetector = new SqlDatabaseTransientErrorDetectionStrategy();
        
        /// <summary>
        /// Transient failure detector function.
        /// </summary>
        private static Func<Exception, bool> transientErrorDetector = (e) =>
            {
                ShardMapManagerException smmException = null;
                StoreException storeException = null;
                SqlException sqlException = null;

                smmException = e as ShardMapManagerException;

                if (smmException != null)
                {
                    storeException = smmException.InnerException as StoreException;
                }
                else
                {
                    storeException = e as StoreException;
                }

                if (storeException != null)
                {
                    sqlException = storeException.InnerException as SqlException;
                }
                else
                {
                    sqlException = e as SqlException;
                }

                if (sqlException != null)
                {
                    return sqlTransientErrorDetector.IsTransient(sqlException);
                }

                return false;
            };

        /// <summary>
        /// Credentials for store operations.
        /// </summary>
        private readonly SqlShardMapManagerCredentials credentials;

#if DEBUG

        /// <summary>
        /// Event to be raised on disposing global transaction scope.
        /// This event is used for internal testing purpose only.
        /// </summary>
        internal event EventHandler<SqlStoreEventArgs> SqlStoreEventGlobal;

        /// <summary>
        /// Event to be raised on disposing local transaction scope.
        /// This event is used for internal testing purpose only.
        /// </summary>
        internal event EventHandler<SqlStoreEventArgs> SqlStoreEventLocal;

#endif // DEBUG

        /// <summary>
        /// Instantiates a store object using the credentials provided by the user.
        /// </summary>
        /// <param name="credentials">Credentials for store operations.</param>
        protected internal SqlStore(SqlShardMapManagerCredentials credentials)
        {
            Debug.Assert(credentials != null);
            this.credentials = credentials;
        }

        #region Transient Error Detection

        /// <summary>
        /// Returns a function that is capable of detecting transient errors for the store.
        /// </summary>
        /// <returns>Delegate that can detect transient failure for the store.</returns>
        public virtual Func<Exception, bool> GetTransientErrorDetector()
        {
            return SqlStore.transientErrorDetector;
        }

        #endregion Transient Error Detection

        #region Transaction Management

        /// <summary>
        /// Connects to the Global ShardMapManager data source and starts a transaction. When disposed
        /// based on the state of Success property, either the transaction will be committed or it will
        /// be aborted.
        /// </summary>
        /// <returns>Batch scope object.</returns>
        /// <remarks>Use the disposable pattern with the object returned from this method.</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Callers take care of disposal")]
        public virtual IStoreTransactionScope GetTransactionScopeGlobal()
        {
            try
            {
                SqlTransactionScopeGlobal txnScopeGlobal = new SqlTransactionScopeGlobal(this.credentials.ConnectionStringShardMapManager);
                
#if DEBUG
                EventHandler<SqlStoreEventArgs> handler = this.SqlStoreEventGlobal;
                if (handler != null)
                {
                    // If there are any subscribers for SqlStoreEventGlobal event, then register a subscriber function with SqlTransactionScopeGlobal
                    //
                    txnScopeGlobal.TxnScopeGlobalDisposeEvent += TxnScopeGlobalEventHandler;
                }
#endif // DEBUG

                return txnScopeGlobal;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetTransactionScopeGlobal_SqlException,
                    se);
            }
        }

        /// <summary>
        /// Connects to the Local ShardMapManager data source and starts a transaction. When disposed
        /// based on the state of Success property, either the transaction will be committed or it will
        /// be aborted.
        /// </summary>
        /// <returns>Batch scope object.</returns>
        /// <remarks>Use the disposable pattern with the object returned from this method.</remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Callers take care of disposal")]
        public virtual IStoreTransactionScope GetTransactionScopeLocal(ShardLocation location)
        {
            try
            {
                SqlTransactionScopeLocal txnScopeLocal = new SqlTransactionScopeLocal(location, this.credentials.ConnectionStringShard);

#if DEBUG
                EventHandler<SqlStoreEventArgs> handler = this.SqlStoreEventLocal;
                if (handler != null)
                {
                    // If there are any subscribers for SqlStoreEventLocal event, then register a subscriber function with SqlTransactionScopeGlobal
                    //
                    txnScopeLocal.TxnScopeLocalDisposeEvent += TxnScopeLocalEventHandler;
                }
#endif // DEBUG

                return txnScopeLocal;                
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetTransactionScopeLocal_SqlException,
                    se,
                    location);
            }
        }

        #endregion Transaction Management

        #region Storage Structures

        /// <summary>
        /// Check if global shard map manager data structures already exist.
        /// </summary>
        /// <returns>true if data structures already exist, false otherwise.</returns>
        public virtual bool CheckIfExistGlobal()
        {
            try
            {
                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    return SqlStore.CheckIfExistsHelper(cmd, SqlStore.CheckIfExistsGlobalScript);
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CheckIfExistsGlobal_SqlException, 
                    se);
            }
        }

        /// <summary>
        /// Create the global shard map manager data structures for persistence.
        /// </summary>
        public virtual void CreateGlobal()
        {
            try
            {
                SqlStore.ExecuteSqlScriptGlobalHelper(SqlStore.CreateGlobalScript);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CreateDropGlobal_SqlException,
                    se,
                    "Create");
            }
        }

        /// <summary>
        /// Delete the global shard map manager data structures.
        /// </summary>
        public virtual void DropGlobal()
        {
            try
            {
                SqlStore.ExecuteSqlScriptGlobalHelper(SqlStore.DropGlobalScript);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CreateDropGlobal_SqlException,
                    se,
                    "Delete");
            }
        }

        /// <summary>
        /// Create Schema Info Collection global data structures for persistence.
        /// </summary>
        public void CreateSchemaInfoCollectionGlobal()
        {
            try
            {
                SqlStore.ExecuteSqlScriptGlobalHelper(SqlStore.CreateSchemaInfoScript);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CreateDropSchemaInfoCollectionGlobal_SqlException,
                    se,
                    "Create",
                    "CreateSchemaInfoScript");
            }
        }

        /// <summary>
        /// Drop any Schema Info Collection global data structures.
        /// </summary>
        public void DropSchemaInfoCollectionGlobal()
        {
            try
            {
                SqlStore.ExecuteSqlScriptGlobalHelper(SqlStore.DropSchemaInfoScript);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CreateDropSchemaInfoCollectionGlobal_SqlException,
                    se,
                    "Delete",
                    "DropSchemaInfoScript");
            }
        }

        /// <summary>
        /// Checks if local shard map manager data structures already exist at <paramref name="location"/>.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <returns>true if data structures already exist, false otherwise.</returns>
        public virtual bool CheckIfExistLocal(ShardLocation location)
        {
            try
            {
                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
                {
                    return SqlStore.CheckIfExistsHelper(cmd, SqlStore.CheckIfExistsLocalScript);
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CheckIfExistsLocal_SqlException,
                    se,
                    location);
            }
        }

        /// <summary>
        /// Create the local shard map manager data structures for persistence and consistency at <paramref name="location"/>.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        public virtual void CreateLocal(ShardLocation location)
        {
            try
            {
                SqlStore.ExecuteSqlScriptLocalHelper(location, SqlStore.CreateLocalScript);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CreateDropLocal_SqlException,
                    se,
                    "Create",
                    location);
            }
        }

        /// <summary>
        /// Delete the local shard map manager data structures at given <paramref name="location"/>.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        public virtual void DropLocal(ShardLocation location)
        {
            try
            {
                SqlStore.ExecuteSqlScriptLocalHelper(location, SqlStore.DropLocalScript);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_CreateDropLocal_SqlException,
                    se,
                    "Delete",
                    location);
            }
        }

        #endregion Storage Structures

        #region ShardMapInterfaces

        /// <summary>
        /// Obtains all the ShardMaps from the Global ShardMapManager data source.
        /// </summary>
        /// <returns>Result of the execution.</returns>
        public virtual IStoreResults GetShardMapsGlobal()
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_getAllShardMapsGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;
                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetAllShardMapsGlobal_SqlException,
                    se);
            }
        }

        /// <summary>
        /// Adds a shard map entry to Global ShardMapManager data source.
        /// </summary>
        /// <param name="ssm">ShardMap to be added.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddShardMapGlobal(IStoreShardMap ssm)
        {
            return SqlStore.AddAttachShardMapGlobalHelper(ssm, false);
        }

        /// <summary>
        /// Attaches a shard map entry to Global ShardMapManager data source.
        /// </summary>
        /// <param name="ssm">ShardMap to be added.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AttachShardMapGlobal(IStoreShardMap ssm)
        {
            return SqlStore.AddAttachShardMapGlobalHelper(ssm, true);
        }

        /// <summary>
        /// Removes a shard map entry from Global ShardMapManager data source.
        /// </summary>
        /// <param name="ssm">ShardMap to be removed.</param>
        public virtual IStoreResults RemoveShardMapGlobal(IStoreShardMap ssm)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_removeShardMapGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_version", ssm.Version.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMapManager_GlobalOperation_SqlException,
                    se,
                    "Remove");
            }
        }

        /// <summary>
        /// Updates a shard map entry in Global ShardMapManager data source.
        /// </summary>
        /// <param name="ssm">ShardMap information for update.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults UpdateShardMapGlobal(IStoreShardMap ssm)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_updateShardMapGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;
                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_kind", ((int)ssm.Kind).ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("sm_status", ((int)ssm.Status).ToString()),
                        new XElement("sm_keykind", ((int)ssm.KeyKind).ToString()),
                        new XElement("sm_hashkind", ((int)ssm.HashKind).ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMapManager_GlobalOperation_SqlException,
                    se,
                    "Update");
            }
        }

        /// <summary>
        /// Looksup a shard map from Global ShardMapManager data source.
        /// </summary>
        /// <param name="shardMapName">Name of the shard map.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindShardMapByNameGlobal(string shardMapName)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_findShardMapByNameGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;
                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_name", shardMapName)
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMapManager_GlobalOperation_SqlException,
                    se,
                    "FindByName");
            }
        }

        /// <summary>
        /// Looks up a shard map from Global ShardMapManager data source.
        /// </summary>
        /// <param name="smId">Id of the shard map.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindShardMapByIdGlobal(Guid smId)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_findShardMapByIdGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", smId.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMapManager_GlobalOperation_SqlException,
                    se,
                    "FindById");
            }
        }

        /// <summary>
        /// Obtains all the ShardMaps from the Local ShardMapManager data source.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <returns>Result of the execution.</returns>
        public virtual IStoreResults GetShardMapsLocal(ShardLocation location)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
                {
                    cmd.CommandText = @"__ShardManagement.smm_getAllShardMapsLocal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("lsm_version", SqlStoreLsmVersion.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetShardMapsLocal_SqlException,
                    se,
                    location);
            }
        }


        /// <summary>
        /// Adds a shard map entry to Local Shard data source.
        /// </summary>
        /// <param name="ss">Shard where shard map is being added.</param>
        /// <param name="ssm">ShardMap to be added.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddShardMapLocal(IStoreShard ss, IStoreShardMap ssm)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(ss.Location))
                {
                    cmd.CommandText = @"__ShardManagement.smm_addShardMapLocal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_name", ssm.Name),
                        new XElement("sm_kind", ((int)ssm.Kind).ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("sm_status", ((int)ssm.Status).ToString()),
                        new XElement("sm_keykind", ((int)ssm.KeyKind).ToString()),
                        new XElement("sm_hashkind", ((int)ssm.HashKind).ToString()),
                        new XElement("s_id", ss.Id.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_LocalOperation_SqlException,
                    se,
                    "Add",
                    ssm.Name,
                    ss.Location);
            }
        }

        /// <summary>
        /// Removes a shard map entry from Local Shard data source.
        /// </summary>
        /// <param name="ss">Shard where shard map is being removed.</param>
        /// <param name="ssm">ShardMap to be removed.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemoveShardMapLocal(IStoreShard ss, IStoreShardMap ssm)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(ss.Location))
                {
                    cmd.CommandText = @"__ShardManagement.smm_removeShardMapLocal";
                    cmd.CommandType = CommandType.StoredProcedure;
                    
                    XElement input = new XElement(cmd.CommandText,
                        new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("s_id", ss.Id.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_LocalOperation_SqlException,
                    se,
                    "Remove",
                    ssm.Name,
                    ss.Location);
            }
        }

        #endregion ShardMapInterfaces

        #region ShardInterfaces

        /// <summary>
        /// Obtains <paramref name="numShards"/>shards from the Global ShardMapManager data 
        /// source for a given shard map which specify the given status.
        /// </summary>
        /// <param name="ssm">Shard map to get shards for.</param>
        /// <param name="numShards">Optional number of shards to get.</param>
        /// <param name="status">Optional shard status to be matched.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults GetShardsGlobal(IStoreShardMap ssm, int? numShards, int? status)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_getShardsGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;
                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("num_shards",
                                numShards.HasValue ? numShards.ToString() : null,
                                new XAttribute("is_null", numShards.HasValue ? "false" : "true")
                                ),
                        new XElement("s_status",
                            status.HasValue ? status.ToString() : null,
                            new XAttribute("is_null", status.HasValue ? "false" : "true")
                            )
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetShardsGlobal_SqlException,
                    se,
                    ssm.Name);
            }
        }

        /// <summary>
        /// Obtains a Shard given the location.
        /// </summary>
        /// <param name="ssm">Shard map to get shard for.</param>
        /// <param name="location">Location of the shard.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults GetShardByLocationGlobal(IStoreShardMap ssm, ShardLocation location)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_getShardByLocationGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                    new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("sm_version", ssm.Version.ToString()),
                    new XElement("s_datasource", location.DataSource),
                    new XElement("s_database", location.Database)
                    );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetShardByLocationGlobal_SqlException,
                    se,
                    location,
                    ssm.Name);
            }
        }

        /// <summary>
        /// Adds a shard to Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to add shard to.</param>
        /// <param name="ss">Shard to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddShardGlobal(IStoreShardMap ssm, IStoreShard ss)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_addShardGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("s_id", ss.Id.ToString()),
                        new XElement("s_datasource", ss.Location.DataSource),
                        new XElement("s_database", ss.Location.Database),
                        new XElement("s_version", ss.Version.ToString()),
                        new XElement("s_status", ss.Status.ToString()),
                        new XElement("s_custom", (ss.Custom == null) ? null : StringUtils.ByteArrayToString(ss.Custom),
                            new XAttribute("is_null", (ss.Custom == null) ? "true" : "false")
                            )
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_GlobalOperation_SqlException,
                    se,
                    "Add",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Removes a shard from Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to remove shard from.</param>
        /// <param name="ss">Shard to remove</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemoveShardGlobal(IStoreShardMap ssm, IStoreShard ss)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_removeShardGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("s_id", ss.Id.ToString()),
                        new XElement("s_version", ss.Version.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_GlobalOperation_SqlException,
                    se,
                    "Remove",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Detaches a shard from Global ShardMap.
        /// </summary>
        /// <param name="location">Location of the shard.</param>
        /// <param name="shardmapName">Optional string to filter on shardmapName</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults DetachShardGlobal(ShardLocation location, string shardmapName = null)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_detachShardGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_name", shardmapName,
                            new XAttribute("is_null", (null == shardmapName) ? "true" : "false")
                            ),
                        new XElement("s_datasource", location.DataSource),
                        new XElement("s_database", location.Database)
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMapManager_GlobalOperation_SqlException,
                    se,
                    "DetachShard");
            }
        }

        /// <summary>
        /// Updates a shard in Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to update shard in.</param>
        /// <param name="ssOld">Old snapshot of the shard in the cache before update.</param>
        /// <param name="ssNew">Updated shard.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults UpdateShardGlobal(IStoreShardMap ssm, IStoreShard ssOld, IStoreShard ssNew)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_updateShardGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("s_id", ssNew.Id.ToString()),
                        new XElement("s_original_version", ssOld.Version.ToString()),
                        new XElement("s_status", ssNew.Status.ToString()),
                        new XElement("s_custom", (ssNew.Custom == null) ? null : StringUtils.ByteArrayToString(ssNew.Custom),
                            new XAttribute("is_null", (ssNew.Custom == null) ? "true" : "false")
                            )
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_GlobalOperation_SqlException,
                    se,
                    "Update",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Validate the version of a shard and it's containing shard map.
        /// </summary>
        /// <param name="ssm">Shard map that owns the shard.</param>
        /// <param name="ss">Shard to validate.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults ValidateShardGlobal(IStoreShardMap ssm, IStoreShard ss)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_validateShardGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("s_id", ss.Id.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ValidateShard_SqlException,
                    se,
                    ss.Location,
                    ssm.Name);
            }
        }

        #endregion ShardInterfaces

        #region SchemaInfoInterfaces

        /// <summary>
        /// Create sharding schma info with a given name. This information can be used during Split\Merge or
        /// any any other operation that requires such metadata.
        /// </summary>
        /// <param name="name">Name associated with the schema info.</param>
        /// <param name="si">The schma info in XML format.</param>
        /// <returns>Storage operation result.</returns>
        public IStoreResults AddShardingSchemaInfo(string name, SqlXml si)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_setShardingSchemaInfo";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("metadata_name", name),
                        new XElement("schema_info",
                            XElement.Load(si.CreateReader())));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_SchemaInfo_SqlException,
                    se,
                    "creating",
                    name);
            }
        }

        /// <summary>
        /// Update sharding schma info with a given name.
        /// </summary>
        /// <param name="name">Name associated with the schma info.</param>
        /// <param name="si">The schma info in XML format.</param>
        /// <returns>Storage operation result.</returns>
        public IStoreResults UpdateShardingSchemaInfo(string name, SqlXml si)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_updateShardingSchemaInfo";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("metadata_name", name),
                        new XElement("schema_info",
                            XElement.Load(si.CreateReader())));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_SchemaInfo_SqlException,
                    se,
                    "updating",
                    name);
            }
        }

        /// <summary>
        /// Fetch sharding schma info with a given name.
        /// </summary>
        /// <param name="name">Name associated with the schma info.</param>
        /// <param name="si">The schma info in XML format.</param>
        /// <returns>Storage operation result.</returns>
        public IStoreResults GetShardingSchemaInfo(string name, out SqlXml si)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_getShardingSchemaInfo";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("metadata_name", name));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    result.Result = (StoreResult)resultParam.Value;
                    si = (StoreResult.Success == result.Result) ? result.StoreSchemaInfoCollection.Single().ShardingSchemaInfo : null;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_SchemaInfo_SqlException,
                    se,
                    "retrieving",
                    name);
            }
        }

        /// <summary>
        /// Fetch all the schema info.
        /// </summary>
        /// <param name="siCollection">The schema info collection.</param>
        /// <returns>Storage operation result.</returns>
        public IStoreResults GetShardingSchemaInfoAll(out Dictionary<string, SqlXml> siCollection)
        {
            siCollection = new Dictionary<string, SqlXml>();

            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_getAllShardingSchemaInfo";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    result.Result = (StoreResult)resultParam.Value;


                    foreach (IStoreSchemaInfo si in result.StoreSchemaInfoCollection)
                    {
                        siCollection.Add(si.Name, si.ShardingSchemaInfo);
                    }

                    return result;
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_SchemaInfo_SqlException,
                    se,
                    "retrieving",
                    "all persisted names");
            }
        }

        /// <summary>
        /// Delete a schma info entry with a given name.
        /// </summary>
        /// <param name="name">Name associated with the schma info.</param>
        /// <returns>Storage operation result.</returns>        
        public IStoreResults DeleteShardingSchemaInfo(string name)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_deleteShardingSchemaInfo";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("metadata_name", name));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_SchemaInfo_SqlException,
                    se,
                    "deleting",
                    name);
            }
        }

        #endregion

        #region MappingInterfaces

        #region PointMappingInterfaces

#if FUTUREWORK
        /// <summary>
        /// Add point mapping to Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddPointMappingsLocal(ShardLocation location, IEnumerable<IStoreMapping> sms)
        {
            try
            {
                return SqlStore.BulkAddMappingLocalHelper(location, sms);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "AddMappings",
                    "Point",
                    location);
            }
        }

        /// <summary>
        /// Bulk add point mappings to Global ShardMap without consistency checks.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sms">Shard mappings to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddPointMappingsGlobal(IStoreShardMap ssm, IEnumerable<IStoreMapping> sms)
        {
            try
            {
                return SqlStore.BulkAddMappingGlobalHelper(ssm, sms);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "AddMappings",
                    "Point",
                    ssm.Name);
            }
        }
#endif

        /// <summary>
        /// Add point mapping to Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddPointMappingGlobal(IStoreShardMap ssm, IStoreMapping sm)
        {
            try
            {
                return SqlStore.AddMappingGlobalHelper(ssm, sm, @"__ShardManagement.smm_addPointShardMappingGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "AddMapping",
                    "Point",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Removes point mapping from Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to remove mapping from.</param>
        /// <param name="sm">Shard mapping to remove.</param>
        /// <param name="force">Whether to ignore the Online status.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemovePointMappingGlobal(IStoreShardMap ssm, IStoreMapping sm, bool force = false)
        {
            try
            {
                return SqlStore.RemoveMappingGlobalHelper(ssm, sm, force, @"__ShardManagement.smm_removePointShardMappingGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "RemoveMapping",
                    "Point",
                    ssm.Name);
            }

        }

        /// <summary>
        /// Finds point mapping which contain the given key for the given ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="shardKey">ShardKey being searched.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindPointMappingByKeyGlobal(IStoreShardMap ssm, ShardKey shardKey)
        {
            try
            {
                return SqlStore.FindMappingByKeyGlobalHelper(ssm, shardKey.RawValue, @"__ShardManagement.smm_findPointMappingByKeyGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "FindMappingByKey",
                    "Point",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Finds point mapping which contain the given range for the shard map. 
        /// If range is not given:
        ///     If shard is given, finds all mappings for the shard.
        ///     If shard is also not given, finds all the mappings.
        /// If range is given:
        ///     If shard is given, finds all mappings for the shard in the range.
        ///     If shard is not given, finds all the mappings in the range.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="range">Optional range to find mappings in.</param>
        /// <param name="shard">Option shard to find mappings in.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindPointMappingByRangeGlobal(IStoreShardMap ssm, ShardRange range, IStoreShard shard)
        {
            try
            {
                return SqlStore.FindMappingByRangeGlobalHelper(ssm, range, shard, @"__ShardManagement.smm_getAllPointShardMappingsGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "FindMappingsForRange",
                    "Point",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Add point mapping to local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddPointMappingLocal(ShardLocation location, IStoreMapping sm)
        {
            try
            {
                return SqlStore.AddMappingLocalHelper(location, sm, @"__ShardManagement.smm_addPointShardMappingLocal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "AddMapping",
                    "Point",
                    location);
            }
        }

        /// <summary>
        /// Removes point mapping from local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="sm">Shard mapping to remove.</param>
        /// <param name="force">Whether to force removal of mapping irrespective of status.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemovePointMappingLocal(ShardLocation location, IStoreMapping sm, bool force)
        {
            try
            {
                return SqlStore.RemoveMappingLocalHelper(location, sm, force, @"__ShardManagement.smm_removePointShardMappingLocal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "RemoveMapping",
                    "Point",
                    location);
            }
        }

        /// <summary>
        /// Finds point mapping which contain the given range for the shard map. 
        /// If range is not given:
        ///     If shard is given, finds all mappings for the shard.
        ///     If shard is also not given, finds all the mappings.
        /// If range is given:
        ///     If shard is given, finds all mappings for the shard in the range.
        ///     If shard is not given, finds all the mappings in the range.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="range">Optional range to find mappings in.</param>
        /// <param name="shard">Shard to find mappings in.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindPointMappingByRangeLocal(IStoreShardMap ssm, ShardRange range, IStoreShard shard)
        {
            try
            {
                return SqlStore.FindMappingByRangeLocalHelper(ssm, range, shard, @"__ShardManagement.smm_getAllPointShardMappingsLocal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "FindMappingsForRange",
                    "Point",
                    shard.Location);
            }
        }

        /// <summary>
        /// Finds point mapping which contain the given range for the shard map. 
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="key">Key to which a corresponding mapping should be found.</param>
        /// <param name="shard">Shard container requiring Id and ShardLocation.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindPointMappingByKeyLocal(IStoreShardMap ssm, ShardKey key, IStoreShard shard)
        {
            try
            {
                return SqlStore.FindMappingByKeyLocalHelper(ssm, key, shard);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "FindMappingForKey",
                    "Point",
                    shard.Location);
            }
        }

        #endregion PointMappingInterfaces

        #region RangeMappingInterfaces

#if FUTUREWORK
        /// <summary>
        /// Bulk add range mappings to Global ShardMap without consistency checks.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sms">List of shard mapping to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddRangeMappingsGlobal(IStoreShardMap ssm, IEnumerable<IStoreMapping> sms)
        {
            try
            {
                return SqlStore.BulkAddMappingGlobalHelper(ssm, sms);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "AddMappings",
                    "Range",
                    ssm.Name);
            }
        }
#endif

        /// <summary>
        /// Add range mapping to Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddRangeMappingGlobal(IStoreShardMap ssm, IStoreMapping sm)
        {
            try
            {
                return SqlStore.AddMappingGlobalHelper(ssm, sm, @"__ShardManagement.smm_addRangeShardMappingGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "AddMapping",
                    "Range",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Add range mapping to Global ShardMap within existing range mapping.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <param name="range">Range to add.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddRangeWithinRangeMappingGlobal(IStoreShardMap ssm, IStoreMapping sm, ShardRange range, Guid lockOwnerId)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_addRangeShardMappingWithinRangeGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", sm.ShardMapId.ToString()),
                        new XElement("lo_id", lockOwnerId.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("m_id", sm.Id.ToString()),
                        new XElement("m_min_value", StringUtils.ByteArrayToString(range.Low.RawValue)),
                        new XElement("m_max_value",
                            (range.High == null) ? null : StringUtils.ByteArrayToString(range.High.RawValue),
                            new XAttribute("is_null", (range.High == null) ? "true" : "false")
                            ),
                        new XElement("m_version", sm.Version.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_RangeShardMap_AddRemoveRange_SqlException,
                    se,
                    "AddRange",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Removes range mapping from Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to remove mapping from.</param>
        /// <param name="sm">Shard mapping to remove.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <param name="force">Whether to ignore Online status</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemoveRangeMappingGlobal(IStoreShardMap ssm, IStoreMapping sm, Guid lockOwnerId, bool force = true)
        {
            try
            {
                return SqlStore.RemoveMappingGlobalHelper(ssm, sm, force, @"__ShardManagement.smm_removeRangeShardMappingGlobal", true, lockOwnerId);
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "RemoveMapping",
                    "Range",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Removes range mapping within existing range mapping.
        /// </summary>
        /// <param name="ssm">Shard map to remove range mapping to.</param>
        /// <param name="sm">Shard mapping to remove range from.</param>
        /// <param name="range">Range to remove.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemoveRangeWithinRangeMappingGlobal(IStoreShardMap ssm, IStoreMapping sm, ShardRange range, Guid lockOwnerId)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_removeRangeShardMappingWithinRangeGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", sm.ShardMapId.ToString()),
                        new XElement("lo_id", lockOwnerId.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("m_id", sm.Id.ToString()),
                        new XElement("m_min_value", StringUtils.ByteArrayToString(range.Low.RawValue)),
                        new XElement("m_max_value",
                            (range.High == null) ? null : StringUtils.ByteArrayToString(range.High.RawValue),
                            new XAttribute("is_null", (range.High == null) ? "true" : "false")
                            ),
                        new XElement("m_version", sm.Version.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_RangeShardMap_AddRemoveRange_SqlException,
                    se,
                    "RemoveRange",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Splits given range mapping into 2 new mappings.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="sm">Store mapping to split.</param>
        /// <param name="splitPoint">Split point in the mapping.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults SplitRangeMappingGlobal(IStoreShardMap ssm, IStoreMapping sm, ShardKey splitPoint, Guid lockOwnerId)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_splitRangeShardMappingGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("lo_id", lockOwnerId.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("m_id", sm.Id.ToString()),
                        new XElement("m_version", sm.Version.ToString()),
                        new XElement("split_point", StringUtils.ByteArrayToString(splitPoint.RawValue))
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_RangeShardMap_SplitMerge_SqlException,
                    se,
                    "Split",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Merges the given range mappings into a single mapping.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="smLeft">Left store mapping to merge.</param>
        /// <param name="smRight">Right store mapping to merge.</param>
        /// <param name="leftLockOwnerId">Left mapping lock owner id</param>
        /// <param name="rightLockOwnerId">Right mapping lock owner id</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults MergeRangeMappingsGlobal(IStoreShardMap ssm, IStoreMapping smLeft, IStoreMapping smRight,
            Guid leftLockOwnerId, Guid rightLockOwnerId)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_mergeRangeShardMappingsGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("m_id_left", smLeft.Id.ToString()),
                        new XElement("m_version_left", smLeft.Version.ToString()),
                        new XElement("lo_id_left", leftLockOwnerId.ToString()),
                        new XElement("m_id_right", smRight.Id.ToString()),
                        new XElement("m_version_right", smRight.Version.ToString()),
                        new XElement("lo_id_right", rightLockOwnerId.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_RangeShardMap_SplitMerge_SqlException,
                    se,
                    "Merge",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Finds range mapping which contain the given key for the given ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="shardKey">ShardKey being searched.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindRangeMappingByKeyGlobal(IStoreShardMap ssm, ShardKey shardKey)
        {
            try
            {
                return SqlStore.FindMappingByKeyGlobalHelper(ssm, shardKey.RawValue, @"__ShardManagement.smm_findRangeMappingByKeyGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "FindMappingByKey",
                    "Range",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Finds range mapping which contain the given range for the shard map. 
        /// If range is not given:
        ///     If shard is given, finds all mappings for the shard.
        ///     If shard is also not given, finds all the mappings.
        /// If range is given:
        ///     If shard is given, finds all mappings for the shard in the range.
        ///     If shard is not given, finds all the mappings in the range.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="range">Optional range to find mappings in.</param>
        /// <param name="shard">Option shard to find mappings in.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindRangeMappingByRangeGlobal(IStoreShardMap ssm, ShardRange range, IStoreShard shard)
        {
            try
            {
                return SqlStore.FindMappingByRangeGlobalHelper(ssm, range, shard, @"__ShardManagement.smm_getAllRangeShardMappingsGlobal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingGlobal_SqlException,
                    se,
                    "FindMappingsForRange",
                    "Range",
                    ssm.Name);
            }
        }

        /// <summary>
        /// Locks or unlocks the given range mapping
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <param name="lockOwnerId">The lock owner id of this mapping</param>
        /// <param name="lockOwnerIdOpType">Operation to perform on this mapping with the given lockOwnerId</param>
        /// <returns></returns>
        public virtual IStoreResults LockOrUnlockRangeMappingGlobal(IStoreShardMap ssm, IStoreMapping sm, Guid lockOwnerId, LockOwnerIdOpType lockOwnerIdOpType)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_lockOrUnlockShardMappingGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("m_id", sm.Id.ToString()),
                        new XElement("sm_id", sm.ShardMapId.ToString()),
                        new XElement("lo_id", lockOwnerId.ToString()),
                        new XElement("s_id", sm.StoreShard.Id.ToString()),
                        new XElement("m_version", sm.Version.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("lo_id_op_type", (int)lockOwnerIdOpType));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_UpdateMappingGlobal_SqlException,
                    se,
                    "LockOrUnlockMapping",
                    sm.StoreShard.Location,
                    ssm.Name);
            }
        }

        /// <summary>
        /// Unlocks all mappings in the specified map that 
        /// belong to the given lock owner id
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="lockOwnerId">The lock owner id of this mapping</param>
        /// <returns></returns>
        public virtual IStoreResults UnlockAllMappingsWithLockOwnerIdGobal(IStoreShardMap ssm, Guid lockOwnerId)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_unlockAllMappingsWithLockIdGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("lo_id", lockOwnerId.ToString()),
                        new XElement("s_id", ssm.ShardId.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()));

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_UnlockAllMappingsWithLockOwnerId_SqlException,
                    se,
                    lockOwnerId,
                    ssm.Name);
            }
        }

        /// <summary>
        /// Add range mapping to local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults AddRangeMappingLocal(ShardLocation location, IStoreMapping sm)
        {
            try
            {
                return SqlStore.AddMappingLocalHelper(location, sm, @"__ShardManagement.smm_addRangeShardMappingLocal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "AddMapping",
                    "Range",
                    location);
            }
        }

        /// <summary>
        /// Replace range mapping in Local ShardMap with upto 3 new mappings.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="smOld">Original store mapping.</param>
        /// <param name="smList">List of store mappings to add.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults ReplaceRangeMappingLocal(ShardLocation location, IStoreMapping smOld, IEnumerable<IStoreMapping> smList)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
                {
                    XElement[] xe = new XElement[3];

                    using (IEnumerator<IStoreMapping> smEnum = smList.GetEnumerator())
                    {
                        for (int i = 1; i <= 3; i++)
                        {
                            string index = i.ToString(CultureInfo.InvariantCulture);

                            IStoreMapping mapping = smEnum.MoveNext() ? smEnum.Current : null;

                            xe[i - 1] = new XElement("root",
                                new XElement("m_id_" + index, (mapping == null) ? null : mapping.Id.ToString(),
                                    new XAttribute("is_null", (mapping == null) ? "true" : "false" )
                                    ),
                                new XElement("m_min_value_" + index, (mapping == null) ? null : StringUtils.ByteArrayToString(mapping.MinValue),
                                    new XAttribute("is_null", (mapping == null) ? "true" : "false" )
                                    ),
                                new XElement("m_max_value_" + index, (mapping == null) ? null : ((mapping.MaxValue == null) ? null : StringUtils.ByteArrayToString(mapping.MaxValue)),
                                    new XAttribute("is_null", (mapping == null) ? "true" : ((mapping.MaxValue == null) ? "true" : "false" ))
                                    )
                                );
                        }
                    }

                    cmd.CommandText = @"__ShardManagement.smm_replaceRangeShardMappingsLocal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                        new XElement("sm_id", smOld.ShardMapId.ToString()),
                        new XElement("m_id", smOld.Id.ToString()),
                        new XElement("m_version", smOld.Version.ToString()),
                        xe[0].Elements(),
                        xe[1].Elements(),
                        xe[2].Elements()
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_RangeShardMap_MergeReplaceLocal_SqlException,
                    se,
                    "ReplaceRangeMappings",
                    location);
            }
        }

        /// <summary>
        /// Removes range mapping from local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="sm">Shard mapping to remove.</param>
        /// <param name="force">Whether to force removal of mapping irrespective of status.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults RemoveRangeMappingLocal(ShardLocation location, IStoreMapping sm, bool force)
        {
            try
            {
                return SqlStore.RemoveMappingLocalHelper(location, sm, force, @"__ShardManagement.smm_removeRangeShardMappingLocal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "RemoveMapping",
                    "Range",
                    location);
            }
        }

        /// <summary>
        /// Merges the given range mappings into a single mapping.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="smLeft">Left store mapping to merge.</param>
        /// <param name="smRight">Right store mapping to merge.</param>
        /// <param name="smMerged">Store mapping resulting from merge operation.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults MergeRangeMappingsLocal(ShardLocation location, IStoreMapping smLeft, IStoreMapping smRight, IStoreMapping smMerged)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
                {
                    cmd.CommandText = @"__ShardManagement.smm_mergeRangeShardMappingsLocal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", smLeft.ShardMapId.ToString()),
                        new XElement("m_id_left", smLeft.Id.ToString()),
                        new XElement("m_version_left", smLeft.Version.ToString()),
                        new XElement("m_id_right", smRight.Id.ToString()),
                        new XElement("m_version_right", smRight.Version.ToString()),
                        new XElement("m_id_new", smMerged.Id.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_RangeShardMap_MergeReplaceLocal_SqlException,
                    se,
                    "MergeRangeMappings",
                    location);
            }
        }

        /// <summary>
        /// Finds range mapping which contain the given range for the shard map. 
        /// If range is not given:
        ///     If shard is given, finds all mappings for the shard.
        ///     If shard is also not given, finds all the mappings.
        /// If range is given:
        ///     If shard is given, finds all mappings for the shard in the range.
        ///     If shard is not given, finds all the mappings in the range.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="range">Optional range to find mappings in.</param>
        /// <param name="shard">Shard to find mappings in.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults FindRangeMappingByRangeLocal(IStoreShardMap ssm, ShardRange range, IStoreShard shard)
        {
            try
            {
                return SqlStore.FindMappingByRangeLocalHelper(ssm, range, shard, @"__ShardManagement.smm_getAllRangeShardMappingsLocal");
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMap_AddRemoveUpdateFindMappingLocal_SqlException,
                    se,
                    "FindMappingsForRange",
                    "Range",
                    shard.Location);
            }
        }

        #endregion RangeMappingInterfaces

        /// <summary>
        /// Update mapping in Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to update mapping in.</param>
        /// <param name="smOld">Old snapshot of shard mapping in cache before update.</param>
        /// <param name="smNew">Updated shard mapping.</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults UpdateMappingGlobal(IStoreShardMap ssm, IStoreMapping smOld, IStoreMapping smNew, Guid lockOwnerId = default(Guid))
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_updateShardMappingGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", smNew.ShardMapId.ToString()),
                        new XElement("lo_id", lockOwnerId.ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("s_id", smNew.StoreShard.Id.ToString()),
                        new XElement("m_id", smNew.Id.ToString()),
                        new XElement("m_old_version", smOld.Version.ToString()),
                        new XElement("m_min_value", StringUtils.ByteArrayToString(smNew.MinValue)),
                        new XElement("m_max_value",
                            (smNew.MaxValue == null) ? null : StringUtils.ByteArrayToString(smNew.MaxValue),
                            new XAttribute("is_null", (smNew.MaxValue == null) ? "true" : "false")
                            ),
                        new XElement("m_version", null,
                            new XAttribute("is_null", "true")
                            ),
                        new XElement("m_status", smNew.Status.ToString()),
                        new XElement("m_custom", (smNew.Custom == null) ? null : StringUtils.ByteArrayToString(smNew.Custom),
                            new XAttribute("is_null", (smNew.Custom == null) ? "true" : "false")
                            )
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_UpdateMappingGlobal_SqlException,
                    se,
                    "UpdateMapping",
                    smOld.StoreShard.Location,
                    ssm.Name);
            }
        }

        /// <summary>
        /// Update mapping in local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="smOld">Old snapshot of shard mapping in cache before update.</param>
        /// <param name="smNew">Updated shard mapping.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults UpdateMappingLocal(ShardLocation location, IStoreMapping smOld, IStoreMapping smNew)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
                {
                    cmd.CommandText = @"__ShardManagement.smm_updateShardMappingLocal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                        new XElement("sm_id", smNew.ShardMapId.ToString()),
                        new XElement("s_id", smNew.StoreShard.Id.ToString()),
                        new XElement("m_id", smNew.Id.ToString()),
                        new XElement("m_old_version", smOld.Version.ToString()),
                        new XElement("m_min_value", StringUtils.ByteArrayToString(smNew.MinValue)),
                        new XElement("m_max_value",
                            (smNew.MaxValue == null) ? null : StringUtils.ByteArrayToString(smNew.MaxValue),
                            new XAttribute("is_null", (smNew.MaxValue == null) ? "true" : "false")
                            ),
                        new XElement("m_version", smNew.Version.ToString()),
                        new XElement("m_status", smNew.Status.ToString()),
                        new XElement("m_custom", (smNew.Custom == null) ? null : StringUtils.ByteArrayToString(smNew.Custom),
                            new XAttribute("is_null", (smNew.Custom == null) ? "true" : "false")
                            )
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_UpdateMappingLocal_SqlException,
                    se,
                    "UpdateMapping",
                    location);
            }
        }

        /// <summary>
        /// Validates that the given mapping exists in local shard map.
        /// </summary>
        /// <param name="sm">Mapping being validated.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults ValidateMappingGlobal(IStoreMapping sm)
        {
            try
            {
                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    return SqlStore.ValidateMappingHelper(cmd, sm, @"__ShardManagement.smm_validateShardMappingGlobal");
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ValidateMappingGlobal_SqlException,
                    se,
                    sm.StoreShard.Location);
            }
        }

        /// <summary>
        /// Validates that the given mapping exists in local shard map.
        /// </summary>
        /// <param name="conn">Connection to use for validation.</param>
        /// <param name="sm">Mapping being validated.</param>
        /// <returns>Storage operation result.</returns>
        public virtual IStoreResults ValidateMappingLocal(SqlConnection conn, IStoreMapping sm)
        {
                // CONSIDER(wbasheer): Check for dead connections.
                Debug.Assert(conn.State == ConnectionState.Open);

                using (SqlCommand cmd = conn.CreateCommand())
                {
                    return SqlStore.ValidateMappingHelper(cmd, sm, @"__ShardManagement.smm_validateShardMappingLocal");
                }
            }

        /// <summary>
        /// Kills all local sessions at the given location whose patterns match the given
        /// <paramref name="mappingPattern"/>.
        /// </summary>
        /// <param name="location">Location at which to kill sessions.</param>
        /// <param name="shardMapName">Name of shard map.</param>
        /// <param name="mappingPattern">Pattern of the mapping.</param>
        /// <returns>Result of kill operations.</returns>
        public virtual IStoreResults KillSessionsForMappingLocal(ShardLocation location, string shardMapName, string mappingPattern)
        {
            try
            {
                SqlConnectionStringBuilder lcsb = new SqlConnectionStringBuilder(this.credentials.ConnectionStringShard.ConnectionString);

                lcsb.DataSource = location.DataSource;
                lcsb.InitialCatalog = location.Database;

                using (SqlConnection conn = new SqlConnection(lcsb.ConnectionString))
                {
                    conn.Open();

                    SqlResults result = new SqlResults();

                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"__ShardManagement.smm_KillSessionsForMappingLocal";
                        cmd.CommandType = CommandType.StoredProcedure;

                        XElement input = new XElement(cmd.CommandText,
                            new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                            new XElement("pattern", mappingPattern));

                        SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                        SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            result.Fetch(reader);
                        }

                        // Output parameter will be used to specify the outcome.
                        result.Result = (StoreResult)resultParam.Value;
                    }

                    return result;
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_KillSessionsForMapping_SqlException,
                    se,
                    location,
                    shardMapName);
            }
        }

        #endregion MappingInterfaces

        #region Upgrade and Version Interfaces

        /// <summary>
        /// Obtains distinct shard locations from Global ShardMapManager.
        /// </summary>
        /// <returns>Result of the execution.</returns>
        public virtual IStoreResults GetDistinctShardLocationsGlobal()
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_getDistinctLocationsGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;
                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_GetDistinctLocationsGlobal_SqlException,
                    se);
            }
        }

        #endregion Upgrade and Version Interfaces

        #region Internal Testing Events

#if DEBUG

        /// <summary>
        /// Publisher for SqlStoreEventGlobal event.
        /// </summary>
        /// <param name="ssArgs">Event argument to handle transaction finishing action</param>
        internal void OnSqlStoreEventGlobal(SqlStoreEventArgs ssArgs)
        {
            EventHandler<SqlStoreEventArgs> handler = this.SqlStoreEventGlobal;

            if (handler != null)
            {
                handler(this, ssArgs);
            }
        }

        /// <summary>
        /// Publisher for SqlStoreEventLocal event.
        /// </summary>
        /// <param name="ssArgs">Event argument to handle transaction finishing action</param>
        internal void OnSqlStoreEventLocal(SqlStoreEventArgs ssArgs)
        {
            EventHandler<SqlStoreEventArgs> handler = this.SqlStoreEventLocal;

            if (handler != null)
            {
                handler(this, ssArgs);
            }
        }

        /// <summary>
        /// Subscriber function for SqlTransactionScopeGlobal.TxnScopeGlobalDisposeEvent event.
        /// This function is used for internal testing purpose only.
        /// </summary>
        /// <param name="sender">sender object (SqlTransactionScopeGlobal)</param>
        /// <param name="arg">event argument</param>
        internal void TxnScopeGlobalEventHandler(object sender, SqlStoreEventArgs arg)
        {
            this.OnSqlStoreEventGlobal(arg);
        }

        /// <summary>
        /// Subscriber function for SqlTransactionScopeLocal.TxnScopeLocalDisposeEvent event.
        /// This function is used for internal testing purpose only.
        /// </summary>
        /// <param name="sender">sender object (SqlTransactionScopeLocal)</param>
        /// <param name="arg">event argument</param>
        internal void TxnScopeLocalEventHandler(object sender, SqlStoreEventArgs arg)
        {
            this.OnSqlStoreEventLocal(arg);
        }

#endif // DEBUG

        #endregion Internal Testing Events

        #region Helper Functions

        /// <summary>
        /// Adds or Attaches a shard map entry to Global ShardMapManager data source.
        /// </summary>
        /// <param name="ssm">ShardMap to be added.</param>
        /// <param name="isAttach">Whether we are adding or attaching.</param>
        /// <returns>Storage operation result.</returns>
        private static IStoreResults AddAttachShardMapGlobalHelper(IStoreShardMap ssm, bool isAttach)
        {
            try
            {
                SqlResults result = new SqlResults();

                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = @"__ShardManagement.smm_addShardMapGlobal";
                    cmd.CommandType = CommandType.StoredProcedure;

                    XElement input = new XElement(cmd.CommandText,
                        new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                        new XElement("sm_id", ssm.Id.ToString()),
                        new XElement("sm_name", ssm.Name),
                        new XElement("sm_kind", ((int)ssm.Kind).ToString()),
                        new XElement("sm_version", ssm.Version.ToString()),
                        new XElement("sm_status", ((int)ssm.Status).ToString()),
                        new XElement("sm_keykind", ((int)ssm.KeyKind).ToString()),
                        new XElement("sm_hashkind", ((int)ssm.HashKind).ToString()),
                        new XElement("for_attach", isAttach.ToString())
                        );

                    SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                    SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        result.Fetch(reader);
                    }

                    // Output parameter will be used to specify the outcome.
                    result.Result = (StoreResult)resultParam.Value;
                }

                return result;
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlStore_ShardMapManager_GlobalOperation_SqlException,
                    se,
                    isAttach ? "Attach" : "Add");
            }
        }

#if FUTUREWORK
        /// <summary>
        /// Add mapping to global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sms">Shard mappings to store. (presume these are all in the same ShardLocation.</param>
        /// <returns>Storage operation result.</returns>
        private static IStoreResults BulkAddMappingGlobalHelper(IStoreShardMap ssm, IEnumerable<IStoreMapping> sms)
        {
            var result = new SqlResults();
            if (!sms.Any())
            {
                result.Result = StoreResult.Failure;
                return result;
            }

            DataTable schema;

            using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
            {
                cmd.CommandText = "select * from __ShardManagement.shard_mappings_global where 0 = 1";
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    schema = reader.GetSchemaTable();
                }
            }

            DataTable dt = new DataTable();

            foreach (DataRow drow in schema.Rows)
            {
                string columnName = System.Convert.ToString(drow["ColumnName"]);
                DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                column.Unique = (bool)drow["IsUnique"];
                column.AllowDBNull = (bool)drow["AllowDBNull"];
                column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                dt.Columns.Add(column);
            }

            foreach (var sm in sms)
            {
                DataRow row = dt.NewRow();
                row[0] = sm.Id;
                row[1] = sm.ShardMapId;
                row[2] = sm.MinValue;
                row[3] = sm.MaxValue;
                row[4] = sm.Version;
                row[5] = sm.Status;
                row[6] = sm.Custom;
                row[7] = sm.StoreShard.Id;
                dt.Rows.Add(row);
            }

            using (SqlBulkCopy bcp = SqlTransactionScopeGlobal.CreateBulkCopy())
            {
                bcp.DestinationTableName = "_shard_mappings_global";
                bcp.WriteToServer(dt);
            }

            result.Result = StoreResult.Success;
            return result;
        }

        /// <summary>
        /// Add mapping to local ShardMap.
        /// </summary>
        /// <param name="location">Shard to add mappings to.</param>
        /// <param name="sms">Shard mappings to store. (presume these are all in the same ShardLocation.</param>
        /// <returns>Storage operation result.</returns>
        private static IStoreResults BulkAddMappingLocalHelper(ShardLocation location, IEnumerable<IStoreMapping> sms)
        {
            if (!sms.Any())
            {
                return StoreResult.Failure;
            }

            DataTable schema;

            using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
            {
                cmd.CommandText = "select * from __ShardManagement.shard_mappings_local where 0 = 1";
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    schema = reader.GetSchemaTable();
                }
            }

            DataTable dt = new DataTable();

            foreach (DataRow drow in schema.Rows)
            {
                string columnName = System.Convert.ToString(drow["ColumnName"]);
                DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                column.Unique = (bool)drow["IsUnique"];
                column.AllowDBNull = (bool)drow["AllowDBNull"];
                column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                dt.Columns.Add(column);
            }

            foreach (var sm in sms)
            {
                Debug.Assert(sm.StoreShard.Location == location);
                DataRow row = dt.NewRow();
                row[0] = sm.Id;
                row[1] = sm.ShardMapId;
                row[2] = sm.MinValue;
                row[3] = sm.MaxValue;
                row[4] = sm.Version;
                row[5] = sm.Status;
                row[6] = sm.Custom;
                row[7] = sm.StoreShard.Id;
                dt.Rows.Add(row);
            }

            using (SqlBulkCopy bcp = SqlTransactionScopeLocal.CreateBulkCopy(location))
            {
                bcp.DestinationTableName = "_shard_mappings_local";
                bcp.WriteToServer(dt);
            }

            return StoreResult.Success;
        }
#endif

        /// <summary>
        /// Add mapping to Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to add mapping to.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <param name="storedProcName">Name of stored proc to execute</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security",
            "CA2100:Review SQL queries for security vulnerabilities", Justification = 
            "StoredProcName does not come from user input")]
        private static IStoreResults AddMappingGlobalHelper(IStoreShardMap ssm, IStoreMapping sm, string storedProcName)
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("sm_version", ssm.Version.ToString()),
                    new XElement("s_id", sm.StoreShard.Id.ToString()),
                    new XElement("m_id", sm.Id.ToString()),
                    new XElement("m_min_value", StringUtils.ByteArrayToString(sm.MinValue)),
                    new XElement("m_max_value", 
                        (sm.MaxValue == null) ? null : StringUtils.ByteArrayToString(sm.MaxValue),
                        new XAttribute ("is_null", (sm.MaxValue == null) ? "true" : "false") 
                        ),
                    new XElement("m_version", sm.Version.ToString()),
                    new XElement("m_status", sm.Status.ToString()),
                    new XElement("m_custom", (sm.Custom == null) ? null : StringUtils.ByteArrayToString(sm.Custom),
                        new XAttribute("is_null", (sm.Custom == null) ? "true" : "false") 
                        )
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// Removes mapping from Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to remove mapping from.</param>
        /// <param name="sm">Shard mapping to remove.</param>
        /// <param name="force">Whether to ignore the Online status</param>
        /// <param name="storedProcName">Name of stored proc to execute</param>
        /// <param name="isRangeMapping">Whether this is a range mapping</param>
        /// <param name="lockOwnerId">Lock owner id of this mapping</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
            "StoredProcName does not come from user input")]
        private static IStoreResults RemoveMappingGlobalHelper(IStoreShardMap ssm, IStoreMapping sm, bool force, string storedProcName, 
            bool isRangeMapping = false, Guid lockOwnerId = default(Guid))
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("sm_version", ssm.Version.ToString()),
                    new XElement("m_id", sm.Id.ToString()),
                    new XElement("m_version", sm.Version.ToString()),
                    new XElement("force_remove", force ? "1" : "0")
                    );

                if (isRangeMapping)
                {
                    input.Add(new XElement("lo_id", lockOwnerId.ToString()));
                }
                
                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// find mappings in Global ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="range">Optional range to find mappings in.</param>
        /// <param name="shard">Option shard to find mappings in.</param>
        /// <param name="storedProcName">Name of stored proc to execute.</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification = 
            "StoredProcName does not come from user input")]
        private static IStoreResults FindMappingByRangeGlobalHelper(IStoreShardMap ssm, ShardRange range, IStoreShard shard, string storedProcName)
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("sm_version", ssm.Version.ToString()),
                    new XElement("s_id", shard == null ? null : shard.Id.ToString(),
                        new XAttribute("is_null", (shard == null) ? "true" : "false")
                        ),
                    new XElement("m_min_value", range == null ? null : StringUtils.ByteArrayToString(range.Low.RawValue),
                        new XAttribute("is_null", (range == null) ? "true" : "false")
                        ),
                    new XElement("m_max_value", (range == null) ? null : StringUtils.ByteArrayToString(range.High.RawValue),
                        new XAttribute("is_null", (range == null) ? "true" : "false")
                        )
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// find mappings in Local ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="range">Optional range to find mappings in.</param>
        /// <param name="shard">Shard to find mappings in.</param>
        /// <param name="storedProcName">Name of stored proc to execute.</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification = 
            "StoredProcName does not come from user input")]
        private static IStoreResults FindMappingByRangeLocalHelper(IStoreShardMap ssm, ShardRange range, IStoreShard shard, string storedProcName)
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(shard.Location))
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("s_id", shard == null ? null : shard.Id.ToString(),
                        new XAttribute("is_null", (shard == null) ? "true" : "false")
                        ),
                    new XElement("m_min_value", range == null ? null : StringUtils.ByteArrayToString(range.Low.RawValue),
                        new XAttribute("is_null", (range == null) ? "true" : "false")
                        ),
                    new XElement("m_max_value", (range == null) ? null : StringUtils.ByteArrayToString(range.High.RawValue),
                        new XAttribute("is_null", (range == null) ? "true" : "false")
                        )
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// find mappings in Local ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="key">Key corresponding to a point mapping.</param>
        /// <param name="shard">Shard to find mappings in.</param>
        /// <returns>Storage operation result.</returns>
        private static IStoreResults FindMappingByKeyLocalHelper(IStoreShardMap ssm, ShardKey key, IStoreShard shard)
        {
            string storedProcName = "__ShardManagement.smm_getPointShardMappingLocal";
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(shard.Location))
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("s_id", shard.Id.ToString()),
                    new XElement("m_value", StringUtils.ByteArrayToString(key.RawValue))
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }


        /// <summary>
        /// Add mapping to local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="sm">Shard mapping to store.</param>
        /// <param name="storedProcName">Name of stored proc to execute</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
            "StoredProcName does not come from user input")]
        private static IStoreResults AddMappingLocalHelper(ShardLocation location, IStoreMapping sm, string storedProcName)
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("lsm_version", SqlStoreLsmVersion.ToString()),
                    new XElement("sm_id", sm.ShardMapId.ToString()),
                    new XElement("s_id", sm.StoreShard.Id.ToString()),
                    new XElement("m_id", sm.Id.ToString()),
                    new XElement("m_min_value", StringUtils.ByteArrayToString(sm.MinValue)),
                    new XElement("m_max_value",
                        (sm.MaxValue == null) ? null : StringUtils.ByteArrayToString(sm.MaxValue),
                        new XAttribute("is_null", (sm.MaxValue == null) ? "true" : "false")
                        ),
                    new XElement("m_version", sm.Version.ToString()),
                    new XElement("m_status", sm.Status.ToString()),
                    new XElement("m_custom", (sm.Custom == null) ? null : StringUtils.ByteArrayToString(sm.Custom),
                        new XAttribute("is_null", (sm.Custom == null) ? "true" : "false")
                        )
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString());
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// Removes mapping from local ShardMap.
        /// </summary>
        /// <param name="location">Location of shard.</param>
        /// <param name="sm">Shard mapping to remove.</param>
        /// <param name="storedProcName">Name of stored proc to execute</param>
        /// <param name="force">Force the removal of mapping irrespective of the status.</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification = 
            "StoredProcName does not come from user input")]
        private static IStoreResults RemoveMappingLocalHelper(ShardLocation location, IStoreMapping sm, bool force, string storedProcName)
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                    new XElement("sm_id", sm.ShardMapId.ToString()),
                    new XElement("m_id", sm.Id.ToString()),
                    new XElement("m_version", sm.Version.ToString()),
                    new XElement("force_remove", force ? "1" : "0")
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// Finds mapping which contain the given key for the given ShardMap.
        /// </summary>
        /// <param name="ssm">Shard map to find mappings in.</param>
        /// <param name="shardKey">ShardKey being searched.</param>
        /// <param name="storedProcName">Name of stored proc to execute</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification = 
            "StoredProcName does not come from user input")]
        private static IStoreResults FindMappingByKeyGlobalHelper(IStoreShardMap ssm, byte[] shardKey, string storedProcName)
        {
            SqlResults result = new SqlResults();

            using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
            {
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;

                XElement input = new XElement(storedProcName,
                    new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                    new XElement("sm_id", ssm.Id.ToString()),
                    new XElement("sm_version", ssm.Version.ToString()),
                    new XElement("s_key", StringUtils.ByteArrayToString(shardKey))
                    );

                SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
                SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    result.Fetch(reader);
                }

                // Output parameter will be used to specify the outcome.
                result.Result = (StoreResult)resultParam.Value;
            }

            return result;
        }

        /// <summary>
        /// Validates that the given mapping exists in the shard map.
        /// </summary>
        /// <param name="cmd">Command to use for validation.</param>
        /// <param name="sm">Mapping being validated.</param>
        /// <param name="storedProcName">Name of stored proc to execute.</param>
        /// <returns>Storage operation result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
            "StoredProcName does not come from user input")]
        private static IStoreResults ValidateMappingHelper(SqlCommand cmd, IStoreMapping sm, string storedProcName)
        {
            SqlResults result = new SqlResults();

            cmd.CommandText = storedProcName;
            cmd.CommandType = CommandType.StoredProcedure;

            XElement input = new XElement(storedProcName,
                new XElement("gsm_version", SqlStoreGsmVersion.ToString()),
                new XElement("sm_id", sm.ShardMapId.ToString()),
                new XElement("m_id", sm.Id.ToString()),
                new XElement("m_version", sm.Version.ToString())
                );

            SqlStore.AddCommandParameter(cmd, "@input", SqlDbType.Xml, ParameterDirection.Input, 0, input.ToString()); 
            SqlParameter resultParam = SqlStore.AddCommandParameter(cmd, "@result", SqlDbType.Int, ParameterDirection.Output, 0, 0);

            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                result.Fetch(reader);
            }

            // Output parameter will be used to specify the outcome.
            result.Result = (StoreResult)resultParam.Value;

            return result;
        }

        /// <summary>
        /// Gets the result of execution of existence check for shard map manager.
        /// </summary>
        /// <param name="cmd">Command object used for executing the request.</param>
        /// <param name="script">Script to execute.</param>
        /// <returns>true if check succeeds, false otherwise.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
            "StoredProcName does not come from user input")]
        private static bool CheckIfExistsHelper(SqlCommand cmd, IEnumerable<StringBuilder> script)
        {
            bool exists = false;

            cmd.CommandText = script.Single().ToString();
            cmd.CommandType = CommandType.Text;

            using (SqlDataReader rdr = cmd.ExecuteReader())
            {
                exists = rdr.HasRows;
            }

            return exists;
        }

        /// <summary>
        /// Executes the given script on global shard map manager data source.
        /// </summary>
        /// <param name="script">Script to execute</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
            "StoredProcName does not come from user input")]
        private static void ExecuteSqlScriptGlobalHelper(IEnumerable<StringBuilder> script)
        {
            foreach (StringBuilder batch in script)
            {
                using (SqlCommand cmd = SqlTransactionScopeGlobal.CreateSqlCommand())
                {
                    cmd.CommandText = batch.ToString();
                    cmd.CommandType = CommandType.Text;

                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Executes the given script on local shard map manager data source.
        /// </summary>
        /// <param name="location">Location of local data source.</param>
        /// <param name="script">Script to execute</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", 
            "CA2100:Review SQL queries for security vulnerabilities", Justification =
            "StoredProcName does not come from user input")]
        private static void ExecuteSqlScriptLocalHelper(ShardLocation location, IEnumerable<StringBuilder> script)
        {
            foreach (StringBuilder batch in script)
            {
                using (SqlCommand cmd = SqlTransactionScopeLocal.CreateSqlCommand(location))
                {
                    cmd.CommandText = batch.ToString();
                    cmd.CommandType = CommandType.Text;

                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Splits the input script into batches of individual commands, the go token is
        /// considered the separation boundary. Also skips comment lines.
        /// </summary>
        /// <param name="script">Input script.</param>
        /// <returns>Collection of string builder that represent batches of commands.</returns>
        private static IEnumerable<StringBuilder> SplitScriptCommands(string script)
        {
            List<StringBuilder> batches = new List<StringBuilder>();

            using (StringReader sr = new StringReader(script))
            {
                StringBuilder current = new StringBuilder();
                string currentLine;

                while ((currentLine = sr.ReadLine()) != null)
                {
                    // Break at the go token boundary.
                    if (SqlStore.GoTokenRegularExpression.IsMatch(currentLine))
                    {
                        batches.Add(current);
                        current = new StringBuilder();
                    }
                    else if (!SqlStore.CommentLineRegularExpression.IsMatch(currentLine))
                    {
                        // Add the line to the batch if it is not a comment.
                        current.AppendLine(currentLine);
                    }
                }
            }

            return batches;
        }

        /// <summary>
        /// Adds parameter to given command.
        /// </summary>
        /// <param name="cmd">Command to add parameter to.</param>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter type.</param>
        /// <param name="direction">Parameter direction.</param>
        /// <param name="size">Size of parameter, useful for variable length types only.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns>Parameter object this created.</returns>
        private static SqlParameter AddCommandParameter(
            SqlCommand cmd,
            string parameterName,
            SqlDbType dbType,
            ParameterDirection direction,
            int size,
            object value)
        {
            SqlParameter p = new SqlParameter(parameterName, dbType)
            {
                Direction = direction,
                Value = value ?? DBNull.Value
            };

            if ((dbType == SqlDbType.NVarChar) || (dbType == SqlDbType.VarBinary))
            {
                p.Size = size;
            }

            cmd.Parameters.Add(p);

            return p;
        }

        #endregion Helper Functions
    }

    /// <summary>
    /// Represents a global transaction scope. Opens a new connection and starts
    /// a transaction on that connection.
    /// </summary>
    internal class SqlTransactionScopeGlobal : IStoreTransactionScope
    {
        /// <summary>
        /// Whether the object has already been disposed.
        /// </summary>
        private bool disposed;

        /// <summary>
        /// Connection used for accessing database.
        /// </summary>
        private SqlConnection conn;

        /// <summary>
        /// Transaction under which operations happen on the connection.
        /// </summary>
        private SqlTransaction tran;

        /// <summary>
        /// Current transaction scope.
        /// </summary>
        [ThreadStatic]
        private static SqlTransactionScopeGlobal current;

        /// <summary>
        /// Constructs a new instance of the transaction scope. Uses the 
        /// credentials scope for obtaining the credentials.
        /// </summary>
        /// <param name="shardMapManagerConnectionString">Credentials for accessing shard map manager database.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope", Justification =
            "Connection is disposed when this is disposed")]
        protected internal SqlTransactionScopeGlobal(SqlConnectionStringBuilder shardMapManagerConnectionString)
        {
            Debug.Assert(SqlTransactionScopeGlobal.current == null);

            SqlConnection conn = new SqlConnection(shardMapManagerConnectionString.ConnectionString);

            conn.Open();

            SqlTransaction tran = conn.BeginTransaction(IsolationLevel.Serializable);

            this.conn = conn;
            this.tran = tran;
            SqlTransactionScopeGlobal.current = this;
        }

        /// <summary>
        /// Indicator for whether to commit or rollback transaction.
        /// </summary>
        public bool Success
        {
            get;
            set;
        }

#if DEBUG
        /// <summary>
        /// Event raised by Dispose() method.
        /// This event is used for internal testing purpose only.
        /// </summary>
        internal event EventHandler<SqlStoreEventArgs> TxnScopeGlobalDisposeEvent;

#endif // DEBUG

        /// <summary>
        /// Disposes off the scope.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

#if FUTUREWORK
            /// <summary>
            /// Create a command object for executing requests. The command
            /// is associated with the underlying connection and transaction
            /// objects.
            /// </summary>
            /// <returns>Command object which is used for executing requests.</returns>
            internal static SqlBulkCopy CreateBulkCopy(SqlBulkCopyOptions options = SqlBulkCopyOptions.Default)
            {
                SqlTransactionScopeGlobal tsGlobal = SqlTransactionScopeGlobal.GetAmbientTransactionScopeGlobal();
                SqlBulkCopy bcp = new SqlBulkCopy(tsGlobal.conn, options, tsGlobal.tran);
                return bcp;
            }
#endif

        /// <summary>
        /// Create a command object for executing requests. The command
        /// is associated with the underlying connection and transaction
        /// objects.
        /// </summary>
        /// <returns>Command object which is used for executing requests.</returns>
        internal static SqlCommand CreateSqlCommand()
        {
            SqlTransactionScopeGlobal tsGlobal = SqlTransactionScopeGlobal.GetAmbientTransactionScopeGlobal();

            SqlCommand cmd = tsGlobal.conn.CreateCommand();
            cmd.Transaction = tsGlobal.tran;
            return cmd;
        }

        /// <summary>
        /// Protected implementation of Dispose pattern.
        /// </summary>
        /// <param name="disposing">Whether the object is being Disposed.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (this.disposed)
            {
                return;
            }

            if (disposing)
            {
                this.FinishTransaction();
            }

            this.disposed = true;
        }

        /// <summary>
        /// Completes the currently active transaction.
        /// </summary>
        internal void FinishTransaction()
        {
            Debug.Assert(this.conn != null);
            Debug.Assert(this.tran != null);
            try
            {
#if DEBUG
                // Raise event and check if current transaction should be aborted.
                //
                SqlStoreEventArgs eventArgs = new SqlStoreEventArgs();

                OnDisposeEvent(eventArgs);

                if (eventArgs.action == SqlStoreEventArgs.SqlStoreTxnFinishAction.TxnAbort)
                {
                    throw new StoreException(
                        Errors.SqlTransactionScopeGlobal_SqlException,
                        this.Success ? "Commit" : "Rollback");
                }
                else
#endif // DEBUG
                {
                    if (this.Success)
                    {
                        this.tran.Commit();
                    }
                    else
                    {
                        this.tran.Rollback();
                    }
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlTransactionScopeGlobal_SqlException,
                    se,
                    this.Success ? "Commit" : "Rollback");
            }
            finally
            {
                this.tran.Dispose();
                this.conn.Dispose();
                SqlTransactionScopeGlobal.current = null;
            }
        }

#if DEBUG
        /// <summary>
        /// Publisher function for TxnScopeGlobalDisposeEvent event.
        /// This function is used for internal testing purpose only.
        /// </summary>
        /// <param name="args">Argument to capture transaction finish action</param>
        protected void OnDisposeEvent(SqlStoreEventArgs args)
        {
            EventHandler<SqlStoreEventArgs> handler = TxnScopeGlobalDisposeEvent;
            if (handler != null)
            {
                handler(this, args);
            }
        }

#endif // DEBUG

        /// <summary>
        /// Ensures that there is an ambient global transaction scope in the thread.
        /// </summary>
        private static SqlTransactionScopeGlobal GetAmbientTransactionScopeGlobal()
        {
            SqlTransactionScopeGlobal tsGlobal = SqlTransactionScopeGlobal.current as SqlTransactionScopeGlobal;

            if (tsGlobal == null)
            {
                throw new StoreException(Errors.SqlStore_TransactionScope_DoesNotExist);
            }

            return tsGlobal;
        }
    }

    /// <summary>
    /// Represents a local transaction scope. Opens a new connection and starts
    /// a transaction on that connection.
    /// </summary>
    internal class SqlTransactionScopeLocal : IStoreTransactionScope
    {
        /// <summary>
        /// Whether the object has already been disposed.
        /// </summary>
        private bool disposed;

        /// <summary>
        /// Connection used for accessing database.
        /// </summary>
        private SqlConnection conn;

        /// <summary>
        /// Transaction under which operations happen on the connection.
        /// </summary>
        private SqlTransaction tran;

        /// <summary>
        /// Current transaction scope.
        /// </summary>
        [ThreadStatic]
        private static SqlTransactionScopeLocal current;

        /// <summary>
        /// Constructs a new instance of the transaction scope. Uses the 
        /// credentials scope for obtaining the credentials.
        /// </summary>
        /// <param name="location">Location of local shard.</param>
        /// <param name="shardConnectionString">Credentials for accessing a shard.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope", Justification =
            "Connection is disposed when this is disposed")]
        protected internal SqlTransactionScopeLocal(ShardLocation location, SqlConnectionStringBuilder shardConnectionString)
        {
            Debug.Assert(SqlTransactionScopeLocal.current == null);
            Debug.Assert(location != null);
            Debug.Assert(location.DataSource != null);
            Debug.Assert(location.Database != null);

            // Copy the builder.
            SqlConnectionStringBuilder lcsb = new SqlConnectionStringBuilder(shardConnectionString.ConnectionString);

            lcsb.DataSource = location.DataSource;
            lcsb.InitialCatalog = location.Database;

            SqlConnection conn = new SqlConnection(lcsb.ConnectionString);

            conn.Open();

            SqlTransaction tran = conn.BeginTransaction(IsolationLevel.Serializable);

            this.Location = location;
            this.conn = conn;
            this.tran = tran;
            SqlTransactionScopeLocal.current = this;
        }

        /// <summary>
        /// Indicator for whether to commit or rollback transaction.
        /// </summary>
        public bool Success
        {
            get;
            set;
        }

        /// <summary>
        /// Location of data source against which operations are to be performed.
        /// </summary>
        private ShardLocation Location
        {
            get;
            set;
        }

#if DEBUG
        /// <summary>
        /// Event raised by Dispose() method.
        /// This event is used for internal testing purpose only.
        /// </summary>
        internal event EventHandler<SqlStoreEventArgs> TxnScopeLocalDisposeEvent;

#endif // DEBUG

        /// <summary>
        /// Disposes off the scope, makes decision on commit or rollback based
        /// on the Success property.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Create a command object for executing requests. The command
        /// is associated with the underlying connection and transaction
        /// objects.
        /// </summary>
        /// <param name="location">Target location. Only useful for local operations.</param>
        /// <returns>Command object which is used for executing requests.</returns>
        internal static SqlCommand CreateSqlCommand(ShardLocation location)
        {
            SqlTransactionScopeLocal tsLocal = SqlTransactionScopeLocal.GetAmbientTransactionScopeLocal(location);

            SqlCommand cmd = tsLocal.conn.CreateCommand();
            cmd.Transaction = tsLocal.tran;
            return cmd;
        }

#if FUTUREWORK
            /// <summary>
            /// Create a command object for executing requests. The command
            /// is associated with the underlying connection and transaction
            /// objects.
            /// </summary>
            /// <param name="location">Target location. Only useful for local operations.</param>
            /// <returns>Command object which is used for executing requests.</returns>
            internal static SqlBulkCopy CreateBulkCopy(ShardLocation location, SqlBulkCopyOptions options = SqlBulkCopyOptions.Default)
            {
                SqlTransactionScopeLocal tsLocal = SqlTransactionScopeLocal.GetAmbientTransactionScopeLocal(location);

                SqlBulkCopy bcp = new SqlBulkCopy(tsLocal.conn, options, tsLocal.tran);
                return bcp;
            }
#endif

        /// <summary>
        /// Protected implementation of Dispose pattern.
        /// </summary>
        /// <param name="disposing">Whether the object is being Disposed.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (this.disposed)
            {
                return;
            }

            if (disposing)
            {
                this.FinishTransaction();
            }

            this.disposed = true;
        }

        /// <summary>
        /// Completes the currently active transaction.
        /// </summary>
        internal void FinishTransaction()
        {
            Debug.Assert(this.conn != null);
            Debug.Assert(this.tran != null);
            try
            {
#if DEBUG
                // Raise event and check if current transaction should be aborted.
                //
                SqlStoreEventArgs eventArgs = new SqlStoreEventArgs();

                OnDisposeEvent(eventArgs);

                if (eventArgs.action == SqlStoreEventArgs.SqlStoreTxnFinishAction.TxnAbort)
                {
                    throw new StoreException(
                        Errors.SqlTransactionScopeLocal_SqlException,
                        this.Success ? "Commit" : "Rollback",
                        this.Location);
                }
                else
#endif // DEBUG
                {
                    if (this.Success)
                    {
                        this.tran.Commit();
                    }
                    else
                    {
                        this.tran.Rollback();
                    }
                }
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors.SqlTransactionScopeLocal_SqlException,
                    se,
                    this.Success ? "Commit" : "Rollback",
                    this.Location);
            }
            finally
            {
                this.tran.Dispose();
                this.conn.Dispose();
                SqlTransactionScopeLocal.current = null;
            }
        }

#if DEBUG
        /// <summary>
        /// Publisher function for TxnScopeGlobalDisposeEvent event.
        /// This function is used for internal testing purpose only.
        /// </summary>
        /// <param name="args">Argument to capture transaction finish action</param>
        protected void OnDisposeEvent(SqlStoreEventArgs args)
        {
            EventHandler<SqlStoreEventArgs> handler = TxnScopeLocalDisposeEvent;
            if (handler != null)
            {
                handler(this, args);
            }
        }

#endif // DEBUG

        /// <summary>
        /// Ensures that there is an ambient local transaction scope in the thread.
        /// </summary>
        private static SqlTransactionScopeLocal GetAmbientTransactionScopeLocal(ShardLocation location)
        {
            SqlTransactionScopeLocal tsLocal = SqlTransactionScopeLocal.current as SqlTransactionScopeLocal;
            if (tsLocal == null)
            {
                throw new StoreException(Errors.SqlStore_TransactionScope_DoesNotExist);
            }

            if (!tsLocal.Location.Equals(location))
            {
                throw new StoreException(
                    Errors.SqlStore_TransactionScope_LocationMismatch,
                    tsLocal.Location,
                    location);
            }

            return tsLocal;
        }
    }

#if DEBUG

    /// <summary>
    /// Internal class to handle arguments for SqlStore Events
    /// </summary>
    internal sealed class SqlStoreEventArgs : EventArgs
    {
        /// <summary>
        /// Action to be performed on active GSM transaction
        /// </summary>
        internal enum SqlStoreTxnFinishAction
        {
            None,
            TxnAbort,
        };

        /// <summary>
        /// Default constructor.
        /// </summary>
        internal SqlStoreEventArgs()
        {
            action = SqlStoreTxnFinishAction.None;
        }

        /// <summary>
        /// Constructor accepting txnFinishAction.
        /// </summary>
        /// <param name="txnAction">Action to be taken for active GSM transaction</param>
        internal SqlStoreEventArgs(SqlStoreTxnFinishAction txnAction)
        {
            action = txnAction;
        }

        /// <summary>
        /// SqlStoreTxnFinishAction variable that will be updated by Subscriber of events.
        /// </summary>
        internal SqlStoreTxnFinishAction action;
    }
#endif // DEBUG
}
