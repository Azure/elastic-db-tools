using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Container for results of Store operations.
    /// </summary>
    internal sealed class SqlResults : IStoreResults
    {
        /// <summary>
        /// Kinds of results from storage operations.
        /// </summary>
        private enum SqlResultType
        {
            ShardMap,
            Shard,
            ShardMapping,
            ShardLocation,
            StoreVersion,
            Operation,
            SchemaInfo
        }

        /// <summary>
        /// Collection of shard maps in result.
        /// </summary>
        private List<IStoreShardMap> ssm;

        /// <summary>
        /// Collection of shards in result.
        /// </summary>
        private List<IStoreShard> ss;

        /// <summary>
        /// Collection of shard mappings in result.
        /// </summary>
        private List<IStoreMapping> sm;

        /// <summary>
        /// Collection of shard locations in result.
        /// </summary>
        private List<IStoreLocation> sl;

        /// <summary>
        /// Collection of store operations in result.
        /// </summary>
        private List<IStoreLogEntry> ops;

        /// <summary>
        /// Collection of Schema info in result.
        /// </summary>
        private List<IStoreSchemaInfo> si;
        
        /// <summary>
        /// Version of global or local shard map in result.
        /// </summary>
        private IStoreVersion version;

        /// <summary>
        /// Constructs instance of SqlResults.
        /// </summary>
        internal SqlResults()
        {
            this.Result = StoreResult.Success;

            this.ssm = new List<IStoreShardMap>();
            this.ss = new List<IStoreShard>();
            this.sm = new List<IStoreMapping>();
            this.sl = new List<IStoreLocation>();
            this.si = new List<IStoreSchemaInfo>();
            this.version = null;
            this.ops = new List<IStoreLogEntry>();
        }

        /// <summary>
        /// Populates instance of SqlResults using rows from SqlDataReader.
        /// </summary>
        /// <param name="reader">SqlDataReader whose rows are to be read.</param>
        internal void Fetch(SqlDataReader reader)
        {
            do
            {
                if (reader.FieldCount > 0)
                {
                    SqlResultType resultType = SqlResults.SqlResultTypeFromColumnName(reader.GetSchemaTable().Rows[1]["ColumnName"].ToString());

                    switch (resultType)
                    {
                        case SqlResultType.ShardMap:
                            while (reader.Read())
                            {
                                this.ssm.Add(new SqlShardMap(reader, 1));
                            }
                            break;
                        case SqlResultType.Shard:
                            while (reader.Read())
                            {
                                this.ss.Add(new SqlShard(reader, 1));
                            }
                            break;
                        case SqlResultType.ShardMapping:
                            while (reader.Read())
                            {
                                this.sm.Add(new SqlMapping(reader, 1));
                            }
                            break;
                        case SqlResultType.ShardLocation:
                            while (reader.Read())
                            {
                                this.sl.Add(new SqlLocation(reader, 1));
                            }
                            break;
                        case SqlResultType.SchemaInfo:
                            while (reader.Read())
                            {
                                this.si.Add(new SqlSchemaInfo(reader, 1));
                            }
                            break;
                        case SqlResultType.StoreVersion:
                            while (reader.Read())
                            {
                                this.version = new SqlVersion(reader, 1);
                            }
                            break;
                        case SqlResultType.Operation:
                            while (reader.Read())
                            {
                                this.ops.Add(new SqlLogEntry(reader, 1));
                            }
                            break;
                        default:
                            // This code is unreachable, since the all values of the SqlResultType enum are explicitly handled above.
                            Debug.Assert(false);
                            break;
                    }
                }
            }
            while (reader.NextResult());
        }

        /// <summary>
        /// Asynchronously populates instance of SqlResults using rows from SqlDataReader.
        /// </summary>
        /// <param name="reader">SqlDataReader whose rows are to be read.</param>
        /// <returns>A task to await read completion</returns>
        internal async Task FetchAsync(SqlDataReader reader)
        {
            Func<Action, Task> ReadAsync = async (readAction) =>
            {
                while (await reader.ReadAsync())
                {
                    readAction();
                }
            };

            do
            {
                if (reader.FieldCount > 0)
                {
                    SqlResultType resultType = SqlResults.SqlResultTypeFromColumnName(reader.GetSchemaTable().Rows[1]["ColumnName"].ToString());

                    switch (resultType)
                    {
                        case SqlResultType.ShardMap:
                            await ReadAsync(() => this.ssm.Add(new SqlShardMap(reader, 1)));
                            break;
                        case SqlResultType.Shard:
                            await ReadAsync(() => this.ss.Add(new SqlShard(reader, 1)));
                            break;
                        case SqlResultType.ShardMapping:
                            await ReadAsync(() => this.sm.Add(new SqlMapping(reader, 1)));
                            break;
                        case SqlResultType.ShardLocation:
                            await ReadAsync(() => this.sl.Add(new SqlLocation(reader, 1)));
                            break;
                        case SqlResultType.SchemaInfo:
                            await ReadAsync(() => this.si.Add(new SqlSchemaInfo(reader, 1)));
                            break;
                        case SqlResultType.StoreVersion:
                            await ReadAsync(() => this.version = new SqlVersion(reader, 1));
                            break;
                        case SqlResultType.Operation:
                            await ReadAsync(() => this.ops.Add(new SqlLogEntry(reader, 1)));
                            break;
                        default:
                            // This code is unreachable, since the all values of the SqlResultType enum are explicitly handled above.
                            Debug.Assert(false);
                            break;
                    }
                }
            }
            while (await reader.NextResultAsync());
        }

        /// <summary>
        /// Storage operation result.
        /// </summary>
        public StoreResult Result
        {
            get;
            internal set;
        }

        /// <summary>
        /// Collection of shard maps.
        /// </summary>
        public IEnumerable<IStoreShardMap> StoreShardMaps
        {
            get 
            {
                return this.ssm;
            }
        }

        /// <summary>
        /// Collection of shards.
        /// </summary>
        public IEnumerable<IStoreShard> StoreShards
        {
            get 
            { 
                return this.ss; 
            }
        }

        /// <summary>
        /// Collection of mappings.
        /// </summary>
        public IEnumerable<IStoreMapping> StoreMappings
        {
            get 
            { 
                return this.sm; 
            }
        }

        /// <summary>
        /// Collection of store operations.
        /// </summary>
        public IEnumerable<IStoreLogEntry> StoreOperations
        {
            get
            {
                return this.ops;
            }
        }

        /// <summary>
        /// Collection of locations.
        /// </summary>
        public IEnumerable<IStoreLocation> StoreLocations
        {
            get
            {
                return this.sl;
            }
        }

        /// <summary>
        /// Collection of SchemaInfo objects.
        /// </summary>
        public IEnumerable<IStoreSchemaInfo> StoreSchemaInfoCollection
        {
            get
            {
                return this.si;
            }
        }
            
        /// <summary>
        /// Store version.
        /// </summary>
        public IStoreVersion StoreVersion
        {
            get
            {
                return this.version;
            }
        }

        /// <summary>
        /// Obtains the result type from first column's name.
        /// </summary>
        /// <param name="columnName">First column's name.</param>
        /// <returns>Sql result type.</returns>
        private static SqlResultType SqlResultTypeFromColumnName(string columnName)
        {
            switch (columnName)
            {
                case "ShardMapId":
                    return SqlResultType.ShardMap;
                case "ShardId":
                    return SqlResultType.Shard;
                case "MappingId":
                    return SqlResultType.ShardMapping;
                case "Protocol":
                    return SqlResultType.ShardLocation;
                case "StoreVersion":
                case "StoreVersionMajor": 
                    return SqlResultType.StoreVersion;
                case "Name":
                    return SqlResultType.SchemaInfo;
                default:
                    Debug.Assert(columnName == "OperationId");
                    return SqlResultType.Operation;
            }
        }
    }
}
