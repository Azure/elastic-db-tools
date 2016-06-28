// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
        /// Mapping from column name to result type.
        /// </summary>
        private static Dictionary<string, SqlResultType> s_resultFromColumnName = new Dictionary<string, SqlResultType>(StringComparer.OrdinalIgnoreCase)
            {
                { "ShardMapId", SqlResultType.ShardMap },
                { "ShardId", SqlResultType.Shard },
                { "MappingId", SqlResultType.ShardMapping },
                { "Protocol", SqlResultType.ShardLocation },
                { "StoreVersion", SqlResultType.StoreVersion },
                { "StoreVersionMajor", SqlResultType.StoreVersion },
                { "Name", SqlResultType.SchemaInfo },
                { "OperationId", SqlResultType.Operation }
            };

        /// <summary>
        /// Collection of shard maps in result.
        /// </summary>
        private List<IStoreShardMap> _ssm;

        /// <summary>
        /// Collection of shards in result.
        /// </summary>
        private List<IStoreShard> _ss;

        /// <summary>
        /// Collection of shard mappings in result.
        /// </summary>
        private List<IStoreMapping> _sm;

        /// <summary>
        /// Collection of shard locations in result.
        /// </summary>
        private List<IStoreLocation> _sl;

        /// <summary>
        /// Collection of store operations in result.
        /// </summary>
        private List<IStoreLogEntry> _ops;

        /// <summary>
        /// Collection of Schema info in result.
        /// </summary>
        private List<IStoreSchemaInfo> _si;

        /// <summary>
        /// Version of global or local shard map in result.
        /// </summary>
        private IStoreVersion _version;

        /// <summary>
        /// Constructs instance of SqlResults.
        /// </summary>
        internal SqlResults()
        {
            this.Result = StoreResult.Success;

            _ssm = new List<IStoreShardMap>();
            _ss = new List<IStoreShard>();
            _sm = new List<IStoreMapping>();
            _sl = new List<IStoreLocation>();
            _si = new List<IStoreSchemaInfo>();
            _version = null;
            _ops = new List<IStoreLogEntry>();
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
                                _ssm.Add(new SqlShardMap(reader, 1));
                            }
                            break;
                        case SqlResultType.Shard:
                            while (reader.Read())
                            {
                                _ss.Add(new SqlShard(reader, 1));
                            }
                            break;
                        case SqlResultType.ShardMapping:
                            while (reader.Read())
                            {
                                _sm.Add(new SqlMapping(reader, 1));
                            }
                            break;
                        case SqlResultType.ShardLocation:
                            while (reader.Read())
                            {
                                _sl.Add(new SqlLocation(reader, 1));
                            }
                            break;
                        case SqlResultType.SchemaInfo:
                            while (reader.Read())
                            {
                                _si.Add(new SqlSchemaInfo(reader, 1));
                            }
                            break;
                        case SqlResultType.StoreVersion:
                            while (reader.Read())
                            {
                                _version = new SqlVersion(reader, 1);
                            }
                            break;
                        case SqlResultType.Operation:
                            while (reader.Read())
                            {
                                _ops.Add(new SqlLogEntry(reader, 1));
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
                while (await reader.ReadAsync().ConfigureAwait(false))
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
                            await ReadAsync(() => _ssm.Add(new SqlShardMap(reader, 1))).ConfigureAwait(false);
                            break;
                        case SqlResultType.Shard:
                            await ReadAsync(() => _ss.Add(new SqlShard(reader, 1))).ConfigureAwait(false);
                            break;
                        case SqlResultType.ShardMapping:
                            await ReadAsync(() => _sm.Add(new SqlMapping(reader, 1))).ConfigureAwait(false);
                            break;
                        case SqlResultType.ShardLocation:
                            await ReadAsync(() => _sl.Add(new SqlLocation(reader, 1))).ConfigureAwait(false);
                            break;
                        case SqlResultType.SchemaInfo:
                            await ReadAsync(() => _si.Add(new SqlSchemaInfo(reader, 1))).ConfigureAwait(false);
                            break;
                        case SqlResultType.StoreVersion:
                            await ReadAsync(() => _version = new SqlVersion(reader, 1)).ConfigureAwait(false);
                            break;
                        case SqlResultType.Operation:
                            await ReadAsync(() => _ops.Add(new SqlLogEntry(reader, 1))).ConfigureAwait(false);
                            break;
                        default:
                            // This code is unreachable, since the all values of the SqlResultType enum are explicitly handled above.
                            Debug.Assert(false);
                            break;
                    }
                }
            }
            while (await reader.NextResultAsync().ConfigureAwait(false));
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
                return _ssm;
            }
        }

        /// <summary>
        /// Collection of shards.
        /// </summary>
        public IEnumerable<IStoreShard> StoreShards
        {
            get
            {
                return _ss;
            }
        }

        /// <summary>
        /// Collection of mappings.
        /// </summary>
        public IEnumerable<IStoreMapping> StoreMappings
        {
            get
            {
                return _sm;
            }
        }

        /// <summary>
        /// Collection of store operations.
        /// </summary>
        public IEnumerable<IStoreLogEntry> StoreOperations
        {
            get
            {
                return _ops;
            }
        }

        /// <summary>
        /// Collection of locations.
        /// </summary>
        public IEnumerable<IStoreLocation> StoreLocations
        {
            get
            {
                return _sl;
            }
        }

        /// <summary>
        /// Collection of SchemaInfo objects.
        /// </summary>
        public IEnumerable<IStoreSchemaInfo> StoreSchemaInfoCollection
        {
            get
            {
                return _si;
            }
        }

        /// <summary>
        /// Store version.
        /// </summary>
        public IStoreVersion StoreVersion
        {
            get
            {
                return _version;
            }
        }

        /// <summary>
        /// Obtains the result type from first column's name.
        /// </summary>
        /// <param name="columnName">First column's name.</param>
        /// <returns>Sql result type.</returns>
        private static SqlResultType SqlResultTypeFromColumnName(string columnName)
        {
            return s_resultFromColumnName[columnName];
        }
    }
}
