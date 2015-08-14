// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema
{
    /// <summary>
    /// Represents information identifying the list of sharded tables and the list of reference
    /// tables associated with a sharding scheme. Reference tables are replicated across shards.
    /// This class is thread safe.
    /// </summary>
    [Serializable()]
    [DataContract(Name = "Schema", Namespace = "")]
    [KnownType(typeof(HashSet<ShardedTableInfo>))]
    [KnownType(typeof(HashSet<ReferenceTableInfo>))]
    public class SchemaInfo
    {
        /// <summary>
        /// This is the list of sharded tables in the sharding schema along with their 
        /// sharding key column names.
        /// </summary>
        [DataMember()]
        private ISet<ShardedTableInfo> _shardedTableSet;

        /// <summary>
        /// This is the list of reference tables in the sharding scheme.
        /// </summary>
        [DataMember()]
        private ISet<ReferenceTableInfo> _referenceTableSet;

        /// <summary>
        /// Synchronization object used when adding table entries to the current 
        /// <see cref="SchemaInfo"/> object.
        /// </summary>
        private object _syncObject;

        /// <summary>
        /// Read-only list of information concerning all sharded tables.
        /// </summary>
        public ReadOnlyCollection<ShardedTableInfo> ShardedTables
        {
            get { return _shardedTableSet.ToList().AsReadOnly(); }
        }

        /// <summary>
        /// Read-only list of information concerning all reference tables.
        /// </summary>
        public ReadOnlyCollection<ReferenceTableInfo> ReferenceTables
        {
            get { return _referenceTableSet.ToList().AsReadOnly(); }
        }

        /// <summary>
        /// Initialize any non-DataMember objects post deserialization.
        /// </summary>
        [OnDeserialized()]
        private void SetValuesOnDeserializing(StreamingContext context)
        {
            Initialize();
        }

        private void Initialize()
        {
            _shardedTableSet = _shardedTableSet ?? new HashSet<ShardedTableInfo>();
            _referenceTableSet = _referenceTableSet ?? new HashSet<ReferenceTableInfo>();
            _syncObject = new object();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaInfo"/> class.
        /// </summary>
        public SchemaInfo()
        {
            _shardedTableSet = new HashSet<ShardedTableInfo>();
            _referenceTableSet = new HashSet<ReferenceTableInfo>();
            Initialize();
        }

        /// <summary>
        /// Adds information about a sharded table.
        /// </summary>
        /// <param name="shardedTableInfo">Sharded table info.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0",
            Justification = "ShardedTableInfo class already validates its members.")]
        public void Add(ShardedTableInfo shardedTableInfo)
        {
            ExceptionUtils.DisallowNullArgument<ShardedTableInfo>(shardedTableInfo, "shardedTableInfo");

            string existingTableType;

            lock (_syncObject)
            {
                if (CheckIfTableExists(shardedTableInfo, out existingTableType))
                {
                    throw new SchemaInfoException(
                        SchemaInfoErrorCode.TableInfoAlreadyPresent,
                        Errors._SchemaInfo_TableInfoAlreadyExists,
                        existingTableType,
                        shardedTableInfo.SchemaName,
                        shardedTableInfo.TableName);
                }

                bool result = _shardedTableSet.Add(shardedTableInfo);
                // Adding to the sharded table set shouldn't fail since we have done all necessary
                // verification apriori.
                Debug.Assert(result, "Addition of new sharded table info failed.");
            }
        }

        /// <summary>
        /// Adds information about a reference table.
        /// </summary>
        /// <param name="referenceTableInfo">Reference table info.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0",
            Justification = "ReferenceTableInfo class already validates its members.")]
        public void Add(ReferenceTableInfo referenceTableInfo)
        {
            ExceptionUtils.DisallowNullArgument<ReferenceTableInfo>(referenceTableInfo, "referenceTableInfo");

            string existingTableType;

            lock (_syncObject)
            {
                if (CheckIfTableExists(referenceTableInfo, out existingTableType))
                {
                    throw new SchemaInfoException(
                        SchemaInfoErrorCode.TableInfoAlreadyPresent,
                        Errors._SchemaInfo_TableInfoAlreadyExists,
                        existingTableType,
                        referenceTableInfo.SchemaName,
                        referenceTableInfo.TableName);
                }

                bool result = _referenceTableSet.Add(referenceTableInfo);
                // Adding to the reference table set shouldn't fail since we have done all necessary
                // verification apriori.
                Debug.Assert(result, "Addition of new sharded table info failed.");
            }
        }

        /// <summary>
        /// Removes information about a sharded table.
        /// </summary>
        /// <param name="shardedTableInfo">Sharded table info.</param>
        public bool Remove(ShardedTableInfo shardedTableInfo)
        {
            return _shardedTableSet.Remove(shardedTableInfo);
        }

        /// <summary>
        /// Removes information about a reference table.
        /// </summary>
        /// <param name="referenceTableInfo">Reference table info.</param>
        public bool Remove(ReferenceTableInfo referenceTableInfo)
        {
            return _referenceTableSet.Remove(referenceTableInfo);
        }

        /// <summary>
        /// Check is either a sharded table or a reference table exists by the given name.
        /// </summary>
        /// <param name="tableInfo">Sharded or reference table info.</param>
        /// <param name="tableType">sharded, reference or null.</param>
        /// <returns></returns>
        private bool CheckIfTableExists(TableInfo tableInfo, out string tableType)
        {
            tableType = null;

            if (_shardedTableSet.Any(
                s =>
                    String.Compare(s.SchemaName, tableInfo.SchemaName, StringComparison.OrdinalIgnoreCase) == 0 &&
                    String.Compare(s.TableName, tableInfo.TableName, StringComparison.OrdinalIgnoreCase) == 0))
            {
                tableType = "sharded";
                return true;
            }

            if (this.ReferenceTables.Any(
                s =>
                    String.Compare(s.SchemaName, tableInfo.SchemaName, StringComparison.OrdinalIgnoreCase) == 0 &&
                    String.Compare(s.TableName, tableInfo.TableName, StringComparison.OrdinalIgnoreCase) == 0))
            {
                tableType = "reference";
                return true;
            }

            return false;
        }
    }
}
