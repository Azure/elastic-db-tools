// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema
{
    /// <summary>
    /// Provides storage services to a client for storing\updating\retrieving schema information associated with a sharding scheme 
    /// and assigning names to individual buckets of information. The class doesn't store the association between a sharding scheme 
    /// and the metadata unit. It's the caller's responsibility to maintain the mapping.
    /// </summary>
    public class SchemaInfoCollection : IEnumerable<KeyValuePair<string, SchemaInfo>>
    {
        /// <summary>
        /// Constructs an instance of schema info collection.
        /// </summary>
        /// <param name="manager">Shard map manager object.</param>
        internal SchemaInfoCollection(ShardMapManager manager)
        {
            this.Manager = manager;
        }

        /// <summary>
        /// Shard map manager object.
        /// </summary>
        private ShardMapManager Manager
        {
            get;
            set;
        }

        /// <summary>
        /// Adds a <see cref="SchemaInfo"/> that is associated with the given <see cref="ShardMap"/> name. 
        /// The associated data contains information concerning sharded tables and
        /// reference tables. If you try to add a <see cref="SchemaInfo"/> with an existing name, 
        /// a name-conflict exception will be thrown
        /// </summary>
        /// <param name="shardMapName">The name of the <see cref="ShardMap"/> that 
        /// the <see cref="SchemaInfo"/> will be associated with</param>
        /// <param name="schemaInfo">Sharding schema information.</param>
        public void Add(string shardMapName, SchemaInfo schemaInfo)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");
            ExceptionUtils.DisallowNullArgument<SchemaInfo>(schemaInfo, "schemaInfo");

            DefaultStoreSchemaInfo dssi = new DefaultStoreSchemaInfo(
                shardMapName,
                SerializationHelper.SerializeXmlData<SchemaInfo>(schemaInfo));

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateAddShardingSchemaInfoGlobalOperation(
                this.Manager,
                "Add",
                dssi))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Replaces the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name.
        /// </summary>
        /// <param name="shardMapName">The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/> will be replaced.</param>
        /// <param name="schemaInfo">Sharding schema information.</param>
        public void Replace(string shardMapName, SchemaInfo schemaInfo)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");
            ExceptionUtils.DisallowNullArgument<SchemaInfo>(schemaInfo, "schemaInfo");

            DefaultStoreSchemaInfo dssi = new DefaultStoreSchemaInfo(
                shardMapName,
                SerializationHelper.SerializeXmlData<SchemaInfo>(schemaInfo));

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateUpdateShardingSchemaInfoGlobalOperation(
                this.Manager,
                "Replace",
                dssi))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Tries to fetch the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name without 
        /// raising any exception if data doesn't exist.
        /// </summary>
        /// <param name="shardMapName">The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/>
        /// will be fetched</param>
        /// <param name="schemaInfo">The <see cref="SchemaInfo"/> that was fetched or null if retrieval failed</param>
        /// <returns>true if schema info exists with given name, false otherwise.</returns>
        public bool TryGet(string shardMapName, out SchemaInfo schemaInfo)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

            schemaInfo = null;

            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindShardingSchemaInfoGlobalOperation(
                this.Manager,
                "TryGet",
                shardMapName))
            {
                result = op.Do();
            }

            if (result.Result == StoreResult.SchemaInfoNameDoesNotExist)
            {
                return false;
            }

            schemaInfo = result.StoreSchemaInfoCollection
                         .Select(si => SerializationHelper.DeserializeXmlData<SchemaInfo>(si.ShardingSchemaInfo))
                         .Single();

            return true;
        }

        /// <summary>
        /// Fetches the <see cref="SchemaInfo"/> stored with the supplied <see cref="ShardMap"/> name.
        /// </summary>
        /// <param name="shardMapName">The name of the <see cref="ShardMap"/> to get.</param>
        /// <returns>SchemaInfo object.</returns>
        public SchemaInfo Get(string shardMapName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateFindShardingSchemaInfoGlobalOperation(
                this.Manager,
                "Get",
                shardMapName))
            {
                result = op.Do();
            }

            if (result.Result == StoreResult.SchemaInfoNameDoesNotExist)
            {
                throw new SchemaInfoException(
                    SchemaInfoErrorCode.SchemaInfoNameDoesNotExist,
                    Errors._Store_SchemaInfo_NameDoesNotExist,
                    "Get",
                    shardMapName);
            }

            return result.StoreSchemaInfoCollection
                         .Select(si => SerializationHelper.DeserializeXmlData<SchemaInfo>(si.ShardingSchemaInfo))
                         .Single();
        }

        /// <summary>
        /// Removes the <see cref="SchemaInfo"/> with the given <see cref="ShardMap"/> name.
        /// </summary>
        /// <param name="shardMapName">The name of the <see cref="ShardMap"/> whose <see cref="SchemaInfo"/>
        /// will be removed</param>
        public void Remove(string shardMapName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(shardMapName, "shardMapName");

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateRemoveShardingSchemaInfoGlobalOperation(
                this.Manager,
                "Remove",
                shardMapName))
            {
                op.Do();
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the <see cref="SchemaInfoCollection"/>.
        /// </summary>
        /// <returns>Enumerator of key-value pairs of name and <see cref="SchemaInfo"/> objects.</returns>
        public IEnumerator<KeyValuePair<string, SchemaInfo>> GetEnumerator()
        {
            IStoreResults result;

            using (IStoreOperationGlobal op = this.Manager.StoreOperationFactory.CreateGetShardingSchemaInfosGlobalOperation(
                this.Manager,
                "GetEnumerator"))
            {
                result = op.Do();
            }

            Dictionary<string, SchemaInfo> mdCollection = new Dictionary<string, SchemaInfo>();

            foreach (IStoreSchemaInfo ssi in result.StoreSchemaInfoCollection)
            {
                mdCollection.Add(ssi.Name, SerializationHelper.DeserializeXmlData<SchemaInfo>(ssi.ShardingSchemaInfo));
            }

            return mdCollection.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through this <see cref="SchemaInfoCollection"/>.
        /// </summary>
        /// <returns>Enumerator of key-value pairs of name and <see cref="SchemaInfo"/> objects.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
