// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.CacheStore
    /// </summary>
    [DebuggerDisplay("Stub of CacheStore")]
    [DebuggerNonUserCode]
    internal class StubCacheStore : CacheStore
    {
        /// <summary>
        /// Sets the stub of CacheStore.AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
        /// </summary>
        internal Action<IStoreMapping, CacheStoreMappingUpdatePolicy> AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy;
        /// <summary>
        /// Sets the stub of CacheStore.AddOrUpdateShardMap(IStoreShardMap shardMap)
        /// </summary>
        internal Action<IStoreShardMap> AddOrUpdateShardMapIStoreShardMap;
        /// <summary>
        /// Sets the stub of CacheStore.Clear()
        /// </summary>
        public Action Clear01;
        /// <summary>
        /// Sets the stub of CacheStore.DeleteMapping(IStoreMapping mapping)
        /// </summary>
        internal Action<IStoreMapping> DeleteMappingIStoreMapping;
        /// <summary>
        /// Sets the stub of CacheStore.DeleteShardMap(IStoreShardMap shardMap)
        /// </summary>
        internal Action<IStoreShardMap> DeleteShardMapIStoreShardMap;
        /// <summary>
        /// Sets the stub of CacheStore.Dispose(Boolean disposing)
        /// </summary>
        public Action<bool> DisposeBoolean;
        /// <summary>
        /// Sets the stub of CacheStore.LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
        /// </summary>
        internal Func<IStoreShardMap, ShardKey, ICacheStoreMapping> LookupMappingByKeyIStoreShardMapShardKey;
        /// <summary>
        /// Sets the stub of CacheStore.LookupShardMapByName(String shardMapName)
        /// </summary>
        internal Func<string, IStoreShardMap> LookupShardMapByNameString;
        private bool ___callBase;
        private IStubBehavior ___instanceBehavior;

        /// <summary>
        /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
        /// </summary>
        public bool CallBase
        {
            get
            {
                return this.___callBase;
            }
            set
            {
                this.___callBase = value;
            }
        }

        /// <summary>
        /// Gets or sets the instance behavior.
        /// </summary>
        public IStubBehavior InstanceBehavior
        {
            get
            {
                return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
            }
            set
            {
                this.___instanceBehavior = value;
            }
        }

        /// <summary>
        /// Initializes a new instance
        /// </summary>
        public StubCacheStore()
        {
            this.InitializeStub();
        }

        /// <summary>
        /// Sets the stub of CacheStore.AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
        /// </summary>
        public override void AddOrUpdateMapping(IStoreMapping mapping, CacheStoreMappingUpdatePolicy policy)
        {
            Action<IStoreMapping, CacheStoreMappingUpdatePolicy> action1 = this.AddOrUpdateMappingIStoreMappingCacheStoreMappingUpdatePolicy;
            if (action1 != null)
                action1(mapping, policy);
            else if (this.___callBase)
                base.AddOrUpdateMapping(mapping, policy);
            else
                this.InstanceBehavior.VoidResult<StubCacheStore>(this, "AddOrUpdateMapping");
        }

        /// <summary>
        /// Sets the stub of CacheStore.AddOrUpdateShardMap(IStoreShardMap shardMap)
        /// </summary>
        public override void AddOrUpdateShardMap(IStoreShardMap shardMap)
        {
            Action<IStoreShardMap> action1 = this.AddOrUpdateShardMapIStoreShardMap;
            if (action1 != null)
                action1(shardMap);
            else if (this.___callBase)
                base.AddOrUpdateShardMap(shardMap);
            else
                this.InstanceBehavior.VoidResult<StubCacheStore>(this, "AddOrUpdateShardMap");
        }

        /// <summary>
        /// Sets the stub of CacheStore.Clear()
        /// </summary>
        public override void Clear()
        {
            Action action1 = this.Clear01;
            if (action1 != null)
                action1();
            else if (this.___callBase)
                base.Clear();
            else
                this.InstanceBehavior.VoidResult<StubCacheStore>(this, "Clear");
        }

        /// <summary>
        /// Sets the stub of CacheStore.DeleteMapping(IStoreMapping mapping)
        /// </summary>
        public override void DeleteMapping(IStoreMapping mapping)
        {
            Action<IStoreMapping> action1 = this.DeleteMappingIStoreMapping;
            if (action1 != null)
                action1(mapping);
            else if (this.___callBase)
                base.DeleteMapping(mapping);
            else
                this.InstanceBehavior.VoidResult<StubCacheStore>(this, "DeleteMapping");
        }

        /// <summary>
        /// Sets the stub of CacheStore.DeleteShardMap(IStoreShardMap shardMap)
        /// </summary>
        public override void DeleteShardMap(IStoreShardMap shardMap)
        {
            Action<IStoreShardMap> action1 = this.DeleteShardMapIStoreShardMap;
            if (action1 != null)
                action1(shardMap);
            else if (this.___callBase)
                base.DeleteShardMap(shardMap);
            else
                this.InstanceBehavior.VoidResult<StubCacheStore>(this, "DeleteShardMap");
        }

        /// <summary>
        /// Sets the stub of CacheStore.Dispose(Boolean disposing)
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            Action<bool> action1 = this.DisposeBoolean;
            if (action1 != null)
                action1(disposing);
            else if (this.___callBase)
                base.Dispose(disposing);
            else
                this.InstanceBehavior.VoidResult<StubCacheStore>(this, "Dispose");
        }

        /// <summary>
        /// Initializes a new instance of type StubCacheStore
        /// </summary>
        private void InitializeStub()
        {
        }

        /// <summary>
        /// Sets the stub of CacheStore.LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
        /// </summary>
        public override ICacheStoreMapping LookupMappingByKey(IStoreShardMap shardMap, ShardKey key)
        {
            Func<IStoreShardMap, ShardKey, ICacheStoreMapping> func1 = this.LookupMappingByKeyIStoreShardMapShardKey;
            if (func1 != null)
                return func1(shardMap, key);
            if (this.___callBase)
                return base.LookupMappingByKey(shardMap, key);
            return this.InstanceBehavior.Result<StubCacheStore, ICacheStoreMapping>(this, "LookupMappingByKey");
        }

        /// <summary>
        /// Sets the stub of CacheStore.LookupShardMapByName(String shardMapName)
        /// </summary>
        public override IStoreShardMap LookupShardMapByName(string shardMapName)
        {
            Func<string, IStoreShardMap> func1 = this.LookupShardMapByNameString;
            if (func1 != null)
                return func1(shardMapName);
            if (this.___callBase)
                return base.LookupShardMapByName(shardMapName);
            return this.InstanceBehavior.Result<StubCacheStore, IStoreShardMap>(this, "LookupShardMapByName");
        }
    }
}
