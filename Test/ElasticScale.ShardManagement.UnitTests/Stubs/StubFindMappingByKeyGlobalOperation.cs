// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.FindMappingByKeyGlobalOperation
    /// </summary>
    [DebuggerDisplay("Stub of FindMappingByKeyGlobalOperation")]
    [DebuggerNonUserCode]
    internal class StubFindMappingByKeyGlobalOperation : FindMappingByKeyGlobalOperation
    {
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.Dispose(Boolean disposing)
        /// </summary>
        public Action<bool> DisposeBoolean;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecuteAsync(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, Task<IStoreResults>> DoGlobalExecuteAsyncIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> DoGlobalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePost(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> DoGlobalUpdateCachePostIStoreResults;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePre(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> DoGlobalUpdateCachePreIStoreResults;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
        /// </summary>
        public Func<ShardManagementErrorCategory> ErrorCategoryGet;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.HandleDoGlobalExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleDoGlobalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.OnStoreException(StoreException se)
        /// </summary>
        public Func<StoreException, ShardManagementException> OnStoreExceptionStoreException;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
        /// </summary>
        public Func<bool> ReadOnlyGet;
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
        /// </summary>
        internal Func<IStoreLogEntry, Task> UndoPendingStoreOperationsAsyncIStoreLogEntry;
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperations(IStoreLogEntry logEntry)
        /// </summary>
        internal Action<IStoreLogEntry> UndoPendingStoreOperationsIStoreLogEntry;
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
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                Func<ShardManagementErrorCategory> func1 = this.ErrorCategoryGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ErrorCategory;
                return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, ShardManagementErrorCategory>(this, "get_ErrorCategory");
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
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
        /// </summary>
        public override bool ReadOnly
        {
            get
            {
                Func<bool> func1 = this.ReadOnlyGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ReadOnly;
                return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, bool>(this, "get_ReadOnly");
            }
        }

        /// <summary>
        /// Initializes a new instance
        /// </summary>
        public StubFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure)
          : base(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure)
        {
            this.InitializeStub();
        }

        /// <summary>
        /// Sets the stub of StoreOperationGlobal.Dispose(Boolean disposing)
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            Action<bool> action1 = this.DisposeBoolean;
            if (action1 != null)
                action1(disposing);
            else if (this.___callBase)
                base.Dispose(disposing);
            else
                this.InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "Dispose");
        }

        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.DoGlobalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoGlobalExecute(ts);
            return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, IStoreResults>(this, "DoGlobalExecute");
        }

        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecuteAsync(IStoreTransactionScope ts)
        /// </summary>
        public override Task<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, Task<IStoreResults>> func1 = this.DoGlobalExecuteAsyncIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoGlobalExecuteAsync(ts);
            return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, Task<IStoreResults>>(this, "DoGlobalExecuteAsync");
        }

        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePost(IStoreResults result)
        /// </summary>
        public override void DoGlobalUpdateCachePost(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.DoGlobalUpdateCachePostIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.DoGlobalUpdateCachePost(result);
            else
                this.InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "DoGlobalUpdateCachePost");
        }

        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePre(IStoreResults result)
        /// </summary>
        public override void DoGlobalUpdateCachePre(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.DoGlobalUpdateCachePreIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.DoGlobalUpdateCachePre(result);
            else
                this.InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "DoGlobalUpdateCachePre");
        }

        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.HandleDoGlobalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoGlobalExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleDoGlobalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoGlobalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "HandleDoGlobalExecuteError");
        }

        /// <summary>
        /// Initializes a new instance of type StubFindMappingByKeyGlobalOperation
        /// </summary>
        private void InitializeStub()
        {
        }

        /// <summary>
        /// Sets the stub of StoreOperationGlobal.OnStoreException(StoreException se)
        /// </summary>
        public override ShardManagementException OnStoreException(StoreException se)
        {
            Func<StoreException, ShardManagementException> func1 = this.OnStoreExceptionStoreException;
            if (func1 != null)
                return func1(se);
            if (this.___callBase)
                return base.OnStoreException(se);
            return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, ShardManagementException>(this, "OnStoreException");
        }

        /// <summary>
        /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperations(IStoreLogEntry logEntry)
        /// </summary>
        protected override void UndoPendingStoreOperations(IStoreLogEntry logEntry)
        {
            Action<IStoreLogEntry> action1 = this.UndoPendingStoreOperationsIStoreLogEntry;
            if (action1 != null)
                action1(logEntry);
            else if (this.___callBase)
                base.UndoPendingStoreOperations(logEntry);
            else
                this.InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "UndoPendingStoreOperations");
        }

        /// <summary>
        /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
        /// </summary>
        protected override Task UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
        {
            Func<IStoreLogEntry, Task> func1 = this.UndoPendingStoreOperationsAsyncIStoreLogEntry;
            if (func1 != null)
                return func1(logEntry);
            if (this.___callBase)
                return base.UndoPendingStoreOperationsAsync(logEntry);
            return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, Task>(this, "UndoPendingStoreOperationsAsync");
        }
    }
}
