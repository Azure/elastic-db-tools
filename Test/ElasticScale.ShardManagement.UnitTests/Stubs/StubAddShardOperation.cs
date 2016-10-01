// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.AddShardOperation
    /// </summary>
    [DebuggerDisplay("Stub of AddShardOperation")]
    [DebuggerNonUserCode]
    internal class StubAddShardOperation : AddShardOperation
    {
        /// <summary>
        /// Sets the stub of StoreOperation.Dispose(Boolean disposing)
        /// </summary>
        public Action<bool> DisposeBoolean;
        /// <summary>
        /// Sets the stub of AddShardOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> DoGlobalPostLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.DoGlobalPostLocalUpdateCache(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> DoGlobalPostLocalUpdateCacheIStoreResults;
        /// <summary>
        /// Sets the stub of AddShardOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> DoGlobalPreLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of AddShardOperation.DoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> DoLocalSourceExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> DoLocalTargetExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of AddShardOperation.get_ErrorCategory()
        /// </summary>
        public Func<ShardManagementErrorCategory> ErrorCategoryGet;
        /// <summary>
        /// Sets the stub of AddShardOperation.get_ErrorSourceLocation()
        /// </summary>
        public Func<ShardLocation> ErrorSourceLocationGet;
        /// <summary>
        /// Sets the stub of AddShardOperation.get_ErrorTargetLocation()
        /// </summary>
        public Func<ShardLocation> ErrorTargetLocationGet;
        /// <summary>
        /// Sets the stub of AddShardOperation.GetStoreConnectionInfo()
        /// </summary>
        internal Func<StoreConnectionInfo> GetStoreConnectionInfo01;
        /// <summary>
        /// Sets the stub of AddShardOperation.HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleDoGlobalPostLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of AddShardOperation.HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleDoGlobalPreLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of AddShardOperation.HandleDoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleDoLocalSourceExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.HandleDoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleDoLocalTargetExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of AddShardOperation.HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of AddShardOperation.HandleUndoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleUndoLocalSourceExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        internal Action<IStoreResults> HandleUndoLocalTargetExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.OnStoreException(StoreException se, StoreOperationState state)
        /// </summary>
        internal Func<StoreException, StoreOperationState, ShardManagementException> OnStoreExceptionStoreExceptionStoreOperationState;
        /// <summary>
        /// Sets the stub of AddShardOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> UndoGlobalPostLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> UndoGlobalPreLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of AddShardOperation.UndoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> UndoLocalSourceExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.UndoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        internal Func<IStoreTransactionScope, IStoreResults> UndoLocalTargetExecuteIStoreTransactionScope;
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
        /// Sets the stub of AddShardOperation.get_ErrorCategory()
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
                return this.InstanceBehavior.Result<StubAddShardOperation, ShardManagementErrorCategory>(this, "get_ErrorCategory");
            }
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.get_ErrorSourceLocation()
        /// </summary>
        protected override ShardLocation ErrorSourceLocation
        {
            get
            {
                Func<ShardLocation> func1 = this.ErrorSourceLocationGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ErrorSourceLocation;
                return this.InstanceBehavior.Result<StubAddShardOperation, ShardLocation>(this, "get_ErrorSourceLocation");
            }
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.get_ErrorTargetLocation()
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                Func<ShardLocation> func1 = this.ErrorTargetLocationGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ErrorTargetLocation;
                return this.InstanceBehavior.Result<StubAddShardOperation, ShardLocation>(this, "get_ErrorTargetLocation");
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
        public StubAddShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
          : base(shardMapManager, shardMap, shard)
        {
            this.InitializeStub();
        }

        /// <summary>
        /// Sets the stub of StoreOperation.Dispose(Boolean disposing)
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            Action<bool> action1 = this.DisposeBoolean;
            if (action1 != null)
                action1(disposing);
            else if (this.___callBase)
                base.Dispose(disposing);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "Dispose");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.DoGlobalPostLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoGlobalPostLocalExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "DoGlobalPostLocalExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.DoGlobalPostLocalUpdateCache(IStoreResults result)
        /// </summary>
        public override void DoGlobalPostLocalUpdateCache(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.DoGlobalPostLocalUpdateCacheIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.DoGlobalPostLocalUpdateCache(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "DoGlobalPostLocalUpdateCache");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.DoGlobalPreLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoGlobalPreLocalExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "DoGlobalPreLocalExecute");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.DoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.DoLocalSourceExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoLocalSourceExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "DoLocalSourceExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.DoLocalTargetExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoLocalTargetExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "DoLocalTargetExecute");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.GetStoreConnectionInfo()
        /// </summary>
        public override StoreConnectionInfo GetStoreConnectionInfo()
        {
            Func<StoreConnectionInfo> func1 = this.GetStoreConnectionInfo01;
            if (func1 != null)
                return func1();
            if (this.___callBase)
                return base.GetStoreConnectionInfo();
            return this.InstanceBehavior.Result<StubAddShardOperation, StoreConnectionInfo>(this, "GetStoreConnectionInfo");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleDoGlobalPostLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoGlobalPostLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleDoGlobalPostLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleDoGlobalPreLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoGlobalPreLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleDoGlobalPreLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.HandleDoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoLocalSourceExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleDoLocalSourceExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoLocalSourceExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleDoLocalSourceExecuteError");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.HandleDoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoLocalTargetExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleDoLocalTargetExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoLocalTargetExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleDoLocalTargetExecuteError");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoGlobalPostLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleUndoGlobalPostLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoGlobalPreLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleUndoGlobalPreLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.HandleUndoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoLocalSourceExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleUndoLocalSourceExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoLocalSourceExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleUndoLocalSourceExecuteError");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoLocalTargetExecuteError(IStoreResults result)
        {
            Action<IStoreResults> action1 = this.HandleUndoLocalTargetExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoLocalTargetExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubAddShardOperation>(this, "HandleUndoLocalTargetExecuteError");
        }

        /// <summary>
        /// Initializes a new instance of type StubAddShardOperation
        /// </summary>
        private void InitializeStub()
        {
        }

        /// <summary>
        /// Sets the stub of StoreOperation.OnStoreException(StoreException se, StoreOperationState state)
        /// </summary>
        public override ShardManagementException OnStoreException(StoreException se, StoreOperationState state)
        {
            Func<StoreException, StoreOperationState, ShardManagementException> func1 = this.OnStoreExceptionStoreExceptionStoreOperationState;
            if (func1 != null)
                return func1(se, state);
            if (this.___callBase)
                return base.OnStoreException(se, state);
            return this.InstanceBehavior.Result<StubAddShardOperation, ShardManagementException>(this, "OnStoreException");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoGlobalPostLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoGlobalPostLocalExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "UndoGlobalPostLocalExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoGlobalPreLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoGlobalPreLocalExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "UndoGlobalPreLocalExecute");
        }

        /// <summary>
        /// Sets the stub of AddShardOperation.UndoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoLocalSourceExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoLocalSourceExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "UndoLocalSourceExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.UndoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoLocalTargetExecute(IStoreTransactionScope ts)
        {
            Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoLocalTargetExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoLocalTargetExecute(ts);
            return this.InstanceBehavior.Result<StubAddShardOperation, IStoreResults>(this, "UndoLocalTargetExecute");
        }
    }
}
