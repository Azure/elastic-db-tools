﻿using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.QualityTools.Testing.Fakes;
using Microsoft.QualityTools.Testing.Fakes.Stubs;
using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.RemoveShardOperation
    /// </summary>
    [StubClass(typeof(RemoveShardOperation))]
    [DebuggerDisplay("Stub of RemoveShardOperation")]
    [DebuggerNonUserCode]
    internal class StubRemoveShardOperation : RemoveShardOperation, IStub<RemoveShardOperation>, IStub, IStubObservable, IPartialStub
    {
        /// <summary>
        /// Sets the stub of StoreOperation.Dispose(Boolean disposing)
        /// </summary>
        public FakesDelegates.Action<bool> DisposeBoolean;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> DoGlobalPostLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.DoGlobalPostLocalUpdateCache(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> DoGlobalPostLocalUpdateCacheIStoreResults;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> DoGlobalPreLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.DoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> DoLocalSourceExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> DoLocalTargetExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.get_ErrorCategory()
        /// </summary>
        public FakesDelegates.Func<ShardManagementErrorCategory> ErrorCategoryGet;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.get_ErrorSourceLocation()
        /// </summary>
        public FakesDelegates.Func<ShardLocation> ErrorSourceLocationGet;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.get_ErrorTargetLocation()
        /// </summary>
        public FakesDelegates.Func<ShardLocation> ErrorTargetLocationGet;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.GetStoreConnectionInfo()
        /// </summary>
        internal FakesDelegates.Func<StoreConnectionInfo> GetStoreConnectionInfo01;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleDoGlobalPostLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleDoGlobalPreLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleDoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleDoLocalSourceExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.HandleDoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleDoLocalTargetExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleUndoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleUndoLocalSourceExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleUndoLocalTargetExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperation.OnStoreException(StoreException se, StoreOperationState state)
        /// </summary>
        internal FakesDelegates.Func<StoreException, StoreOperationState, ShardManagementException> OnStoreExceptionStoreExceptionStoreOperationState;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> UndoGlobalPostLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> UndoGlobalPreLocalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of RemoveShardOperation.UndoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> UndoLocalSourceExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of StoreOperation.UndoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> UndoLocalTargetExecuteIStoreTransactionScope;
        private bool ___callBase;
        private IStubBehavior ___instanceBehavior;
        private IStubObserver ___instanceObserver;

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
        /// Sets the stub of RemoveShardOperation.get_ErrorCategory()
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<ShardManagementErrorCategory> func = (FakesDelegates.Func<ShardManagementErrorCategory>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<ShardManagementErrorCategory>), (object)this, typeof(RemoveShardOperation), "ErrorCategory", true, typeof(ShardManagementErrorCategory));
                    instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func);
                }
                FakesDelegates.Func<ShardManagementErrorCategory> func1 = this.ErrorCategoryGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ErrorCategory;
                return this.InstanceBehavior.Result<StubRemoveShardOperation, ShardManagementErrorCategory>(this, "get_ErrorCategory");
            }
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.get_ErrorSourceLocation()
        /// </summary>
        protected override ShardLocation ErrorSourceLocation
        {
            get
            {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<ShardLocation> func = (FakesDelegates.Func<ShardLocation>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<ShardLocation>), (object)this, typeof(RemoveShardOperation), "ErrorSourceLocation", true, typeof(ShardLocation));
                    instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func);
                }
                FakesDelegates.Func<ShardLocation> func1 = this.ErrorSourceLocationGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ErrorSourceLocation;
                return this.InstanceBehavior.Result<StubRemoveShardOperation, ShardLocation>(this, "get_ErrorSourceLocation");
            }
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.get_ErrorTargetLocation()
        /// </summary>
        protected override ShardLocation ErrorTargetLocation
        {
            get
            {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<ShardLocation> func = (FakesDelegates.Func<ShardLocation>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<ShardLocation>), (object)this, typeof(RemoveShardOperation), "ErrorTargetLocation", true, typeof(ShardLocation));
                    instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func);
                }
                FakesDelegates.Func<ShardLocation> func1 = this.ErrorTargetLocationGet;
                if (func1 != null)
                    return func1();
                if (this.___callBase)
                    return base.ErrorTargetLocation;
                return this.InstanceBehavior.Result<StubRemoveShardOperation, ShardLocation>(this, "get_ErrorTargetLocation");
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
        /// Gets or sets the instance observer.
        /// </summary>
        public IStubObserver InstanceObserver
        {
            get
            {
                return StubObservers.GetValueOrCurrent(this.___instanceObserver);
            }
            set
            {
                this.___instanceObserver = value;
            }
        }

        /// <summary>
        /// Initializes a new instance
        /// </summary>
        public StubRemoveShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shard)
          : base(shardMapManager, shardMap, shard)
        {
            this.InitializeStub();
        }

        /// <summary>
        /// Sets the stub of StoreOperation.Dispose(Boolean disposing)
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<bool> action = new FakesDelegates.Action<bool>(base.Dispose);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)action, (object)disposing);
            }
            FakesDelegates.Action<bool> action1 = this.DisposeBoolean;
            if (action1 != null)
                action1(disposing);
            else if (this.___callBase)
                base.Dispose(disposing);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "Dispose");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).DoGlobalPostLocalExecute);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.DoGlobalPostLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoGlobalPostLocalExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "DoGlobalPostLocalExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.DoGlobalPostLocalUpdateCache(IStoreResults result)
        /// </summary>
        public override void DoGlobalPostLocalUpdateCache(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).DoGlobalPostLocalUpdateCache);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.DoGlobalPostLocalUpdateCacheIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.DoGlobalPostLocalUpdateCache(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "DoGlobalPostLocalUpdateCache");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).DoGlobalPreLocalExecute);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.DoGlobalPreLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoGlobalPreLocalExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "DoGlobalPreLocalExecute");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.DoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).DoLocalSourceExecute);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.DoLocalSourceExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoLocalSourceExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "DoLocalSourceExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).DoLocalTargetExecute);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.DoLocalTargetExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.DoLocalTargetExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "DoLocalTargetExecute");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.GetStoreConnectionInfo()
        /// </summary>
        public override StoreConnectionInfo GetStoreConnectionInfo()
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<StoreConnectionInfo> func = new FakesDelegates.Func<StoreConnectionInfo>(((StoreOperation)this).GetStoreConnectionInfo);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func);
            }
            FakesDelegates.Func<StoreConnectionInfo> func1 = this.GetStoreConnectionInfo01;
            if (func1 != null)
                return func1();
            if (this.___callBase)
                return base.GetStoreConnectionInfo();
            return this.InstanceBehavior.Result<StubRemoveShardOperation, StoreConnectionInfo>(this, "GetStoreConnectionInfo");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleDoGlobalPostLocalExecuteError);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleDoGlobalPostLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoGlobalPostLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleDoGlobalPostLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleDoGlobalPreLocalExecuteError);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleDoGlobalPreLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoGlobalPreLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleDoGlobalPreLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleDoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoLocalSourceExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleDoLocalSourceExecuteError);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleDoLocalSourceExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoLocalSourceExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleDoLocalSourceExecuteError");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.HandleDoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleDoLocalTargetExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleDoLocalTargetExecuteError);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleDoLocalTargetExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleDoLocalTargetExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleDoLocalTargetExecuteError");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleUndoGlobalPostLocalExecuteError);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoGlobalPostLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleUndoGlobalPostLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleUndoGlobalPreLocalExecuteError);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoGlobalPreLocalExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleUndoGlobalPreLocalExecuteError");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.HandleUndoLocalSourceExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoLocalSourceExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleUndoLocalSourceExecuteError);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleUndoLocalSourceExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoLocalSourceExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleUndoLocalSourceExecuteError");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.HandleUndoLocalTargetExecuteError(IStoreResults result)
        /// </summary>
        public override void HandleUndoLocalTargetExecuteError(IStoreResults result)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperation)this).HandleUndoLocalTargetExecuteError);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleUndoLocalTargetExecuteErrorIStoreResults;
            if (action1 != null)
                action1(result);
            else if (this.___callBase)
                base.HandleUndoLocalTargetExecuteError(result);
            else
                this.InstanceBehavior.VoidResult<StubRemoveShardOperation>(this, "HandleUndoLocalTargetExecuteError");
        }

        /// <summary>
        /// Initializes a new instance of type StubRemoveShardOperation
        /// </summary>
        private void InitializeStub()
        {
        }

        /// <summary>
        /// Sets the stub of StoreOperation.OnStoreException(StoreException se, StoreOperationState state)
        /// </summary>
        public override ShardManagementException OnStoreException(StoreException se, StoreOperationState state)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<StoreException, StoreOperationState, ShardManagementException> func = new FakesDelegates.Func<StoreException, StoreOperationState, ShardManagementException>(((StoreOperation)this).OnStoreException);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)func, (object)se, (object)state);
            }
            FakesDelegates.Func<StoreException, StoreOperationState, ShardManagementException> func1 = this.OnStoreExceptionStoreExceptionStoreOperationState;
            if (func1 != null)
                return func1(se, state);
            if (this.___callBase)
                return base.OnStoreException(se, state);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, ShardManagementException>(this, "OnStoreException");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).UndoGlobalPostLocalExecute);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoGlobalPostLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoGlobalPostLocalExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "UndoGlobalPostLocalExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).UndoGlobalPreLocalExecute);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoGlobalPreLocalExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoGlobalPreLocalExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "UndoGlobalPreLocalExecute");
        }

        /// <summary>
        /// Sets the stub of RemoveShardOperation.UndoLocalSourceExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).UndoLocalSourceExecute);
                instanceObserver.Enter(typeof(RemoveShardOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoLocalSourceExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoLocalSourceExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "UndoLocalSourceExecute");
        }

        /// <summary>
        /// Sets the stub of StoreOperation.UndoLocalTargetExecute(IStoreTransactionScope ts)
        /// </summary>
        public override IStoreResults UndoLocalTargetExecute(IStoreTransactionScope ts)
        {
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperation)this).UndoLocalTargetExecute);
                instanceObserver.Enter(typeof(StoreOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.UndoLocalTargetExecuteIStoreTransactionScope;
            if (func1 != null)
                return func1(ts);
            if (this.___callBase)
                return base.UndoLocalTargetExecute(ts);
            return this.InstanceBehavior.Result<StubRemoveShardOperation, IStoreResults>(this, "UndoLocalTargetExecute");
        }
    }
}
