// Decompiled with JetBrains decompiler
// Type: Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Fakes.StubFindMappingByKeyGlobalOperation
// Assembly: Microsoft.Azure.SqlDatabase.ElasticScale.Client.Fakes, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 4B4C742C-94FD-4E0A-8D2B-C003E4ADC4B7
// Assembly location: C:\git\elastic-db-tools\Test\ElasticScale.ShardManagement.UnitTests\FakesAssemblies\Microsoft.Azure.SqlDatabase.ElasticScale.Client.Fakes.dll

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.QualityTools.Testing.Fakes;
using Microsoft.QualityTools.Testing.Fakes.Stubs;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs
{
    /// <summary>
    /// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.FindMappingByKeyGlobalOperation
    /// </summary>
    [StubClass(typeof(FindMappingByKeyGlobalOperation))]
    [DebuggerDisplay("Stub of FindMappingByKeyGlobalOperation")]
    [DebuggerNonUserCode]
    internal class StubFindMappingByKeyGlobalOperation : FindMappingByKeyGlobalOperation, IStub<FindMappingByKeyGlobalOperation>, IStub, IStubObservable, IPartialStub
    {
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.Dispose(Boolean disposing)
        /// </summary>
        public FakesDelegates.Action<bool> DisposeBoolean;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecuteAsync(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, Task<IStoreResults>> DoGlobalExecuteAsyncIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecute(IStoreTransactionScope ts)
        /// </summary>
        internal FakesDelegates.Func<IStoreTransactionScope, IStoreResults> DoGlobalExecuteIStoreTransactionScope;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePost(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> DoGlobalUpdateCachePostIStoreResults;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePre(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> DoGlobalUpdateCachePreIStoreResults;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
        /// </summary>
        public FakesDelegates.Func<ShardManagementErrorCategory> ErrorCategoryGet;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.HandleDoGlobalExecuteError(IStoreResults result)
        /// </summary>
        internal FakesDelegates.Action<IStoreResults> HandleDoGlobalExecuteErrorIStoreResults;
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.OnStoreException(StoreException se)
        /// </summary>
        public FakesDelegates.Func<StoreException, ShardManagementException> OnStoreExceptionStoreException;
        /// <summary>
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
        /// </summary>
        public FakesDelegates.Func<bool> ReadOnlyGet;
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
        /// </summary>
        internal FakesDelegates.Func<IStoreLogEntry, Task> UndoPendingStoreOperationsAsyncIStoreLogEntry;
        /// <summary>
        /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperations(IStoreLogEntry logEntry)
        /// </summary>
        internal FakesDelegates.Action<IStoreLogEntry> UndoPendingStoreOperationsIStoreLogEntry;
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
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
        /// </summary>
        protected override ShardManagementErrorCategory ErrorCategory
        {
            get
            {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<ShardManagementErrorCategory> func = (FakesDelegates.Func<ShardManagementErrorCategory>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<ShardManagementErrorCategory>), (object)this, typeof(FindMappingByKeyGlobalOperation), "ErrorCategory", true, typeof(ShardManagementErrorCategory));
                    instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)func);
                }
                FakesDelegates.Func<ShardManagementErrorCategory> func1 = this.ErrorCategoryGet;
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
        /// Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
        /// </summary>
        public override bool ReadOnly
        {
            get
            {
                IStubObserver instanceObserver = this.InstanceObserver;
                if (instanceObserver != null)
                {
                    FakesDelegates.Func<bool> func = (FakesDelegates.Func<bool>)StubRuntime.BindProperty(typeof(FakesDelegates.Func<bool>), (object)this, typeof(FindMappingByKeyGlobalOperation), "ReadOnly", true, typeof(bool));
                    instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)func);
                }
                FakesDelegates.Func<bool> func1 = this.ReadOnlyGet;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<bool> action = new FakesDelegates.Action<bool>(base.Dispose);
                instanceObserver.Enter(typeof(StoreOperationGlobal), (Delegate)action, (object)disposing);
            }
            FakesDelegates.Action<bool> action1 = this.DisposeBoolean;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func = new FakesDelegates.Func<IStoreTransactionScope, IStoreResults>(((StoreOperationGlobal)this).DoGlobalExecute);
                instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, IStoreResults> func1 = this.DoGlobalExecuteIStoreTransactionScope;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreTransactionScope, Task<IStoreResults>> func = new FakesDelegates.Func<IStoreTransactionScope, Task<IStoreResults>>(((StoreOperationGlobal)this).DoGlobalExecuteAsync);
                instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)func, (object)ts);
            }
            FakesDelegates.Func<IStoreTransactionScope, Task<IStoreResults>> func1 = this.DoGlobalExecuteAsyncIStoreTransactionScope;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperationGlobal)this).DoGlobalUpdateCachePost);
                instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.DoGlobalUpdateCachePostIStoreResults;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperationGlobal)this).DoGlobalUpdateCachePre);
                instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.DoGlobalUpdateCachePreIStoreResults;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreResults> action = new FakesDelegates.Action<IStoreResults>(((StoreOperationGlobal)this).HandleDoGlobalExecuteError);
                instanceObserver.Enter(typeof(FindMappingByKeyGlobalOperation), (Delegate)action, (object)result);
            }
            FakesDelegates.Action<IStoreResults> action1 = this.HandleDoGlobalExecuteErrorIStoreResults;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<StoreException, ShardManagementException> func = new FakesDelegates.Func<StoreException, ShardManagementException>(((StoreOperationGlobal)this).OnStoreException);
                instanceObserver.Enter(typeof(StoreOperationGlobal), (Delegate)func, (object)se);
            }
            FakesDelegates.Func<StoreException, ShardManagementException> func1 = this.OnStoreExceptionStoreException;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Action<IStoreLogEntry> action = new FakesDelegates.Action<IStoreLogEntry>(base.UndoPendingStoreOperations);
                instanceObserver.Enter(typeof(StoreOperationGlobal), (Delegate)action, (object)logEntry);
            }
            FakesDelegates.Action<IStoreLogEntry> action1 = this.UndoPendingStoreOperationsIStoreLogEntry;
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
            IStubObserver instanceObserver = this.InstanceObserver;
            if (instanceObserver != null)
            {
                FakesDelegates.Func<IStoreLogEntry, Task> func = new FakesDelegates.Func<IStoreLogEntry, Task>(base.UndoPendingStoreOperationsAsync);
                instanceObserver.Enter(typeof(StoreOperationGlobal), (Delegate)func, (object)logEntry);
            }
            FakesDelegates.Func<IStoreLogEntry, Task> func1 = this.UndoPendingStoreOperationsAsyncIStoreLogEntry;
            if (func1 != null)
                return func1(logEntry);
            if (this.___callBase)
                return base.UndoPendingStoreOperationsAsync(logEntry);
            return this.InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, Task>(this, "UndoPendingStoreOperationsAsync");
        }
    }
}
