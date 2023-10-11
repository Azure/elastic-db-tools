// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;

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
    private IStubBehavior ___instanceBehavior;

    /// <summary>
    /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
    /// </summary>
    public bool CallBase { get; set; }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.get_ErrorCategory()
    /// </summary>
    protected override ShardManagementErrorCategory ErrorCategory
    {
        get
        {
            var func1 = ErrorCategoryGet;
            return func1 != null
                ? func1()
                : CallBase
                ? base.ErrorCategory
                : InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, ShardManagementErrorCategory>(this, "get_ErrorCategory");
        }
    }

    /// <summary>
    /// Gets or sets the instance behavior.
    /// </summary>
    public IStubBehavior InstanceBehavior
    {
        get => StubBehaviors.GetValueOrCurrent(___instanceBehavior);
        set => ___instanceBehavior = value;
    }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.get_ReadOnly()
    /// </summary>
    public override bool ReadOnly
    {
        get
        {
            var func1 = ReadOnlyGet;
            return func1 != null
                ? func1()
                : CallBase ? base.ReadOnly : InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, bool>(this, "get_ReadOnly");
        }
    }

    /// <summary>
    /// Initializes a new instance
    /// </summary>
    public StubFindMappingByKeyGlobalOperation(ShardMapManager shardMapManager, string operationName, IStoreShardMap shardMap, ShardKey key, CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, bool cacheResults, bool ignoreFailure)
      : base(shardMapManager, operationName, shardMap, key, policy, errorCategory, cacheResults, ignoreFailure) => InitializeStub();

    /// <summary>
    /// Sets the stub of StoreOperationGlobal.Dispose(Boolean disposing)
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        var action1 = DisposeBoolean;
        if (action1 != null)
            action1(disposing);
        else if (CallBase)
            base.Dispose(disposing);
        else
            InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "Dispose");
    }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults DoGlobalExecute(IStoreTransactionScope ts)
    {
        var func1 = DoGlobalExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.DoGlobalExecute(ts)
            : InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, IStoreResults>(this, "DoGlobalExecute");
    }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalExecuteAsync(IStoreTransactionScope ts)
    /// </summary>
    public override Task<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts)
    {
        var func1 = DoGlobalExecuteAsyncIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? (Task<IStoreResults>)base.DoGlobalExecuteAsync(ts)
            : InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, Task<IStoreResults>>(this, "DoGlobalExecuteAsync");
    }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePost(IStoreResults result)
    /// </summary>
    public override void DoGlobalUpdateCachePost(IStoreResults result)
    {
        var action1 = DoGlobalUpdateCachePostIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.DoGlobalUpdateCachePost(result);
        else
            InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "DoGlobalUpdateCachePost");
    }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.DoGlobalUpdateCachePre(IStoreResults result)
    /// </summary>
    public override void DoGlobalUpdateCachePre(IStoreResults result)
    {
        var action1 = DoGlobalUpdateCachePreIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.DoGlobalUpdateCachePre(result);
        else
            InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "DoGlobalUpdateCachePre");
    }

    /// <summary>
    /// Sets the stub of FindMappingByKeyGlobalOperation.HandleDoGlobalExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleDoGlobalExecuteError(IStoreResults result)
    {
        var action1 = HandleDoGlobalExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleDoGlobalExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "HandleDoGlobalExecuteError");
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
        var func1 = OnStoreExceptionStoreException;
        return func1 != null
            ? func1(se)
            : CallBase
            ? base.OnStoreException(se)
            : InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, ShardManagementException>(this, "OnStoreException");
    }

    /// <summary>
    /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperations(IStoreLogEntry logEntry)
    /// </summary>
    protected override void UndoPendingStoreOperations(IStoreLogEntry logEntry)
    {
        var action1 = UndoPendingStoreOperationsIStoreLogEntry;
        if (action1 != null)
            action1(logEntry);
        else if (CallBase)
            base.UndoPendingStoreOperations(logEntry);
        else
            InstanceBehavior.VoidResult<StubFindMappingByKeyGlobalOperation>(this, "UndoPendingStoreOperations");
    }

    /// <summary>
    /// Sets the stub of StoreOperationGlobal.UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
    /// </summary>
    protected override Task UndoPendingStoreOperationsAsync(IStoreLogEntry logEntry)
    {
        var func1 = UndoPendingStoreOperationsAsyncIStoreLogEntry;
        return func1 != null
            ? func1(logEntry)
            : CallBase
            ? base.UndoPendingStoreOperationsAsync(logEntry)
            : InstanceBehavior.Result<StubFindMappingByKeyGlobalOperation, Task>(this, "UndoPendingStoreOperationsAsync");
    }
}
