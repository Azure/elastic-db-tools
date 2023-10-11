// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;
using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Stubs;

/// <summary>
/// Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UpdateShardOperation
/// </summary>
[DebuggerDisplay("Stub of UpdateShardOperation")]
[DebuggerNonUserCode]
internal class StubUpdateShardOperation : UpdateShardOperation
{
    /// <summary>
    /// Sets the stub of StoreOperation.Dispose(Boolean disposing)
    /// </summary>
    public Action<bool> DisposeBoolean;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> DoGlobalPostLocalExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of StoreOperation.DoGlobalPostLocalUpdateCache(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> DoGlobalPostLocalUpdateCacheIStoreResults;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> DoGlobalPreLocalExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.DoLocalSourceExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> DoLocalSourceExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of StoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> DoLocalTargetExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.get_ErrorCategory()
    /// </summary>
    public Func<ShardManagementErrorCategory> ErrorCategoryGet;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.get_ErrorSourceLocation()
    /// </summary>
    public Func<ShardLocation> ErrorSourceLocationGet;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.get_ErrorTargetLocation()
    /// </summary>
    public Func<ShardLocation> ErrorTargetLocationGet;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.GetStoreConnectionInfo()
    /// </summary>
    internal Func<StoreConnectionInfo> GetStoreConnectionInfo01;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleDoGlobalPostLocalExecuteError(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> HandleDoGlobalPostLocalExecuteErrorIStoreResults;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleDoGlobalPreLocalExecuteError(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> HandleDoGlobalPreLocalExecuteErrorIStoreResults;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleDoLocalSourceExecuteError(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> HandleDoLocalSourceExecuteErrorIStoreResults;
    /// <summary>
    /// Sets the stub of StoreOperation.HandleDoLocalTargetExecuteError(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> HandleDoLocalTargetExecuteErrorIStoreResults;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
    /// <summary>
    /// Sets the stub of StoreOperation.HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
    /// </summary>
    internal Action<IStoreResults> HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleUndoLocalSourceExecuteError(IStoreResults result)
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
    /// Sets the stub of UpdateShardOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> UndoGlobalPostLocalExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of StoreOperation.UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> UndoGlobalPreLocalExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of UpdateShardOperation.UndoLocalSourceExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> UndoLocalSourceExecuteIStoreTransactionScope;
    /// <summary>
    /// Sets the stub of StoreOperation.UndoLocalTargetExecute(IStoreTransactionScope ts)
    /// </summary>
    internal Func<IStoreTransactionScope, IStoreResults> UndoLocalTargetExecuteIStoreTransactionScope;
    private IStubBehavior ___instanceBehavior;

    /// <summary>
    /// Gets or sets a value that indicates if the base method should be called instead of the fallback behavior
    /// </summary>
    public bool CallBase { get; set; }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.get_ErrorCategory()
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
                : InstanceBehavior.Result<StubUpdateShardOperation, ShardManagementErrorCategory>(this, "get_ErrorCategory");
        }
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.get_ErrorSourceLocation()
    /// </summary>
    protected override ShardLocation ErrorSourceLocation
    {
        get
        {
            var func1 = ErrorSourceLocationGet;
            return func1 != null
                ? func1()
                : CallBase
                ? base.ErrorSourceLocation
                : InstanceBehavior.Result<StubUpdateShardOperation, ShardLocation>(this, "get_ErrorSourceLocation");
        }
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.get_ErrorTargetLocation()
    /// </summary>
    protected override ShardLocation ErrorTargetLocation
    {
        get
        {
            var func1 = ErrorTargetLocationGet;
            return func1 != null
                ? func1()
                : CallBase
                ? base.ErrorTargetLocation
                : InstanceBehavior.Result<StubUpdateShardOperation, ShardLocation>(this, "get_ErrorTargetLocation");
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
    /// Initializes a new instance
    /// </summary>
    public StubUpdateShardOperation(ShardMapManager shardMapManager, IStoreShardMap shardMap, IStoreShard shardOld, IStoreShard shardNew)
      : base(shardMapManager, shardMap, shardOld, shardNew) => InitializeStub();

    /// <summary>
    /// Sets the stub of StoreOperation.Dispose(Boolean disposing)
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        var action1 = DisposeBoolean;
        if (action1 != null)
            action1(disposing);
        else if (CallBase)
            base.Dispose(disposing);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "Dispose");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
    {
        var func1 = DoGlobalPostLocalExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.DoGlobalPostLocalExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "DoGlobalPostLocalExecute");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.DoGlobalPostLocalUpdateCache(IStoreResults result)
    /// </summary>
    public override void DoGlobalPostLocalUpdateCache(IStoreResults result)
    {
        var action1 = DoGlobalPostLocalUpdateCacheIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.DoGlobalPostLocalUpdateCache(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "DoGlobalPostLocalUpdateCache");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts)
    {
        var func1 = DoGlobalPreLocalExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.DoGlobalPreLocalExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "DoGlobalPreLocalExecute");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.DoLocalSourceExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
    {
        var func1 = DoLocalSourceExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.DoLocalSourceExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "DoLocalSourceExecute");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
    {
        var func1 = DoLocalTargetExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.DoLocalTargetExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "DoLocalTargetExecute");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.GetStoreConnectionInfo()
    /// </summary>
    public override StoreConnectionInfo GetStoreConnectionInfo()
    {
        var func1 = GetStoreConnectionInfo01;
        return func1 != null
            ? func1()
            : CallBase
            ? base.GetStoreConnectionInfo()
            : InstanceBehavior.Result<StubUpdateShardOperation, StoreConnectionInfo>(this, "GetStoreConnectionInfo");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleDoGlobalPostLocalExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleDoGlobalPostLocalExecuteError(IStoreResults result)
    {
        var action1 = HandleDoGlobalPostLocalExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleDoGlobalPostLocalExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleDoGlobalPostLocalExecuteError");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleDoGlobalPreLocalExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleDoGlobalPreLocalExecuteError(IStoreResults result)
    {
        var action1 = HandleDoGlobalPreLocalExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleDoGlobalPreLocalExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleDoGlobalPreLocalExecuteError");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleDoLocalSourceExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleDoLocalSourceExecuteError(IStoreResults result)
    {
        var action1 = HandleDoLocalSourceExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleDoLocalSourceExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleDoLocalSourceExecuteError");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.HandleDoLocalTargetExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleDoLocalTargetExecuteError(IStoreResults result)
    {
        var action1 = HandleDoLocalTargetExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleDoLocalTargetExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleDoLocalTargetExecuteError");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
    {
        var action1 = HandleUndoGlobalPostLocalExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleUndoGlobalPostLocalExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleUndoGlobalPostLocalExecuteError");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleUndoGlobalPreLocalExecuteError(IStoreResults result)
    {
        var action1 = HandleUndoGlobalPreLocalExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleUndoGlobalPreLocalExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleUndoGlobalPreLocalExecuteError");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.HandleUndoLocalSourceExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleUndoLocalSourceExecuteError(IStoreResults result)
    {
        var action1 = HandleUndoLocalSourceExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleUndoLocalSourceExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleUndoLocalSourceExecuteError");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.HandleUndoLocalTargetExecuteError(IStoreResults result)
    /// </summary>
    public override void HandleUndoLocalTargetExecuteError(IStoreResults result)
    {
        var action1 = HandleUndoLocalTargetExecuteErrorIStoreResults;
        if (action1 != null)
            action1(result);
        else if (CallBase)
            base.HandleUndoLocalTargetExecuteError(result);
        else
            InstanceBehavior.VoidResult<StubUpdateShardOperation>(this, "HandleUndoLocalTargetExecuteError");
    }

    /// <summary>
    /// Initializes a new instance of type StubUpdateShardOperation
    /// </summary>
    private void InitializeStub()
    {
    }

    /// <summary>
    /// Sets the stub of StoreOperation.OnStoreException(StoreException se, StoreOperationState state)
    /// </summary>
    public override ShardManagementException OnStoreException(StoreException se, StoreOperationState state)
    {
        var func1 = OnStoreExceptionStoreExceptionStoreOperationState;
        return func1 != null
            ? func1(se, state)
            : CallBase
            ? base.OnStoreException(se, state)
            : InstanceBehavior.Result<StubUpdateShardOperation, ShardManagementException>(this, "OnStoreException");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
    {
        var func1 = UndoGlobalPostLocalExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.UndoGlobalPostLocalExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "UndoGlobalPostLocalExecute");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults UndoGlobalPreLocalExecute(IStoreTransactionScope ts)
    {
        var func1 = UndoGlobalPreLocalExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.UndoGlobalPreLocalExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "UndoGlobalPreLocalExecute");
    }

    /// <summary>
    /// Sets the stub of UpdateShardOperation.UndoLocalSourceExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
    {
        var func1 = UndoLocalSourceExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.UndoLocalSourceExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "UndoLocalSourceExecute");
    }

    /// <summary>
    /// Sets the stub of StoreOperation.UndoLocalTargetExecute(IStoreTransactionScope ts)
    /// </summary>
    public override IStoreResults UndoLocalTargetExecute(IStoreTransactionScope ts)
    {
        var func1 = UndoLocalTargetExecuteIStoreTransactionScope;
        return func1 != null
            ? func1(ts)
            : CallBase
            ? base.UndoLocalTargetExecute(ts)
            : InstanceBehavior.Result<StubUpdateShardOperation, IStoreResults>(this, "UndoLocalTargetExecute");
    }
}
