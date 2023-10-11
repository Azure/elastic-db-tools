// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Store;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests.Decorators;

internal class StoreOperationDecorator : IStoreOperation
{
    protected readonly IStoreOperation inner;

    internal StoreOperationDecorator(IStoreOperation inner) => this.inner = inner;

    public IStoreResults Do() => inner.Do();

    public void Undo() => inner.Undo();

    public virtual StoreConnectionInfo GetStoreConnectionInfo() => inner.GetStoreConnectionInfo();

    public virtual IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) => inner.DoGlobalPreLocalExecute(ts);

    public virtual void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => inner.HandleDoGlobalPreLocalExecuteError(result);

    public virtual IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) => inner.DoLocalSourceExecute(ts);

    public virtual void HandleDoLocalSourceExecuteError(IStoreResults result) => inner.HandleDoLocalSourceExecuteError(result);

    public virtual IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts) => inner.DoLocalTargetExecute(ts);

    public virtual void HandleDoLocalTargetExecuteError(IStoreResults result) => inner.HandleDoLocalTargetExecuteError(result);

    public virtual IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) => inner.DoGlobalPostLocalExecute(ts);

    public virtual void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => inner.HandleDoGlobalPostLocalExecuteError(result);

    public virtual void DoGlobalPostLocalUpdateCache(IStoreResults result) => inner.DoGlobalPostLocalUpdateCache(result);

    public virtual IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) => inner.UndoLocalSourceExecute(ts);

    public virtual void HandleUndoLocalSourceExecuteError(IStoreResults result) => inner.HandleUndoLocalSourceExecuteError(result);

    public virtual IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => inner.UndoGlobalPostLocalExecute(ts);

    public virtual void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => inner.HandleUndoGlobalPostLocalExecuteError(result);

    public virtual ShardManagementException OnStoreException(StoreException se, StoreOperationState state) => inner.OnStoreException(se, state);

    public virtual void Dispose() => inner.Dispose();
    public IStoreResults Do() => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults Do() => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults Do() => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults Do() => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.Do() => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.Do() => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.Do() => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.Do() => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPreLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPreLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoLocalTargetExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoLocalTargetExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.DoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleDoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    public void DoGlobalPostLocalUpdateCache(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoLocalSourceExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoLocalSourceExecuteError(IStoreResults result) => throw new System.NotImplementedException();
    IStoreResults IStoreOperation.UndoGlobalPostLocalExecute(IStoreTransactionScope ts) => throw new System.NotImplementedException();
    public void HandleUndoGlobalPostLocalExecuteError(IStoreResults result) => throw new System.NotImplementedException();
}
