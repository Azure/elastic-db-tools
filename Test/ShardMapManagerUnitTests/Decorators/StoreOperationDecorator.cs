// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    internal class StoreOperationDecorator : IStoreOperation
    {
        protected readonly IStoreOperation inner;

        internal StoreOperationDecorator(IStoreOperation inner)
        {
            this.inner = inner;
        }

        public IStoreResults Do()
        {
            return this.inner.Do();
        }

        public void Undo()
        {
            this.inner.Undo();
        }

        public virtual StoreConnectionInfo GetStoreConnectionInfo()
        {
            return this.inner.GetStoreConnectionInfo();
        }

        public virtual IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts)
        {
            return this.inner.DoGlobalPreLocalExecute(ts);
        }

        public virtual void HandleDoGlobalPreLocalExecuteError(IStoreResults result)
        {
            this.inner.HandleDoGlobalPreLocalExecuteError(result);
        }

        public virtual IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts)
        {
            return this.inner.DoLocalSourceExecute(ts);
        }

        public virtual void HandleDoLocalSourceExecuteError(IStoreResults result)
        {
            this.inner.HandleDoLocalSourceExecuteError(result);
        }

        public virtual IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts)
        {
            return this.inner.DoLocalTargetExecute(ts);
        }

        public virtual void HandleDoLocalTargetExecuteError(IStoreResults result)
        {
            this.inner.HandleDoLocalTargetExecuteError(result);
        }

        public virtual IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            return this.inner.DoGlobalPostLocalExecute(ts);
        }

        public virtual void HandleDoGlobalPostLocalExecuteError(IStoreResults result)
        {
            this.inner.HandleDoGlobalPostLocalExecuteError(result);
        }

        public virtual void DoGlobalPostLocalUpdateCache(IStoreResults result)
        {
            this.inner.DoGlobalPostLocalUpdateCache(result);
        }

        public virtual IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts)
        {
            return this.inner.UndoLocalSourceExecute(ts);
        }

        public virtual void HandleUndoLocalSourceExecuteError(IStoreResults result)
        {
            this.inner.HandleUndoLocalSourceExecuteError(result);
        }

        public virtual IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts)
        {
            return this.inner.UndoGlobalPostLocalExecute(ts);
        }

        public virtual void HandleUndoGlobalPostLocalExecuteError(IStoreResults result)
        {
            this.inner.HandleUndoGlobalPostLocalExecuteError(result);
        }

        public virtual ShardManagementException OnStoreException(StoreException se, StoreOperationState state)
        {
            return this.inner.OnStoreException(se, state);
        }

        public virtual void Dispose()
        {
            this.inner.Dispose();
        }
    }
}
