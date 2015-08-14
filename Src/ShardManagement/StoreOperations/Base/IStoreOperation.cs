// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Distributed Store operation.
    /// </summary>
    internal interface IStoreOperation : IDisposable
    {
        /// <summary>
        /// Performs the store operation.
        /// </summary>
        /// <returns>Results of the operation.</returns>
        IStoreResults Do();

        /// <summary>
        /// Performs the undo store operation.
        /// </summary>
        void Undo();

        /// <summary>
        /// Requests the derived class to provide information regarding the connections
        /// needed for the operation.
        /// </summary>
        /// <returns>Information about shards involved in the operation.</returns>
        StoreConnectionInfo GetStoreConnectionInfo();

        /// <summary>
        /// Performs the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        IStoreResults DoGlobalPreLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the initial GSM operation prior to LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleDoGlobalPreLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        IStoreResults DoLocalSourceExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the the LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleDoLocalSourceExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the LSM operation on the target shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        IStoreResults DoLocalTargetExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleDoLocalTargetExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        IStoreResults DoGlobalPostLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleDoGlobalPostLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Refreshes the cache on successful commit of the final GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void DoGlobalPostLocalUpdateCache(IStoreResults result);

        /// <summary>
        /// Performs the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Result of the operation.</returns>
        IStoreResults UndoLocalSourceExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the undo of LSM operation on the source shard.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleUndoLocalSourceExecuteError(IStoreResults result);

        /// <summary>
        /// Performs the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>Pending operations on the target objects if any.</returns>
        IStoreResults UndoGlobalPostLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the undo of GSM operation after LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleUndoGlobalPostLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Returns the ShardManagementException to be thrown corresponding to a StoreException.
        /// </summary>
        /// <param name="se">Store Exception that has been raised.</param>
        /// <param name="state">SQL operation state.</param>
        /// <returns>ShardManagementException to be thrown.</returns>
        ShardManagementException OnStoreException(StoreException se, StoreOperationState state);
    }
}
