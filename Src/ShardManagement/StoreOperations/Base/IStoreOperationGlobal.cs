// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a GSM only store operation.
    /// </summary>
    internal interface IStoreOperationGlobal : IDisposable
    {
        /// <summary>
        /// Whether this is a read-only operation.
        /// </summary>
        bool ReadOnly
        {
            get;
        }

        /// <summary>
        /// Performs the store operation.
        /// </summary>
        /// <returns>Results of the operation.</returns>
        IStoreResults Do();

        /// <summary>
        /// Asynchronously performs the store operation.
        /// </summary>
        /// <returns>Task encapsulating results of the operation.</returns>
        Task<IStoreResults> DoAsync();

        /// <summary>
        /// Execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        IStoreResults DoGlobalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Asynchronously execute the operation against GSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Task encapsulating results of the operation.
        /// </returns>
        Task<IStoreResults> DoGlobalExecuteAsync(IStoreTransactionScope ts);

        /// <summary>
        /// Invalidates the cache on unsuccessful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void DoGlobalUpdateCachePre(IStoreResults result);

        /// <summary>
        /// Handles errors from the GSM operation after the LSM operations.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleDoGlobalExecuteError(IStoreResults result);

        /// <summary>
        /// Refreshes the cache on successful commit of the GSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void DoGlobalUpdateCachePost(IStoreResults result);

        /// <summary>
        /// Returns the ShardManagementException to be thrown corresponding to a StoreException.
        /// </summary>
        /// <param name="se">Store exception that has been raised.</param>
        /// <returns>ShardManagementException to be thrown.</returns>
        ShardManagementException OnStoreException(StoreException se);
    }
}
