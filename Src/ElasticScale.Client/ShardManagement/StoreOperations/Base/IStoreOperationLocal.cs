// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents an LSM only store operation.
    /// </summary>
    internal interface IStoreOperationLocal : IDisposable
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
        /// Execute the operation against LSM in the current transaction scope.
        /// </summary>
        /// <param name="ts">Transaction scope.</param>
        /// <returns>
        /// Results of the operation.
        /// </returns>
        IStoreResults DoLocalExecute(IStoreTransactionScope ts);

        /// <summary>
        /// Handles errors from the LSM operation.
        /// </summary>
        /// <param name="result">Operation result.</param>
        void HandleDoLocalExecuteError(IStoreResults result);

        /// <summary>
        /// Returns the ShardManagementException to be thrown corresponding to a StoreException.
        /// </summary>
        /// <param name="se">Store exception that has been raised.</param>
        /// <returns>ShardManagementException to be thrown.</returns>
        ShardManagementException OnStoreException(StoreException se);
    }
}
