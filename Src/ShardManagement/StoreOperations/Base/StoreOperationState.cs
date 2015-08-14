// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// States of the operation.
    /// </summary>
    internal enum StoreOperationState
    {
        /// <summary>
        /// Initial state on Do.
        /// </summary>
        DoBegin = 0,

        /// <summary>
        /// Before connect GSM on Do.
        /// </summary>
        DoGlobalConnect,

        /// <summary>
        /// Before connect LSM source on Do.
        /// </summary>
        DoLocalSourceConnect,

        /// <summary>
        /// Before connect LSM target on Do.
        /// </summary>
        DoLocalTargetConnect,

        /// <summary>
        /// Before GSM operation pre LSM operations about to Start Transaction.
        /// </summary>
        DoGlobalPreLocalBeginTransaction,

        /// <summary>
        /// Before GSM operation pre LSM operations about to execute.
        /// </summary>
        DoGlobalPreLocalExecute,

        /// <summary>
        /// Before GSM operation pre LSM operations about to commit transaction.
        /// </summary>
        DoGlobalPreLocalCommitTransaction,

        /// <summary>
        /// Before LSM operation on Source shard about to start transaction.
        /// </summary>
        DoLocalSourceBeginTransaction,

        /// <summary>
        /// Before LSM operation on Source shard about to execute.
        /// </summary>
        DoLocalSourceExecute,

        /// <summary>
        /// Before LSM operation on Source shard about to commit transaction transaction.
        /// </summary>
        DoLocalSourceCommitTransaction,

        /// <summary>
        /// Before LSM operation on Target shard about to start transaction.
        /// </summary>
        DoLocalTargetBeginTransaction,

        /// <summary>
        /// Before LSM operation on Target shard about to execute.
        /// </summary>
        DoLocalTargetExecute,

        /// <summary>
        /// Before LSM operation on Target shard about to commit transaction transaction.
        /// </summary>
        DoLocalTargetCommitTransaction,

        /// <summary>
        /// Before GSM operation post LSM operations about to Start Transaction.
        /// </summary>
        DoGlobalPostLocalBeginTransaction,

        /// <summary>
        /// Before GSM operation post LSM operations about to execute.
        /// </summary>
        DoGlobalPostLocalExecute,

        /// <summary>
        /// Before GSM operation post LSM operations about to commit transaction.
        /// </summary>
        DoGlobalPostLocalCommitTransaction,

        /// <summary>
        /// Before disconnect on Do.
        /// </summary>
        DoEnd,

        /// <summary>
        /// Initial state on Undo.
        /// </summary>
        UndoBegin = 100,

        /// <summary>
        /// Before connect GSM on Undo.
        /// </summary>
        UndoGlobalConnect,

        /// <summary>
        /// Before connect LSM source on Undo.
        /// </summary>
        UndoLocalSourceConnect,

        /// <summary>
        /// Before connect LSM target on Undo.
        /// </summary>
        UndoLocalTargetConnect,

        /// <summary>
        /// Before GSM operation pre LSM operations about to Start Transaction.
        /// </summary>
        UndoGlobalPreLocalBeginTransaction,

        /// <summary>
        /// Before GSM operation pre LSM operations about to execute.
        /// </summary>
        UndoGlobalPreLocalExecute,

        /// <summary>
        /// Before GSM operation pre LSM operations about to commit transaction.
        /// </summary>
        UndoGlobalPreLocalCommitTransaction,

        /// <summary>
        /// Before LSM operation on Target shard about to start transaction.
        /// </summary>
        UndoLocalTargetBeginTransaction,

        /// <summary>
        /// Before LSM operation on Target shard about to execute.
        /// </summary>
        UndoLocalTargetExecute,

        /// <summary>
        /// Before LSM operation on Target shard about to commit transaction transaction.
        /// </summary>
        UndoLocalTargetCommitTransaction,

        /// <summary>
        /// Before LSM operation on Source shard about to start transaction.
        /// </summary>
        UndoLocalSourceBeginTransaction,

        /// <summary>
        /// Before LSM operation on Source shard about to execute.
        /// </summary>
        UndoLocalSourceExecute,

        /// <summary>
        /// Before LSM operation on Source shard about to commit transaction transaction.
        /// </summary>
        UndoLocalSourceCommitTransaction,

        /// <summary>
        /// Before GSM operation post LSM operations about to Start Transaction.
        /// </summary>
        UndoGlobalPostLocalBeginTransaction,

        /// <summary>
        /// Before GSM operation post LSM operations about to execute.
        /// </summary>
        UndoGlobalPostLocalExecute,

        /// <summary>
        /// Before GSM operation post LSM operations about to commit transaction.
        /// </summary>
        UndoGlobalPostLocalCommitTransaction,

        /// <summary>
        /// Before disconnect on Undo.
        /// </summary>
        UndoEnd
    }
}
