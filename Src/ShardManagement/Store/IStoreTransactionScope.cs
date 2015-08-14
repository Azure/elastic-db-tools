// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Type of transaction scope.
    /// </summary>
    internal enum StoreTransactionScopeKind
    {
        /// <summary>
        /// A non-transactional scope, uses auto-commit transaction mode.
        /// Useful for performing operations that are not allowed to be 
        /// executed within transactions such as Kill connections.
        /// </summary>
        NonTransactional,

        /// <summary>
        /// Read only transaction scope, uses read-committed transaction mode.
        /// Read locks are acquired purely during row read and then released.
        /// </summary>
        ReadOnly,

        /// <summary>
        /// Read write transaction scope, uses repeatable-read transaction mode.
        /// Read locks are held till Commit or Rollback.
        /// </summary>
        ReadWrite
    }


    /// <summary>
    /// Allows scoping of a transactional operation on the store.
    /// </summary>
    internal interface IStoreTransactionScope : IDisposable
    {
        /// <summary>
        /// Type of transaction scope.
        /// </summary>
        StoreTransactionScopeKind Kind
        {
            get;
        }

        /// <summary>
        /// When set to true, implies that the transaction is ready for commit.
        /// </summary>
        bool Success
        {
            get;
            set;
        }

        /// <summary>
        /// Executes the given operation using the <paramref name="operationData"/> values
        /// as input to the operation.
        /// </summary>
        /// <param name="operationName">Operation to execute.</param>
        /// <param name="operationData">Input data for operation.</param>
        /// <returns>Storage results object.</returns>
        IStoreResults ExecuteOperation(string operationName, XElement operationData);

        /// <summary>
        /// Asynchronously executes the given operation using the <paramref name="operationData"/> values
        /// as input to the operation.
        /// </summary>
        /// <param name="operationName">Operation to execute.</param>
        /// <param name="operationData">Input data for operation.</param>
        /// <returns>Task encapsulating storage results object.</returns>
        Task<IStoreResults> ExecuteOperationAsync(string operationName, XElement operationData);

        /// <summary>
        /// Executes the given command.
        /// </summary>
        /// <param name="command">Command to execute.</param>
        /// <returns>Storage results object.</returns>
        IStoreResults ExecuteCommandSingle(StringBuilder command);

        /// <summary>
        /// Executes the given set of commands.
        /// </summary>
        /// <param name="commands">Collection of commands to execute.</param>
        void ExecuteCommandBatch(IEnumerable<StringBuilder> commands);
    }
}
