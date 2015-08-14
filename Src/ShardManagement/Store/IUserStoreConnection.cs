// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Instance of a user connection to store.
    /// </summary>
    internal interface IUserStoreConnection : IDisposable
    {
        /// <summary>
        /// Underlying SQL server connection.
        /// </summary>
        SqlConnection Connection
        {
            get;
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        void Open();

        /// <summary>
        /// Asynchronously opens the connection.
        /// </summary>
        /// <returns>Task to await completion of the Open</returns> 
        Task OpenAsync();
    }
}
