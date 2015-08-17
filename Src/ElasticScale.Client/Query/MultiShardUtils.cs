// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Various utilities used by other classes in this project

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    internal static class MultiShardUtils
    {
        /// <summary>
        /// Asynchronously opens the given connection.
        /// </summary>
        /// <param name="shardConnection">The connection to Open</param>
        /// <param name="cancellationToken">The cancellation token to be passed down</param>
        /// <returns>The task handling the Open. A completed task if the conn is already Open</returns>
        internal static Task OpenShardConnectionAsync(DbConnection shardConnection, CancellationToken cancellationToken)
        {
            if (shardConnection.State != ConnectionState.Open)
            {
                return shardConnection.OpenAsync(cancellationToken);
            }
            else
            {
                return Task.FromResult<object>(null);
            }
        }

        /// <summary>
        /// The retry policy to use when connecting to sql databases 
        /// </summary>
        /// <param name="retryPolicyPerShard">An instance of the <see cref="RetryPolicy"/> class</param>
        /// <param name="retryBehavior">Behavior to use for detecting transient faults.</param>
        /// <returns>An instance of the <see cref="RetryPolicy"/> class</returns>
        /// <remarks>Separate method from the one below because we 
        /// might allow for custom retry strategies in the near future</remarks>
        internal static TransientFaultHandling.RetryPolicy GetSqlConnectionRetryPolicy(RetryPolicy retryPolicyPerShard, RetryBehavior retryBehavior)
        {
            return new TransientFaultHandling.RetryPolicy(
                new MultiShardQueryTransientErrorDetectionStrategy(retryBehavior),
                retryPolicyPerShard.GetRetryStrategy());
        }

        /// <summary>
        /// The retry policy to use when executing commands against sql databases
        /// </summary>
        /// <param name="retryPolicyPerShard">An instance of the <see cref="RetryPolicy"/> class</param>
        /// <param name="retryBehavior">Behavior to use for detecting transient faults.</param>
        /// <returns>An instance of the <see cref="RetryPolicy"/> class</returns>
        internal static TransientFaultHandling.RetryPolicy GetSqlCommandRetryPolicy(RetryPolicy retryPolicyPerShard, RetryBehavior retryBehavior)
        {
            return new TransientFaultHandling.RetryPolicy(
                new MultiShardQueryTransientErrorDetectionStrategy(retryBehavior),
                retryPolicyPerShard.GetRetryStrategy());
        }

        /// <summary>
        /// Clones the given command object and associates with the given connection.
        /// </summary>
        /// <param name="cmd">Command object to clone.</param>
        /// <param name="conn">Connection associated with the cloned command.</param>
        /// <returns>Clone of <paramref name="cmd"/>.</returns>
        internal static DbCommand CloneDbCommand(DbCommand cmd, DbConnection conn)
        {
            DbCommand clone = (DbCommand)(cmd as ICloneable).Clone();
            clone.Connection = conn;

            return clone;
        }
    }

    /// <summary>
    /// Provides logging utilities
    /// </summary>
    internal static class Logger
    {
        /// <summary>
        /// For now just pretty print to console with date time
        /// DEVNOTE: Look into logging frameworks like NLog
        /// </summary>
        /// <param name="format"></param>
        /// <param name="data"></param>
        public static void Log(string format, params object[] data)
        {
            string combinedFormat = string.Format("[{{{0}}}]: {1}", data.Length, format);
            object[] combinedData = new object[data.Length + 1];
            Array.Copy(data, combinedData, data.Length);
            combinedData[data.Length] = DateTime.Now;
            Console.WriteLine(combinedFormat, combinedData);
        }
    }
}
