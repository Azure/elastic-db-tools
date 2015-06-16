//******************************************************************************
// Copyright (c) Microsoft Corporation
//
// @File: MultiShardUtils.cs
//
// @Owner: raveeram
// @Test:
//
// Purpose:
// Various utilities used by other classes in this project
//
//******************************************************************************

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
        /// Creates a task that will complete when all <paramref name="tasks"/>
        /// have completed.
        /// However, upon failure of even a single task, the rest of the tasks 
        /// that haven't been completed will be cancelled and we'll move on
        /// </summary>
        /// <param name="tasks"></param>
        /// <returns></returns>
        internal static Task WhenAllOrError(params Task[] tasks)
        {
            var errorCompletion = new TaskCompletionSource<object>();

            foreach (var task in tasks)
            {
                task.ContinueWith((t) =>
                {
                    if (t.IsFaulted)
                    {
                        errorCompletion.TrySetException(t.Exception.InnerExceptions);
                    }
                    else if (t.IsCanceled)
                    {
                        errorCompletion.TrySetCanceled();
                    }

                }, TaskContinuationOptions.ExecuteSynchronously);
            }

            return Task.WhenAny(errorCompletion.Task, Task.WhenAll(tasks)).Unwrap();
        }

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
