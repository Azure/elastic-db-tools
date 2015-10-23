// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Complements the MultiShardConnection and abstracts away
// the work of running a given dbcommand against mulitple shards
//
// Notes:
// * This class is NOT thread-safe.
// * Since the Sync API internally invokes the async API, connections to shards with 
// connection string property "context connection = true" are not supported.
// * Transaction semantics are not supported

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// <summary>
    /// Complements the <see cref="MultiShardConnection"/> with a command object 
    /// similar to the triad of <see cref="SqlConnection"/>, <see cref="SqlCommand"/>, and <see cref="SqlDataReader"/>. 
    /// The <see cref="MultiShardCommand"/> takes a T-SQL command statement as its input and executes the 
    /// command across its collection of shards specified by its corresponding <see cref="MultiShardConnection"/>.
    /// The results from processing the <see cref="MultiShardCommand"/> are made available through the 
    /// execute methods and the <see cref="MultiShardDataReader"/>.
    /// </summary>
    [System.ComponentModel.DesignerCategory("Code")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi")]
    public sealed class MultiShardCommand : DbCommand
    {
        #region Global Vars

        /// <summary>
        /// Default command timeout per shard in seconds
        /// </summary>
        private const int DefaultCommandTimeoutPerShard = 30;

        /// <summary>
        /// Default command timeout in seconds
        /// </summary>
        private const int DefaultCommandTimeout = 300;

        /// <summary>
        /// The ILogger
        /// </summary>
        private static readonly ILogger s_tracer = TraceHelper.Tracer;

        /// <summary>
        /// The sql command to be executed against shards
        /// </summary>
        private readonly DbCommand _dbCommand;

        /// <summary>
        /// Lock to enable thread-safe Cancel()
        /// </summary>
        private readonly object _cancellationLock = new Object();

        /// <summary>
        /// Global token source to enable cancellation of commands being executed 
        /// </summary>
        private CancellationTokenSource _innerCts = new CancellationTokenSource();

        /// <summary>
        /// Task associated with current command invocation 
        /// </summary>
        private Task _currentCommandTask = Task.FromResult<object>(null);

        /// <summary>
        /// ActivityId of the current command being executed
        /// </summary>
        private Guid _activityId;

        /// <summary>
        /// Whether this command has already been disposed.
        /// </summary>
        private bool _disposed = false;

        #endregion

        #region Ctors

        /// <summary>
        /// Creates an instance of this class
        /// </summary>
        /// <param name="connection">The connection to shards</param>
        /// <param name="command">The command to execute against the shards</param>
        /// <param name="commandTimeout">Command timeout<paramref name="command"/> 
        /// against ALL shards</param>
        private MultiShardCommand(MultiShardConnection connection, DbCommand command, int commandTimeout)
        {
            this.Connection = connection;
            this.CommandTimeout = commandTimeout;

            _dbCommand = command;

            // Set defaults
            this.RetryPolicy = RetryPolicy.DefaultRetryPolicy;
            this.RetryBehavior = RetryBehavior.DefaultRetryBehavior;
            this.ExecutionPolicy = MultiShardExecutionPolicy.CompleteResults;
            this.ExecutionOptions = MultiShardExecutionOptions.None;
        }

        #endregion

        #region Instance Factories

        // Suppression rationale:
        //   The SqlCommand underlies the object we will return.  We don't want to dispose it.
        //   The point of this c-tor is to allow the user to specify whatever sql text they wish.
        /// <summary>
        /// Instance constructor of this class
        /// Default command timeout of 300 seconds is used
        /// </summary>
        /// <param name="connection">The connection to shards</param>
        /// <param name="commandText">The command text to execute against shards</param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        internal static MultiShardCommand Create(MultiShardConnection connection, string commandText)
        {
            SqlCommand cmd = new SqlCommand(commandText);
            cmd.CommandTimeout = MultiShardCommand.DefaultCommandTimeoutPerShard; // Default sql command timeout of 30secs per shard
            cmd.CommandType = CommandType.Text;

            return MultiShardCommand.Create(
                connection,
                cmd,
                MultiShardCommand.DefaultCommandTimeout);
        }

        // Suppression rationale:
        //   The SqlCommand underlies the object we will return.  We don't want to dispose it.
        //   The point of this c-tor is to allow the user to specify whatever sql text they wish.
        /// <summary>
        /// Instance constructor of this class
        /// Default command type is text
        /// </summary>
        /// <param name="connection">The connection to shards</param>
        /// <param name="commandText"></param>
        /// <param name="commandTimeout">Command timeout for given commandText to be run 
        /// against ALL shards</param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        internal static MultiShardCommand Create(MultiShardConnection connection, string commandText, int commandTimeout)
        {
            SqlCommand cmd = new SqlCommand(commandText);
            cmd.CommandTimeout = MultiShardCommand.DefaultCommandTimeoutPerShard; // Default sql command timeout of 30secs per shard
            cmd.CommandType = CommandType.Text;

            return MultiShardCommand.Create(
                connection,
                cmd,
                commandTimeout);
        }

        /// <summary>
        /// Creates an instance of this class
        /// </summary>
        /// <param name="connection">The connection handle to all shards</param>
        /// <param name="command">A sql command from which the commandText, commandTimeout, commandType will be inferred. Should implement ICloneable</param>
        /// <param name="commandTimeout">Command timeout for given commandText to be run against ALL shards</param>
        /// <returns></returns>
        /// <remarks>DEVNOTE: Should we expose a DbCommand instead? Do we even want to expose this at all?</remarks>
        internal static MultiShardCommand Create(MultiShardConnection connection, DbCommand command, int commandTimeout)
        {
            Contract.Requires(command is ICloneable);

            return new MultiShardCommand(connection, command, commandTimeout);
        }

        #endregion

        #region Public Properties

        // Suppression rationale:  The point of this property is precisely to allow the user to specify whatever SQL they wish.
        /// <summary>
        /// Gets or sets the command text to execute against the set of shards.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public override string CommandText
        {
            get
            {
                return _dbCommand.CommandText;
            }
            set
            {
                _dbCommand.CommandText = value;
            }
        }

        /// <summary>
        /// Time in seconds to wait for the command to be executed on ALL shards.
        /// A value of 0 indicates no wait time limit. The default is 300 seconds.
        /// </summary>
        public override int CommandTimeout
        {
            get;
            set;
        }

        /// <summary>
        /// This property controls the timeout for running
        /// a command against individual shards.
        /// </summary>
        public int CommandTimeoutPerShard
        {
            get
            {
                return _dbCommand.CommandTimeout;
            }
            set
            {
                _dbCommand.CommandTimeout = value;
            }
        }

        /// <summary>
        /// The <see cref="CommandType"/>of the command to be executed.
        /// </summary>
        public override CommandType CommandType
        {
            get
            {
                return _dbCommand.CommandType;
            }
            set
            {
                _dbCommand.CommandType = value;
            }
        }

        /// <summary>
        /// The <see cref="SqlParameterCollection"/> associated with this command.
        /// </summary>
        public new SqlParameterCollection Parameters
        {
            get
            {
                return (SqlParameterCollection)_dbCommand.Parameters;
            }
        }

        /// <summary>
        /// The retry behavior for detecting transient faults that could occur when connecting to and
        /// executing commands against individual shards.
        /// </summary>
        /// <remarks>
        /// The <see cref="Microsoft.Azure.SqlDatabase.ElasticScale.RetryBehavior.DefaultRetryBehavior"/> is the default.
        /// </remarks>
        public RetryBehavior RetryBehavior
        {
            get;
            set;
        }

        /// <summary>
        /// The execution policy to use when executing
        /// commands against shards. Through this policy,
        /// users can control whether complete results are required,
        /// or whether partial results are acceptable.
        /// </summary>
        public MultiShardExecutionPolicy ExecutionPolicy
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the current instance of the <see cref="MultiShardConnection"/> associated with this command.
        /// </summary>
        public new MultiShardConnection Connection
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets or sets options that control how the command is executed.
        /// For instance, you can use this to include the shard name as 
        /// an additional column into the result.
        /// </summary>
        public MultiShardExecutionOptions ExecutionOptions
        {
            get;
            set;
        }

        /// <summary>
        /// The event handler invoked when execution has begun on a given shard.
        /// </summary>
        public event EventHandler<ShardExecutionEventArgs> ShardExecutionBegan;

        /// <summary>
        /// The event handler invoked when execution has successfully completed on a given shard or its 
        /// shard-specific <see cref="IDataReader"/> has been returned.
        /// </summary>
        public event EventHandler<ShardExecutionEventArgs> ShardExecutionSucceeded;

        /// <summary>
        /// The event handler invoked when execution on a given shard has faulted. This handler is only
        /// invoked on exceptions for which execution could not be retried further as a result of 
        /// the exception's non-transience or as a result of the chosen <see cref="RetryBehavior"/>.
        /// </summary>
        public event EventHandler<ShardExecutionEventArgs> ShardExecutionFaulted;

        /// <summary>
        /// The event handler invoked when execution on a given shard is canceled, either explicitly via
        /// the provided <see cref="CancellationToken"/> or implicitly as a result of the chosen
        /// <see cref="MultiShardExecutionPolicy"/>.
        /// </summary>
        public event EventHandler<ShardExecutionEventArgs> ShardExecutionCanceled;

        /// <summary>
        /// The event handler invoked when ExecuteDataReader on a certain shard has successfully returned
        /// a reader. This is an internal-only method, and differs from ShardExecutionSucceeded in that
        /// it is invoked BEFORE the reader is added to the MultiShardDataReader; this adding is rife
        /// with side effects that are difficult to isolate.
        /// </summary>
        internal event EventHandler<ShardExecutionEventArgs> ShardExecutionReaderReturned;

        #endregion Public Properties

        #region Internal Properties

        /// <summary>
        /// The retry policy to use when connecting to and
        /// executing commands against individual shards.
        /// </summary>
        internal RetryPolicy RetryPolicy
        {
            get;
            set;
        }

        #endregion Internal Properties

        #region Protected Properties

        /// <summary>
        /// Gets the SqlParameter Collection
        /// </summary>
        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return this.Parameters;
            }
        }

        #endregion Protected Properties

        #region APIs

        #region Supported DbCommand APIs

        #region ExecuteReader Methods

        #region Synchronous Methods

        /// <summary>
        /// The ExecuteReader methods of the MultiShardCommand execute the given command statement on each shard 
        /// and return the concatenation (i.e. UNION ALL) of the individual results from the shards in a
        /// <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can be controlled
        /// by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
        /// </summary>
        /// <returns> the <see cref="MultiShardDataReader"/> instance with the overall concatenated result set. </returns>
        /// <exception cref="System.InvalidOperationException">thrown if the commandText is null or empty</exception>
        /// <exception cref="System.TimeoutException">thrown if the CommandTimeout elapsed prior to completion</exception>
        public new MultiShardDataReader ExecuteReader()
        {
            return this.ExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        /// - Runs the given query against all shards and returns
        ///     a reader that encompasses results from them.
        /// - Uses the MultiShardExecutionPolicy.CompleteResults as the default execution policy
        /// - Includes the $ShardName pseudo column in the results
        /// <param name="behavior">Command behavior to use</param>
        /// <returns>MultiShardDataReader instance that encompasses results from all shards</returns>
        /// <exception cref="System.InvalidOperationException">If the commandText is null or empty</exception>
        /// <exception cref="System.InvalidOperationException">If the command behavior is not supported 
        /// (CloseConnection or SingleResult or SingleRow)</exception>
        /// <exception cref="System.TimeoutException">If the CommandTimeout elapsed prior to completion</exception>
        /// </summary>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return this.ExecuteReader(behavior);
        }

        /// <summary>
        /// The ExecuteReader methods of the MultiShardCommand execute the given command statement on each shard 
        /// and return the concatenation (i.e. UNION ALL) of the individual results from the shards in a
        /// <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can be controlled
        /// by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
        /// </summary>
        /// <returns> the <see cref="MultiShardDataReader"/> instance with the overall concatenated result set. </returns>
        /// <exception cref="System.InvalidOperationException">thrown if the commandText is null or empty, or if the 
        /// specified command behavior is not supported such as CloseConnection or SingleRow.</exception>
        /// <exception cref="System.TimeoutException">thrown if the CommandTimeout elapsed prior to completion.</exception>
        /// <param name="behavior"> specifies the <see cref="CommandBehavior"/> to use.</param>
        public new MultiShardDataReader ExecuteReader(CommandBehavior behavior)
        {
            return this.ExecuteReader(
                behavior,
                MultiShardUtils.GetSqlCommandRetryPolicy(this.RetryPolicy, this.RetryBehavior),
                MultiShardUtils.GetSqlConnectionRetryPolicy(this.RetryPolicy, this.RetryBehavior),
                this.ExecutionPolicy);
        }

        /// <summary>
        /// - Runs the given query against all shards and returns
        ///     a reader that encompasses results from them.
        /// 
        /// Design Principles
        /// - Commands are executed in a parallel, non-blocking manner. 
        /// - Only the calling thread is blocked until the command is complete against all shards.
        /// </summary>
        /// <param name="behavior">Command behavior to use</param>
        /// <param name="commandRetryPolicy">The retry policy to use when executing commands against the shards</param>
        /// <param name="connectionRetryPolicy">The retry policy to use when connecting to shards</param>
        /// <param name="executionPolicy">The execution policy to use</param>
        /// <returns>MultiShardDataReader instance that encompasses results from all shards</returns>
        /// <exception cref="System.InvalidOperationException">If the commandText is null or empty</exception>
        /// <exception cref="System.TimeoutException">If the CommandTimeout elapsed prior to completion</exception>
        /// <exception cref="MultiShardAggregateException">If one or more errors occured while executing the command</exception>
        internal MultiShardDataReader ExecuteReader(
            CommandBehavior behavior,
            TransientFaultHandling.RetryPolicy commandRetryPolicy,
            TransientFaultHandling.RetryPolicy connectionRetryPolicy,
            MultiShardExecutionPolicy executionPolicy)
        {
            Contract.Requires(commandRetryPolicy != null && connectionRetryPolicy != null);

            try
            {
                return this.ExecuteReaderAsync(
                    behavior,
                    CancellationToken.None,
                    commandRetryPolicy,
                    connectionRetryPolicy,
                    executionPolicy).Result;
            }
            catch (Exception ex)
            {
                AggregateException aex = ex as AggregateException;

                if (null != aex)
                {
                    throw aex.Flatten().InnerException;
                }

                throw;
            }
        }

        #endregion

        #region Async Methods

        /// <summary>
        /// The ExecuteReader methods of the MultiShardCommand execute the given command statement on each shard 
        /// and return the concatenation (i.e. UNION ALL) of the individual results from the shards in a
        /// <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can be controlled
        /// by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
        /// </summary>
        /// <returns> a task warapping the <see cref="MultiShardDataReader"/> instance with the overall concatenated result set. </returns>
        /// <exception cref="System.InvalidOperationException">thrown if the commandText is null or empty, or if the 
        /// specified command behavior is not supported such as CloseConnection or SingleRow.</exception>
        /// <exception cref="System.TimeoutException">thrown if the CommandTimeout elapsed prior to completion.</exception>
        /// <remarks>Any exceptions during command execution are conveyed via the returned Task.</remarks>
        public new Task<MultiShardDataReader> ExecuteReaderAsync()
        {
            return this.ExecuteReaderAsync(CancellationToken.None);
        }

        /// <summary>
        /// The ExecuteReader methods of the MultiShardCommand execute the given command statement on each shard 
        /// and return the concatenation (i.e. UNION ALL) of the individual results from the shards in a
        /// <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can be controlled
        /// by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
        /// </summary>
        /// <returns> a task warapping the <see cref="MultiShardDataReader"/> instance with the overall concatenated result set. </returns>
        /// <exception cref="System.InvalidOperationException">thrown if the commandText is null or empty, or if the 
        /// specified command behavior is not supported such as CloseConnection or SingleRow.</exception>
        /// <exception cref="System.TimeoutException">thrown if the CommandTimeout elapsed prior to completion.</exception>
        /// <param name="cancellationToken">Cancellation token to cancel the command execution</param>
        /// <remarks>Any exceptions during command execution are conveyed via the returned Task.</remarks>
        public new Task<MultiShardDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
        {
            return this.ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);
        }

        /// <summary>
        /// - Executes the given query against all shards asynchronously
        /// - Uses the MultiShardExecutionPolicy.CompleteResults as the default execution policy
        /// - Includes the $ShardName pseudo column in the results
        /// </summary>
        /// <param name="behavior">Command behavior to use</param>
        /// <param name="cancellationToken">Cancellation token to cancel the command execution</param>
        /// <returns>A task with a TResult that encompasses results from all shards</returns>
        /// <remarks>Any exceptions during command execution are conveyed via the returned Task</remarks>
        /// <exception cref="System.InvalidOperationException">If the commandText is null or empty</exception>
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return this.ExecuteReaderAsync(behavior, cancellationToken)
                .ContinueWith<DbDataReader>(
                    (t) =>
                    {
                        if (t.IsFaulted)
                        {
                            throw t.Exception.InnerException;
                        }

                        return t.Result;
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.NotOnCanceled,
                    TaskScheduler.Default);
        }

        /// <summary>
        /// The ExecuteReader methods of the MultiShardCommand execute the given command statement on each shard 
        /// and return the concatenation (i.e. UNION ALL) of the individual results from the shards in a
        /// <see cref="MultiShardDataReader"/>. The execution policy regarding result completeness can be controlled
        /// by setting the <see cref="MultiShardExecutionPolicy"/>. The default execution policy is to return complete results.
        /// </summary>
        /// <returns> a task warapping the <see cref="MultiShardDataReader"/> instance with the overall concatenated result set. </returns>
        /// <exception cref="System.InvalidOperationException">thrown if the commandText is null or empty, or if the 
        /// specified command behavior is not supported such as CloseConnection or SingleRow.</exception>
        /// <exception cref="System.TimeoutException">thrown if the CommandTimeout elapsed prior to completion.</exception>
        /// <param name="behavior">Command behavior to use</param>
        /// <param name="cancellationToken">Cancellation token to cancel the command execution</param>
        /// <remarks>Any exceptions during command execution are conveyed via the returned Task.</remarks>
        public new Task<MultiShardDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return this.ExecuteReaderAsync(
                behavior,
                cancellationToken,
                MultiShardUtils.GetSqlCommandRetryPolicy(this.RetryPolicy, this.RetryBehavior),
                MultiShardUtils.GetSqlConnectionRetryPolicy(this.RetryPolicy, this.RetryBehavior),
                this.ExecutionPolicy);
        }

        // Suppression rationale:
        //   We want to return exceptions via the task so that they can be dealt with on the main thread.  Gotta catch 'em all.
        //   We are returning the shardedReader variable via the task.  We don't want to dispose it.
        //   This method is part of the defined API.  We can't move it to a different class.
        //
        /// <summary>
        /// Executes the given query against all shards asynchronously
        /// </summary>
        /// <param name="behavior">Command behavior to use</param>
        /// <param name="outerCancellationToken">Cancellation token to cancel the command execution</param>
        /// <param name="commandRetryPolicy">The retry policy to use when executing commands against the shards</param>
        /// <param name="connectionRetryPolicy">The retry policy to use when connecting to shards</param>
        /// <param name="executionPolicy">The execution policy to use</param>
        /// <returns>A task with a TResult that encompasses results from all shards</returns>
        /// <remarks>Any exceptions during command execution are conveyed via the returned Task</remarks>
        /// <exception cref="System.InvalidOperationException">If the commandText is null or empty</exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal Task<MultiShardDataReader> ExecuteReaderAsync(
            CommandBehavior behavior,
            CancellationToken outerCancellationToken,
            TransientFaultHandling.RetryPolicy commandRetryPolicy,
            TransientFaultHandling.RetryPolicy connectionRetryPolicy,
            MultiShardExecutionPolicy executionPolicy)
        {
            TaskCompletionSource<MultiShardDataReader> currentCompletion = new TaskCompletionSource<MultiShardDataReader>();

            // Check if cancellation has already been requested by the user
            if (outerCancellationToken.IsCancellationRequested)
            {
                currentCompletion.SetCanceled();
                return currentCompletion.Task;
            }

            try
            {
                this.ValidateCommand(behavior);

                // Create a list of sql commands to run against each of the shards
                List<Tuple<ShardLocation, DbCommand>> shardCommands = this.GetShardDbCommands();

                // Don't allow a new invocation if a Cancel() is already in progress
                lock (_cancellationLock)
                {
                    // Set the activity id
                    _activityId = Guid.NewGuid();
                    using (var activityIdScope = new ActivityIdScope(_activityId))
                    {
                        Stopwatch stopwatch = Stopwatch.StartNew();

                        // Setup the Cancellation manager
                        CommandCancellationManager cmdCancellationMgr = new CommandCancellationManager(
                            _innerCts.Token,
                            outerCancellationToken,
                            executionPolicy,
                            this.CommandTimeout);

                        s_tracer.TraceInfo(
                            "MultiShardCommand.ExecuteReaderAsync",
                            "Start; Command Timeout: {0}; Command Text: {1}; Execution Policy: {2}",
                            this.CommandTimeout,
                            this.CommandText,
                            this.ExecutionPolicy);

                        FanOutTask fanOutTask = this.ExecuteReaderAsyncInternal(
                            behavior,
                            shardCommands,
                            cmdCancellationMgr,
                            commandRetryPolicy,
                            connectionRetryPolicy,
                            executionPolicy);

                        Task<MultiShardDataReader> commandTask = fanOutTask
                            .OuterTask
                            .ContinueWith<Task<MultiShardDataReader>>(
                            (t) =>
                            {
                                stopwatch.Stop();

                                string completionTrace = string.Format("Complete; Execution Time: {0}", stopwatch.Elapsed);

                                switch (t.Status)
                                {
                                    case TaskStatus.Faulted:
                                        // Close any active readers.
                                        if (this.ExecutionPolicy == MultiShardExecutionPolicy.CompleteResults)
                                        {
                                            MultiShardCommand.TerminateActiveCommands(fanOutTask.InnerTasks);
                                        }

                                        this.HandleCommandExecutionException(
                                            currentCompletion,
                                            new MultiShardAggregateException(t.Exception.InnerExceptions),
                                            completionTrace);
                                        break;

                                    case TaskStatus.Canceled:
                                        // Close any active readers.
                                        if (this.ExecutionPolicy == MultiShardExecutionPolicy.CompleteResults)
                                        {
                                            MultiShardCommand.TerminateActiveCommands(fanOutTask.InnerTasks);
                                        }

                                        this.HandleCommandExecutionCanceled(
                                            currentCompletion,
                                            cmdCancellationMgr,
                                            completionTrace);
                                        break;

                                    case TaskStatus.RanToCompletion:
                                        try
                                        {
                                            s_tracer.TraceInfo("MultiShardCommand.ExecuteReaderAsync", completionTrace);

                                            // If all child readers have exceptions, then aggregate the exceptions into this parent task.
                                            IEnumerable<MultiShardException> childExceptions = t.Result.Select(r => r.Exception);

                                            if (childExceptions.All(e => e != null))
                                            {
                                                // All child readers have exceptions

                                                // This should only happen on PartialResults, because if we were in 
                                                // CompleteResults then any failed child reader should have caused
                                                // the task to be in TaskStatus.Faulted
                                                Debug.Assert(this.ExecutionPolicy == MultiShardExecutionPolicy.PartialResults);

                                                this.HandleCommandExecutionException(
                                                    currentCompletion,
                                                    new MultiShardAggregateException(childExceptions),
                                                    completionTrace);
                                            }
                                            else
                                            {
                                                // At least one child reader has succeeded
                                                bool includeShardNameColumn = (this.ExecutionOptions & MultiShardExecutionOptions.IncludeShardNameColumn) != 0;

                                                // Hand-off the responsibility of cleanup to the MultiShardDataReader.
                                                MultiShardDataReader shardedReader = new MultiShardDataReader(
                                                    this,
                                                    t.Result,
                                                    executionPolicy,
                                                    includeShardNameColumn);

                                                currentCompletion.SetResult(shardedReader);
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            HandleCommandExecutionException(currentCompletion, new MultiShardAggregateException(ex));
                                        }
                                        break;

                                    default:
                                        currentCompletion.SetException(new InvalidOperationException("Unexpected task status."));
                                        break;
                                }

                                return currentCompletion.Task;
                            },
                            TaskContinuationOptions.ExecuteSynchronously)
                        .Unwrap();

                        _currentCommandTask = commandTask;

                        return commandTask;
                    }
                }
            }
            catch (Exception ex)
            {
                currentCompletion.SetException(ex);
                return currentCompletion.Task;
            }
        }

        #endregion

        /// <summary>
        /// Terminates any active commands/readers for scenarios where we fail the request due to
        /// strict execution policy or cancellation.
        /// </summary>
        /// <param name="readerTasks">Collection of reader tasks associated with execution across all shards.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "We do not want to throw Close or Cancel errors.")]
        private static void TerminateActiveCommands(Task<LabeledDbDataReader>[] readerTasks)
        {
            for (int i = 0; i < readerTasks.Length; i++)
            {
                if (readerTasks[i].Status == TaskStatus.RanToCompletion)
                {
                    Debug.Assert(readerTasks[i].Result != null, "Must have a LabeledDbDataReader if task finished.");
                    LabeledDbDataReader labeledReader = readerTasks[i].Result;

                    // This is a candidate for closing since we are in a faulted state.
                    Debug.Assert(labeledReader.DbDataReader != null, "Expecting reader for completed task.");

                    try
                    {
                        using (labeledReader.Command)
                        using (labeledReader.DbDataReader)
                        {
                            // Invoke cancellation before closing the reader. This is safe from deadlocks that
                            // arise potentially due to parallel Cancel and Close calls because this is the only
                            // thread that will be responsible for cleanup.
                            labeledReader.Command.Cancel();
                            labeledReader.DbDataReader.Close();
                        }
                    }
                    catch (Exception)
                    {
                        // Catch everything for Cancel/Close.
                    }
                }
            }
        }

        private FanOutTask ExecuteReaderAsyncInternal(
            CommandBehavior behavior,
            List<Tuple<ShardLocation, DbCommand>> commands,
            CommandCancellationManager cancellationToken,
            TransientFaultHandling.RetryPolicy commandRetryPolicy,
            TransientFaultHandling.RetryPolicy connectionRetryPolicy,
            MultiShardExecutionPolicy executionPolicy)
        {
            Task<LabeledDbDataReader>[] shardCommandTasks = new Task<LabeledDbDataReader>[commands.Count];

            for (int i = 0; i < shardCommandTasks.Length; i++)
            {
                Tuple<ShardLocation, DbCommand> shardCommand = commands[i];

                shardCommandTasks[i] = this.GetLabeledDbDataReaderTask(
                    behavior,
                    shardCommand,
                    cancellationToken,
                    commandRetryPolicy,
                    connectionRetryPolicy,
                    executionPolicy);
            }

            return new FanOutTask
            {
                OuterTask = Task.WhenAll<LabeledDbDataReader>(shardCommandTasks),
                InnerTasks = shardCommandTasks
            };
        }

        // Suppression rationale: We are returning the LabeledDataReader via the task.  We don't want to dispose it.
        //
        /// <summary>
        /// Helper that generates a Task to return a LabaledDbDataReader rather than just a plain DbDataReader
        /// so that we can affiliate the shard label with the Task returned from a call to DbCommand.ExecuteReaderAsync.
        /// </summary>
        /// <param name="behavior">Command behavior to use</param>
        /// <param name="commandTuple">A tuple of the Shard and the command to be executed</param>
        /// <param name="cmdCancellationMgr">Manages the cancellation tokens</param>
        /// <param name="commandRetryPolicy">The retry policy to use when executing commands against the shards</param>
        /// <param name="connectionRetryPolicy">The retry policy to use when connecting to shards</param>
        /// <param name="executionPolicy">The execution policy to use</param>
        /// <returns>A Task that will return a LabaledDbDataReader.</returns>
        /// <remarks>
        /// We should be able to tap into this code to trap and gracefully deal with command execution errors as well.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private Task<LabeledDbDataReader> GetLabeledDbDataReaderTask(
            CommandBehavior behavior,
            Tuple<ShardLocation, DbCommand> commandTuple,
            CommandCancellationManager cmdCancellationMgr,
            TransientFaultHandling.RetryPolicy commandRetryPolicy,
            TransientFaultHandling.RetryPolicy connectionRetryPolicy,
            MultiShardExecutionPolicy executionPolicy)
        {
            TaskCompletionSource<LabeledDbDataReader> currentCompletion = new TaskCompletionSource<LabeledDbDataReader>();

            ShardLocation shard = commandTuple.Item1;
            DbCommand command = commandTuple.Item2;
            Stopwatch stopwatch = Stopwatch.StartNew();

            // Always the close connection once the reader is done
            //
            // Commented out because of VSTS BUG# 3936154: When this command behavior is enabled,
            // SqlClient seems to be running into a deadlock when we invoke a cancellation on
            // SqlCommand.ExecuteReaderAsync(cancellationToken) with a SqlCommand.CommandText that would
            // lead to an error (Ex. "select * from non_existant_table").
            // As a workaround, we now explicitly close the connection associated with each shard's SqlDataReader 
            // once we are done reading through it in MultiShardDataReader.
            // Please refer to the bug to find a sample app with a repro, dump and symbols.
            // 
            // behavior |= CommandBehavior.CloseConnection;

            s_tracer.TraceInfo(
                "MultiShardCommand.GetLabeledDbDataReaderTask",
                "Starting command execution for Shard: {0}; Behavior: {1}; Retry Policy: {2}",
                shard,
                behavior,
                this.RetryPolicy);

            Task<Tuple<DbDataReader, DbCommand>> commandExecutionTask = commandRetryPolicy.ExecuteAsync<Tuple<DbDataReader, DbCommand>>(
            () =>
            {
                // Execute command in the Threadpool
                return Task.Run(
                async () =>
                {
                    // In certain cases sqlcommand doesn't reset its internal state correctly upon 
                    // failure of an async command (especially if the connection is still open). 
                    // This leads to unsuccessful retries on our part (see bug#2711396). 
                    // The recommendation from the Sqlclient team is to either start off with a new sqlcommand instance 
                    // on every retry or close and reopen the connection. 
                    // We're going with the former approach here. 
                    DbCommand commandToExecute = MultiShardUtils.CloneDbCommand(command, command.Connection);

                    // Open the connection if it isn't already
                    await this.OpenConnectionWithRetryAsync(
                        commandToExecute,
                        cmdCancellationMgr.Token,
                        connectionRetryPolicy)
                        .ConfigureAwait(false);

                    // The connection to the shard has been successfully opened and the per-shard command is about to execute.
                    // Raise the ShardExecutionBegan event.
                    this.OnShardExecutionBegan(shard);

                    DbDataReader perShardReader = await commandToExecute.ExecuteReaderAsync(
                        behavior,
                        cmdCancellationMgr.Token)
                        .ConfigureAwait(false);

                    return new Tuple<DbDataReader, DbCommand>(perShardReader, commandToExecute);
                });
            },
            cmdCancellationMgr.Token);

            return commandExecutionTask.ContinueWith<Task<LabeledDbDataReader>>(
            (t) =>
            {
                stopwatch.Stop();

                string traceMsg = string.Format(
                    "Completed command execution for Shard: {0}; Execution Time: {1}; Task Status: {2}",
                    shard,
                    stopwatch.Elapsed,
                    t.Status);

                switch (t.Status)
                {
                    case TaskStatus.Faulted:
                        MultiShardException exception = new MultiShardException(shard, t.Exception.InnerException);

                        // Close the connection
                        command.Connection.Close();

                        // Workaround: SqlCommand sets the task status to Faulted if the token was
                        // canceled while ExecuteReaderAsync was in progress. Interpret it and raise a canceled event instead.
                        if (cmdCancellationMgr.Token.IsCancellationRequested)
                        {
                            s_tracer.TraceError(
                                "MultiShardCommand.GetLabeledDbDataReaderTask",
                                exception,
                                "Command was canceled. {0}",
                                traceMsg);

                            currentCompletion.SetCanceled();

                            // Raise the ShardExecutionCanceled event.
                            this.OnShardExecutionCanceled(shard);
                        }
                        else
                        {
                            s_tracer.TraceError(
                                "MultiShardCommand.GetLabeledDbDataReaderTask",
                                exception,
                                "Command failed. {0}",
                                traceMsg);

                            if (executionPolicy == MultiShardExecutionPolicy.CompleteResults)
                            {
                                currentCompletion.SetException(exception);

                                // Cancel any other tasks in-progress
                                cmdCancellationMgr.CompleteResultsCts.Cancel();
                            }
                            else
                            {
                                LabeledDbDataReader failedLabeledReader = new LabeledDbDataReader(exception, shard, command);

                                currentCompletion.SetResult(failedLabeledReader);
                            }

                            // Raise the ShardExecutionFaulted event.
                            this.OnShardExecutionFaulted(shard, t.Exception.InnerException);
                        }

                        break;

                    case TaskStatus.Canceled:
                        s_tracer.TraceWarning(
                            "MultiShardCommand.GetLabeledDbDataReaderTask",
                            "Command was canceled. {0}",
                            traceMsg);

                        command.Connection.Close();

                        currentCompletion.SetCanceled();

                        // Raise the ShardExecutionCanceled event.
                        this.OnShardExecutionCanceled(shard);
                        break;

                    case TaskStatus.RanToCompletion:
                        s_tracer.TraceInfo("MultiShardCommand.GetLabeledDbDataReaderTask", traceMsg);

                        LabeledDbDataReader labeledReader = new LabeledDbDataReader(t.Result.Item1, shard, t.Result.Item2);

                        // Raise the ShardExecutionReaderReturned event.
                        this.OnShardExecutionReaderReturned(shard, labeledReader);

                        currentCompletion.SetResult(labeledReader);

                        // Raise the ShardExecutionSucceeded event.
                        this.OnShardExecutionSucceeded(shard, labeledReader);
                        break;

                    default:
                        currentCompletion.SetException(new InvalidOperationException("Unexpected task status.."));
                        break;
                }

                return currentCompletion.Task;
            },
            TaskContinuationOptions.ExecuteSynchronously)
            .Unwrap();
        }

        private async Task OpenConnectionWithRetryAsync(
            DbCommand shardCommand,
            CancellationToken cancellationToken,
            TransientFaultHandling.RetryPolicy connectionRetryPolicy)
        {
            var shardConnection = shardCommand.Connection;

            await connectionRetryPolicy.ExecuteAsync(
            () => MultiShardUtils.OpenShardConnectionAsync(
                shardConnection,
                cancellationToken),
            cancellationToken)
            .ConfigureAwait(false);
        }

        #endregion ExecuteReader Methods

        // Suppression rationale: We don't want cancel throwing any exceptions.  Just cancel.
        //
        /// <summary>
        /// Attempts to cancel an in progress <see cref="MultiShardCommand"/> 
        /// and any ongoing work that is performed at the shards on behalf of the command. 
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public override void Cancel()
        {
            lock (_cancellationLock)
            {
                try
                {
                    Task currentTask = _currentCommandTask;

                    if (currentTask != null)
                    {
                        if (IsExecutionInProgress())
                        {
                            // Call could've been made from a worker thread
                            using (ActivityIdScope activityIdScope = new ActivityIdScope(_activityId))
                            {
                                s_tracer.TraceWarning(
                                    "MultiShardCommand.Cancel",
                                    "Command was canceled; Current task status: {0}",
                                    currentTask.Status);

                                _innerCts.Cancel();

                                currentTask.Wait();
                            }
                        }

                        Debug.Assert(currentTask.IsCompleted, "Current task should be complete.");

                        // For tasks that failed or were cancelled we assume that they are already cleaned up.
                        if (currentTask.Status == TaskStatus.RanToCompletion)
                        {
                            Task<MultiShardDataReader> executeReaderTask = currentTask as Task<MultiShardDataReader>;

                            if (currentTask != null)
                            {
                                // Cancel all the active readers on MultiShardDataReader.
                                executeReaderTask.Result.Cancel();
                            }
                        }
                    }
                }
                catch // Cancel doesn't throw any exceptions
                {
                }
                finally
                {
                    if (_innerCts.IsCancellationRequested)
                    {
                        _innerCts = new CancellationTokenSource();
                    }
                }
            }
        }

        /// <summary>
        /// Creates a new instance of a <see cref="SqlParameter"/> object.
        /// </summary>
        /// <returns></returns>
        public static new SqlParameter CreateParameter()
        {
            return new SqlParameter();
        }

        /// <summary>
        /// Creates an instance of the DbParameter
        /// </summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter()
        {
            return CreateParameter();
        }

        // Suppression rationale: We purposely want to ignore exceptions when disposing.
        //
        /// <summary>
        /// Dispose off any unmanaged/managed resources held
        /// </summary>
        /// <param name="disposing"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    try
                    {
                        // Cancel any commands that are in progress
                        this.Cancel();

                        // Close any open connections
                        this.Connection.Close();
                    }
                    catch (Exception) // Ignore any exceptions
                    {
                    }

                    // Dispose the cancellation token source
                    using (_innerCts)
                    {
                    }

                    _disposed = true;

                    s_tracer.TraceWarning("MultiShardCommand.Dispose", "Command disposed");
                }

                base.Dispose(disposing);
            }
        }

        #endregion Supported DbCommand APIs

        /// <summary>
        /// Resets the <see cref="CommandTimeout"/> property
        /// to its default value
        /// </summary>
        public void ResetCommandTimeout()
        {
            this.CommandTimeout = MultiShardCommand.DefaultCommandTimeout;
        }

        /// <summary>
        /// Resets the <see cref="CommandTimeoutPerShard"/> property
        /// to its default value
        /// </summary>
        public void ResetCommandTimeoutPerShard()
        {
            this.CommandTimeoutPerShard = MultiShardCommand.DefaultCommandTimeoutPerShard;
        }

        #endregion APIs

        #region Helpers

        private void ValidateCommand(CommandBehavior behavior)
        {
            // Enforce only one async invocation at a time
            if (IsExecutionInProgress())
            {
                var ex = new InvalidOperationException("The command execution cannot proceed due to a pending asynchronous operation already in progress.");

                s_tracer.TraceError(
                    "MultiShardCommand.ValidateCommand",
                    ex,
                    "Current Task Status: {0}",
                    _currentCommandTask.Status);

                throw ex;
            }

            // Make sure command text is valid
            if (string.IsNullOrWhiteSpace(CommandText))
            {
                throw new InvalidOperationException("CommandText cannot be null");
            }

            // Validate the command behavior
            ValidateCommandBehavior(behavior);

            // Validate the parameters
            ValidateParameters();
        }

        private static void ValidateCommandBehavior(CommandBehavior cmdBehavior)
        {
            if (((cmdBehavior & CommandBehavior.CloseConnection) != 0) ||
                ((cmdBehavior & CommandBehavior.SingleResult) != 0) ||
                ((cmdBehavior & CommandBehavior.SingleRow) != 0))
            {
                throw new NotSupportedException(string.Format("CommandBehavior {0} is not supported", cmdBehavior));
            }
        }

        private void ValidateParameters()
        {
            int parameterCount = Parameters.Count;
            for (int i = 0; i < parameterCount; i++)
            {
                SqlParameter parameter = Parameters[i];

                // We only allow input parameters
                if (parameter.Direction != ParameterDirection.Input)
                {
                    throw new NotSupportedException(string.Format("Only ParameterDirection.Input is currently supported. Parameter Name: {0} Direction: {1}",
                        parameter.ParameterName, parameter.Direction));
                }

                // We don't allow streaming to sqlserver
                if (parameter.Value != null &&
                   (parameter.Value is Stream ||
                    parameter.Value is TextReader ||
                    parameter.Value is XmlReader ||
                    parameter.Value is DbDataReader))
                {
                    throw new NotSupportedException(string.Format("Streaming to sql server is currently not supported. Parameter Name: {0} Value type: {1}",
                        parameter.ParameterName, parameter.Value.GetType()));
                }
            }
        }

        /// <summary>
        /// Whether execution is already in progress
        /// against this command instance
        /// </summary>
        /// <returns>True if execution is in progress</returns>
        private bool IsExecutionInProgress()
        {
            Task currentTask = _currentCommandTask;

            return currentTask != null && !currentTask.IsCompleted;
        }

        /// <summary>
        /// Creates a list of commands to be executed against the shards associated with the connection.
        /// </summary>
        /// <returns>Pairs of shard locations and associated commands.</returns>
        private List<Tuple<ShardLocation, DbCommand>> GetShardDbCommands()
        {
            return this.Connection
                       .ShardConnections
                       .Select(sc => new Tuple<ShardLocation, DbCommand>(sc.Item1, MultiShardUtils.CloneDbCommand(_dbCommand, sc.Item2)))
                       .ToList();
        }

        private void HandleCommandExecutionException<TResult>(
            TaskCompletionSource<TResult> tcs,
            Exception ex,
            string trace = "")
        {
            // Close any open connections
            this.Connection.Close();
            s_tracer.TraceError("MultiShardCommand.ExecuteReaderAsync", ex, trace);
            tcs.SetException(ex);
        }

        private void HandleCommandExecutionCanceled<TResult>(
            TaskCompletionSource<TResult> tcs,
            CommandCancellationManager cancellationMgr,
            string trace = "")
        {
            // Close any open connections
            this.Connection.Close();

            s_tracer.TraceWarning("MultiShardCommand.ExecuteReaderAsync", "Command was canceled; {0}", trace);

            if (cancellationMgr.HasTimeoutExpired)
            {
                // The ConnectionTimeout elapsed
                tcs.SetException(new TimeoutException
                    (string.Format("Command timeout of {0} elapsed.", CommandTimeout)));
            }
            else
            {
                tcs.SetCanceled();
            }
        }

        #region Event Raisers

        /// <summary>
        /// Raise the ShardExecutionBegan event.
        /// </summary>
        /// <param name="shardLocation">The shard for which this event is raised.</param>
        private void OnShardExecutionBegan(ShardLocation shardLocation)
        {
            if (ShardExecutionBegan != null)
            {
                ShardExecutionEventArgs args = new ShardExecutionEventArgs()
                {
                    ShardLocation = shardLocation,
                    Exception = null
                };

                try
                {
                    ShardExecutionBegan(this, args);
                }
                catch (Exception e)
                {
                    throw new MultiShardException(shardLocation, e);
                }
            }
        }

        /// <summary>
        /// Raise the ShardExecutionSucceeded event.
        /// </summary>
        /// <param name="shardLocation">The shard for which this event is raised.</param>
        /// <param name="reader">The reader to pass in the associated eventArgs.</param>
        private void OnShardExecutionSucceeded(ShardLocation shardLocation, LabeledDbDataReader reader)
        {
            if (ShardExecutionSucceeded != null)
            {
                ShardExecutionEventArgs args = new ShardExecutionEventArgs()
                {
                    ShardLocation = shardLocation,
                    Exception = null,
                    Reader = reader
                };

                try
                {
                    ShardExecutionSucceeded(this, args);
                }
                catch (Exception e)
                {
                    throw new MultiShardException(shardLocation, e);
                }
            }
        }

        /// <summary>
        /// Raise the ShardExecutionReaderReturned event.
        /// </summary>
        /// <param name="shardLocation">The shard for which this event is raised.</param>
        /// <param name="reader">The reader to pass in the associated eventArgs.</param>
        private void OnShardExecutionReaderReturned(ShardLocation shardLocation, LabeledDbDataReader reader)
        {
            if (ShardExecutionReaderReturned != null)
            {
                ShardExecutionEventArgs args = new ShardExecutionEventArgs()
                {
                    ShardLocation = shardLocation,
                    Exception = null,
                    Reader = reader
                };

                try
                {
                    ShardExecutionReaderReturned(this, args);
                }
                catch (Exception e)
                {
                    throw new MultiShardException(shardLocation, e);
                }
            }
        }

        /// <summary>
        /// Raise the ShardExecutionFaulted event.
        /// </summary>
        /// <param name="shardLocation">The shard for which this event is raised.</param>
        /// <param name="executionException">The exception causing the execution on this shard to fault.</param>
        private void OnShardExecutionFaulted(ShardLocation shardLocation, Exception executionException)
        {
            if (ShardExecutionFaulted != null)
            {
                ShardExecutionEventArgs args = new ShardExecutionEventArgs()
                {
                    ShardLocation = shardLocation,
                    Exception = executionException
                };

                try
                {
                    ShardExecutionFaulted(this, args);
                }
                catch (Exception e)
                {
                    throw new MultiShardException(shardLocation, e);
                }
            }
        }

        /// <summary>
        /// Raise the ShardExecutionCanceled event.
        /// </summary>
        /// <param name="shardLocation">The shard for which this event is raised.</param>
        private void OnShardExecutionCanceled(ShardLocation shardLocation)
        {
            if (ShardExecutionCanceled != null)
            {
                ShardExecutionEventArgs args = new ShardExecutionEventArgs()
                {
                    ShardLocation = shardLocation,
                    Exception = null
                };

                try
                {
                    ShardExecutionCanceled(this, args);
                }
                catch (Exception e)
                {
                    throw new MultiShardException(shardLocation, e);
                }
            }
        }

        #endregion Event Raisers

        #endregion Helpers

        #region UnSupported DbCommand APIs

        /// <summary>
        /// This method is currently not supported. Invoking the property will result in an exception.
        /// </summary>
        public override void Prepare()
        {
            throw new NotSupportedException("Prepare is currently not supported");
        }

        #region ExecuteNonQuery Methods

        /// <summary>
        /// ExecuteNonQuery is currently not supported
        /// </summary>
        /// <returns></returns>
        public override int ExecuteNonQuery()
        {
            throw new NotSupportedException("ExecuteNonQuery is not supported");
        }

        /// <summary>
        /// ExecuteNonQueryAsync is currently not supported
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns></returns>
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            return base.ExecuteNonQueryAsync(cancellationToken);
        }

        // Suppression rationale: 
        //   We do want to catch all exceptions and set them on the Task where they can be dealt with on the main thread.
        //
        /// <summary>
        /// Test only for now
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="executionPolicy"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken, MultiShardExecutionPolicy executionPolicy)
        {
            var currentCompletion = new TaskCompletionSource<int>();
            var behavior = CommandBehavior.Default;
            var commandRetryPolicy = MultiShardUtils.GetSqlCommandRetryPolicy(this.RetryPolicy, this.RetryBehavior);
            var connectionRetryPolicy = MultiShardUtils.GetSqlConnectionRetryPolicy(this.RetryPolicy, this.RetryBehavior);

            // Check if cancellation has already been requested by the user
            if (cancellationToken.IsCancellationRequested)
            {
                currentCompletion.SetCanceled();
                return currentCompletion.Task;
            }

            try
            {
                ValidateCommand(behavior);

                // Create a list of sql commands to run against each of the shards
                List<Tuple<ShardLocation, DbCommand>> shardCommands = this.GetShardDbCommands();

                // Don't allow a new invocation if a Cancel() is already in progress
                lock (_cancellationLock)
                {
                    // Setup the Cancellation manager
                    CommandCancellationManager cmdCancellationMgr = new CommandCancellationManager(
                        _innerCts.Token,
                        cancellationToken,
                        executionPolicy,
                        CommandTimeout);

                    var commandTask = ExecuteReaderAsyncInternal(
                        behavior,
                        shardCommands,
                        cmdCancellationMgr,
                        commandRetryPolicy,
                        connectionRetryPolicy,
                        executionPolicy)
                        .OuterTask
                        .ContinueWith<Task<int>>(
                        (t) =>
                        {
                            switch (t.Status)
                            {
                                case TaskStatus.Faulted:
                                    HandleCommandExecutionException(
                                        currentCompletion,
                                        new MultiShardAggregateException(t.Exception.InnerExceptions));
                                    break;

                                case TaskStatus.Canceled:
                                    HandleCommandExecutionCanceled(
                                        currentCompletion,
                                        cmdCancellationMgr);
                                    break;

                                case TaskStatus.RanToCompletion:
                                    // Close all connections to shards
                                    Connection.Close();

                                    // Check for any exceptions if this is a partial results execution policy
                                    bool success = true;

                                    if (executionPolicy == MultiShardExecutionPolicy.PartialResults)
                                    {
                                        var exceptions = new List<Exception>();
                                        foreach (var ldr in t.Result)
                                        {
                                            if (ldr.Exception != null)
                                            {
                                                exceptions.Add(ldr.Exception);
                                            }
                                        }

                                        if (exceptions.Count != 0)
                                        {
                                            success = false;
                                            HandleCommandExecutionException(currentCompletion,
                                                new MultiShardAggregateException(exceptions));
                                        }
                                    }

                                    if (success)
                                    {
                                        currentCompletion.SetResult(-1);
                                    }
                                    break;
                                default:
                                    currentCompletion.SetException(new InvalidOperationException("Unexpected task status."));
                                    break;
                            }

                            return currentCompletion.Task;
                        }, cancellationToken)
                        .Unwrap();

                    _currentCommandTask = commandTask;
                    return commandTask;
                }
            }
            catch (Exception ex)
            {
                currentCompletion.SetException(ex);
                return currentCompletion.Task;
            }
        }

        #endregion

        #region ExecuteScalar Methods

        /// <summary>
        /// ExecuteScalar is currently not supported
        /// </summary>
        /// <returns></returns>
        public override object ExecuteScalar()
        {
            throw new NotSupportedException("ExecuteScalar is not supported");
        }

        /// <summary>
        /// ExecuteScalarAsync is currently not supported
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <returns></returns>
        public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return base.ExecuteScalarAsync(cancellationToken);
        }
        #endregion

        #endregion

        #region UnSupported DbCommand Properties

        // DEVNOTE (VSTS 2202707): We also do not suppport SqlNotificationRequest and
        // NotificationAutoEnlist handling yet

        /// <summary>
        /// This property is currently not supported. Accessing the property will result in an exception.
        /// </summary>
        public override bool DesignTimeVisible
        {
            get
            {
                throw new NotSupportedException("DesignTimeVisible is not supported");
            }
            set
            {
                throw new NotSupportedException("DesignTimeVisible is not supported");
            }
        }

        /// <summary>
        /// This property is currently not supported. Accessing the property will result in an exception.
        /// </summary>
        protected override DbTransaction DbTransaction
        {
            get
            {
                throw new NotSupportedException("DbTransaction is not supported");
            }
            set
            {
                throw new NotSupportedException("DbTransaction is not supported");
            }
        }

        /// <summary>
        /// This property is currently not supported. Accessing the property will result in an exception.
        /// </summary>
        /// DEVNOTE(VSTS 2202707): Do we want to support this?
        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                throw new NotSupportedException("UpdatedRowSource is not supported");
            }
            set
            {
                throw new NotSupportedException("UpdatedRowSource is not supported");
            }
        }

        /// <summary>
        /// Connections to shards. Not supported/exposed
        /// since connections are managed internally by this instance
        /// </summary>
        protected override DbConnection DbConnection
        {
            get
            {
                throw new NotSupportedException("Connections to shards are not exposed");
            }
            set
            {
                throw new NotSupportedException("Connections to shards are not exposed");
            }
        }

        #endregion

        #region Inner Helper Classes

        /// <summary>
        /// Sets up and manages the cancellation of the Execute* methods.
        /// </summary>
        private class CommandCancellationManager : IDisposable
        {
            private readonly CancellationTokenSource _timeoutCts;
            private readonly CancellationTokenSource _completeResultsCts;

            private readonly CancellationToken _linkedToken;

            private bool _disposed;

            public CommandCancellationManager(
                CancellationToken innerCts,
                CancellationToken outerCts,
                MultiShardExecutionPolicy executionPolicy,
                int commandTimeout)
            {
                // Create a Cts to cancel any tasks in-progress if a
                // complete results execution policy is used
                if (executionPolicy == MultiShardExecutionPolicy.CompleteResults)
                {
                    _completeResultsCts = new CancellationTokenSource();
                }

                // Setup the command timeout Cts
                if (commandTimeout > 0)
                {
                    _timeoutCts = new CancellationTokenSource();
                    _timeoutCts.CancelAfter(TimeSpan.FromSeconds(commandTimeout));
                }

                // Create the uber-token
                _linkedToken = CreateLinkedToken(innerCts, outerCts);
            }

            #region Properties

            public CancellationToken Token
            {
                get
                {
                    return _linkedToken;
                }
            }

            public CancellationTokenSource CompleteResultsCts
            {
                get
                {
                    if (_completeResultsCts == null)
                    {
                        throw new InvalidOperationException("No CancellationTokenSource exists for specified execution policy.");
                    }

                    return _completeResultsCts;
                }
            }
            public bool HasTimeoutExpired
            {
                get
                {
                    return _timeoutCts != null && _timeoutCts.IsCancellationRequested;
                }
            }

            #endregion

            public void Dispose()
            {
                if (!_disposed)
                {
                    if (null != _timeoutCts)
                    {
                        _timeoutCts.Dispose();
                    }

                    if (null != _completeResultsCts)
                    {
                        _completeResultsCts.Dispose();
                    }

                    _disposed = true;
                }
            }

            // Suppression rationale: We are returning the object.  We don't want to dispose it.
            //
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
            private CancellationToken CreateLinkedToken(CancellationToken innerToken, CancellationToken outerToken)
            {
                CancellationToken completeResultsToken = (_completeResultsCts == null) ? CancellationToken.None : _completeResultsCts.Token;

                CancellationToken timeoutToken = (_timeoutCts == null) ? CancellationToken.None : _timeoutCts.Token;

                return CancellationTokenSource.CreateLinkedTokenSource(
                    innerToken,
                    outerToken,
                    completeResultsToken,
                    timeoutToken)
                    .Token;
            }
        }

        /// <summary>
        /// Encapsulates data structures representing state of tasks executing across all the shards.
        /// </summary>
        private class FanOutTask
        {
            /// <summary>
            /// Parent task of all per-shard tasks.
            /// </summary>
            internal Task<LabeledDbDataReader[]> OuterTask { get; set; }

            /// <summary>
            /// Collection of inner tasks that run against each shard.
            /// </summary>
            internal Task<LabeledDbDataReader>[] InnerTasks { get; set; }
        }

        #endregion Inner Helper Classes
    }
}
