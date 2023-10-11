// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
// Unit tests for the cross shard query client libary
// Tests create mock SqlConnection, SqlCommand and SqlDataReader objects
// to enable greater flexibility in crafting test scenarios and also eliminate
// the need for a running sqlserver instance.
//
// DEVNOTE (VSTS 2202789): Work in progress.

using Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query.Exceptions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests;

[TestClass]
public class MultiShardUnitTests
{
    #region Tests

    /// <summary>
    /// Test that an exception in Open()
    /// by a particular shard is propagated by
    /// MultiShardConnection back to the user
    /// </summary>
    [TestMethod]
    [ExpectedException(typeof(InsufficientMemoryException))]
    public void TestShardConnectionOpenException()
    {
        try
        {
            static void executeOnOpen() => throw new InsufficientMemoryException();

            var shardConnections = CreateConnections(10, executeOnOpen);
            var mockCmd = new MockSqlCommand
            {
                CommandText = "Select 1"
            };
            using var conn = new MultiShardConnection(shardConnections);
            using var cmd = MultiShardCommand.Create(conn, mockCmd, 100);
            cmd.CommandText = "select 1";
            _ = cmd.ExecuteReader();
        }
        catch (Exception ex)
        {
            if (ex is MultiShardAggregateException maex)
            {
                Logger.Log("Exception message: {0}.\n Exception tostring: {1}",
                    ex.Message, ex.ToString());

                throw maex.InnerException.InnerException;
            }

            throw;
        }
    }

    /// <summary>
    /// Simulate a long running command on a shard
    /// and validate that MultiShardCommand throws a 
    /// timeout exception to the user
    /// </summary>
    [TestMethod]
    [ExpectedException(typeof(TimeoutException))]
    public void TestShardCommandTimeoutException()
    {
        var shardConnections = CreateConnections(10, () => { });

        static DbDataReader executeReaderFunc(CancellationToken token, MockSqlCommand cmd)
        {
            Thread.Sleep(TimeSpan.FromSeconds(2));
            return new MockSqlDataReader();
        }

        var mockCmd = new MockSqlCommand
        {
            ExecuteReaderFunc = executeReaderFunc,
            CommandText = "Select 1"
        };
        using var conn = new MultiShardConnection(shardConnections);
        using var cmd = MultiShardCommand.Create(conn, mockCmd, 1);
        _ = cmd.ExecuteReader();
    }

    /// <summary>
    /// Test the command Cancel()
    /// </summary>
    [TestMethod]
    public void TestShardCommandFaultHandler()
    {
        var shardConnections = CreateConnections(10, () => { });

        static DbDataReader executeReaderFunc(CancellationToken token, MockSqlCommand cmd) => throw new InsufficientMemoryException();

        var mockCmd = new MockSqlCommand
        {
            ExecuteReaderFunc = executeReaderFunc,
            CommandText = "Select 1"
        };
        var passedLocations = new ConcurrentDictionary<ShardLocation, bool>();
        using (var conn = new MultiShardConnection(shardConnections))
        {
            using var cmd = MultiShardCommand.Create(conn, mockCmd, 1);
            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
            cmd.CommandTimeout = 300;
            cmd.ShardExecutionFaulted += new EventHandler<ShardExecutionEventArgs>((obj, eventArgs) =>
                {
                    Assert.IsTrue(shardConnections.Select(x => x.Item1).Contains(eventArgs.ShardLocation), "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
                    passedLocations[eventArgs.ShardLocation] = true;
                    Assert.IsInstanceOfType(eventArgs.Exception, typeof(InsufficientMemoryException), "An incorrect exception type was passed to the event handler.");
                });
            try
            {
                // We want to execute to completion so we get to the validation at the end of the function.
                _ = cmd.ExecuteReader();
            }
            catch { }
        }

        Assert.AreEqual(shardConnections.Count, passedLocations.Count, "Not every ShardLocation had its corresponding event handler invoked.");
    }

    /// <summary>
    /// Test the command Cancel()
    /// </summary>
    [TestMethod]
    public void TestShardCommandCancellation()
    {
        // Create connections to a few shards
        var shardConnections = CreateConnections(10, () => { });

        var mockCmd = new MockSqlCommand();
        var cmdStartEvent = new ManualResetEvent(false);
        mockCmd.ExecuteReaderFunc = (token, cmd) =>
            {
                while (true)
                {
                    if (token == null)
                        break;
                    token.ThrowIfCancellationRequested();
                    Thread.Sleep(500);
                    _ = cmdStartEvent.Set();
                }

                return new MockSqlDataReader();
            };
        mockCmd.CommandText = "select 1";
        using var conn = new MultiShardConnection(shardConnections);
        using var cmd = MultiShardCommand.Create(conn, mockCmd, 300);
        try
        {
            // start the Cancel on a separate thread
            var executeTask = Task.Run(() =>
            {
                _ = cmdStartEvent.WaitOne();
                cmd.Cancel();
            });

            _ = cmd.ExecuteReader();
            executeTask.Wait();
            Assert.Fail("We should always be throwing an exception.");
        }
        catch (Exception ex)
        {
            Assert.IsTrue(ex is OperationCanceledException, "OperationCanceledException expected. Found {0}!", ex.ToString());
        }
    }

    /// <summary>
    /// Test the command Cancel()
    /// </summary>
    [TestMethod]
    public void TestShardCommandCancellationHandler()
    {
        // Create connections to a few shards
        var shardConnections = CreateConnections(10, () => { });

        var mockCmd = new MockSqlCommand();
        var cmdStartEvent = new ManualResetEvent(false);
        mockCmd.ExecuteReaderFunc = (token, cmd) =>
        {
            while (true)
            {
                if (token == null)
                    break;
                token.ThrowIfCancellationRequested();
                Thread.Sleep(500);
                _ = cmdStartEvent.Set();
            }

            return new MockSqlDataReader();
        };

        mockCmd.CommandText = "select 1";
        var passedLocations = new ConcurrentDictionary<ShardLocation, bool>();
        using (var conn = new MultiShardConnection(shardConnections))
        {
            using var cmd = MultiShardCommand.Create(conn, mockCmd, 300);
            cmd.ShardExecutionCanceled += new EventHandler<ShardExecutionEventArgs>((obj, eventArgs) =>
            {
                Assert.IsTrue(shardConnections.Select(x => x.Item1).Contains(eventArgs.ShardLocation),
                    "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
                passedLocations[eventArgs.ShardLocation] = true;
            });
            try
            {
                // start the Cancel on a separate thread
                var executeTask = Task.Run(() =>
                {
                    _ = cmdStartEvent.WaitOne();
                    cmd.Cancel();
                });

                _ = cmd.ExecuteReader();
                executeTask.Wait();
                Assert.Fail("We should always be throwing an exception.");
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is OperationCanceledException, "OperationCanceledException expected. Found {0}!", ex.ToString());
            }
        }

        Assert.AreEqual(shardConnections.Count, passedLocations.Count, "Not every ShardLocation had its corresponding event handler invoked.");
    }

    /// <summary>
    /// Test the command behavior validation
    /// </summary>
    [TestMethod]
    [ExpectedException(typeof(InvalidOperationException))]
    [DeploymentItem("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests.dll.config")]
    public void TestShardCommandBehavior()
    {
        var shardConnections = CreateConnections(10, () => { });
        using var conn = new MultiShardConnection(shardConnections);
        using var cmd = conn.CreateCommand();
        var behavior = CommandBehavior.SingleResult;
        behavior &= CommandBehavior.SingleRow;
        _ = cmd.ExecuteReader(behavior);
    }

    /// <summary>
    /// Test the event handler for OnShardBegin, ensuring that every shard in a successful execution has begin called at least once.
    /// </summary>
    [TestMethod]
    [DeploymentItem("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests.dll.config")]
    public void TestShardCommandBeginHandler()
    {
        var shardConnections = CreateConnections(10, () => { });
        var passedLocations = new ConcurrentDictionary<ShardLocation, bool>();
        using (var conn = new MultiShardConnection(shardConnections))
        {
            static DbDataReader executeReaderFunc(CancellationToken token, MockSqlCommand cmd)
            {
                Thread.Sleep(TimeSpan.FromSeconds(2));
                return new MockSqlDataReader();
            }

            var mockCmd = new MockSqlCommand
            {
                ExecuteReaderFunc = executeReaderFunc,
                CommandText = "Select 1"
            };
            using (var cmd = MultiShardCommand.Create(conn, mockCmd, 10))
            {
                cmd.ShardExecutionBegan += new EventHandler<ShardExecutionEventArgs>((obj, eventArgs) =>
                {
                    Assert.IsTrue(shardConnections.Select(x => x.Item1).Contains(eventArgs.ShardLocation),
                        "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
                    passedLocations[eventArgs.ShardLocation] = true;
                });
                var behavior = CommandBehavior.Default;
                _ = cmd.ExecuteReader(behavior);
            }
        }

        Assert.AreEqual(shardConnections.Count, passedLocations.Count, "Not every ShardLocation had its corresponding event handler invoked.");
    }

    /// <summary>
    /// Test the event handler for OnShardBegin, ensuring that every shard in a successful execution has begin called at least once.
    /// </summary>
    [TestMethod]
    [DeploymentItem("Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests.dll.config")]
    public void TestShardCommandSucceedHandler()
    {
        var shardConnections = CreateConnections(10, () => { });
        var passedLocations = new ConcurrentDictionary<ShardLocation, bool>();
        using (var conn = new MultiShardConnection(shardConnections))
        {
            static DbDataReader executeReaderFunc(CancellationToken token, MockSqlCommand cmd)
            {
                Thread.Sleep(TimeSpan.FromSeconds(2));
                return new MockSqlDataReader();
            }

            var mockCmd = new MockSqlCommand
            {
                ExecuteReaderFunc = executeReaderFunc,
                CommandText = "Select 1"
            };
            using (var cmd = MultiShardCommand.Create(conn, mockCmd, 10))
            {
                cmd.ShardExecutionSucceeded += new EventHandler<ShardExecutionEventArgs>((obj, eventArgs) =>
                {
                    Assert.IsTrue(shardConnections.Select(x => x.Item1).Contains(eventArgs.ShardLocation), "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
                    passedLocations[eventArgs.ShardLocation] = true;
                });
                var behavior = CommandBehavior.Default;
                _ = cmd.ExecuteReader(behavior);
            }
        }

        Assert.AreEqual(shardConnections.Count, passedLocations.Count, "Not every ShardLocation had its corresponding event handler invoked.");
    }

    /// <summary>
    /// Simple test that validates that 
    /// the retry logic works as expected:
    /// - Create a retry policy so we retry upto n times on failure
    /// - Have the MockSqlConnection throw a transient exception upto the (n-1)th retry
    /// - Validate that the MultiShardCommand indeed retries upto (n-1) times for each
    /// shard and succeeds on the nth retry.
    /// </summary>
    [TestMethod]
    public void TestShardCommandRetryBasic()
    {
        var retryPolicy = new RetryPolicy(4, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
        var openRetryCounts = new int[10];
        var shardConnections = new List<Tuple<ShardLocation, DbConnection>>();

        // Create ten mocked connections, each will retry retryPolicy.RetryCount - 1 times, 
        // and keep it's own actual retry count in one of the elements of openRetryCounts
        for (var i = 0; i < 10; i++)
        {
            var database = string.Format("Shard{0}", i);

            // We want to close on the value of i
            var j = i;
            void executeOnOpen()
            {
                if (openRetryCounts[j] < (retryPolicy.RetryCount - 1))
                {
                    Logger.Log("Current retry count for database: {0} is {1}", database, openRetryCounts[j]);
                    openRetryCounts[j]++;
                    throw new TimeoutException();
                }
            }

            var mockCon = new MockSqlConnection(database, executeOnOpen);
            shardConnections.Add(new Tuple<ShardLocation, DbConnection>(new ShardLocation("test", database), mockCon));
        }

        var mockCmd = new MockSqlCommand
        {
            ExecuteReaderFunc = (t, c) => new MockSqlDataReader(),
            CommandText = "select 1"
        };
        using (var conn = new MultiShardConnection(shardConnections))
        {
            using var cmd = MultiShardCommand.Create(conn, mockCmd, 300);
            cmd.ExecutionOptions = MultiShardExecutionOptions.None;
            cmd.RetryPolicy = retryPolicy;
            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
            _ = cmd.ExecuteReader(CommandBehavior.Default);
        }

        for (var i = 0; i < openRetryCounts.Length; i++)
        {
            Assert.AreEqual(retryPolicy.RetryCount - 1, openRetryCounts[i]);
        }
    }

    /// <summary>
    /// - Verify that upon retry exhaustion, the underlying exception
    /// is caught correctly. 
    /// - Also validate that any open connections are closed.
    /// </summary>
    [TestMethod]
    public void TestShardCommandRetryExhaustion()
    {
        var retryPolicy = new RetryPolicy(2, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
        var shardConnections = new List<Tuple<ShardLocation, DbConnection>>();

        // Create ten mocked connections, half of them will throw exceptions on Open
        for (var i = 0; i < 10; i++)
        {
            var database = string.Format("Shard{0}", i);

            var j = i;
            void executeOnOpen()
            {
                if (j < 5)
                {
                    throw new TimeoutException();
                }
            }

            var mockCon = new MockSqlConnection(database, executeOnOpen);
            shardConnections.Add(new Tuple<ShardLocation, DbConnection>(new ShardLocation("test", database), mockCon));
        }

        var mockCmd = new MockSqlCommand
        {
            ExecuteReaderFunc = (t, c) => new MockSqlDataReader(),
            CommandText = "select 1"
        };
        using var conn = new MultiShardConnection(shardConnections);
        using var cmd = MultiShardCommand.Create(conn, mockCmd, 300);
        cmd.ExecutionOptions = MultiShardExecutionOptions.None;
        cmd.RetryPolicy = retryPolicy;
        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
        var rdr = cmd.ExecuteReader(CommandBehavior.Default);

        // Validate the right exception is re-thrown
        Assert.IsTrue(rdr.MultiShardExceptions.Count == 5, "Expected MultiShardExceptions!");
        foreach (var ex in rdr.MultiShardExceptions)
        {
            Assert.IsTrue(ex.InnerException is TimeoutException, "Expected TimeoutException!");
        }

        // Validate that the connections for the faulted readers are closed
        for (var i = 0; i < 5; i++)
        {
            Assert.IsTrue(shardConnections[i].Item2.State == ConnectionState.Closed,
                "Expected Connection to be Closed!");
        }
    }

    /// <summary>
    /// - Close the connection upon hitting a transient exception
    /// - Validate that the command is re-tried and 
    /// that the connection is re-opened
    /// </summary>
    [TestMethod]
    public void TestShardCommandRetryConnectionReopen()
    {
        var retryPolicy = new RetryPolicy(4, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
        var shardConnections = new List<Tuple<ShardLocation, DbConnection>>();

        // Callback to execute when the MockCommand is invoked
        Func<CancellationToken, MockSqlCommand, DbDataReader> ExecuteReaderFunc = null;

        // Number of times each command has been retried
        var commandRetryCounts = new int[10];

        // Create ten mocked connections, 
        // a few of them will throw an exception on Open
        // and the rest will throw an exception on command execution upto 2 retries
        // At the end, all commands should complete successfully.
        for (var i = 0; i < 10; i++)
        {
            var database = string.Format("{0}", i);

            var j = i;
            var retryCount = 0;
            void executeOnOpen()
            {
                if (j < 5)
                {
                    if (retryCount < 3)
                    {
                        retryCount++;
                        throw new TimeoutException();
                    }
                }
            }

            var mockCon = new MockSqlConnection(database, executeOnOpen);
            shardConnections.Add(new Tuple<ShardLocation, DbConnection>(new ShardLocation("Shard", database), mockCon));
        }

        ExecuteReaderFunc = (t, r) =>
        {
            var index = int.Parse(((MockSqlConnection)r.Connection).ConnectionString);
            if (r.Connection.State == ConnectionState.Closed)
            {
                throw new InvalidOperationException("Command shouldn't be executed on a closed connection!");
            }

            if (index > 5 && commandRetryCounts[index] < 3)
            {
                commandRetryCounts[index]++;
                r.RetryCount++;
                r.Connection.Close();
                throw new TimeoutException();
            }
            else
            {
                var mockRdr = new MockSqlDataReader
                {
                    ExecuteOnReadAsync = (rdr) =>
                {
                    return Task.Run<bool>(() =>
                    {
                        var isClosed = rdr.IsClosed;
                        rdr.Close();
                        return !isClosed;
                    });
                }
                };
                return mockRdr;
            }
        };

        var mockCmd = new MockSqlCommand
        {
            ExecuteReaderFunc = ExecuteReaderFunc,
            CommandText = "select 1"
        };
        using var conn = new MultiShardConnection(shardConnections);
        using var cmd = MultiShardCommand.Create(conn, mockCmd, 300);
        cmd.RetryPolicy = retryPolicy;
        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
        using var reader = cmd.ExecuteReaderAsync().Result;
        // Validate that we successfully received a reader 
        // from each one of the shards
        var readerCount = 0;
        while (reader.Read())
        {
            readerCount++;
        }

        Assert.AreEqual(10, readerCount, "Expected 10 readers!");
    }

    /// <summary>
    /// Test the custom serializion logic for exceptions
    /// </summary>
    [TestMethod]
    public void TestExceptionSerialization()
    {
        var sl1 = new ShardLocation("dataSource1", "database1");
        var sl2 = new ShardLocation("dataSource2", "database2");

        var innerEx1 = new MultiShardException(sl1);
        var innerEx2 = new MultiShardException(sl2);

        var exList = new List<Exception>
        {
            innerEx1,
            innerEx2
        };

        var aggEx = new MultiShardAggregateException(exList);

        var deserialized1 = CommonTestUtils.SerializeDeserialize(innerEx1);
        CompareForEquality(innerEx1, deserialized1);

        var deserialized2 = CommonTestUtils.SerializeDeserialize(innerEx2);
        CompareForEquality(innerEx2, deserialized2);

        var deserialized3 = CommonTestUtils.SerializeDeserialize(aggEx);
        CompareForEquality(aggEx, deserialized3);
    }

    /// <summary>
    /// Validates that the MultiShardDataReader
    /// handles an exception during Read() properly
    /// </summary>
    [TestMethod]
    public void TestDataReaderReadException()
    {
        // Setup two data readers from shards
        var mockReader1 = new MockSqlDataReader("Reader1");
        var mockReader2 = new MockSqlDataReader("Reader2");
        var movedOnToNextReader = false;
        var invokeCount = 1;
        Task<bool> ExecuteOnReadAsync(MockSqlDataReader r) => Task.Run<bool>(() =>
                                                                       {
                                                                           // First reader throws an exception when Read
                                                                           if (r.Name == "Reader1")
                                                                           {
                                                                               if (invokeCount == 2)
                                                                               {
                                                                                   throw new InvalidOperationException();
                                                                               }
                                                                           }
                                                                           else
                                                                           {
                                                                               movedOnToNextReader = true;
                                                                           }

                                                                           return true;
                                                                       });

        void ExecuteOnGetColumn(MockSqlDataReader r)
        {
            if (r.Name == "Reader1")
            {
                throw new InvalidOperationException();
            }
        }

        mockReader1.ExecuteOnReadAsync = ExecuteOnReadAsync;
        mockReader1.ExecuteOnGetColumn = ExecuteOnGetColumn;
        mockReader2.ExecuteOnReadAsync = ExecuteOnReadAsync;
        var labeledDataReaders = new LabeledDbDataReader[2];

        labeledDataReaders[0] = new LabeledDbDataReader(mockReader1, new ShardLocation("test", "Shard1"),
            new MockSqlCommand() { Connection = new MockSqlConnection("", () => { }) });
        labeledDataReaders[1] = new LabeledDbDataReader(mockReader2, new ShardLocation("test", "Shard2"),
            new MockSqlCommand() { Connection = new MockSqlConnection("", () => { }) });

        // Create the MultiShardDataReader
        var mockMultiShardCmd = MultiShardCommand.Create(null, "test");
        var multiShardDataReader = new MultiShardDataReader(mockMultiShardCmd, labeledDataReaders,
            MultiShardExecutionPolicy.PartialResults, false);

        // Validate that if an exception is thrown when reading a column,
        // it is propagated back to the user
        try
        {
            _ = multiShardDataReader.Read();
            invokeCount++;
            _ = multiShardDataReader.GetInt32(0);
        }
        catch (Exception ex)
        {
            Assert.IsTrue(ex is InvalidOperationException, "Expected InvalidOperationException!");
        }

        // Validate that we didn't automatically move on to the next reader when we
        // hit an exception whilst reading the column and that 
        // an exception from a second Read() call is stored and the reader is closed
        _ = multiShardDataReader.Read();
        Assert.AreEqual(multiShardDataReader.MultiShardExceptions.Count, 1, "Expected exception to be recorded");
        Assert.IsTrue(mockReader1.IsClosed, "Expected reader to be closed!");

        // Validate we immediately moved on to the next reader
        _ = multiShardDataReader.Read();
        Assert.IsTrue(movedOnToNextReader, "Should've moved on to next reader");
    }

    /// <summary>
    /// Verify MultiShardDataReader handling of readers with a null schema 
    /// for the following cases:
    /// - Case #1: All Readers have a null schema. Verify that no exception is thrown.
    /// - Case #2: The first half of readers have a null schema and the rest are non-null.
    ///   Verify that a MultiShardDataReaderInternalException is thrown.
    /// - Case #3: The first half of readers have a non-null schema and the rest are null.
    ///   Verify that a MultiShardDataReaderInternalException is thrown.
    /// </summary>
    [TestMethod]
    public void TestAddDataReaderWithNullSchema()
    {
        // Creates a MultiShardDataReader and verifies that the right exception is thrown
        static bool createMultiShardReader(LabeledDbDataReader[] readers)
        {
            var hitNullSchemaException = false;

            try
            {
                var mockMultiShardCmd = MultiShardCommand.Create(null, "test");
                var multiShardDataReader = new MultiShardDataReader(mockMultiShardCmd, readers,
                    MultiShardExecutionPolicy.PartialResults, false, readers.Length);
            }
            catch (MultiShardDataReaderInternalException ex)
            {
                hitNullSchemaException = ex.Message.Contains("null schema");
            }

            return hitNullSchemaException;
        }

        var labeledDataReaders = new LabeledDbDataReader[10];

        // Create a few mock readers. All with a null schema
        for (var i = 0; i < labeledDataReaders.Length; i++)
        {
            var mockReader = new MockSqlDataReader(string.Format("Reader{0}", i), null /* Null schema */);
            labeledDataReaders[i] = new LabeledDbDataReader(mockReader,
                new ShardLocation("test", string.Format("Shard{0}", i)), new MockSqlCommand() { Connection = new MockSqlConnection("", () => { }) });
        }

        // Case #1
        var hitException = createMultiShardReader(labeledDataReaders);
        Assert.IsFalse(hitException, "Unexpected exception! All readers have a null schema.");

        // Case #2
        for (var i = 0; i < labeledDataReaders.Length; i++)
        {
            var mockReader = (MockSqlDataReader)labeledDataReaders[i].DbDataReader;
            mockReader.Open();

            if (i > labeledDataReaders.Length / 2)
            {
                mockReader.DataTable = new DataTable();
            }
        }

        hitException = createMultiShardReader(labeledDataReaders);
        Assert.IsTrue(hitException, "Exception not hit! Second half of readers don't have a null schema!");

        // Case #3
        for (var i = 0; i < labeledDataReaders.Length; i++)
        {
            var mockReader = (MockSqlDataReader)labeledDataReaders[i].DbDataReader;
            mockReader.Open();

            mockReader.DataTable = i < labeledDataReaders.Length / 2 ? new DataTable() : null;
        }

        hitException = createMultiShardReader(labeledDataReaders);
        Assert.IsTrue(hitException, "Exception not hit! First half of readers don't have a null schema!");
    }

    #endregion

    private void CompareForEquality(Exception first, Exception second)
    {
        Assert.AreEqual(first.GetType(), second.GetType());

        if (first is MultiShardException)
        {
            DoExceptionComparison((MultiShardException)first, (MultiShardException)second);
            return;
        }

        if (first is MultiShardAggregateException)
        {
            DoExceptionComparison((MultiShardAggregateException)first, (MultiShardAggregateException)second);
            return;
        }

        Assert.Fail(string.Format("Unknown exception type: {0}", first.GetType()));
    }

    private void DoExceptionComparison(MultiShardException first, MultiShardException second)
    {
        Assert.AreEqual(first.ShardLocation.Database, first.ShardLocation.Database);
        Assert.AreEqual(second.ShardLocation.DataSource, second.ShardLocation.DataSource);
    }

    private void DoExceptionComparison(MultiShardAggregateException first, MultiShardAggregateException second)
    {
        Assert.AreEqual(first.InnerExceptions.Count, second.InnerExceptions.Count);
        for (var i = 0; i < first.InnerExceptions.Count; i++)
        {
            CompareForEquality((MultiShardException)first.InnerExceptions[i], (MultiShardException)second.InnerExceptions[i]);
        }
    }

    private List<Tuple<ShardLocation, DbConnection>> CreateConnections(int count, Action executeOnOpen)
    {
        var shardConnections = new List<Tuple<ShardLocation, DbConnection>>();

        for (var i = 0; i < count; i++)
        {
            var database = string.Format("Shard{0}", i);
            var mockCon = new MockSqlConnection(database, executeOnOpen);
            shardConnections.Add(new Tuple<ShardLocation, DbConnection>(new ShardLocation("test", database), mockCon));
        }

        return shardConnections;
    }

    internal class MockTransientErrorDetectionStrategy : TransientFaultHandling.ITransientErrorDetectionStrategy
    {
        public MockTransientErrorDetectionStrategy(Func<Exception, bool> evaluateException) => EvaluateException = evaluateException;

        public Func<Exception, bool> EvaluateException { get; set; }

        public bool IsTransient(Exception ex) => EvaluateException(ex);
    }

    #region Boilerplate

    /// <summary>
    ///Gets or sets the test context which provides
    ///information about and functionality for the current test run.
    ///</summary>
    public TestContext TestContext { get; set; }

    [ClassInitialize()]
    public static void MyClassInitialize(TestContext testContext)
    {
    }

    [ClassCleanup()]
    public static void MyClassCleanup()
    {
    }

    /// <summary>
    /// Open up a clean connection to each test database prior to each test.
    /// </summary>
    [TestInitialize()]
    public void MyTestInitialize()
    {
    }

    /// <summary>
    /// Close our connections to each test database after each test.
    /// </summary>
    [TestCleanup()]
    public void MyTestCleanup()
    {
    }

    #endregion
}
