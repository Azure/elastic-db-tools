// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
//  Basic unit testing for the MultiShardDataReader class.  Will integrate with
//  build at a later date.
//
// Notes:
//  Aim is to integrate this within a broader cleint-side wrapper framework.
//  As a result, unit testing will likely be relatively significantly
//  restructured once we have the rest of the wrapper classes in place.
//  *NOTE: Unit tests currently assume that a sql server instance is
//  accessible on localhost.
//  *NOTE: Unit tests will blow away and recreate databases called Test1, Test2,
//  and Test3.  Should change these database names to guids at some point, but
//  deferring that until our unit testing (and functional testing) framework
//  is more settled.
//  *NOTE: Unit tests will blow away and recreate a login called TestUser
//  and grant it "control server" permissions.  Will likely need to revisit this
//  at some point in the future.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
#if NETFRAMEWORK
using System.Runtime.Remoting;
#endif
using System.IO;
using System.Threading.Tasks;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    /// <summary>
    /// Very basic unit tests for the MultiShardDataReader class.
    /// Just enough to ensure that simple scenarios working as expected.
    /// </summary>
    [TestClass]
    public class MultiShardDataReaderTests
    {
        /// <summary>
        /// Currently doesn't do anything special.
        /// </summary>
        public MultiShardDataReaderTests()
        {
        }

        private SqlConnection _conn1;
        private SqlConnection _conn2;
        private SqlConnection _conn3;
        private IEnumerable<SqlConnection> _conns;

        /// <summary>
        /// Handle on conn1, conn2 and conn3
        /// </summary>
        private MultiShardConnection _shardConnection;

        /// <summary>
        /// Placeholder object for us to pass into MSDRs that we create without going through a command.
        /// </summary>
        private MultiShardCommand _dummyCommand;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

#region Additional test attributes

        /// <summary>
        /// Sets up our three test databases that we drive the unit testing off of.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            // Drop and recreate the test databases, tables, and data that we will use to verify
            // the functionality.
            // For now I have hardcoded the server location and database names.  A better approach would be
            // to make the server location configurable and the database names be guids.
            // Not the top priority right now, though.
            //
            SqlConnection.ClearAllPools();
            MultiShardTestUtils.DropAndCreateDatabases();
            MultiShardTestUtils.CreateAndPopulateTables();
        }

        /// <summary>
        /// Blow away our three test databases that we drove the tests off of.
        /// Doing this so that we don't leave objects littered around.
        /// </summary>
        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            // We need to clear the connection pools so that we don't get a database still in use error
            // resulting from our attenpt to drop the databases below.
            //
            SqlConnection.ClearAllPools();
            MultiShardTestUtils.DropDatabases();
        }

        /// <summary>
        /// Open up a clean connection to each test database prior to each test.
        /// </summary>
        [TestInitialize()]
        public void MyTestInitialize()
        {
            ShardMap sm = MultiShardTestUtils.CreateAndGetTestShardMap();

            // Use the MultiShardConnection to open up connections

            // Validate the connections to shards
            _shardConnection = new MultiShardConnection(sm.GetShards(), MultiShardTestUtils.ShardConnectionString);
            _dummyCommand = MultiShardCommand.Create(_shardConnection, "SELECT 1");

            // DEVNOTE: The MultiShardCommand object handles the connection opening logic.
            //          BUT, since we are writing tests at a lower level than that, we need to open
            //          the connections manually here.  Hence the loop below.
            //
            foreach (var conn in _shardConnection.GetShardConnections())
            {
                conn.Item2.Open();
            }

            _conn1 = (SqlConnection)_shardConnection.GetShardConnections()[0].Item2;
            _conn2 = (SqlConnection)_shardConnection.GetShardConnections()[1].Item2;
            _conn3 = (SqlConnection)_shardConnection.GetShardConnections()[2].Item2;
            _conns = _shardConnection.GetShardConnections().Select(x => (SqlConnection)x.Item2);
        }

        /// <summary>
        /// Close our connections to each test database after each test.
        /// </summary>
        [TestCleanup()]
        public void MyTestCleanup()
        {
            foreach (var conn in _shardConnection.GetShardConnections())
            {
                conn.Item2.Close();
            }
        }

#endregion

        /// <summary>
        /// Validate MultiShardDataReader can be supplied as argument to DataTable.Load
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestDataTableLoad()
        {
            // What we're doing:
            // Obtain MultiShardDataReader,
            // Pass it to DataTable.Load and ensure correct number of rows is loaded.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field FROM ConsistentShardedTable";

            using (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql))
            {
                DataTable dataTable = new DataTable();
                dataTable.Load(sdr);

                Assert.AreEqual(9, dataTable.Rows.Count, "Expected 9 rows loaded to DataTable");

                int recordsRetrieved = 0;
                foreach (DataRow row in dataTable.Rows)
                {
                    recordsRetrieved++;
                    string dbNameField = row.Field<string>(0);
                    int testIntField = row.Field<int>(1);
                    Int64 testBigIntField = row.Field<Int64>(2);
                    string shardIdPseudoColumn = row.Field<string>(3);
                    string logRecord = string.Format("RecordRetrieved: dbNameField: {0}, TestIntField: {1}, TestBigIntField: {2}, shardIdPseudoColumnField: {3}, RecordCount: {4}",
                        dbNameField, testIntField, testBigIntField, shardIdPseudoColumn, recordsRetrieved);
                    Logger.Log(logRecord);
                    Debug.WriteLine(logRecord);
                }
                Assert.AreEqual(recordsRetrieved, 9);
            }
        }

        /// <summary>
        /// Check that we can turn the $ShardName pseudo column on and off as expected.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardNamePseudoColumnOption()
        {
            // What we're doing:
            // Grab all rows from each test database.
            // Load them into a MultiShardDataReader.
            // Iterate through the rows and make sure that we have 9 rows total with
            // the Pseudo column present (or not) as per the setting we used.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
            bool[] pseudoColumnPresentOptions = new bool[2];
            pseudoColumnPresentOptions[0] = true;
            pseudoColumnPresentOptions[1] = false;

            foreach (bool pseudoColumnPresent in pseudoColumnPresentOptions)
            {
                LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
                readers[0] = GetReader(_conn1, selectSql);
                readers[1] = GetReader(_conn2, selectSql);
                readers[2] = GetReader(_conn3, selectSql);

                IList<MultiShardSchemaMismatchException> exceptions;

                using (MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers, out exceptions, pseudoColumnPresent))
                {
                    Assert.AreEqual(0, exceptions.Count);

                    int recordsRetrieved = 0;

                    int expectedFieldCount = pseudoColumnPresent ? 4 : 3;
                    int expectedVisibleFieldCount = pseudoColumnPresent ? 4 : 3;
                    Assert.AreEqual(expectedFieldCount, sdr.FieldCount);
                    Assert.AreEqual(expectedVisibleFieldCount, sdr.VisibleFieldCount);

                    while (sdr.Read())
                    {
                        recordsRetrieved++;

                        string dbNameField = sdr.GetString(0);
                        int testIntField = sdr.GetFieldValue<int>(1);
                        Int64 testBigIntField = sdr.GetFieldValue<Int64>(2);
                        try
                        {
                            string shardIdPseudoColumn = sdr.GetFieldValue<string>(3);
                            if (!pseudoColumnPresent)
                            {
                                Assert.Fail("Should not have been able to pull the pseudo column.");
                            }
                        }
                        catch (IndexOutOfRangeException)
                        {
                            if (pseudoColumnPresent)
                            {
                                Assert.Fail("Should not have encountered an exception.");
                            }
                        }
                    }

                    sdr.Close();
                    Assert.AreEqual(recordsRetrieved, 9);
                }
            }
        }

        /// <summary>
        /// Check that we can handle empty result sets interspersed with non-empty result sets as expected.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestMiddleResultEmptyOnSelect()
        {
            // What we're doing:
            // Grab all rows from each test database that satisfy a particular predicate (there should be 3 from db1 and
            // db3 and 0 from db2).
            // Load them into a MultiShardDataReader.
            // Iterate through the rows and make sure that we have 6 rows.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='Test0' OR dbNameField='Test2'";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            readers[2] = GetReader(_conn3, selectSql);

            IList<MultiShardSchemaMismatchException> exceptions;

            using (MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers, out exceptions, true))
            {
                Assert.AreEqual(0, exceptions.Count);

                int recordsRetrieved = 0;
                while (sdr.Read())
                {
                    recordsRetrieved++;
                }

                sdr.Close();

                Assert.AreEqual(recordsRetrieved, 6);
            }
        }

        /// <summary>
        /// Check that we can handle non-empty result sets interspersed with empty result sets as expected.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestOuterResultsEmptyOnSelect()
        {
            // What we're doing:
            // Grab all rows from each test database that satisfy a particular predicate (there should be 0 from db1 and
            // db3 and 3 from db2).
            // Load them into a MultiShardDataReader.
            // Iterate through the rows and make sure that we have 3 rows.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='Test1'";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            readers[2] = GetReader(_conn3, selectSql);

            IList<MultiShardSchemaMismatchException> exceptions;

            using (MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers, out exceptions, true))
            {
                Assert.AreEqual(0, exceptions.Count);

                int recordsRetrieved = 0;
                while (sdr.Read())
                {
                    recordsRetrieved++;
                }

                sdr.Close();

                Assert.AreEqual(recordsRetrieved, 3);
            }
        }

        /// <summary>
        /// Check that we collect an exception and expose it on the ShardedReader
        /// when encountering schema mismatches across result sets due to different
        /// column names.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestMismatchedSchemasWrongColumnName()
        {
            // What we're doing:
            // Issue different queries to readers 1 & 2 so that we have the same column count and types but we have a
            // column name mismatch.
            // Try to load them into a MultiShardDataReader.
            // Should see an exception on the MultiShardDataReader.
            // Should also be able to successfully iterate through some records.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable;";
            string alternateSelectSql = "SELECT dbNameField as DifferentName, Test_int_Field, Test_bigint_Field FROM ConsistentShardedTable;";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, alternateSelectSql);

            IList<MultiShardSchemaMismatchException> exceptions;

            using (MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers, out exceptions, true))
            {
                if ((null == exceptions) || (exceptions.Count != 1))
                {
                    Assert.Fail("Expected an element in the InvalidReaders collection.");
                }
                else
                {
                    int recordsRetrieved = 0;

                    while (sdr.Read())
                    {
                        recordsRetrieved++;
                    }

                    Assert.AreEqual(recordsRetrieved, 3);
                }
                sdr.Close();
            }
        }

        /// <summary>
        /// Check that we throw as expected when encountering schema mismatches across result sets due to different
        /// column types.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestMismatchedSchemasWrongType()
        {
            // What we're doing:
            // Issue different queries to readers 1 & 2 so that we have the same column count and names but we have a
            // column type mismatch.
            // Try to load them into a MultiShardDataReader.
            // Should see an exception on the MultiShardDataReader.
            // Should also be able to successfully iterate through some records.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable;";
            string alternateSelectSql = "SELECT dbNameField, Test_int_Field, Test_int_Field as Test_bigint_Field FROM ConsistentShardedTable;";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, alternateSelectSql);

            IList<MultiShardSchemaMismatchException> exceptions;

            using (MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers, out exceptions, true))
            {
                if ((null == exceptions) || (exceptions.Count != 1))
                {
                    Assert.Fail("Expected an element in the InvalidReaders collection.");
                }
                else
                {
                    int recordsRetrieved = 0;

                    while (sdr.Read())
                    {
                        recordsRetrieved++;
                    }

                    Assert.AreEqual(recordsRetrieved, 3);
                }
                sdr.Close();
            }
        }

        /// <summary>
        /// Check that we throw as expected when trying to add a reader after the sharded reader has been marked complete.
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(MultiShardDataReaderInternalException))]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddReaderAfterReaderMarkedComplete()
        {
            // What we're doing:
            // Set up a new sharded reader
            // Add two readers to it.
            // Mark it as closed.
            // Try to add a third reader to it.
            // Verify that we threw as expected.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            readers[2] = GetReader(_conn3, selectSql);

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true))
            {
                // Just a single call to AddReader should be sufficient to check this logic.
                //
                sdr.AddReader(readers[0]);
            }
        }

        /// <summary>
        /// Check that we throw as expected when trying to add a null reader.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddNullReader()
        {
            // What we're doing:
            // Set up a new sharded reader
            // Add two readers to it.
            // Try to add a third reader to it that is null.
            // We should not throw here since initial constructor AddReader call and subsequent AddReader call are now on par.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            readers[2] = null;

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true, 6))
            {
                sdr.AddReader(readers[0]);
                sdr.AddReader(readers[1]);
                sdr.AddReader(readers[2]);
            }
        }

        /// <summary>
        /// Validate basic ReadAsync behavior.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestReadAsync()
        {
            LabeledDbDataReader[] readers = new LabeledDbDataReader[1];
            readers[0] = GetReader(_conn1, "select 1");
            int numRowsRead = 0;

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true))
            {
                while (sdr.ReadAsync().Result)
                {
                    numRowsRead++;
                }

                Assert.AreEqual(1, numRowsRead, "ReadAsync didn't return the expeceted number of rows.");
            }
        }

        /// <summary>
        /// Validate ReadAsync() behavior when multiple data readers are involved. This test is same as existing test TestMiddleResultEmptyOnSelect
        /// except that we are using ReadAsync() in this case instead of Read() to read individual rows.
        ///
        /// NOTE: We needn't replicate every single Read() test for ReadAsync() since Read() ends up calling ReadAsync().Result under the
        /// hood. So, by validating Read(), we are also validating ReadAsync() indirectly.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestReadSyncWithMultipleDataReaders()
        {
            // What we're doing:
            // Grab all rows from each test database that satisfy a particular predicate (there should be 3 from db1 and
            // db3 and 0 from db2).
            // Load them into a MultiShardDataReader.
            // Iterate through the rows using ReadAsync() and make sure that we have 6 rows.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='Test0' OR dbNameField='Test2'";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            readers[2] = GetReader(_conn3, selectSql);

            IList<MultiShardSchemaMismatchException> exceptions;

            using (MultiShardDataReader sdr = GetMultiShardDataReaderFromDbDataReaders(readers, out exceptions, true))
            {
                Assert.AreEqual(0, exceptions.Count);

                int recordsRetrieved = 0;
                while (sdr.ReadAsync().Result)
                {
                    recordsRetrieved++;
                }

                sdr.Close();

                Assert.AreEqual(recordsRetrieved, 6);
            }
        }

        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestMultiShardQueryCancellation()
        {
            ManualResetEvent rollback = new ManualResetEvent(false);
            ManualResetEvent readerInitialized = new ManualResetEvent(false);
            string dbToUpdate = _conn2.Database;

            // Start a task that would begin a transaction, update the rows on the second shard and then
            // block on an event. While the transaction is still open and the task is blocked, another
            // task will try to read rows off the shard.
            Task lockRowTask = Task.Factory.StartNew(() =>
                {
                    using (SqlConnection conn = new SqlConnection(_conn2.ConnectionString))
                    {
                        conn.Open();
                        SqlTransaction tran = conn.BeginTransaction();

                        using (SqlCommand cmd = new SqlCommand(
                            string.Format(
                                "UPDATE ConsistentShardedTable SET dbNameField='TestN' WHERE dbNameField='{0}'",
                                dbToUpdate),
                            conn, tran))
                        {
                            // This will X-lock all rows in the second shard.
                            cmd.ExecuteNonQuery();

                            if (rollback.WaitOne())
                            {
                                tran.Rollback();
                            }
                        }
                    }
                });

            CancellationTokenSource tokenSource = new CancellationTokenSource();

            // Create a new task that would try to read rows off the second shard while they are locked by the previous task
            // and block therefore.
            Task readToBlockTask = Task.Factory.StartNew(() =>
                {
                    string selectSql = string.Format(
                        "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE dbNameField='{0}'",
                        dbToUpdate);

                    using (MultiShardDataReader sdr = GetShardedDbReaderAsync(_shardConnection, selectSql, tokenSource.Token))
                    {
                        readerInitialized.Set();

                        // This call should block.
                        while (sdr.ReadAsync(tokenSource.Token).Result);
                    }
                });

            // Cancel the second task.This should trigger the cancellation of the multi-shard query.
            tokenSource.Cancel();

            try
            {
                readToBlockTask.Wait();
                Assert.IsTrue(false, "The task expected to block ran to completion.");
            }
            catch(AggregateException aggex)
            {
                TaskCanceledException ex = aggex.Flatten().InnerException as TaskCanceledException;

                Assert.IsTrue(ex != null, "A task canceled exception was not received upon cancellation.");
            }

            // Set the event signaling the first task to rollback its update transaction.
            rollback.Set();

            lockRowTask.Wait();
        }

        /// <summary>
        /// Check that we do not hang when trying to read after adding null readers.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestReadFromNullReader()
        {
            // The code below exposes a flaw in our current implementation related to
            // CompleteResults semantics and the internal c-tor.  The flaw does not
            // leak out to customers because the MultiShardCommand object manages the
            // necessary logic, but we need to patch the flaw so it doesn't end up
            // inadvertently leaking out to customers.
            // See VSTS 2616238 (i believe).  Philip will be modofying logic and
            // augmenting tests to deal with this issue.
            //

            // Pass a null reader and verify that read does not hang.
            LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
            readers[0] = GetReader(_conn1, "select 1");
            readers[1] = null;

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true))
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    while (sdr.Read())
                    {
                    }
                });

                Thread.Sleep(500);
                Assert.AreEqual(TaskStatus.RanToCompletion, t.Status, "Read hung on the null reader.");
                sdr.ExpectNoMoreReaders();
            }
        }

        /// <summary>
        /// Check that we do not hang when trying to read after adding a reader with an exception.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestReadFromReaderWithException()
        {
            // The code below exposes a flaw in our current implementation related to
            // CompleteResults semantics and the internal c-tor.  The flaw does not
            // leak out to customers because the MultiShardCommand object manages the
            // necessary logic, but we need to patch the flaw so it doesn't end up
            // inadvertently leaking out to customers.
            // See VSTS 2616238 (i believe).  Philip will be modofying logic and
            // augmenting tests to deal with this issue.
            //

            // Pass a reader with an exception that read does not hang.
            LabeledDbDataReader[] readers = new LabeledDbDataReader[2];
            readers[0] = GetReader(_conn1, "select 1");
            readers[1] = new LabeledDbDataReader(new MultiShardException(),
                new ShardLocation("foo", "bar"), new SqlCommand() { Connection = _conn2 });

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true))
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    while (sdr.Read())
                    {
                    }
                });

                Thread.Sleep(500);
                Assert.AreEqual(TaskStatus.RanToCompletion, t.Status, "Read hung on the garbage reader.");
                sdr.ExpectNoMoreReaders();
            }
        }

        /// <summary>
        /// Validate that we throw an exception and invalidate the
        /// MultiShardDataReader when we encounter a reader that has
        /// multiple result sets
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestReadFromReaderWithNextResultException()
        {
            string selectSql = @"SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876;
SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";

            LabeledDbDataReader[] readers = new LabeledDbDataReader[1];
            readers[0] = GetReader(_conn1, selectSql);

            MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
                MultiShardExecutionPolicy.CompleteResults, true);

            AssertExtensions.WaitAndAssertThrows<NotSupportedException>(sdr.NextResultAsync());
            Assert.IsTrue(sdr.IsClosed, "Expected MultiShardDataReader to be closed!");
        }

        /// <summary>
        /// Check that we throw as expected when trying to add a LabeledDataReader with a null DbDataReader underneath.
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddLabeledDataReaderWithNullDbDataReader()
        {
            // What we're doing:
            // Set up a new sharded reader
            // Add two readers to it.
            // Try to add a third reader to it that has a null DbDataReader underneath.
            // Verify that we threw as expected.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            DbDataReader nothing = null;
            readers[2] = new LabeledDbDataReader(nothing, new ShardLocation(_conn2.DataSource, _conn2.Database),
                new SqlCommand() { Connection = _conn2 });

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true))
            {
            }
        }

        /// <summary>
        /// Check that we throw as expected when trying to add a LabeledDataReader after the sharded data reader should be closed.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddDataReaderAfterShardedReaderIsClosed()
        {
            // What we're doing:
            // Set up a new sharded reader
            // Add two readers to it.
            // Close it.
            // Try to add a third reader to it.
            // Verify that we threw as expected.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            LabeledDbDataReader readerToAddAfterClose = GetReader(_conn3, selectSql);

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true, 5))
            {
                sdr.AddReader(readers[0]);
                sdr.AddReader(readers[1]);
                sdr.Close();
                Assert.IsTrue(sdr.IsClosed);
                ExpectException<MultiShardDataReaderInternalException, LabeledDbDataReader>(sdr.AddReader, readerToAddAfterClose);
            }
        }

        /// <summary>
        /// Check that we can successfully read from readers before all readers are added.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestReadsWhileReadersBeingAddedWithPartialResults()
        {
            string selectSql = "SELECT 1";

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, new LabeledDbDataReader[0], MultiShardExecutionPolicy.PartialResults, true, 1000))
            {
                for (int i = 0; i < 1000; i++)
                {
                    sdr.AddReader(GetReader(_conn1, selectSql));
                    Assert.IsTrue(sdr.Read(), "MultiShardReader did not pick up newly added readers.");
                }
            }
        }

        /// <summary>
        /// Check that we throw as expected when trying to add a closed LabeledDataReader
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddClosedDataReaderAfterShardedReader()
        {
            // The code below exposes a flaw in our current implementation related to
            // CompleteResults semantics and the internal c-tor.  The flaw does not
            // leak out to customers because the MultiShardCommand object manages the
            // necessary logic, but we need to patch the flaw so it doesn't end up
            // inadvertently leaking out to customers.
            // See VSTS 2616238 (i believe).  Philip will be modofying logic and
            // augmenting tests to deal with this issue.
            //


            // What we're doing:
            // Set up a new sharded reader
            // Add two readers to it.
            // Try to add a third closed reader to it.
            // Verify that we threw as expected.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            LabeledDbDataReader closedReaderToAdd = GetReader(_conn3, selectSql);

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true, 6))
            {
                sdr.AddReader(readers[0]);
                sdr.AddReader(readers[1]);
                closedReaderToAdd.DbDataReader.Close();
                Assert.IsTrue(closedReaderToAdd.DbDataReader.IsClosed, "labeledDataReader was not successfully closed.");
                sdr.AddReader(closedReaderToAdd);
                Assert.AreEqual(1, sdr.MultiShardExceptions.Count, "Adding a closed reader did not trigger the logging of an exception.");
                Assert.IsInstanceOfType(sdr.MultiShardExceptions.First().InnerException, typeof(MultiShardDataReaderClosedException), "The incorrect exception type was detected.");
            }
        }

        /// <summary>
        /// Check that we successfuly support the addition of readers after initial creation when the expected number of readers is greater than those provided.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddDataReaderWhileExpectingAdditionalReaders()
        {
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable WHERE Test_int_Field = 876";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[3];
            readers[0] = GetReader(_conn1, selectSql);
            readers[1] = GetReader(_conn2, selectSql);
            readers[2] = GetReader(_conn3, selectSql);

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true, 5))
            {
                sdr.AddReader(readers[0]);
                sdr.AddReader(readers[1]);
                ExpectException<MultiShardDataReaderInternalException, LabeledDbDataReader>(sdr.AddReader, readers[2]);
            }
        }

        /// <summary>
        /// Check that we successfuly support the asynchronous addition of readers while we are in the process of reading.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddDataReaderWhileReadingRows()
        {
            List<Tuple<int, DateTime>> readersAddedTimes = new List<Tuple<int, DateTime>>();
            List<Tuple<int, DateTime>> readersReadTimes = new List<Tuple<int, DateTime>>();
            List<SqlConnection> connections = new List<SqlConnection>() {_conn1, _conn2, _conn3};

            LabeledDbDataReader[] readers = new LabeledDbDataReader[0];

            using (MultiShardDataReader sdr = new MultiShardDataReader(
                    _dummyCommand,
                    readers,
                    MultiShardExecutionPolicy.CompleteResults,
                    addShardNamePseudoColumn: true,
                    expectedReaderCount: connections.Count))
            {
                Task.Factory.StartNew(() =>
                {
                    for (int readerIndex = 0; readerIndex < connections.Count; readerIndex++)
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(500));
                        readersAddedTimes.Add(new Tuple<int, DateTime>(readerIndex, DateTime.UtcNow));
                        sdr.AddReader(GetReader(connections[readerIndex], "select " + readerIndex.ToString()));
                    }
                });

                int i = 0;
                while (sdr.Read())
                {
                    readersReadTimes.Add(new Tuple<int, DateTime>(int.Parse(sdr[0].ToString()), DateTime.UtcNow));
                    i++;
                }

                foreach (Tuple<int, DateTime> tuple in readersAddedTimes)
                {
                    Trace.TraceInformation("Reader {0} was added at {1:O}", tuple.Item1, tuple.Item2);
                }
                foreach (Tuple<int, DateTime> tuple in readersReadTimes)
                {
                    Trace.TraceInformation("Reader {0} was read at {1:O}", tuple.Item1, tuple.Item2);
                }

                Assert.AreEqual(3, i, "Not all rows successfully returned.");
                foreach (bool happenedInOrder in readersAddedTimes.Zip(readersReadTimes, (x, y) => y.Item2 >= x.Item2))
                {
                    Assert.IsTrue(happenedInOrder,
                        "The next row was somehow able to be retrieved before its corresponding reader was added.");
                }
            }
        }

        /// <summary>
        /// Check that we successfuly support the asynchronous addition of readers while we are in the process of reading, when we start
        /// with some readers already added.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddDataReaderWhileReadingRowsWhenReadersAlreadyPresent()
        {
            List<Tuple<int, DateTime>> readersAddedTimes = new List<Tuple<int, DateTime>>();
            List<Tuple<int, DateTime>> readersReadTimes = new List<Tuple<int, DateTime>>();
            List<SqlConnection> connections = new List<SqlConnection>() { _conn1, _conn2, _conn3 };

            LabeledDbDataReader[] readers = new LabeledDbDataReader[1]
            {
                GetReader(connections[0], "SELECT 0")
            };

            using (MultiShardDataReader sdr = new MultiShardDataReader(
                _dummyCommand,
                readers,
                MultiShardExecutionPolicy.CompleteResults,
                addShardNamePseudoColumn: true,
                expectedReaderCount: connections.Count))
            {
                readersAddedTimes.Add(new Tuple<int, DateTime>(0, DateTime.UtcNow));

                Task.Factory.StartNew(() =>
                {
                    // First reader is already added, add two remaining asynchronously
                    for (int readerIndex = 1; readerIndex < connections.Count; readerIndex++)
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(500));
                        readersAddedTimes.Add(new Tuple<int, DateTime>(readerIndex, DateTime.UtcNow));
                        sdr.AddReader(GetReader(connections[readerIndex], "select " + readerIndex.ToString()));
                    }
                });

                int i = 0;
                while (sdr.Read())
                {
                    readersReadTimes.Add(new Tuple<int, DateTime>(int.Parse(sdr[0].ToString()), DateTime.UtcNow));
                    i++;
                }

                foreach (Tuple<int, DateTime> tuple in readersAddedTimes)
                {
                    Trace.TraceInformation("Reader {0} was added at {1:O}", tuple.Item1, tuple.Item2);
                }
                foreach (Tuple<int, DateTime> tuple in readersReadTimes)
                {
                    Trace.TraceInformation("Reader {0} was read at {1:O}", tuple.Item1, tuple.Item2);
                }

                Assert.AreEqual(3, i, "Not all rows successfully returned.");
                foreach (bool happenedInOrder in readersAddedTimes.Zip(readersReadTimes, (x, y) => y.Item2 >= x.Item2))
                {
                    Assert.IsTrue(happenedInOrder, "The next row was somehow able to be retrieved before its corresponding reader was added.");
                }
            }
        }

        /// <summary>
        /// Check that we wait a long time until we explicitly call ExpectNoMoreReaders
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddDataReaderWaitsALongTimeForExpectedReadersUntilExpectNoMoreReadersIsCalled()
        {
            string selectSql = "SELECT 1";
            LabeledDbDataReader[] readers = new LabeledDbDataReader[0];

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers, MultiShardExecutionPolicy.CompleteResults, true, 4))
            {
                // Launch a task that adds readers (but 1 too few). We expect 4, but we only add 3.
                sdr.AddReader(GetReader(_conn1, selectSql));
                sdr.AddReader(GetReader(_conn2, selectSql));
                sdr.AddReader(GetReader(_conn3, selectSql));
                bool readingCompleted = false;

                CancellationTokenSource ctSource = new CancellationTokenSource();

                // Launch a task that reads rows.
                Task readerTask = Task.Factory.StartNew(() =>
                    {
                        while (sdr.Read())
                        {
                        }
                        readingCompleted = true;
                    }, ctSource.Token);

                try
                {
                    while (readerTask.Status != TaskStatus.Canceled && readerTask.Status != TaskStatus.RanToCompletion && readerTask.Status != TaskStatus.Faulted)
                    {
                        Thread.Sleep(50);
                        sdr.ExpectNoMoreReaders();
                    }
                    Assert.IsTrue(readingCompleted, "The reader's read call did not return false after ExpectNoMoreReaders was called.");
                }
                finally
                {
                    ctSource.Cancel();
                }
            }
        }

        /// <summary>
        /// A little stress test that tries adding a few readers concurrently. Make sure no exceptions were thrown.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddDataReadersConcurrently()
        {
            string selectSql = "SELECT 1";
            LabeledDbDataReader[] empty = new LabeledDbDataReader[0];
            IEnumerable<LabeledDbDataReader> readers = _conns.Select((x) => GetReader(x, selectSql));

            using (MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, empty, MultiShardExecutionPolicy.CompleteResults, true, 3))
            {
                foreach (LabeledDbDataReader reader in readers)
                {
                    Task.Factory.StartNew(() =>
                    {
                        sdr.AddReader(reader);
                    });
                }

                int rowsRead = 0;
                while (sdr.Read())
                {
                    rowsRead++;
                }
                Assert.AreEqual(3, rowsRead, "Not all expected rows were read.");
            }
        }

#if NETFRAMEWORK
        /// <summary>
        /// Check that we throw as expected when trying to call CreateObjRef.
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(RemotingException))]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestCreateObjRefFails()
        {
            // What we're doing:
            // Set up a new sharded reader
            // Try to call CreateObjRef on it.
            // Verify that we threw as expected.
            //
            string selectSql = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";

            using (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql))
            {
                sdr.CreateObjRef(typeof(MultiShardDataReader));
            }
        }
#endif

        /// <summary>
        /// Check that we can iterate through the result sets as expected comparing all the values
        /// returned from the getters plus some of the properties.
        /// Check everythign both with and without the $ShardName pseudo column.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestGettersPositiveCases()
        {
            TestGettersPositiveCasesHelper(true);
            TestGettersPositiveCasesHelper(false);
        }

        /// <summary>
        /// Check that we can iterate through the result sets as expected comparing all the values
        /// returned from the getters plus some of the properties.
        /// </summary>
        private void TestGettersPositiveCasesHelper(bool includeShardNamePseudoColumn)
        {
            // What we're doing:
            // Grab all rows from each test database.
            // Load them into a MultiShardDataReader.
            // Iterate through the rows and make sure that we have 9 total.
            // Also iterate through all columns and make sure that the getters that should work do work.
            //
            IReadOnlyList<MutliShardTestCaseColumn> toCheck = MutliShardTestCaseColumn.DefinedColumns;
            MutliShardTestCaseColumn pseudoColumn = MutliShardTestCaseColumn.ShardNamePseudoColumn;

            foreach (MutliShardTestCaseColumn curCol in toCheck)
            {
                string selectSql = string.Format("SELECT {0} FROM ConsistentShardedTable", curCol.TestColumnName);

                using (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql, includeShardNamePseudoColumn))
                {
                    int recordsRetrieved = 0;
                    Logger.Log("Starting to get records");
                    while (sdr.Read())
                    {
                        int expectedFieldCount = includeShardNamePseudoColumn ? 2 : 1; // 2 columns if we have the shard name, 1 column if not.
                        Assert.AreEqual(expectedFieldCount, sdr.FieldCount);
                        Assert.AreEqual(expectedFieldCount, sdr.VisibleFieldCount);

                        recordsRetrieved++;

                        // Do verification for the test column.
                        //
                        CheckDataTypeName(sdr, curCol, 0);
                        CheckColumnName(sdr, curCol, 0);
                        VerifyAllGettersPositiveCases(sdr, curCol, 0);

                        // Then also do it for the $ShardName PseudoColumn if necessary.
                        //
                        if (includeShardNamePseudoColumn)
                        {
                            CheckDataTypeName(sdr, pseudoColumn, 1);
                            CheckColumnName(sdr, pseudoColumn, 1);
                            VerifyAllGettersPositiveCases(sdr, pseudoColumn, 1);
                        }
                    }

                    sdr.Close();

                    Assert.AreEqual(recordsRetrieved, 9);
                }
            }
        }

        /// <summary>
        /// Test what happens when we try to get a value without calling read first.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestBadlyPlacedGetValueCalls()
        {
            // What we're doing:
            // Set up a new sharded reader
            // Try to get a value without calling read first and see what happens.
            // Should throw.
            //
            string selectSql = "SELECT 1";
            using (MultiShardDataReader sdr = GetShardedDbReader(_shardConnection, selectSql))
            {
                ExpectException<InvalidOperationException>(sdr.GetValue, 0);

                while (sdr.Read())
                {
                    sdr.GetValue(0);
                }

                ExpectException<InvalidOperationException>(sdr.GetValue, 0);

                sdr.Close();

                ExpectException<MultiShardDataReaderClosedException>(sdr.GetValue, 0);

                // And try to close it again.
                //
                sdr.Close();
            }
        }

#region Helpers

        private void ExpectException<T>(Func<int, object> func, int ordinal) where T : Exception
        {
            try
            {
                func(ordinal);
                Assert.Fail(string.Format("Should have hit {0}.", typeof(T)));
            }
            catch (T)
            {
            }
        }

        private void ExpectException<T, U>(Action<U> func, U input) where T : Exception
        {
            try
            {
                func(input);
                Assert.Fail(string.Format("Should have hit {0}.", typeof(T)));
            }
            catch (T)
            {
            }
        }

        private void CheckColumnName(MultiShardDataReader reader, MutliShardTestCaseColumn column, int ordinal)
        {
            Assert.AreEqual(column.TestColumnName, reader.GetName(ordinal));
            Assert.AreEqual(ordinal, reader.GetOrdinal(column.TestColumnName));
        }

        private void CheckDataTypeName(MultiShardDataReader reader, MutliShardTestCaseColumn column, int ordinal)
        {
            // Not happy about this hack for numeric, but not sure how else to deal with it.
            //
            if (column.SqlServerDatabaseEngineType.Equals("numeric"))
            {
                Assert.AreEqual(reader.GetDataTypeName(ordinal), "decimal");
            }
            else
            {
                Assert.AreEqual(reader.GetDataTypeName(ordinal), column.SqlServerDatabaseEngineType);
            }
        }

        private void VerifyAllGettersPositiveCases(MultiShardDataReader reader, MutliShardTestCaseColumn column, int ordinal)
        {
            // General pattern here:
            // Grab the value through the regular getter, through the getValue,
            // through the sync GetFieldValue, and through the async GetFieldValue to ensure we are
            // getting back the same thing from all calls.
            //
            // Then grab through the Sql getter to make sure it works. (should we compare again?)
            //
            // Then verify that the field types are as we expect.
            //
            // Note: For the array-based getters we can't do the sync/async comparison.
            //

            // These are indexes into our .NET type array.
            //
            int ValueResult = 0;
            int ItemOrdinalResult = 1;
            int ItemNameResult = 2;
            int GetResult = 3;
            int SyncResult = 4;
            int AsyncResult = 5;
            object[] results = new object[AsyncResult + 1];

            // These first three we can pull consistently for all fields.
            // The rest have type specific getters.
            //
            results[ValueResult] = reader.GetValue(ordinal);
            results[ItemOrdinalResult] = reader[ordinal];
            results[ItemNameResult] = reader[column.TestColumnName];


            // And these are indexes into our SQL type array.
            //
            int SqlValueResult = 0;
            int SqlGetResult = 1;
            object[] sqlResults = new object[SqlGetResult + 1];

            sqlResults[SqlValueResult] = reader.GetSqlValue(ordinal);

            switch (column.DbType)
            {
                case SqlDbType.BigInt:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetInt64(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Int64>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Int64>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlInt64(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Int64));
                    }
                    break;
                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.VarBinary:
                    if (!reader.IsDBNull(ordinal))
                    {
                        // Do the bytes and the stream.  Can also compare them.
                        //
                        Byte[] byteBuffer = new Byte[column.FieldLength];
                        reader.GetBytes(ordinal, 0, byteBuffer, 0, column.FieldLength);

                        Stream theStream = reader.GetStream(ordinal);
                        Assert.AreEqual(theStream.Length, column.FieldLength);
                        Byte[] byteBufferFromStream = new Byte[column.FieldLength];

                        int bytesRead = theStream.Read(byteBufferFromStream, 0, column.FieldLength);
                        PerformArrayComparison<Byte>(byteBuffer, byteBufferFromStream);

                        // The value getter comes through as a SqlBinary, so we don't pull
                        // the Sql getter here.
                        //
                        reader.GetSqlBytes(ordinal);

                        sqlResults[SqlGetResult] = reader.GetSqlBinary(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Byte[]));
                    }
                    break;
                case SqlDbType.Bit:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetBoolean(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Boolean>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Boolean>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlBoolean(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Boolean));
                    }
                    break;
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                    if (!reader.IsDBNull(ordinal))
                    {
                        Char[] charBuffer = new Char[column.FieldLength];
                        long bufferLength = reader.GetChars(ordinal, 0, charBuffer, 0, column.FieldLength);
                        charBuffer = new Char[bufferLength];  //size it right for the string compare below.
                        reader.GetChars(ordinal, 0, charBuffer, 0, column.FieldLength);

                        // The value getter comes through as a SqlString, so we
                        // don't pull the Sql getter here.
                        //
                        reader.GetSqlChars(ordinal);

                        results[GetResult] = reader.GetString(ordinal);
                        results[SyncResult] = reader.GetFieldValue<string>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<string>(ordinal).Result;
                        AssertAllAreEqual(results);

                        // Also compare the string result to our char result.
                        //
                        PerformArrayComparison<Char>(charBuffer, reader.GetString(ordinal).Trim().ToCharArray());

                        // and get a text reader.
                        //
                        string fromTr = reader.GetTextReader(ordinal).ReadToEnd();
                        Assert.AreEqual(fromTr, results[GetResult]);

                        sqlResults[SqlGetResult] = reader.GetSqlString(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(string));
                    }
                    break;
                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetDateTime(ordinal);
                        results[SyncResult] = reader.GetFieldValue<DateTime>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<DateTime>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlDateTime(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(DateTime));
                    }
                    break;
                case SqlDbType.Date:  // NOTE: docs say this can come back via SqlDateTime, but apparently it can't.
                case SqlDbType.DateTime2:
                    // differs from above in the sql specific Getter.
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetDateTime(ordinal);
                        results[SyncResult] = reader.GetFieldValue<DateTime>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<DateTime>(ordinal).Result;
                        AssertAllAreEqual(results);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(DateTime));
                    }
                    break;
                case SqlDbType.DateTimeOffset:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetDateTimeOffset(ordinal);
                        results[SyncResult] = reader.GetFieldValue<DateTimeOffset>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<DateTimeOffset>(ordinal).Result;
                        AssertAllAreEqual(results);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(DateTimeOffset));
                    }
                    break;
                case SqlDbType.Decimal:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetDecimal(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Decimal>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Decimal>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlDecimal(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Decimal));
                    }
                    break;
                case SqlDbType.Float:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetDouble(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Double>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Double>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlDouble(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Double));
                    }
                    break;
                case SqlDbType.Int:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetInt32(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Int32>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Int32>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlInt32(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Int32));
                    }
                    break;
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetDecimal(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Decimal>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Decimal>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlMoney(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Decimal));
                    }
                    break;
                case SqlDbType.Real:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetFloat(ordinal);
                        results[SyncResult] = reader.GetFieldValue<float>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<float>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlSingle(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(float));
                    }
                    break;
                case SqlDbType.SmallInt:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetInt16(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Int16>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Int16>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlInt16(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Int16));
                    }
                    break;
                case SqlDbType.Time: // NOTE: docs say this can come back via GetDateTime, but apparently it can't.
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetTimeSpan(ordinal);
                        results[SyncResult] = reader.GetFieldValue<TimeSpan>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<TimeSpan>(ordinal).Result;
                        AssertAllAreEqual(results);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(TimeSpan));
                    }
                    break;
                case SqlDbType.Timestamp:
                    if (!reader.IsDBNull(ordinal))
                    {
                        // Differs from the other binaries in that it doesn't
                        // support GetStream.
                        //
                        Byte[] byteBuffer = new Byte[column.FieldLength];
                        reader.GetBytes(ordinal, 0, byteBuffer, 0, column.FieldLength);

                        // The value getter comes through as a SqlBinary, so we don't pull
                        // the Sql getter here.
                        //
                        reader.GetSqlBytes(ordinal);

                        sqlResults[SqlGetResult] = reader.GetSqlBinary(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Byte[]));
                    }
                    break;
                case SqlDbType.TinyInt:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetByte(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Byte>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Byte>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlByte(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Byte));
                    }
                    break;
                case SqlDbType.UniqueIdentifier:
                    if (!reader.IsDBNull(ordinal))
                    {
                        results[GetResult] = reader.GetGuid(ordinal);
                        results[SyncResult] = reader.GetFieldValue<Guid>(ordinal);
                        results[AsyncResult] = reader.GetFieldValueAsync<Guid>(ordinal).Result;
                        AssertAllAreEqual(results);

                        sqlResults[SqlGetResult] = reader.GetSqlGuid(ordinal);
                        AssertAllAreEqual(sqlResults);

                        Assert.AreEqual(reader.GetFieldType(ordinal), typeof(Guid));
                    }
                    break;
                default:
                    throw new ArgumentException(column.DbType.ToString());
            }
        }

        private void AssertAllAreEqual(object[] toCheck)
        {
            object baseline = toCheck[0];
            foreach (object curObject in toCheck)
            {
                Assert.AreEqual(baseline, curObject);
            }
        }

        private void PerformArrayComparison<T>(T[] first, T[] second)
        {
            Assert.AreEqual(first.Length, second.Length);
            for (int i = 0; i < first.Length; i++)
            {
                Assert.AreEqual(first[i], second[i]);
            }
        }

        /// <summary>
        /// Gets a SqlDataReader by executing the passed in t-sql over the passed in connection.
        /// </summary>
        /// <param name="conn">Connection to the database we wish to execute the t-sql against.</param>
        /// <param name="tsql">The t-sql to execute.</param>
        /// <returns>The SqlDataReader obtained by executin the passed in t-sql over the passed in connection.</returns>
        private LabeledDbDataReader GetReader(DbConnection conn, string tsql)
        {
            string label = GetLabel(conn);
            if (conn.State == ConnectionState.Open)
            {
                conn.Close();
            }
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
            }
            DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = tsql;
            DbDataReader sdr = cmd.ExecuteReader();
            return new LabeledDbDataReader(sdr, new ShardLocation(cmd.Connection.DataSource, cmd.Connection.Database), new MockSqlCommand() { Connection = conn });
        }

        /// <summary>
        /// Gets a shard label based on the datasource and database from a connection;
        /// </summary>
        /// <param name="conn">The connection to pull the datasource/database information from.</param>
        /// <returns>The label of the form 'datasource' ; 'database'.</returns>
        private string GetLabel(DbConnection conn)
        {
            return string.Format("{0} ; {1}", conn.DataSource, conn.Database);
        }

        /// <summary>
        /// Helper that grabs a MultiShardDataReader based on a MultiShardConnection and a tsql string to execute.
        /// </summary>
        /// <param name="conn">The MultiShardConnection to use to get the command/reader.</param>
        /// <param name="tsql">The tsql to execute on the shards.</param>
        /// <returns>The MultiShardDataReader resulting from executing the given tsql on the given connection.</returns>
        private MultiShardDataReader GetShardedDbReader(MultiShardConnection conn, string tsql)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = tsql;
            cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
            return cmd.ExecuteReader();
        }

        /// <summary>
        /// Helper that grabs a MultiShardDataReader based on a MultiShardConnection and a tsql string to execute. This is different
        /// from the GetShardedDbReader method in that it uses ExecuteReaderAsync() API under the hood and is cancellable.
        /// </summary>
        /// <param name="conn">The MultiShardConnection to use to get the command/reader.</param>
        /// <param name="tsql">The tsql to execute on the shards.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>The MultiShardDataReader resulting from executing the given tsql on the given connection.</returns>
        private MultiShardDataReader GetShardedDbReaderAsync(MultiShardConnection conn, string tsql, CancellationToken cancellationToken)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = tsql;
            cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
            return cmd.ExecuteReaderAsync(cancellationToken).Result;
        }

        private MultiShardDataReader GetShardedDbReader(MultiShardConnection conn, string tsql, bool includeShardName)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = tsql;
            cmd.ExecutionOptions = includeShardName ? MultiShardExecutionOptions.IncludeShardNameColumn :
                MultiShardExecutionOptions.None;
            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
            return cmd.ExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        /// Helper method that sets up a MultiShardDataReader based on the given DbDataReaders so that
        /// the MultiShardDataReader is ready to use.
        /// </summary>
        /// <param name="readers">The DbDataReaders that will underlie this MultiShardDataReader.</param>
        /// <param name="exceptions">
        /// Populated with any SchemaMismatchExceptions encountered while setting up the MultiShardDataReader.
        /// </param>
        /// <param name="addShardNamePseudoColumn">True if we should add the $ShardName pseudo column, false if not.</param>
        /// <returns>A new MultiShardDataReader object that is ready to use.</returns>
        /// <remarks>
        /// Note that normally this setup and marking as complete would be hidden from the client (inside
        /// the MultiShardCommand), but since we are doing unit testing at a lower level than the command
        /// we need to perform it ourselves here.
        /// </remarks>
        private MultiShardDataReader GetMultiShardDataReaderFromDbDataReaders(LabeledDbDataReader[] readers, out IList<MultiShardSchemaMismatchException> exceptions, bool addShardNamePseudoColumn)
        {
            exceptions = new List<MultiShardSchemaMismatchException>();

            MultiShardDataReader sdr = new MultiShardDataReader(_dummyCommand, readers,
                MultiShardExecutionPolicy.PartialResults, addShardNamePseudoColumn);

            foreach (var exception in sdr.MultiShardExceptions)
            {
                exceptions.Add((MultiShardSchemaMismatchException)exception);
            }

            return sdr;
        }

#endregion Helpers
    }
}
