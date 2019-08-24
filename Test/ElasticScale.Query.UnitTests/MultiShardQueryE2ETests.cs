// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
// Basic End-To-End test scenarios for the cross shard query client library
//
// Notes:
// Tests currently assume there's a running sqlservr instance.
// * Everything will be automated once we integrate with the larger framework.
// * Currently the tests use the same methods to create shards as MultiShardDataReaderTests

using System.Diagnostics;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    /// <summary>
    /// Tests for end to end scenarios where a user
    /// connects to his shards, executes commands against them
    /// and receives results
    /// </summary>
    [TestClass]
    public class MultiShardQueryE2ETests
    {
        #region Global vars

        private TestContext _testContextInstance;

        /// <summary>
        /// Handle on connections to all shards
        /// </summary>
        private MultiShardConnection _shardConnection;

        /// <summary>
        /// Handle to the ShardMap with our Test databases.
        /// </summary>
        private ShardMap _shardMap;

        #endregion

        #region Boilerplate

        /// <summary>
        /// Gets or sets the test context which provides
        /// information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return _testContextInstance;
            }
            set
            {
                _testContextInstance = value;
            }
        }

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
            _shardMap = MultiShardTestUtils.CreateAndGetTestShardMap();

            _shardConnection = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString);
        }

        /// <summary>
        /// Close our connections to each test database after each test.
        /// </summary>
        [TestCleanup()]
        public void MyTestCleanup()
        {
            // Close connections after each test.
            //
            _shardConnection.Dispose();
        }

        #endregion

        /// <summary>
        /// Check that we can iterate through 3 result sets as expected.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSimpleSelect_PartialResults()
        {
            TestSimpleSelect(MultiShardExecutionPolicy.PartialResults);
        }

        /// <summary>
        /// Check that we can iterate through 3 result sets as expected.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSimpleSelect_CompleteResults()
        {
            TestSimpleSelect(MultiShardExecutionPolicy.CompleteResults);
        }

        public void TestSimpleSelect(MultiShardExecutionPolicy policy)
        {
            // What we're doing:
            // Grab all rows from each test database.
            // Load them into a MultiShardDataReader.
            // Iterate through the rows and make sure that we have 9 total.
            //
            using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
            {
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field FROM ConsistentShardedTable";
                    cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
                    cmd.ExecutionPolicy = policy;

                    using (MultiShardDataReader sdr = cmd.ExecuteReader())
                    {
                        int recordsRetrieved = 0;
                        Logger.Log("Starting to get records");
                        while (sdr.Read())
                        {
                            recordsRetrieved++;
                            string dbNameField = sdr.GetString(0);
                            int testIntField = sdr.GetFieldValue<int>(1);
                            Int64 testBigIntField = sdr.GetFieldValue<Int64>(2);
                            string shardIdPseudoColumn = sdr.GetFieldValue<string>(3);
                            string logRecord =
                                string.Format(
                                    "RecordRetrieved: dbNameField: {0}, TestIntField: {1}, TestBigIntField: {2}, shardIdPseudoColumnField: {3}, RecordCount: {4}",
                                    dbNameField, testIntField, testBigIntField, shardIdPseudoColumn, recordsRetrieved);
                            Logger.Log(logRecord);
                            Debug.WriteLine(logRecord);
                        }

                        sdr.Close();

                        Assert.AreEqual(recordsRetrieved, 9);
                    }
                }
            }
        }


        /// <summary>
        /// Check that we can return an empty result set that has a schema table
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_NoRows_CompleteResults()
        {
            TestSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.CompleteResults);
        }

        /// <summary>
        /// Check that we can return an empty result set that has a schema table
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_NoRows_PartialResults()
        {
            TestSelectNoRows("select 1 where 0 = 1", MultiShardExecutionPolicy.PartialResults);
        }

        public void TestSelectNoRows(string commandText, MultiShardExecutionPolicy policy)
        {
            using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
            {
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = commandText;
                    cmd.ExecutionPolicy = policy;

                    // Read first
                    using (MultiShardDataReader sdr = cmd.ExecuteReader())
                    {
                        Assert.AreEqual(0, sdr.MultiShardExceptions.Count);
                        while (sdr.Read())
                        {
                            Assert.Fail("Should not have gotten any records.");
                        }
                        Assert.IsFalse(sdr.HasRows);
                    }

                    // HasRows first
                    using (MultiShardDataReader sdr = cmd.ExecuteReader())
                    {
                        Assert.AreEqual(0, sdr.MultiShardExceptions.Count);
                        Assert.IsFalse(sdr.HasRows);
                        while (sdr.Read())
                        {
                            Assert.Fail("Should not have gotten any records.");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Check that we can return an empty result set that does not have a schema table
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_NonQuery_CompleteResults()
        {
            TestSelectNonQuery("if (0 = 1) select 1 ", MultiShardExecutionPolicy.CompleteResults);
        }


        /// <summary>
        /// Check that we can return a completely empty result set as expected.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_NonQuery_PartialResults()
        {
            TestSelectNonQuery("if (0 = 1) select 1", MultiShardExecutionPolicy.PartialResults);
        }

        public void TestSelectNonQuery(string commandText, MultiShardExecutionPolicy policy)
        {
            using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
            {
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = commandText;
                    cmd.ExecutionPolicy = policy;

                    using (MultiShardDataReader sdr = cmd.ExecuteReader())
                    {
                        Assert.AreEqual(0, sdr.MultiShardExceptions.Count);

                        // TODO: This is a weird error message, but it's good enough for now
                        // Fixing this will require significant refactoring of MultiShardDataReader,
                        // we should fix it when we finish implementing async adding of child readers
                        AssertExtensions.AssertThrows<MultiShardDataReaderClosedException>(() => sdr.Read());
                    }
                }
            }
        }

        /// <summary>
        /// Check that ExecuteReader throws when all shards have an exception
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_Failure_PartialResults()
        {
            MultiShardAggregateException e = TestSelectFailure(
                "raiserror('blah', 16, 0)",
                MultiShardExecutionPolicy.PartialResults);

            // All children should have failed
            Assert.AreEqual(_shardMap.GetShards().Count(), e.InnerExceptions.Count);
        }

        /// <summary>
        /// Check that ExecuteReader throws when all shards have an exception
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_Failure_CompleteResults()
        {
            MultiShardAggregateException e = TestSelectFailure(
                "raiserror('blah', 16, 0)",
                MultiShardExecutionPolicy.CompleteResults);

            // We don't know exactly how many child exceptions will happen, because the
            // first exception that is seen will cause the children to be canceled.
            AssertExtensions.AssertGreaterThanOrEqualTo(1, e.InnerExceptions.Count);
            AssertExtensions.AssertLessThanOrEqualTo(_shardMap.GetShards().Count(), e.InnerExceptions.Count);
        }

        public MultiShardAggregateException TestSelectFailure(string commandText, MultiShardExecutionPolicy policy)
        {
            using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
            {
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = commandText;
                    cmd.ExecutionPolicy = policy;

                    // ExecuteReader should fail
                    MultiShardAggregateException aggregateException =
                        AssertExtensions.AssertThrows<MultiShardAggregateException>(() => cmd.ExecuteReader());

                    // Sanity check the exceptions are the correct type
                    foreach (Exception e in aggregateException.InnerExceptions)
                    {
                        Assert.IsInstanceOfType(e, typeof(MultiShardException));
                        Assert.IsInstanceOfType(e.InnerException, typeof(SqlException));
                    }

                    // Return the exception so that the caller can do additional validation
                    return aggregateException;
                }
            }
        }

        /// <summary>
        /// Check that we can return a partially succeeded reader when PartialResults policy is on
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_PartialFailure_PartialResults()
        {
            using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
            {
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = GetPartialFailureQuery();
                    cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;

                    using (MultiShardDataReader sdr = cmd.ExecuteReader())
                    {
                        // Exactly one should have failed
                        Assert.AreEqual(1, sdr.MultiShardExceptions.Count);

                        // We should be able to read
                        while (sdr.Read())
                        {
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Check that we fail a partially successful command when CompleteResults policy is on
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSelect_PartialFailure_CompleteResults()
        {
            string query = GetPartialFailureQuery();
            MultiShardAggregateException e = TestSelectFailure(query, MultiShardExecutionPolicy.CompleteResults);

            // Exactly one should have failed
            Assert.AreEqual(1, e.InnerExceptions.Count);
        }

        /// <summary>
        /// Gets a command that fails on one shard, but succeeds on others
        /// </summary>
        private string GetPartialFailureQuery()
        {
            IEnumerable<ShardLocation> shardLocations = _shardMap.GetShards().Select(s => s.Location);

            // Pick an arbitrary one of those shards
            ShardLocation chosenShardLocation = shardLocations.First();

            // This query assumes that the chosen shard location's db name is distinct from all others
            // In other words, only one shard location should have a database equal to the chosen location
            Assert.AreEqual(1, shardLocations.Count(l => l.Database.Equals(chosenShardLocation.Database)));

            // We also assume that there is more than one shard
            AssertExtensions.AssertGreaterThan(1, shardLocations.Count());

            // The command will fail only on the chosen shard
            return string.Format("if db_name() = '{0}' raiserror('blah', 16, 0) else select 1",
                                 shardLocations.First().Database);
        }

        /// <summary>
        /// Basic test for async api(s)
        /// Also demonstrates the async pattern of this library
        /// The Sync api is implicitly tested in MultiShardDataReaderTests::TestSimpleSelect
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestQueryShardsAsync()
        {
            // Create new sharded connection so we can test the OpenAsync call as well.
            //
            using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
            {
                using (MultiShardCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
                    cmd.CommandType = CommandType.Text;

                    using (MultiShardDataReader sdr = ExecAsync(conn, cmd).Result)
                    {
                        int recordsRetrieved = 0;
                        while (sdr.Read())
                        {
                            recordsRetrieved++;
                            var dbNameField = sdr.GetString(0);
                            var testIntField = sdr.GetFieldValue<int>(1);
                            var testBigIntField = sdr.GetFieldValue<Int64>(2);
                            Logger.Log("RecordRetrieved: dbNameField: {0}, TestIntField: {1}, TestBigIntField: {2}, RecordCount: {3}",
                                dbNameField, testIntField, testBigIntField, recordsRetrieved);
                        }

                        Assert.AreEqual(recordsRetrieved, 9);
                    }
                }
            }
        }

        /// <summary>
        /// Basic test for ensuring that we include/don't include the $ShardName pseudo column as desired.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardNamePseudoColumnOption()
        {
            bool[] pseudoColumnOptions = new bool[2];
            pseudoColumnOptions[0] = true;
            pseudoColumnOptions[1] = false;

            // do the loop over the options.
            // add the excpetion handling when referencing the pseudo column
            //
            foreach (bool pseudoColumnPresent in pseudoColumnOptions)
            {
                using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
                {
                    using (MultiShardCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
                        cmd.CommandType = CommandType.Text;

                        cmd.ExecutionPolicy = MultiShardExecutionPolicy.CompleteResults;
                        cmd.ExecutionOptions = pseudoColumnPresent ? MultiShardExecutionOptions.IncludeShardNameColumn :
                            MultiShardExecutionOptions.None;
                        using (MultiShardDataReader sdr = cmd.ExecuteReader(CommandBehavior.Default))
                        {
                            Assert.AreEqual(0, sdr.MultiShardExceptions.Count);

                            int recordsRetrieved = 0;

                            int expectedFieldCount = pseudoColumnPresent ? 4 : 3;
                            int expectedVisibleFieldCount = pseudoColumnPresent ? 4 : 3;
                            Assert.AreEqual(expectedFieldCount, sdr.FieldCount);
                            Assert.AreEqual(expectedVisibleFieldCount, sdr.VisibleFieldCount);

                            while (sdr.Read())
                            {
                                recordsRetrieved++;
                                var dbNameField = sdr.GetString(0);
                                var testIntField = sdr.GetFieldValue<int>(1);
                                var testBigIntField = sdr.GetFieldValue<Int64>(2);

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

                            Assert.AreEqual(recordsRetrieved, 9);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Basic test for ensuring that we don't fail due to a schema mismatch on the shards.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSchemaMismatchErrorPropagation()
        {
            // First we need to alter the schema on one of the shards - we'll choose the last one.
            //
            string origColName = "Test_bigint_Field";
            string newColName = "ModifiedName";

            MultiShardTestUtils.ChangeColumnNameOnShardedTable(2, origColName, newColName);

            // Then create new sharded connection so we can test the error handling logic.
            // We'll wrap this all in a try-catch-finally block so that we can change the schema back
            // to what the other tests will expect it to be in the finally.
            //
            try
            {
                using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
                {
                    using (MultiShardCommand cmd = conn.CreateCommand())
                    {
                        // Need to do a SELECT * in order to get the column name error as a schema mismatcherror.  If we name it explicitly
                        // we will get a command execution error instead.
                        //
                        cmd.CommandText = "SELECT * FROM ConsistentShardedTable";
                        cmd.CommandType = CommandType.Text;

                        using (MultiShardDataReader sdr = ExecAsync(conn, cmd).Result)
                        {
                            // The number of errors we have depends on which shard executed first.
                            // So, we know it should be 1 OR 2.
                            //
                            Assert.IsTrue(
                                ((sdr.MultiShardExceptions.Count == 1) || (sdr.MultiShardExceptions.Count == 2)),
                                string.Format("Expected 1 or 2 execution erros, but saw {0}", sdr.MultiShardExceptions.Count));

                            int recordsRetrieved = 0;
                            while (sdr.Read())
                            {
                                recordsRetrieved++;
                                var dbNameField = sdr.GetString(0);
                            }

                            // We should see 9 records less 3 for each one that had a schema error.
                            int expectedRecords = ((9 - (3 * sdr.MultiShardExceptions.Count)));

                            Assert.AreEqual(recordsRetrieved, expectedRecords);
                        }
                    }
                }
            }
            finally
            {
                MultiShardTestUtils.ChangeColumnNameOnShardedTable(2, newColName, origColName);
            }
        }

        private async Task<MultiShardDataReader> ExecAsync(MultiShardConnection conn, MultiShardCommand cmd)
        {
            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
            cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;

            return await cmd.ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);
        }

        /// <summary>
        /// Try connecting to a non-existant shard
        /// Verify exception is propagated to the user
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(SqlException))]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestQueryShardsInvalidConnectionSync()
        {
            var badShard = new ShardLocation("badLocation", "badDatabase");
            var bldr = new SqlConnectionStringBuilder();
            bldr.DataSource = badShard.DataSource;
            bldr.InitialCatalog = badShard.Database;
            var badConn = new SqlConnection(bldr.ConnectionString);
            try
            {
                using (var conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
                {
                    conn.GetShardConnections().Add(new Tuple<ShardLocation, DbConnection>(badShard,
                        badConn));
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "select 1";
                        cmd.ExecuteReader();
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is MultiShardAggregateException)
                {
                    var maex = (MultiShardAggregateException)ex;
                    Logger.Log("Exception encountered: " + maex.ToString());
                    throw ((MultiShardException)(maex.InnerException)).InnerException;
                }
                throw;
            }
        }

        /// <summary>
        /// Tests passing a tvp as a param
        /// using a datatable
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestQueryShardsTvpParam()
        {
            try
            {
                // Install schema
                string createTbl =
@"
                CREATE TABLE dbo.PageView
(
    PageViewID BIGINT NOT NULL,
    PageViewCount BIGINT NOT NULL
);
CREATE TYPE dbo.PageViewTableType AS TABLE
(
    PageViewID BIGINT NOT NULL
);";
                string createProc =
@"CREATE PROCEDURE dbo.procMergePageView
    @Display dbo.PageViewTableType READONLY
AS
BEGIN
    MERGE INTO dbo.PageView AS T
    USING @Display AS S
    ON T.PageViewID = S.PageViewID
    WHEN MATCHED THEN UPDATE SET T.PageViewCount = T.PageViewCount + 1
    WHEN NOT MATCHED THEN INSERT VALUES(S.PageViewID, 1);
END";
                using (var cmd = _shardConnection.CreateCommand())
                {
                    cmd.CommandText = createTbl;
                    cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
                    cmd.ExecuteReader();

                    cmd.CommandText = createProc;
                    cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults).Wait();
                }

                Logger.Log("Schema installed..");

                // Create the data table
                DataTable table = new DataTable();
                table.Columns.Add("PageViewID", typeof(long));
                int idCount = 3;
                for (int i = 0; i < idCount; i++)
                {
                    table.Rows.Add(i);
                }

                // Execute the command
                using (var cmd = _shardConnection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "dbo.procMergePageView";

                    var param = new SqlParameter("@Display", table);
                    param.TypeName = "dbo.PageViewTableType";
                    param.SqlDbType = SqlDbType.Structured;
                    cmd.Parameters.Add(param);

                    cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults).Wait();
                    cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults).Wait();
                }

                Logger.Log("Command executed..");

                using (var cmd = _shardConnection.CreateCommand())
                {
                    // Validate that the pageviewcount was updated
                    cmd.CommandText = "select PageViewCount from PageView";
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
                    cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;
                    using (var sdr = cmd.ExecuteReader(CommandBehavior.Default))
                    {
                        while (sdr.Read())
                        {
                            long pageCount = (long)sdr["PageViewCount"];
                            Logger.Log("Page view count: {0} obtained from shard: {1}", pageCount, sdr.GetFieldValue<string>(1));
                            Assert.AreEqual(2, pageCount);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    var aex = (AggregateException)ex;
                    Logger.Log("Exception encountered: {0}", aex.InnerException.ToString());
                }
                else
                {
                    Logger.Log(ex.Message);
                }
                throw;
            }
            finally
            {
                string dropSchema =
@"if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[procMergePageView]') and objectproperty(id, N'IsProcedure') = 1)
begin
drop procedure dbo.procMergePageView
end
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[Pageview]'))
begin
drop table dbo.Pageview
end
if exists (select * from sys.types where name = 'PageViewTableType')
begin
drop type dbo.PageViewTableType
end";
                using (var cmd = _shardConnection.CreateCommand())
                {
                    cmd.CommandText = dropSchema;
                    cmd.ExecuteNonQueryAsync(CancellationToken.None, MultiShardExecutionPolicy.PartialResults).Wait();
                }
            }
        }

        /// <summary>
        /// Verifies that the command cancellation events are fired
        /// upon cancellation of a command that is in progress
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestQueryShardsCommandCancellationHandler()
        {
            List<ShardLocation> cancelledShards = new List<ShardLocation>();
            CancellationTokenSource cts = new CancellationTokenSource();

            using (MultiShardCommand cmd = _shardConnection.CreateCommand())
            {
                Barrier barrier = new Barrier(cmd.Connection.Shards.Count() + 1);

                // If the threads don't meet the barrier by this time, then give up and fail the test
                TimeSpan barrierTimeout = TimeSpan.FromSeconds(10);

                cmd.CommandText = "WAITFOR DELAY '00:01:00'";
                cmd.CommandTimeoutPerShard = 12;

                cmd.ShardExecutionCanceled += (obj, args) =>
                {
                    cancelledShards.Add(args.ShardLocation);
                };

                cmd.ShardExecutionBegan += (obj, args) =>
                {
                    // If ShardExecutionBegan were only signaled by one thread,
                    // then this would hang forever.
                    barrier.SignalAndWait(barrierTimeout);
                };

                Task cmdTask = cmd.ExecuteReaderAsync(cts.Token);

                bool syncronized = barrier.SignalAndWait(barrierTimeout);
                Assert.IsTrue(syncronized);

                // Cancel the command once execution begins
                // Sleeps are bad but this is just to really make sure
                // sqlclient has had a chance to begin command execution
                // Will not effect the test outcome
                Thread.Sleep(TimeSpan.FromSeconds(1));
                cts.Cancel();

                // Validate that the task was cancelled
                AssertExtensions.WaitAndAssertThrows<TaskCanceledException>(cmdTask);

                // Validate that the cancellation event was fired for all shards
                List<ShardLocation> allShards = _shardConnection.GetShardConnections().Select(l => l.Item1).ToList();
                CollectionAssert.AreEquivalent(allShards, cancelledShards, "Expected command canceled event to be fired for all shards!");
            }
        }

        /// <summary>
        /// Close the connection to one of the shards behind
        /// MultiShardConnection's back. Verify that we reopen the connection with the built-in retry policy
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestQueryShardsInvalidShardStateSync()
        {
            // Get a shard and close it's connection
            var shardSqlConnections = _shardConnection.GetShardConnections();
            shardSqlConnections[1].Item2.Close();

            try
            {
                // Execute
                using (MultiShardCommand cmd = _shardConnection.CreateCommand())
                {
                    cmd.CommandText = "SELECT dbNameField, Test_int_Field, Test_bigint_Field  FROM ConsistentShardedTable";
                    cmd.CommandType = CommandType.Text;

                    using (MultiShardDataReader sdr = cmd.ExecuteReader())
                    {
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    var aex = (AggregateException)ex;
                    Logger.Log("Exception encountered: " + ex.Message);
                    throw aex.InnerExceptions.FirstOrDefault((e) => e is InvalidOperationException);
                }
                throw;
            }
        }

        /// <summary>
        /// Validate the MultiShardConnectionString's connectionString param.
        /// - Shouldn't be null
        /// - No DataSource/InitialCatalog should be set
        /// - ApplicationName should be enhanced with a MSQ library
        /// specific suffix and should be capped at 128 chars
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestInvalidMultiShardConnectionString()
        {
            MultiShardConnection conn;

            try
            {
                conn = new MultiShardConnection(_shardMap.GetShards(), connectionString: null);
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is ArgumentNullException, "Expected ArgumentNullException!");
            }

            try
            {
                conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardMapManagerConnectionString);
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is ArgumentException, "Expected ArgumentException!");
            }

            // Validate that the ApplicationName is updated properly
            var applicationStringBldr = new StringBuilder();
            for (int i = 0; i < ApplicationNameHelper.MaxApplicationNameLength; i++)
            {
                applicationStringBldr.Append('x');
            }
            string applicationName = applicationStringBldr.ToString();
            SqlConnectionStringBuilder connStringBldr = new SqlConnectionStringBuilder(MultiShardTestUtils.ShardConnectionString);
            connStringBldr.ApplicationName = applicationName;
            conn = new MultiShardConnection(_shardMap.GetShards(), connStringBldr.ConnectionString);

            string updatedApplicationName = new SqlConnectionStringBuilder
                (conn.GetShardConnections()[0].Item2.ConnectionString).ApplicationName;
            Assert.IsTrue(updatedApplicationName.Length == ApplicationNameHelper.MaxApplicationNameLength &&
                updatedApplicationName.EndsWith(MultiShardConnection.ApplicationNameSuffix), "ApplicationName not appended with {0}!",
                MultiShardConnection.ApplicationNameSuffix);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestCreateConnectionWithNoShards()
        {
            using (MultiShardConnection conn = new MultiShardConnection(new Shard[] { }, String.Empty))
            {
                Assert.Fail("Should have failed in the MultiShardConnection c-tor.");
            }
        }

        /// <summary>
        /// Regression test for VSTS Bug# 3936154
        /// - Execute a command that will result in a failure in a loop
        /// - Without the fix (disabling the command behavior)s, the test will hang and timeout.
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        [Timeout(300000)]
        public void TestFailedCommandWithConnectionCloseCmdBehavior()
        {
            Parallel.For(0, 100, i =>
            {
                try
                {
                    using (MultiShardConnection conn = new MultiShardConnection(_shardMap.GetShards(), MultiShardTestUtils.ShardConnectionString))
                    {
                        using (MultiShardCommand cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "select * from table_does_not_exist";
                            cmd.CommandType = CommandType.Text;

                            using (MultiShardDataReader sdr = cmd.ExecuteReader())
                            {
                                while (sdr.Read())
                                {
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Encountered exception: {0} in iteration: {1}",
                        ex.ToString(), i);
                }
                finally
                {
                    Console.WriteLine("Completed execution of iteration: {0}", i);
                }
            });
        }

        /// <summary>
        /// This test induces failures via a ProxyServer in order to validate that:
        ///  a) we are handling reader failures as expected, and
        ///  b) we get all-or-nothing semantics on our reads from a single row
        /// </summary>
        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardResultFailures()
        {
            ProxyServer proxyServer = GetProxyServer();

            try
            {
                // Start up the proxy server.  Do it in a try so we can shut it down in the finally.
                // Also, we have to generate the proxyShardconnections *AFTER* we start up the server
                // so that we know what port the proxy is listening on.  More on the placement
                // of the connection generation below.
                //
                proxyServer.Start();

                // PreKillReads is the number of successful reads to perform before killing
                // all the connections.  We start at 0 to test the no failure case as well.
                //
                for (int preKillReads = 0; preKillReads <= 10; preKillReads++)
                {
                    // Additionally, since we are running inside a loop, we need to regenerate the proxy shard connections each time
                    // so that we don't re-use dead connections.  If we do that we will end up hung in the read call.
                    //
                    List<Tuple<ShardLocation, DbConnection>> proxyShardConnections = GetProxyShardConnections(proxyServer);
                    using (MultiShardConnection conn = new MultiShardConnection(proxyShardConnections))
                    {
                        using (MultiShardCommand cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "SELECT db_name() as dbName1, REPLICATE(db_name(), 1000) as longExpr, db_name() as dbName2 FROM ConsistentShardedTable";
                            cmd.CommandType = CommandType.Text;

                            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
                            cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;

                            using (MultiShardDataReader sdr = cmd.ExecuteReader(CommandBehavior.Default))
                            {
                                int tuplesRead = 0;

                                while (sdr.Read())
                                {
                                    // Read part of the tuple first before killing the connections and
                                    // then attempting to read the rest of the tuple.
                                    //
                                    tuplesRead++;

                                    try
                                    {
                                        // The longExpr should contain the first dbName field multiple times.
                                        //
                                        string dbName1 = sdr.GetString(0);
                                        string longExpr = sdr.GetString(1);
                                        Assert.IsTrue(longExpr.Contains(dbName1));

                                        if (tuplesRead == preKillReads)
                                        {
                                            proxyServer.KillAllConnections();
                                        }

                                        // The second dbName field should be the same as the first dbName field.
                                        //
                                        string dbName2 = sdr.GetString(2);
                                        Assert.AreEqual(dbName1, dbName2);

                                        // The shardId should contain both the first and the second dbName fields.
                                        //
                                        string shardId = sdr.GetString(3);
                                        Assert.IsTrue(shardId.Contains(dbName1));
                                        Assert.IsTrue(shardId.Contains(dbName2));
                                    }
                                    catch (Exception ex)
                                    {
                                        // We've seen some failures here due to an attempt to access a socket after it has
                                        // been disposed.  The only place where we are attempting to access the socket
                                        // is in the call to proxyServer.KillAllConnections.  Unfortunately, it's not clear
                                        // what is causing that problem since it only appears to repro in the lab.
                                        // I (errobins) would rather not blindly start changing things in the code (either
                                        // our code above, our exception handling code here, or the proxyServer code) until
                                        // we know which socket we are trying to access when we hit this problem.
                                        // So, the first step I will take is to pull additional exception information
                                        // so that we can see some more information about what went wrong the next time it repros.
                                        //
                                        Assert.Fail("Unexpected exception, rethrowing.  Here is some info: \n Message: {0} \n Source: {1} \n StackTrace: {2}",
                                            ex.Message, ex.Source, ex.StackTrace);
                                        throw;
                                    }
                                }

                                Assert.IsTrue((tuplesRead <= preKillReads) || (0 == preKillReads),
                                    String.Format("Tuples read was {0}, Pre-kill reads was {1}", tuplesRead, preKillReads));
                            }
                        }
                    }
                }
            }
            finally
            {
                // Be sure to shut down the proxy server.
                //
                string proxyLog = proxyServer.EventLog.ToString();
                Logger.Log(proxyLog);
                proxyServer.Stop();
            }
        }

        /// <summary>
        /// Helper that sets up a proxy server for us and points it at our local host, 1433 SQL Server.
        /// </summary>
        /// <returns>
        /// The newly created proxy server for our local sql server host.
        /// </returns>
        /// <remarks>
        /// Note that we are not inducing any network delay (the first arg).  We coul dchange this if desired.
        /// </remarks>
        private ProxyServer GetProxyServer()
        {
            ProxyServer proxy = new ProxyServer(simulatedPacketDelay: 0, simulatedInDelay: true, simulatedOutDelay: true, bufferSize: 8192);
            proxy.RemoteEndpoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1433);

            return proxy;
        }

        /// <summary>
        /// Helper that provides us with ShardConnections based on the shard map (for the database), but routed through the proxy.
        /// </summary>
        /// <param name="proxy">The proxy to route the connections through.</param>
        /// <returns>
        /// The List of {ShardLocation, DbConnection} tuples that we can use to instantiate our multi-shard connection.
        /// </returns>
        /// <remarks>
        /// Since our shards all reside in the local instance we can just point them at a single proxy server.  If we were using
        /// actual physically distributed shards, then I think we would need a separate proxy for each shard.  We could
        /// augment these tests to use a separate proxy per shard, if we wanted, in order to be able to simulate
        /// a richer variety of failures.  For now, we just simulate total failures of all shards.
        /// </remarks>
        private List<Tuple<ShardLocation, DbConnection>> GetProxyShardConnections(ProxyServer proxy)
        {
            // We'll do this by looking at our pre-existing connections and working from that.
            //
            string baseConnString = MultiShardTestUtils.ShardConnectionString.ToString();
            List<Tuple<ShardLocation, DbConnection>> rVal = new List<Tuple<ShardLocation, DbConnection>>();
            foreach (Shard shard in _shardMap.GetShards())
            {
                // Location doesn't really matter, so just use the same one.
                //
                ShardLocation curLoc = shard.Location;

                // The connection, however, does matter, so set up a connection
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(baseConnString);
                builder.DataSource = "localhost," + proxy.LocalPort;
                builder.InitialCatalog = curLoc.Database;

                SqlConnection curConn = new SqlConnection(builder.ToString());

                Tuple<ShardLocation, DbConnection> curTuple = new Tuple<ShardLocation, DbConnection>(curLoc, curConn);
                rVal.Add(curTuple);
            }
            return rVal;
        }
    }
}
