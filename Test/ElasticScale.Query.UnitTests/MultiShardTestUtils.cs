// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Security;
using System.Text;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    /// <summary>
    /// Common utilities used by tests
    /// </summary>
    internal static class MultiShardTestUtils
    {
        #region Private members

        /// <summary>
        /// Location for shards that we will route fanout queries to.
        /// </summary>
        private static string s_serverLocation = "localhost";

        /// <summary>
        /// UserId to use when connecting to shards during a fanout query.
        /// </summary>
        private static string s_testUserId = "TestUser";

        /// <summary>
        /// User password to use when connecting to shards during a fanout query.
        /// </summary>
        private static string s_testPassword = "dogmat1C";

        /// <summary>
        /// Table name for the sharded table we will issue fanout queries against.
        /// </summary>
        private static string s_testTableName = "ConsistentShardedTable";

        /// <summary>
        /// Field on the sharded table where we will store the database name.
        /// </summary>
        private static string s_dbNameField = "dbNameField";

        /// <summary>
        /// Connection string for local shard user.
        /// </summary>
        internal static string ShardConnectionString = @"Integrated Security=SSPI;";

        /// <summary>
        /// Connection string for global shard map manager operations.
        /// </summary>
        internal static string ShardMapManagerConnectionString = @"Data Source=localhost;Initial Catalog=ShardMapManager;Integrated Security=SSPI;";

        /// <summary>
        /// Name of the database where the ShardMapManager persists its data.
        /// </summary>
        private static string s_shardMapManagerDbName = "ShardMapManager";

        /// <summary>
        /// Name of the test shard map to use.
        /// </summary>
        private static string s_testShardMapName = "TestShardMap";

        /// <summary>
        /// List containing the names of the test databases.
        /// </summary>
        private static List<string> s_testDatabaseNames = GenerateTestDatabaseNames();

        /// <summary>
        /// Class level Random object.
        /// </summary>
        private static Random s_random = new Random();

        #endregion Private Members

        #region Internal Methods

        /// <summary>
        /// Create and populate the test databases with the data we expect for these unit tests to run correctly.
        /// </summary>
        /// <remarks>
        /// Probably will need to change this to integrate with our test framework better.
        /// Will deal with that down the line when the test framework issue has settled out more.
        /// </remarks>
        internal static void CreateAndPopulateTables()
        {
            List<string> commands = new List<string>();
            for (int i = 0; i < s_testDatabaseNames.Count; i++)
            {
                string dbName = s_testDatabaseNames[i];
                commands.Add(string.Format("USE {0};", dbName));

                // First create the table.
                //
                string createTable = GetTestTableCreateCommand();
                commands.Add(createTable);

                // Then add the records.
                //
                string[] insertValuesCommands = GetInsertValuesCommands(3, dbName);
                foreach (string insertValuesCommand in insertValuesCommands)
                {
                    commands.Add(insertValuesCommand);
                }
            }
            ExecuteNonQueries("master", commands);
        }

        /// <summary>
        /// Blow away (if necessary) and create fresh versions of the Test databases we expect for our unit tests.
        /// </summary>
        /// <remarks>
        /// DEVNOTE (VSTS 2202802): we should move to a GUID-based naming scheme.
        /// </remarks>
        internal static void DropAndCreateDatabases()
        {
            List<string> commands = new List<string>();

            // Set up the test user.
            //
            AddDropAndReCreateTestUserCommandsToList(commands);

            // Set up the test databases.
            //
            AddCommandsToManageTestDatabasesToList(create: true, output: commands);

            // Set up the ShardMapManager database.
            //
            AddDropAndCreateDatabaseCommandsToList(s_shardMapManagerDbName, commands);

            ExecuteNonQueries("master", commands);
        }

        /// <summary>
        /// Drop the test databases (if they exist) we expect for these unit tests.
        /// </summary>
        /// <remarks>
        /// DEVNOTE (VSTS 2202802): We should switch to a GUID-based naming scheme.
        /// </remarks>
        internal static void DropDatabases()
        {
            List<string> commands = new List<string>();

            // Drop the test databases.
            //
            AddCommandsToManageTestDatabasesToList(create: false, output: commands);

            // Drop the test login.
            //
            commands.Add(DropLoginCommand());

            //Drop the ShardMapManager database.
            //
            commands.Add(DropDatabaseCommand(s_shardMapManagerDbName));

            ExecuteNonQueries("master", commands);
        }

        /// <summary>
        /// Helper method that alters the column name on one of our test tables in one of our test databases.
        /// Useful for inducing a schema mismatch to test our failure handling.
        /// </summary>
        /// <param name="database">The 0-based index of the test database to change the schema in.</param>
        /// <param name="oldColName">The current name of the column to change.</param>
        /// <param name="newColName">The desired new name of the column.</param>
        internal static void ChangeColumnNameOnShardedTable(int database, string oldColName, string newColName)
        {
            using (SqlConnection conn = new SqlConnection(GetTestConnectionString(s_testDatabaseNames[database])))
            {
                conn.Open();

                string tsql = String.Format("EXEC sp_rename '[{0}].[{1}]', '{2}', 'COLUMN';",
                    s_testTableName, oldColName, newColName);
                ExecuteNonQuery(conn, tsql);

                conn.Close();
            }
        }

        internal static ShardMap CreateAndGetTestShardMap()
        {
            ShardMap sm;
            ShardMapManagerFactory.CreateSqlShardMapManager(MultiShardTestUtils.ShardMapManagerConnectionString, ShardMapManagerCreateMode.ReplaceExisting);
            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(MultiShardTestUtils.ShardMapManagerConnectionString, ShardMapManagerLoadPolicy.Lazy);

            sm = smm.CreateListShardMap<int>(s_testShardMapName);
            for (int i = 0; i < s_testDatabaseNames.Count; i++)
            {
                sm.CreateShard(GetTestShard(s_testDatabaseNames[i]));
            }
            return sm;
        }

        #endregion Internal Methods

        #region Private Methods

        /// <summary>
        /// Generates a connection string for the given database name.  Assumes we wish to connect to localhost.
        /// </summary>
        /// <param name="database">The name of the database to put in the connection string.</param>
        /// <returns>The connection string to the passed in database name on local host.</returns>
        /// <remarks>
        /// Currently assumes we wish to connect to localhost using integrated auth.
        /// We will likely need to change this when we integrate with our test framework better.
        /// </remarks>
        private static string GetTestConnectionString(string database)
        {
            Assert.IsNotNull(database, "null database");
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = s_serverLocation;
            builder.IntegratedSecurity = true;
            builder.InitialCatalog = database;
            return builder.ConnectionString;
        }

        /// <summary>
        /// Generates a ShardLocation object that encapsulates the server location and dbname that we 
        /// should connect to.
        /// Assumes we wish to connect to local host.
        /// </summary>
        /// <param name="database">The name of the database to put in the shard.</param>
        /// <returns>The shard with the specified server location and database parameters.</returns>
        /// <remarks>
        /// Currently assumes we wish to connect to localhost using integrated auth.
        /// We will likely need to change this when we integrate with our test framework better.
        /// </remarks>
        private static ShardLocation GetTestShard(string database)
        {
            Assert.IsNotNull(database, "null database");
            ShardLocation rVal = new ShardLocation(s_serverLocation, database);
            return rVal;
        }

        /// <summary>
        /// Helper to populate a list with our test database names.
        /// </summary>
        /// <returns>A new list containing the test database names.</returns>
        private static List<string> GenerateTestDatabaseNames()
        {
            List<string> rVal = new List<string>();
            for (int i = 0; i < 3; i++)
            {
                rVal.Add(string.Format("Test{0}", i));
            }
            return rVal;
        }

        /// <summary>
        /// Helper that iterates through the Test databases and adds commands to drop and, optionally, re-create 
        /// them, to the passed in list.
        /// </summary>
        /// <param name="create">True if we should create the test databases, false if not.</param>
        /// <param name="output">The list to append the commands to.</param>
        private static void AddCommandsToManageTestDatabasesToList(bool create, List<string> output)
        {
            for (int i = 0; i < s_testDatabaseNames.Count; i++)
            {
                string dbName = s_testDatabaseNames[i];
                output.Add(DropDatabaseCommand(dbName));

                if (create)
                {
                    output.Add(CreateDatabaseCommand(dbName));
                }
            }
        }

        /// <summary>
        /// Helper that provides tsql to drop a database if it exists.
        /// </summary>
        /// <param name="dbName">The name of the database to drop.</param>
        /// <returns>The tsql to drop it if it exists.</returns>
        private static string DropDatabaseCommand(string dbName)
        {
            return string.Format("IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}') DROP DATABASE [{0}]", dbName);
        }

        /// <summary>
        /// Helper that provides tsql to create a database.
        /// </summary>
        /// <param name="dbName">The name of the database to create.</param>
        /// <returns>The tsql to create the database.</returns>
        private static string CreateDatabaseCommand(string dbName)
        {
            return string.Format("CREATE DATABASE [{0}]", dbName);
        }

        /// <summary>
        /// Helper that prodices tsql to drop a database if it exists and then recreate it.  The tsql 
        /// statements get appended to the passed in list.
        /// </summary>
        /// <param name="dbName">The name of the database to drop and recreate.</param>
        /// <param name="output">The list to append the generated tsql into.</param>
        private static void AddDropAndCreateDatabaseCommandsToList(string dbName, List<string> output)
        {
            output.Add(DropDatabaseCommand(dbName));
            output.Add(CreateDatabaseCommand(dbName));
        }

        /// <summary>
        /// Helper that produces tsql to drop the test login if it exists.
        /// </summary>
        /// <returns>The tsql to drop the test login.</returns>
        private static string DropLoginCommand()
        {
            return string.Format(
                "IF EXISTS (SELECT name FROM sys.sql_logins WHERE name = N'{0}') DROP LOGIN {0}", s_testUserId);
        }

        /// <summary>
        /// Helper that appends the commands to drop and recreate the test login to the passed in list.
        /// </summary>
        /// <param name="output">The list to append the commands to.</param>
        private static void AddDropAndReCreateTestUserCommandsToList(List<string> output)
        {
            // First drop it.
            //
            output.Add(DropLoginCommand());

            // Then re create it.
            //
            output.Add(string.Format("CREATE LOGIN {0} WITH Password = '{1}';", s_testUserId, s_testPassword));

            // Then grant it lots of permissions.
            //
            output.Add(string.Format("GRANT CONTROL SERVER TO {0}", s_testUserId));
        }

        /// <summary>
        /// Helper to execute a single tsql batch over the given connection.
        /// </summary>
        /// <param name="theConn">The connection to execute the tsql against.</param>
        /// <param name="theCommand">The tsql to execute.</param>
        private static void ExecuteNonQuery(SqlConnection theConn, string theCommand)
        {
            using (SqlCommand toExecute = theConn.CreateCommand())
            {
                toExecute.CommandText = theCommand;
                toExecute.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Helper to execute multiple tsql batches consecutively over the given connection.
        /// </summary>
        /// <param name="theConn">The connection to execute the tsql against.</param>
        /// <param name="theCommands">Array containing the tsql batches to execute.</param>
        private static void ExecuteNonQueries(string initialCatalog, IEnumerable<string> theCommands)
        {
            using (SqlConnection theConn = new SqlConnection(GetTestConnectionString(initialCatalog)))
            {
                theConn.Open();

                using (SqlCommand toExecute = theConn.CreateCommand())
                {
                    foreach (string tsql in theCommands)
                    {
                        toExecute.CommandText = tsql;
                        toExecute.ExecuteNonQuery();
                    }
                }

                theConn.Close();
            }
        }

        /// <summary>
        /// Helper that constructs the sql script to create our test table.
        /// </summary>
        /// <returns>
        /// T-SQL to create our test table.
        /// </returns>
        private static string GetTestTableCreateCommand()
        {
            StringBuilder createCommand = new StringBuilder();

            // Set up the stem of the statement
            //
            createCommand.AppendFormat("CREATE TABLE {0} ({1} nvarchar(50)", s_testTableName, s_dbNameField);

            IReadOnlyList<MutliShardTestCaseColumn> fieldInfo = MutliShardTestCaseColumn.DefinedColumns;
            for (int i = 0; i < fieldInfo.Count; i++)
            {
                MutliShardTestCaseColumn curField = fieldInfo[i];
                createCommand.AppendFormat(", {0} {1}", curField.TestColumnName, curField.ColumnTypeDeclaration);
            }

            createCommand.Append(");");
            return createCommand.ToString();
        }

        /// <summary>
        /// Helper to generate random field data for a record in the test table.
        /// </summary>
        /// <param name="numCommands">The number of records to generate random data for.</param>
        /// <param name="dbName">The name of the database to put in the dbName column.</param>
        /// <returns>Array filled with the commands to execute to insert the data.</returns>
        private static string[] GetInsertValuesCommands(int numCommands, string dbName)
        {
            string[] commandsToReturn = new string[numCommands];

            StringBuilder insertCommand = new StringBuilder();
            IReadOnlyList<MutliShardTestCaseColumn> fieldInfo = MutliShardTestCaseColumn.DefinedColumns;

            for (int i = 0; i < numCommands; i++)
            {
                insertCommand.Clear();

                // Set up the stem, which includes putting the dbName in the dbNameColumn.
                //
                insertCommand.AppendFormat("INSERT INTO {0} ({1}", s_testTableName, s_dbNameField);

                // Now put in all the column names in the order we will do them.
                //
                for (int j = 0; j < fieldInfo.Count; j++)
                {
                    MutliShardTestCaseColumn curField = fieldInfo[j];

                    // Special case: if we hit the rowversion field let's skip it - it gets updated automatically.
                    //
                    if (!IsTimestampField(curField.DbType))
                    {
                        insertCommand.AppendFormat(", {0}", curField.TestColumnName);
                    }
                }

                // Close the field list
                //
                insertCommand.Append(") ");

                // Now put in the VALUES stem
                //
                insertCommand.AppendFormat("VALUES ('{0}'", dbName);

                // Now put in the individual field values
                //
                for (int j = 0; j < fieldInfo.Count; j++)
                {
                    MutliShardTestCaseColumn curField = fieldInfo[j];

                    // Special case: if we hit the rowversion field let's skip it - it gets updated automatically.
                    //
                    if (!IsTimestampField(curField.DbType))
                    {
                        string valueToPutIn = GetTestFieldValue(curField);
                        insertCommand.AppendFormat(", {0}", valueToPutIn);
                    }
                }

                // Finally, close the values list, terminate the statement, and add it to the array.
                //
                insertCommand.Append(");");
                commandsToReturn[i] = insertCommand.ToString();
            }
            return commandsToReturn;
        }

        #region Random Data Helpers

        /// <summary>
        /// Helper to generate a tsql fragement that will produce a random value of the given type to insert into the test database.
        /// </summary>
        /// <param name="dataTypeInfo">The datatype of the desired value.</param>
        /// <returns>The tsql fragment that will generate a random value of the desired type.</returns>
        private static string GetTestFieldValue(MutliShardTestCaseColumn dataTypeInfo)
        {
            SqlDbType dbType = dataTypeInfo.DbType;
            int? length = dataTypeInfo.FieldLength;

            switch (dbType)
            {
                case SqlDbType.BigInt:
                    // SQL Types: bigint
                    return GetRandomIntCastAsArg(dataTypeInfo);

                case SqlDbType.Binary:
                    // SQL Types: image
                    return GetRandomSqlBinaryValue(length.Value);

                case SqlDbType.Bit:
                    // SQL Types: bit
                    return GetRandomSqlBitValue();

                case SqlDbType.Char:
                    // SQL Types: char[(n)]
                    return GetRandomSqlCharValue(length.Value);

                case SqlDbType.Date:
                    // SQL Types: date
                    return GetRandomSqlDateValue();

                case SqlDbType.DateTime:
                    // SQL Types: datetime, smalldatetime
                    return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);

                case SqlDbType.DateTime2:
                    // SQL Types: datetime2
                    return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);

                case SqlDbType.DateTimeOffset:
                    // SQL Types: datetimeoffset
                    return GetRandomSqlDatetimeoffsetValue();

                case SqlDbType.Decimal:
                    // SQL Types: decimal, numeric
                    // These are the same.
                    return GetRandomSqlDecimalValue(dataTypeInfo);

                case SqlDbType.Float:
                    // SQL Types: float
                    return GetRandomSqlFloatValue(dataTypeInfo);

                case SqlDbType.Image:
                    // SQL Types: image
                    return GetRandomSqlBinaryValue(dataTypeInfo.FieldLength);

                case SqlDbType.Int:
                    // SQL Types: int
                    return GetRandomSqlIntValue();

                case SqlDbType.Money:
                    // SQL Types: money
                    return GetRandomSqlMoneyValue(dataTypeInfo);

                case SqlDbType.NChar:
                    // SQL Types: nchar[(n)]
                    return GetRandomSqlNCharValue(length.Value);

                case SqlDbType.NText:
                    // SQL Types: ntext
                    return GetRandomSqlNCharValue(length.Value);

                case SqlDbType.NVarChar:
                    // SQL Types: nvarchar[(n)]
                    return GetRandomSqlNCharValue(length.Value);

                case SqlDbType.Real:
                    // SQL Types: real
                    return GetRandomSqlRealValue(dataTypeInfo);

                case SqlDbType.SmallDateTime:
                    // SQL Types: smalldatetime
                    return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);

                case SqlDbType.SmallInt:
                    // SQL Types: smallint
                    return GetRandomSqlSmallIntValue(dataTypeInfo);

                case SqlDbType.SmallMoney:
                    // SQL Types: smallmoney
                    return GetRandomSqlSmallMoneyValue(dataTypeInfo);

                case SqlDbType.Text:
                    // SQL Types: text
                    return GetRandomSqlCharValue(length.Value);

                case SqlDbType.Time:
                    // SQL Types: time
                    return GetRandomSqlDatetimeCastAsArg(dataTypeInfo);

                case SqlDbType.Timestamp:
                    // SQL Types: rowversion, timestamp
                    //exclding it should happen automatically.  should not be here. throw.
                    throw new ArgumentException(SqlDbType.Timestamp.ToString());

                case SqlDbType.TinyInt:
                    // SQL Types: tinyint
                    return GetRandomSqlTinyIntValue();

                case SqlDbType.UniqueIdentifier:
                    // SQL Types: uniqueidentifier
                    return GetRandomSqlUniqueIdentifierValue();

                case SqlDbType.VarBinary:
                    // SQL Types: binary[(n)], varbinary[(n)]
                    return GetRandomSqlBinaryValue(length.Value);

                case SqlDbType.VarChar:
                    // SQL Types: varchar[(n)]
                    return GetRandomSqlCharValue(length.Value);

                default:
                    throw new ArgumentException(dbType.ToString());
            }
        }

        /// <summary>
        /// Helper that produces tsql to cast a random int as a particular data type.
        /// </summary>
        /// <param name="column">The column that will determine the data type we wish to insert into.</param>
        /// <returns>The tsql fragment to generate the desired value.</returns>
        private static string GetRandomIntCastAsArg(MutliShardTestCaseColumn column)
        {
            int theValue = s_random.Next();
            return GetSpecificIntCastAsArg(theValue, column);
        }

        /// <summary>
        /// Helper that produces tsql to cast a random int as a particular data type drawn from the SmallInt range.
        /// </summary>
        /// <param name="column">The column that will determine the data type we wish to insert into.</param>
        /// <returns>The tsql fragment to generate the desired value.</returns>
        private static string GetRandomSqlSmallIntValue(MutliShardTestCaseColumn column)
        {
            int theValue = s_random.Next(Int16.MinValue, Int16.MaxValue);
            return GetSpecificIntCastAsArg(theValue, column);
        }

        /// <summary>
        /// Helper that produces tsql to cast a specific int as a particular data type.
        /// </summary>
        /// <param name="column">The column that will determine the data type we wish to insert into.</param>
        /// <param name="theValue">The specific int to cast and insert.</param>
        /// <returns>The tsql fragment to generate the desired value.</returns>
        private static string GetSpecificIntCastAsArg(int theValue, MutliShardTestCaseColumn column)
        {
            return string.Format("CAST({0} AS {1})", theValue, column.SqlServerDatabaseEngineType);
        }

        /// <summary>
        /// Helper that produces tsql to cast a random binary value as a particular data type.
        /// </summary>
        /// <param name="length">The length of the binary value to generate.</param>
        /// <returns>The tsql fragment to generate the desired value.</returns>
        private static string GetRandomSqlBinaryValue(int length)
        {
            Byte[] rawData = new Byte[length];
            s_random.NextBytes(rawData);
            string bytesAsString = BitConverter.ToString(rawData);
            bytesAsString = bytesAsString.Replace("-", "");

            return string.Format("CONVERT(binary({0}), '{1}', 2)", length, bytesAsString); // the 2 means hex with no 0x
        }

        /// <summary>
        /// Helper that produces tsql to cast a random bit value as a bit.
        /// </summary>
        /// <returns>The tsql fragment to generate the desired value.</returns>
        private static string GetRandomSqlBitValue()
        {
            string theVal = (s_random.Next() > s_random.Next()) ? "TRUE" : "FALSE";
            return string.Format("CAST ('{0}' AS bit)", theVal);
        }

        /// <summary>
        /// Helper that produces tsql of a random char value.
        /// </summary>
        /// <param name="length">The length of the char value to generate</param>
        /// <returns>The tsql fragment to generate the desired value.</returns>
        private static string GetRandomSqlCharValue(int length)
        {
            return string.Format("'{0}'", GetRandomString(length));
        }

        /// <summary>
        /// Helper that produces a random SqlDateValue.
        /// </summary>
        /// <returns>The tsql to produce the value.</returns>
        private static string GetRandomSqlDateValue()
        {
            return "GETDATE()";
        }

        /// <summary>
        /// Helper that produces a random SqlDatetime value.
        /// </summary>
        /// <returns>The tsql to produce the value.</returns>
        private static string GetRandomSqlDatetimeValue()
        {
            return "SYSDATETIME()";
        }

        /// <summary>
        /// Helper that produces a random sqldatetime value cast as a particular type.
        /// </summary>
        /// <param name="column">The column whoe type the value should be cast to.</param>
        /// <returns>The tsql to generate the casted value.</returns>
        private static string GetRandomSqlDatetimeCastAsArg(MutliShardTestCaseColumn column)
        {
            return string.Format("CAST(SYSDATETIME() AS {0})", column.SqlServerDatabaseEngineType);
        }

        /// <summary>
        /// Helper that produces a random datetimeoffset value.
        /// </summary>
        /// <returns>The tsql to generate the desired value.</returns>
        private static string GetRandomSqlDatetimeoffsetValue()
        {
            return "SYSDATETIMEOFFSET()";
        }

        /// <summary>
        /// Helper that produces a random double within the smallmoney domain and casts it to the desired column type.
        /// </summary>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>The tsql to generate the casted value.</returns>
        private static string GetRandomSqlSmallMoneyValue(MutliShardTestCaseColumn column)
        {
            double randomSmallMoneyValue = s_random.NextDouble() * (214748.3647);
            return GetSpecificDoubleCastAsArg(randomSmallMoneyValue, column);
        }

        /// <summary>
        /// Helper to produce a random double cast as a particular type.
        /// </summary>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>Tsql to generate the desired value cast as the desired type.</returns>
        private static string GetRandomDoubleCastAsArg(MutliShardTestCaseColumn column)
        {
            double randomDouble = s_random.NextDouble() * double.MaxValue;
            return GetSpecificDoubleCastAsArg(randomDouble, column);
        }

        /// <summary>
        /// Helper to produce a random double drawn from the decimal domain.
        /// </summary>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>Tsql to generate and cast the value.</returns>
        private static string GetRandomSqlDecimalValue(MutliShardTestCaseColumn column)
        {
            // .NET Decimal has less range than SQL decimal, so we need to drop down to the 
            // .NET range to test these consistently.
            //
            double theValue = s_random.NextDouble() * Decimal.ToDouble(Decimal.MaxValue);
            return string.Format("CAST({0} AS {1})", theValue, column.ColumnTypeDeclaration);
        }

        /// <summary>
        /// Helper to generate a random double and cast it as a particular type.
        /// </summary>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>Tsql that will generate the desired value.</returns>
        private static string GetRandomSqlFloatValue(MutliShardTestCaseColumn column)
        {
            double theValue = s_random.NextDouble() * SqlDouble.MaxValue.Value;
            return GetSpecificDoubleCastAsArg(theValue, column);
        }


        /// <summary>
        /// Helper to produce a random double drawn from the money domain.
        /// </summary>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>Tsql to generate and cast the value.</returns>
        private static string GetRandomSqlMoneyValue(MutliShardTestCaseColumn column)
        {
            double theValue = s_random.NextDouble() * SqlMoney.MaxValue.ToDouble();
            return GetSpecificDoubleCastAsArg(theValue, column);
        }

        /// <summary>
        /// Helper to produce a random double drawn from the real (sqlsingle) domain.
        /// </summary>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>Tsql to generate and cast the value.</returns>
        private static string GetRandomSqlRealValue(MutliShardTestCaseColumn column)
        {
            double theValue = s_random.NextDouble() * SqlSingle.MaxValue.Value;
            return GetSpecificDoubleCastAsArg(theValue, column);
        }

        /// <summary>
        //// Helper to cast a particular double as a particular type.
        /// </summary>
        /// <param name="theValue">The value to cast.</param>
        /// <param name="column">The column whose type the value should be cast to.</param>
        /// <returns>Tsql to cast the value.</returns>
        private static string GetSpecificDoubleCastAsArg(double theValue, MutliShardTestCaseColumn column)
        {
            return string.Format("CAST({0} AS {1})", theValue, column.SqlServerDatabaseEngineType);
        }

        /// <summary>
        /// Helper to generate a random int.
        /// </summary>
        /// <returns>The random int.</returns>
        private static string GetRandomSqlIntValue()
        {
            return s_random.Next().ToString();
        }

        /// <summary>
        /// Helper to generate a random nchar value of a particular length.
        /// </summary>
        /// <param name="length">The length of the desired nchar.</param>
        /// <returns>The tsql to produce the desired value.</returns>
        private static string GetRandomSqlNCharValue(int length)
        {
            return string.Format("N'{0}'", GetRandomString(length));
        }

        /// <summary>
        /// Helper to generate a random value drawn from the TinyInt domain.
        /// </summary>
        /// <returns>The tsql to generate the desired value.</returns>
        private static string GetRandomSqlTinyIntValue()
        {
            return s_random.Next(Byte.MinValue, Byte.MaxValue).ToString();
        }

        /// <summary>
        /// Helper to generate a new guid.
        /// </summary>
        /// <returns>Tsql to produce the guid.</returns>
        private static string GetRandomSqlUniqueIdentifierValue()
        {
            return "NEWID()";
        }

        /// <summary>
        /// Helper to generate a random string of a particular length.
        /// </summary>
        /// <param name="length">The length of the string to generate.</param>
        /// <returns>Tsql representation of the random string.</returns>
        private static string GetRandomString(int length)
        {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < length; i++)
            {
                char nextChar = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * s_random.NextDouble() + 65)));
                builder.Append(nextChar);
            }
            return builder.ToString();
        }

        /// <summary>
        /// Helper to determine if a particular SqlDbType is a timestamp.
        /// </summary>
        /// <param name="curFieldType"></param>
        /// <returns></returns>
        private static bool IsTimestampField(SqlDbType curFieldType)
        {
            return SqlDbType.Timestamp == curFieldType;
        }

        #endregion Random Data Helpers

        #endregion Private Methods
    }
}
