// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    [TestClass]
    public class SchemaInfoCollectionTests
    {
        /// <summary>
        /// Initializes common state for tests in this class.
        /// </summary>
        /// <param name="testContext">The TestContext we are running in.</param>
        [ClassInitialize()]
        public static void SchemaInfoTestsInitialize(TestContext testContext)
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();

                // Create ShardMapManager database
                using (SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Cleans up common state for the all tests in this class.
        /// </summary>
        [ClassCleanup()]
        public static void SchemaInfoTestsCleanup()
        {
            // Clear all connection pools.
            SqlConnection.ClearAllPools();

            using (SqlConnection conn = new SqlConnection(Globals.ShardMapManagerTestConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(
                    string.Format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName),
                    conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestAddAndLookupAndDeleteSchemaInfo()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            #region TestAddAndLookup

            SchemaInfoCollection siCollection = shardMapManager.GetSchemaInfoCollection();

            SchemaInfo si = new SchemaInfo();

            ShardedTableInfo stmd1 = new ShardedTableInfo("ShardedTableName1", "ColumnName");
            ShardedTableInfo stmd2 = new ShardedTableInfo("dbo", "ShardedTableName2", "ColumnName");

            si.Add(stmd1);
            si.Add(stmd2);

            Assert.AreEqual(2, si.ShardedTables.Count);

            ReferenceTableInfo rtmd1 = new ReferenceTableInfo("ReferenceTableName1");
            ReferenceTableInfo rtmd2 = new ReferenceTableInfo("dbo", "ReferenceTableName2");

            si.Add(rtmd1);
            si.Add(rtmd2);

            Assert.AreEqual(2, si.ReferenceTables.Count);
            // Add an existing sharded table again. Make sure it doesn't create duplicate entries.
            SchemaInfoException siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => si.Add(new ShardedTableInfo("ShardedTableName1", "ColumnName")));
            Assert.AreEqual(SchemaInfoErrorCode.TableInfoAlreadyPresent, siex.ErrorCode);

            // Add an existing sharded table with a different key column name. This should fail too.
            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => si.Add(new ShardedTableInfo("ShardedTableName1", "ColumnName_Different")));
            Assert.AreEqual(SchemaInfoErrorCode.TableInfoAlreadyPresent, siex.ErrorCode);

            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => si.Add(new ShardedTableInfo("dbo", "ShardedTableName2", "ColumnName_Different")));
            Assert.AreEqual(SchemaInfoErrorCode.TableInfoAlreadyPresent, siex.ErrorCode);

            Assert.AreEqual(2, si.ShardedTables.Count);

            // Add an existing reference tables again. Make sure it doesn't create duplicate entries.
            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => si.Add(new ReferenceTableInfo("dbo", "ReferenceTableName2")));
            Assert.AreEqual(SchemaInfoErrorCode.TableInfoAlreadyPresent, siex.ErrorCode);

            Assert.AreEqual(2, si.ReferenceTables.Count);

            // Now trying adding a reference table as a sharded table and vice versa. Both operations should fail.
            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => si.Add(new ShardedTableInfo("ReferenceTableName1", "ColumnName")));
            Assert.AreEqual(SchemaInfoErrorCode.TableInfoAlreadyPresent, siex.ErrorCode);

            Assert.AreEqual(2, si.ShardedTables.Count);

            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => si.Add(new ReferenceTableInfo("dbo", "ShardedTableName2")));
            Assert.AreEqual(SchemaInfoErrorCode.TableInfoAlreadyPresent, siex.ErrorCode);

            Assert.AreEqual(2, si.ReferenceTables.Count);

            // Try removing an existing table info and adding it back.
            si.Remove(stmd1);
            si.Add(stmd1);
            Assert.AreEqual(2, si.ShardedTables.Count);

            si.Remove(rtmd2);
            si.Add(rtmd2);
            Assert.AreEqual(2, si.ReferenceTables.Count);

            // Test with NULL inputs.
            ArgumentException arex = AssertExtensions.AssertThrows<ArgumentException>(
                () => si.Add((ShardedTableInfo)null));

            arex = AssertExtensions.AssertThrows<ArgumentException>(
                () => si.Add((ReferenceTableInfo)null));

            string mdName = String.Format("TestSI_{0}", Guid.NewGuid());
            siCollection.Add(mdName, si);

            SchemaInfo sdmdRead = siCollection.Get(mdName);

            AssertEqual(si, sdmdRead);

            // Trying to add schema info with the same name again will result in a 'name conflict' exception.
            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => siCollection.Add(mdName, si));
            Assert.AreEqual(SchemaInfoErrorCode.SchemaInfoNameConflict, siex.ErrorCode);

            #endregion

            #region TestLookup

            // Try looking up schema info with a non-existent name.
            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => siCollection.Get(mdName + "Fail"));
            Assert.AreEqual(SchemaInfoErrorCode.SchemaInfoNameDoesNotExist, siex.ErrorCode);


            #endregion

            #region TestDelete

            // Try removing any of the recently created schema info.
            siCollection.Remove(mdName);

            // Lookup should fail on removed data.
            siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => siCollection.Get(mdName));
            Assert.AreEqual(SchemaInfoErrorCode.SchemaInfoNameDoesNotExist, siex.ErrorCode);

            #endregion
        }

        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSetSchemaInfoWithSpecialChars()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            SchemaInfoCollection siCollection = shardMapManager.GetSchemaInfoCollection();

            SchemaInfo si = new SchemaInfo();

            ShardedTableInfo sti = new ShardedTableInfo(NewNameWithSpecialChars(), NewNameWithSpecialChars());

            si.Add(sti);

            string mdName = String.Format("TestSI_{0}", Guid.NewGuid());
            siCollection.Add(mdName, si);

            SchemaInfo sdmdRead = siCollection.Get(mdName);

            AssertEqual(si, sdmdRead);
        }

        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestUpdateSchemaInfo()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);

            SchemaInfoCollection siCollection = shardMapManager.GetSchemaInfoCollection();

            SchemaInfo si = new SchemaInfo();

            ShardedTableInfo sti = new ShardedTableInfo("dbo", "ShardedTableName1", "ColumnName");

            si.Add(sti);

            ReferenceTableInfo rtmd = new ReferenceTableInfo("ReferenceTableName1");

            si.Add(rtmd);

            string mdName = String.Format("TestSI_{0}", Guid.NewGuid());

            // Try updating schema info without adding it first.
            SchemaInfoException siex = AssertExtensions.AssertThrows<SchemaInfoException>(
                () => siCollection.Replace(mdName, si));
            Assert.AreEqual(SchemaInfoErrorCode.SchemaInfoNameDoesNotExist, siex.ErrorCode);

            siCollection.Add(mdName, si);

            SchemaInfo sdmdNew = new SchemaInfo();
            sdmdNew.Add(new ShardedTableInfo("dbo", "NewShardedTableName1", "NewColumnName"));
            sdmdNew.Add(new ReferenceTableInfo("NewReferenceTableName1"));

            siCollection.Replace(mdName, sdmdNew);

            SchemaInfo sdmdRead = siCollection.Get(mdName);

            AssertEqual(sdmdNew, sdmdRead);
        }

        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestGetAll()
        {
            ShardMapManagerFactory.CreateSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerCreateMode.ReplaceExisting);

            ShardMapManager shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                Globals.ShardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy);


            SchemaInfoCollection siCollection = shardMapManager.GetSchemaInfoCollection();

            SchemaInfo[] si = new SchemaInfo[3]
                {
                    new SchemaInfo(),
                    new SchemaInfo(),
                    new SchemaInfo()
                };

            si[0].Add(new ShardedTableInfo("ShardedTableName1", "ColumnName1"));
            si[0].Add(new ShardedTableInfo("dbo", "ShardedTableName2", "ColumnName1"));

            si[0].Add(new ReferenceTableInfo("ReferenceTableName1"));
            si[0].Add(new ReferenceTableInfo("dbo", "ReferenceTableName2"));

            si[1].Add(new ShardedTableInfo("ShardedTableName3", "ColumnName2"));
            si[1].Add(new ShardedTableInfo("dbo", "ShardedTableName4", "ColumnName2"));

            si[1].Add(new ReferenceTableInfo("ReferenceTableName3"));

            si[2].Add(new ShardedTableInfo("dbo", "ShardedTableName3", "ColumnName2"));

            si[2].Add(new ReferenceTableInfo("ReferenceTableName4"));
            si[2].Add(new ReferenceTableInfo("dbo", "ReferenceTableName5"));

            string[] siNames = new string[3]
            {
                String.Format("TestSI_{0}", Guid.NewGuid()),
                String.Format("TestSI_{0}", Guid.NewGuid()),
                String.Format("TestSI_{0}", Guid.NewGuid())
            };

            siCollection.Add(siNames[0], si[0]);
            siCollection.Add(siNames[1], si[1]);
            siCollection.Add(siNames[2], si[2]);

            int i = 0;
            bool success = true;
            foreach (KeyValuePair<string, SchemaInfo> kvp in siCollection)
            {
                SchemaInfo sdmdOriginal;
                try
                {
                    sdmdOriginal = si[Array.IndexOf(siNames, kvp.Key)];
                }
                catch
                {
                    success = false;
                    break;
                }

                AssertEqual(sdmdOriginal, kvp.Value);
                i++;
            }

            Assert.IsTrue(success);
            Assert.AreEqual(3, i);
        }

        [TestMethod]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestSchemaInfoExceptionSerializability()
        {
            SchemaInfoException ex = new SchemaInfoException(SchemaInfoErrorCode.SchemaInfoNameConflict, "message");
            SchemaInfoException deserialized = CommonTestUtils.SerializeDeserialize(ex);

            // Validate
            Assert.AreEqual(ex.ErrorCode, deserialized.ErrorCode, "ErrorCode");
            Assert.AreEqual(ex.ToString(), deserialized.ToString(), "ToString()");
        }

        /// <summary>
        /// Verifies that the serialization format of <see cref="SchemaInfo"/> matches the serialization format
        /// from v1.0.0. If this fails, then an older version of EDCL v1.0.0 will not be able to successfully 
        /// deserialize the <see cref="SchemaInfo"/>.
        /// </summary>
        /// <remarks>
        /// This test will need to be more sophisticated if new fields are added. Since no fields have been added yet,
        /// we can just do a direct string comparison, which is very simple and precise.
        /// </remarks>
        [TestMethod]
        public void SerializeCompatibility()
        {
            SchemaInfo schemaInfo = new SchemaInfo();
            schemaInfo.Add(new ReferenceTableInfo("r1", "r2"));
            schemaInfo.Add(new ShardedTableInfo("s1", "s2", "s3"));

            // Why is this slightly different from the XML in the DeserializeCompatibility test?
            // Because this is the exact formatting that we expect DataContractSerializer will create.
            string expectedSerializedSchemaInfo = @"<?xml version=""1.0"" encoding=""utf-16""?>
<Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <ReferenceTableSet xmlns="""" i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </ReferenceTableSet>
  <ShardedTableSet xmlns="""" i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </ShardedTableSet>
</Schema>";
            string actualSerializedSchemaInfo = ToXml(schemaInfo);
            Assert.AreEqual(
                expectedSerializedSchemaInfo,
                actualSerializedSchemaInfo);

            // Deserialize it back as a sanity check
            SchemaInfo finalSchemaInfo = FromXml(actualSerializedSchemaInfo);
            AssertEqual(schemaInfo, finalSchemaInfo);
        }

        /// <summary>
        /// Verifies that <see cref="SchemaInfo"/>data from EDCL v1.0.0 can be deserialized.
        /// </summary>
        [TestMethod]
        public void DeserializeCompatibilityV100()
        {
            // Why is this slightly different from the XML in the SerializeCompatibility test?
            // Because this XML comes from SQL Server, which uses different formatting than DataContractSerializer.
            // The Deserialize test uses the XML formatted by SQL Server because SQL Server is where it will
            // come from in the end-to-end scenario.
            string originalSchemaInfo = @"<Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <ReferenceTableSet i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </ReferenceTableSet>
  <ShardedTableSet i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </ShardedTableSet>
</Schema>";

            SchemaInfo schemaInfo = FromXml(originalSchemaInfo);
            Assert.AreEqual(1, schemaInfo.ReferenceTables.Count);
            Assert.AreEqual("r1", schemaInfo.ReferenceTables.First().SchemaName);
            Assert.AreEqual("r2", schemaInfo.ReferenceTables.First().TableName);
            Assert.AreEqual(1, schemaInfo.ShardedTables.Count);
            Assert.AreEqual("s1", schemaInfo.ShardedTables.First().SchemaName);
            Assert.AreEqual("s2", schemaInfo.ShardedTables.First().TableName);
            Assert.AreEqual("s3", schemaInfo.ShardedTables.First().KeyColumnName);

            // Serialize the data back. It should not contain _referenceTableSet or _shardedTableSet.
            string expectedFinalSchemaInfo = @"<?xml version=""1.0"" encoding=""utf-16""?>
<Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <ReferenceTableSet xmlns="""" i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </ReferenceTableSet>
  <ShardedTableSet xmlns="""" i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </ShardedTableSet>
</Schema>";
            string actualFinalSchemaInfo = ToXml(schemaInfo);
            Assert.AreEqual(expectedFinalSchemaInfo, actualFinalSchemaInfo);
        }

        /// <summary>
        /// Verifies that <see cref="SchemaInfo"/>data from EDCL v1.1.0 can be deserialized.
        /// </summary>
        [TestMethod]
        public void DeserializeCompatibilityV110()
        {
            // Why is this slightly different from the XML in the SerializeCompatibility test?
            // Because this XML comes from SQL Server, which uses different formatting than DataContractSerializer.
            // The Deserialize test uses the XML formatted by SQL Server because SQL Server is where it will
            // come from in the end-to-end scenario.
            string originalSchemaInfo = @"<Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <_referenceTableSet i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </_referenceTableSet>
  <_shardedTableSet i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </_shardedTableSet>
</Schema>";

            SchemaInfo schemaInfo = FromXml(originalSchemaInfo);
            Assert.AreEqual(1, schemaInfo.ReferenceTables.Count);
            Assert.AreEqual("r1", schemaInfo.ReferenceTables.First().SchemaName);
            Assert.AreEqual("r2", schemaInfo.ReferenceTables.First().TableName);
            Assert.AreEqual(1, schemaInfo.ShardedTables.Count);
            Assert.AreEqual("s1", schemaInfo.ShardedTables.First().SchemaName);
            Assert.AreEqual("s2", schemaInfo.ShardedTables.First().TableName);
            Assert.AreEqual("s3", schemaInfo.ShardedTables.First().KeyColumnName);

            // Serialize the data back. It should be not contain _referenceTableSet or _shardedTableSet.
            string expectedFinalSchemaInfo = @"<?xml version=""1.0"" encoding=""utf-16""?>
<Schema xmlns:i=""http://www.w3.org/2001/XMLSchema-instance"">
  <ReferenceTableSet xmlns="""" i:type=""ArrayOfReferenceTableInfo"">
    <ReferenceTableInfo>
      <SchemaName>r1</SchemaName>
      <TableName>r2</TableName>
    </ReferenceTableInfo>
  </ReferenceTableSet>
  <ShardedTableSet xmlns="""" i:type=""ArrayOfShardedTableInfo"">
    <ShardedTableInfo>
      <SchemaName>s1</SchemaName>
      <TableName>s2</TableName>
      <KeyColumnName>s3</KeyColumnName>
    </ShardedTableInfo>
  </ShardedTableSet>
</Schema>";
            string actualFinalSchemaInfo = ToXml(schemaInfo);
            Assert.AreEqual(expectedFinalSchemaInfo, actualFinalSchemaInfo);
        }

        private string ToXml(SchemaInfo schemaInfo)
        {
            using (StringWriter sw = new StringWriter())
            {
                using (XmlWriter xw = XmlWriter.Create(sw, new XmlWriterSettings() { Indent = true }))
                {
                    DataContractSerializer serializer = new DataContractSerializer(typeof(SchemaInfo));
                    serializer.WriteObject(xw, schemaInfo);
                }
                return sw.ToString();
            }
        }

        private SchemaInfo FromXml(string schemaInfo)
        {
            using (StringReader sr = new StringReader(schemaInfo))
            {
                using (XmlReader xr = XmlReader.Create(sr))
                {
                    DataContractSerializer serializer = new DataContractSerializer(typeof(SchemaInfo));
                    return (SchemaInfo)serializer.ReadObject(xr);
                }
            }
        }

        private string NewNameWithSpecialChars()
        {
            // We include invalid XML characters in the list of special characters since error messages
            // are sent from SchemaInfo to T-SQL in the form of XML strings.
            //
            char[] specialChars = new char[] { '[', ']', '-', ' ', '\'', '"', '\'', '<', '>', '\\', '&', '%', ':' };
            System.Random rand = new System.Random();
            int nameLen = rand.Next(20) + 1;
            string db = "";

            for (int i = 0; i < nameLen; i++)
            {
                if (rand.Next(2) == 1)
                {
                    db += specialChars[rand.Next(0, specialChars.Length - 1)];
                }
                else
                {
                    db += (char)('a' + rand.Next(0, 26));
                }
            }

            return db;
        }

        private void AssertEqual(SchemaInfo x, SchemaInfo y)
        {
            AssertExtensions.AssertSequenceEquivalent(x.ReferenceTables, y.ReferenceTables);
            AssertExtensions.AssertSequenceEquivalent(x.ShardedTables, y.ShardedTables);
        }
    }
}
