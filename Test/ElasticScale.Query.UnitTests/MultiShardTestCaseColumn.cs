// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
//  Utility class that maintains column information about the test columns in our 
//  sharded database table.
//
// Notes:
//  Aim is to centralize the column information to make it easier
//  to perform exhaustive testing on our value getters and conversions, etc.
//  Conversions from: 
//  http://msdn.microsoft.com/en-us/library/vstudio/cc716729(v=vs.110).aspx

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Xml;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    internal class MutliShardTestCaseColumn
    {
        /// <summary>
        /// Private static member that holds our defined test columns.
        /// </summary>
        private static IReadOnlyList<MutliShardTestCaseColumn> s_definedColumns;

        /// <summary>
        /// Private static memeber that holds our pseudo column.
        /// </summary>
        private static MutliShardTestCaseColumn s_shardNamePseudoColumn;

        /// <summary>
        /// Private immutable object member that holds the SqlServer engine type.
        /// </summary>
        private readonly string _sqlServerDatabaseEngineType;

        /// <summary>
        /// The text column type declaration to use. 
        /// </summary>
        private readonly string _columnTypeDeclaration;

        /// <summary>
        /// The SqlDbType for this column. 
        /// </summary>
        private readonly SqlDbType _dbType;

        /// <summary>
        /// The field length (if applicable).
        /// </summary>
        private readonly int _fieldLength;

        /// <summary>
        /// The name to use when creating the column in the test database.
        /// </summary>
        private readonly string _testColumnName;

        /// <summary>
        /// Private, and only, c-tor for MutliShardTestCaseColumn objects.  It is private only
        /// so that we can tightly control the columns that appear in our test code.
        /// </summary>
        /// <param name="engineType">String representing the SQL Server Engine data type name.</param>
        /// <param name="columnTypeDeclaration">
        /// The text to use when declaring the column data type when setting up our test tables.
        /// </param>
        /// <param name="dbType">SqlDbType enum value for this column.</param>
        /// <param name="fieldLength">
        /// The max length to pull for variable length accessors (e.g., GetChars or GetBytes).
        /// </param>
        private MutliShardTestCaseColumn(string engineType, string columnTypeDeclaration, SqlDbType dbType, int fieldLength, string testColumnName)
        {
            _sqlServerDatabaseEngineType = engineType;
            _columnTypeDeclaration = columnTypeDeclaration;
            _dbType = dbType;
            _fieldLength = fieldLength;
            _testColumnName = testColumnName;
        }

        /// <summary>
        /// Static getter that exposes our defined test case columns.
        /// </summary>
        public static IReadOnlyList<MutliShardTestCaseColumn> DefinedColumns
        {
            get
            {
                if (null == s_definedColumns)
                {
                    s_definedColumns = GenerateColumns();
                }
                return s_definedColumns;
            }
        }

        /// <summary>
        /// Static getter that exposes our shard name pseudo column.
        /// </summary>
        public static MutliShardTestCaseColumn ShardNamePseudoColumn
        {
            get
            {
                if (null == s_shardNamePseudoColumn)
                {
                    s_shardNamePseudoColumn = GenerateShardNamePseudoColumn();
                }
                return s_shardNamePseudoColumn;
            }
        }
        /// <summary>
        /// Public getter that exposes the SqlServer Engine data type name for this column.
        /// </summary>
        public string SqlServerDatabaseEngineType { get { return _sqlServerDatabaseEngineType; } }

        /// <summary>
        /// Getter that exposes the string to use to declare the column data type
        /// when creating the test table.  Useful shortcut for allowing us to specify
        /// type length parameters directly without pawing through data structures to generate
        /// them on the fly.
        /// </summary>
        public string ColumnTypeDeclaration { get { return _columnTypeDeclaration; } }

        /// <summary>
        /// Getter that exposes the field length (if applicable) for the data type when accessing it.
        /// Useful shortcut for allowing us to pull variable length data without pawing
        /// through column type data structures.
        /// </summary>
        public int FieldLength { get { return _fieldLength; } }

        /// <summary>
        /// Getter that exposes the SqlDbType for the column type when accessing it.
        /// Useful shortcut for allowing us to pull type information without pawing
        /// through column type data structures.
        /// </summary>
        public SqlDbType DbType { get { return _dbType; } }

        /// <summary>
        /// Getter that gives us the column name for this column in the test database.
        /// </summary>
        public string TestColumnName { get { return _testColumnName; } }

        /// <summary>
        /// Static helper to produce the $ShardName pseudo column for use in comparisons when testing.
        /// </summary>
        /// <returns></returns>
        private static MutliShardTestCaseColumn GenerateShardNamePseudoColumn()
        {
            return new MutliShardTestCaseColumn("nvarchar", "nvarchar(4000)", SqlDbType.NVarChar, 4000, "$ShardName");
        }

        /// <summary>
        /// Static helper to generate the columns we will test.
        /// </summary>
        /// <returns></returns>
        private static IReadOnlyList<MutliShardTestCaseColumn> GenerateColumns()
        {
            List<MutliShardTestCaseColumn> theColumns = new List<MutliShardTestCaseColumn>();

            string sqlName;
            int length;
            string fieldDecl;
            SqlDbType dbType;

            // bigint
            sqlName = "bigint";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.BigInt;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // binary
            sqlName = "binary";
            length = 100;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.Binary;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // bit
            sqlName = "bit";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Bit;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // char
            sqlName = "char";
            length = 50;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.Char;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // date
            sqlName = "date";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Date;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // datetime
            sqlName = "datetime";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.DateTime;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // datetime2
            sqlName = "datetime2";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.DateTime2;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // datetimeoffset
            sqlName = "datetimeoffset";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.DateTimeOffset;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // decimal
            sqlName = "decimal";
            length = 38;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.Decimal;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // FILESTREAM/varbinary(max)

            // float
            sqlName = "float";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Float;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // image
            sqlName = "image";
            length = 75;
            fieldDecl = sqlName;
            dbType = SqlDbType.Image;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // int
            sqlName = "int";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Int;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // money
            sqlName = "money";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Money;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // nchar
            sqlName = "nchar";
            length = 255;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.NChar;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // ntext
            sqlName = "ntext";
            length = 20;
            fieldDecl = sqlName;
            dbType = SqlDbType.NText;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // numeric
            sqlName = "numeric";
            length = 38;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.Decimal;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // nvarchar
            sqlName = "nvarchar";
            length = 10;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.NVarChar;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // real
            sqlName = "real";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Real;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // rowversion

            // smalldatetime
            sqlName = "smalldatetime";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.SmallDateTime;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // smallint
            sqlName = "smallint";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.SmallInt;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // smallmoney
            sqlName = "smallmoney";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.SmallMoney;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // sql_variant

            // text
            sqlName = "text";
            length = 17;
            fieldDecl = sqlName;
            dbType = SqlDbType.Text;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // time
            sqlName = "time";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.Time;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // timestamp
            sqlName = "timestamp";
            length = 8;
            fieldDecl = sqlName;
            dbType = SqlDbType.Timestamp;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // tinyint
            sqlName = "tinyint";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.TinyInt;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // uniqueidentifier
            sqlName = "uniqueidentifier";
            length = -1;
            fieldDecl = sqlName;
            dbType = SqlDbType.UniqueIdentifier;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // varbinary
            sqlName = "varbinary";
            length = 4;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.VarBinary;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // varchar
            sqlName = "varchar";
            length = 13;
            fieldDecl = string.Format("{0}({1})", sqlName, length);
            dbType = SqlDbType.VarChar;
            AddColumnToList(sqlName, fieldDecl, dbType, length, theColumns);

            // xml

            return new ReadOnlyCollection<MutliShardTestCaseColumn>(theColumns);
        }

        /// <summary>
        /// Static helper to package up the relevant info into a MutliShardTestCaseColumn object.
        /// </summary>
        /// <param name="sqlTypeName">The SQL Server database engine data type sqlTypeName.</param>
        /// <param name="sqlFieldDeclarationText">The text to use to create this column in SQL Server.</param>
        /// <param name="dbType">The SqlDbType for this column.</param>
        /// <param name="length">The length of the column (usefulf for char/binary/etc.</param>
        /// <param name="listToAddTo">The list to add the newly created column into.</param>
        private static void AddColumnToList(string sqlTypeName, string sqlFieldDeclarationText, SqlDbType dbType, int length, List<MutliShardTestCaseColumn> listToAddTo)
        {
            string colName = GenerateTestColumnName(sqlTypeName);
            MutliShardTestCaseColumn toAdd = new MutliShardTestCaseColumn(sqlTypeName, sqlFieldDeclarationText, dbType, length, colName);
            listToAddTo.Add(toAdd);
        }

        /// <summary>
        /// Static helper to auto-generate test column names.
        /// </summary>
        /// <param name="sqlTypeName">The sql server type sqlTypeName of the column (e.g., numeric).</param>
        /// <returns>A string formatted as Test_{0}_Field where {0} is the type sqlTypeName parameter.</returns>
        private static string GenerateTestColumnName(string sqlTypeName)
        {
            return string.Format("Test_{0}_Field", sqlTypeName);
        }
    }
}
