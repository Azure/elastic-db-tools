// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
// Mocks SqlDataReader
// Inherits from DbDataReader

using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.IO;
#if NETFRAMEWORK
using System.Runtime.Remoting;
#endif
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    public class MockSqlDataReader : DbDataReader
    {
        private bool _isClosed = false;

        #region Ctors

        public MockSqlDataReader()
            : this("MockReader", new DataTable())
        {
        }

        public MockSqlDataReader(string name)
            : this(name, new DataTable())
        {
            Name = name;
        }

        public MockSqlDataReader(string name, DataTable dataTable)
        {
            Name = name;
            DataTable = dataTable;
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public override int Depth
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public override int FieldCount
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Gets a value that indicates whether this DbDataReader contains one or more rows.
        /// </summary>
        public override bool HasRows
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value indicating whether the DbDataReader is closed.
        /// </summary>
        public override bool IsClosed
        {
            get
            {
                return _isClosed;
            }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override object this[int ordinal]
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of Object.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The value of the specified column.</returns>
        public override object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        /// <remarks>
        /// However, from the SqlDataReader source, it looks like the property is updated before the reader is closed
        /// and is initialized to -1 by default. So, we'll return -1 always since we only allow SELECT statements.
        /// </remarks>
        public override int RecordsAffected
        {
            get { return -1; }
        }

        /// <summary>
        /// </summary>
        public override int VisibleFieldCount
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Callback to execute when ReadAsync is invoked
        /// </summary>
        public Func<MockSqlDataReader, Task<bool>> ExecuteOnReadAsync { get; set; }

        public Action<MockSqlDataReader> ExecuteOnGetColumn { get; set; }

        /// <summary>
        /// The name of this reader
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The data served by this reader
        /// </summary>
        public DataTable DataTable { get; set; }

        #endregion

        #region Suppported methods

        /// <summary>
        /// Closes the reader
        /// </summary>
        public override void Close()
        {
            _isClosed = true;
        }

        /// <summary>
        /// Opens the reader
        /// </summary>
        public void Open()
        {
            _isClosed = false;
        }

#if NETFRAMEWORK
        /// <summary>
        /// Not implemented
        ///</summary>
        public override ObjRef CreateObjRef(Type requestedType)
        {
            throw new NotImplementedException();
        }
#endif

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a byte.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by dataOffset, into the
        /// buffer, starting at the location indicated by bufferOffset.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the row from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy the data.</param>
        /// <param name="bufferOffset">The index with the buffer to which the data will be copied.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <returns>The actual number of bytes read.</returns>
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a single character.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Reads a stream of characters from the specified column, starting at location indicated by dataOffset, into
        /// the buffer, starting at the location indicated by bufferOffset.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the row from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy the data.</param>
        /// <param name="bufferOffset">The index with the buffer to which the data will be copied.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <returns>The actual number of characters read.</returns>
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>A string representing the name of the data type.</returns>
        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a DateTime object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns a DbDataReader object for the requested column ordinal that can be overridden with a
        /// provider-specific implementation.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>A DbDataReader object.</returns>
        /// <remarks>
        /// </remarks>
        protected override DbDataReader GetDbDataReader(int ordinal)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a Decimal object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a Double object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns an IEnumerator that can be used to iterate through the rows in the data reader.
        /// </summary>
        /// <returns>An IEnumerator that can be used to iterate through the rows in the data reader.</returns>
        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The data type of the specified column.</returns>
        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Synchronously gets the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">The Type to get the value of the column as.</typeparam>
        /// <param name="ordinal">The column to be retrieved.</param>
        /// <returns>The column to be retrieved.</returns>
        public override T GetFieldValue<T>(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not implemented
        /// </summary>
        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a single-precision floating point number.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a 16-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a 32-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override int GetInt32(int ordinal)
        {
            ExecuteOnGetColumn(this);
            return 0;
        }

        /// <summary>
        /// Gets the value of the specified column as a 64-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The name of the specified column.</returns>
        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// </summary>
        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// not implmeented
        /// </summary>
        public override object GetProviderSpecificValue(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets all provider-specific attribute columns in the collection for the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of Object in the array.</returns>
        public override int GetProviderSpecificValues(object[] values)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns a DataTable that describes the column metadata of the DbDataReader.
        /// </summary>
        /// <returns>A DataTable that describes the column metadata.</returns>
        public override DataTable GetSchemaTable()
        {
            return DataTable;
        }

        /// <summary>
        /// Retrieves data as a Stream.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The returned object.</returns>
        public override Stream GetStream(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of String.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Retrieves data as a TextReader.
        /// </summary>
        /// <param name="ordinal">Retrieves data as a TextReader.</param>
        /// <returns>The returned object.</returns>
        /// <remarks>
        /// We let SqlDataReader handle it which returns a TextReader to read the column
        /// </remarks>
        public override TextReader GetTextReader(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Populates an array of objects with the column values of the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of Object in the array.</returns>
        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not implemented
        /// </summary>
        public override object InitializeLifetimeService()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains nonexistent or missing values.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>true if the specified column is equivalent to DBNull; otherwise false.</returns>
        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Async DBNull
        /// </summary>
        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.
        /// </summary>
        /// <returns>true if there are more result sets; otherwise false.</returns>
        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Next result in async
        /// </summary>
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public override bool Read()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Async read
        /// </summary>
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ExecuteOnReadAsync(this);
        }

        /// <summary>
        /// Releases the managed resources used by the DbDataReader and optionally releases the unmanaged resources.
        /// </summary>
        /// <param name="disposing">
        /// true to release managed and unmanaged resources; false to release only unmanaged resources.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

#endregion
    }
}
