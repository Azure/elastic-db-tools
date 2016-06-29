// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
//  Logic that wraps multiple DbDataReader objects and aggregates them 
//  (UNION ALL semantics) under the hood to provide the illusion that all 
//  results came from a single DbDataReader.
//
// Notes:
//  Aim is to integrate this within a broader cleint-side wrapper framework.
//  Probably will not expose this as a standalone public class on its own.
//  CLASS IS NOT THREAD SAFE.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Runtime.Remoting;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

// DEVNOTE (VSTS 2202707): This should go into the namespace that we are using for all the Wrapper classes. Since we aren't integrated
// with those classes yet, we'll just use this namespace and have it change later when we integrate.
//

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale:  
    //   MultiShardDataReader is not a collection.
    //   "Multi" is the spelling we want.
    //   We can't move the methods to other types because that would break the interface we are aiming to provide.
    //
    /// <summary>
    /// Provides a way of reading a forward-only stream of rows that is retrieved from a shard set. 
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling"),
     System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1710:IdentifiersShouldHaveCorrectSuffix"),
     System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"),
     System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1010:CollectionsShouldImplementGenericInterface")]
    public sealed class MultiShardDataReader : DbDataReader, IDataReader, IDisposable, IDataRecord
    {
        /// <summary>
        /// Name of Shard Id pseudo column.
        /// </summary>
        private const String NameOfShardIdPseudoColumn = "$ShardName";

        #region Private Fields

        private readonly static ILogger s_tracer = TraceHelper.Tracer;

        /// <summary>
        /// Collection of labeled readers.
        /// </summary>
        private ConcurrentQueue<LabeledDbDataReader> _labeledReaders;

        /// <summary>
        /// Lock for mutually exclusive access to labeled readers collection.
        /// </summary>
        private object _addLabeledReaderLock = new object();

        /// <summary>
        /// Lock for mutual exclusion between cancellation request and close of readers while trying
        /// to read next row from MultiShardDataReader object.
        /// </summary>
        private object _cancelLock = new object();

        /// <summary>
        /// Whether the reader has a shard-id column.
        /// </summary>
        private readonly bool _hasShardIdPseudoColumn;

        private int _numReadersExpected;
        private int _numReadersAdded;
        private int _numReadersFinished;
        private bool _anyReaderHasRows;

        private readonly ConcurrentBag<MultiShardException> _multiShardExceptions;

        private int _indexOfShardIdPseudoColumn;
        private DataTable _schemaComparisonTemplate;
        private DataTable _finalSchemaTable;
        private bool _foundNullSchemaReader;

        private bool _closed;
        private bool _disposed;

        #endregion Private Fields

        #region Constructors

        /// <summary>
        ///     Instantiates a MultiShardDataReader object that wraps DbDataReader objects.
        ///     The DbDataReader objects themselves get added to the MultiShardDataReader via
        ///     calls to AddReader.
        /// </summary>
        /// <param name="command">The <see cref="MultiShardCommand"/> associated with this reader</param>
        /// <param name="inputReaders">The <see cref="LabeledDbDataReader"/> from each shard</param>
        /// <param name="executionPolicy">The execution policy to use</param>
        /// <param name="addShardNamePseudoColumn">True if we should add the $ShardName pseudo column, false if not.</param>
        /// <param name="expectedReaderCount">(Optional) If a number greater than the length of inputReaders is 
        /// specified, the MultiShardDataReader is left open for additional calls to AddReader at a later time.</param>
        /// <exception cref="MultiShardSchemaMismatchException">If the complete results execution policy is used and 
        /// the schema isn't the same across shards</exception>
        internal MultiShardDataReader(
            MultiShardCommand command,
            LabeledDbDataReader[] inputReaders,
            MultiShardExecutionPolicy executionPolicy,
            bool addShardNamePseudoColumn,
            int expectedReaderCount = -1)
        {
            Contract.Requires(command != null);

            this.Connection = command.Connection;

            _labeledReaders = new ConcurrentQueue<LabeledDbDataReader>();

            this.ExecutionPolicy = executionPolicy;
            _hasShardIdPseudoColumn = addShardNamePseudoColumn;

            _multiShardExceptions = new ConcurrentBag<MultiShardException>();
            _numReadersExpected = Math.Max(inputReaders.Length, expectedReaderCount);

            // Add the readers
            foreach (LabeledDbDataReader reader in inputReaders)
            {
                AddReader(reader);
            }

            // Transition the reader to closed if there are no readers
            if (_numReadersExpected == 0)
            {
                this.Close();
            }
        }

        #endregion Constructors

        #region Properties

        /// <summary>
        /// Gets the <see cref="MultiShardConnection"/> associated with the MultiShardDataReader.
        /// </summary>
        public MultiShardConnection Connection
        {
            get;
            private set;
        }

        // Suppression rationale: "Multi" is the spelling we want.
        //
        /// <summary>
        /// Gets the collection of exceptions encountered when processing the command across the shards.
        /// The collection is populated when <see cref="MultiShardExecutionPolicy.PartialResults"/> is chosen
        /// as the execution policy for the command.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi")]
        public IReadOnlyCollection<MultiShardException> MultiShardExceptions
        {
            get
            {
                return _multiShardExceptions.ToArray();
            }
        }

        /// <summary>
        /// Gets the execution policy that will be used to execute commands.
        /// </summary>
        public MultiShardExecutionPolicy ExecutionPolicy
        {
            get;
            private set;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Closes the MultiShardDataReader object.
        /// </summary>
        /// <remarks>
        /// Similar to DbDataReader, close is idempotent.
        /// </remarks>
        public override void Close()
        {
            if (this.IsClosed)
            {
                s_tracer.Verbose("MultiShardDataReader is already closed");
                return;
            }

            // Make sure we are not in the middle of an AddReader call when we attempt to Close this reader.
            // This is _theoretically_ safe since any exceptions thrown here should be expected.
            //
            lock (_addLabeledReaderLock)
            {
                if (this.IsClosed)
                {
                    s_tracer.Verbose("MultiShardDataReader is already closed");
                    return;
                }

                // Perform cancellation before close on all the active readers.
                this.Cancel();

                // Then close the current DataReader and all subsequent DataReaders.
                //
                LabeledDbDataReader currentReader;
                LabeledDbDataReader arbitraryClosedReader = null;
                while (_labeledReaders.TryPeek(out currentReader))
                {
                    this.CloseCurrentDataReader();
                    arbitraryClosedReader = currentReader;
                }

                // To avoid writing special case logic for when the queue is empty, we are going to 
                // add an arbitrary closed reader back to the queue, presuming we have any to choose from.
                // If we don't, then default to the "No Data Reader" terminal state instead of introducing
                // the possibility of a new NullReferenceException.
                //
                if (_numReadersExpected > 0 && arbitraryClosedReader != null)
                {
                    _labeledReaders.Enqueue(arbitraryClosedReader);

                    Monitor.Pulse(_addLabeledReaderLock);
                }

                // Finally, set the Closed flag to true.
                //
                _closed = true;
            }
        }

        /// <summary>
        /// This method is currently not supported. Invoking the method will result in an exception.
        /// </summary>
        /// <param name="requestedType">The <see cref="Type"/> of the object that the new <see cref="ObjRef"/> will reference.</param>
        public override ObjRef CreateObjRef(Type requestedType)
        {
            throw new RemotingException("MultiShardDataReader is not a valid remoting object.");
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object. (Inherited from <see cref="Object"/>.)
        /// </summary>
        /// <param name="obj">the object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return (this == obj);
        }

        // Intentionally NOT overriding Finalize().
        //

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override bool GetBoolean(int ordinal)
        {
            return GetColumn<bool>(GetCurrentDataReader().GetBoolean, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a byte.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override byte GetByte(int ordinal)
        {
            return GetColumn<byte>(GetCurrentDataReader().GetByte, ordinal);
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
            WaitForReaderOrThrow();
            ThrowIfPseudoColumnReference(ordinal);
            return this.GetCurrentDataReader().GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// Gets the value of the specified column as a single character.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        /// <remarks>Per MSDN, this is not supported for SqlDataReader.</remarks>
        public override char GetChar(int ordinal)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();
            if (IsPseudoColumnReference(ordinal))
            {
                throw new NotSupportedException("GetChar not supported");
            }
            return this.GetCurrentDataReader().GetChar(ordinal);
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
            InduceErrorIfClosed();
            WaitForReaderOrThrow();

            if (this.IsPseudoColumnReference(ordinal))
            {
                // It is a reference to our ShardName pseudo column.
                //
                if (dataOffset < 0)
                {
                    return 0;
                }
                char[] source = GetString(ordinal).ToCharArray();
                if (null == buffer)
                {
                    buffer = new char[source.Length];
                    source.CopyTo(buffer, 0);
                    return buffer.Length;
                }
                else
                {
                    long charsCopied = 0;
                    long sourcePos = dataOffset;
                    int bufferPos = bufferOffset;
                    while ((sourcePos < source.Length) && (bufferPos < buffer.Length) && (charsCopied < length))
                    {
                        buffer[bufferPos] = source[sourcePos];
                        bufferPos++;
                        sourcePos++;
                        charsCopied++;
                    }
                    return charsCopied;
                }
            }
            else
            {
                return this.GetCurrentDataReader().GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
            }
        }

        // cannot override DbDataReader GetData(int ordinal).  Should we use new?
        //

        /// <summary>
        /// Gets name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>A string representing the name of the data type.</returns>
        public override string GetDataTypeName(int ordinal)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();

            if (this.IsPseudoColumnReference(ordinal))
            {
                return _finalSchemaTable.Rows[ordinal]["DataTypeName"] as string;
            }
            else
            {
                return this.GetCurrentDataReader().GetDataTypeName(ordinal);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a DateTime object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override DateTime GetDateTime(int ordinal)
        {
            return GetColumn<DateTime>(GetCurrentDataReader().GetDateTime, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a DateTimeOffset object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            return GetColumn<DateTimeOffset>(GetCurrentDataReaderAsSqlDataReader().GetDateTimeOffset, ordinal);
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
            throw new NotSupportedException("GetDbDataReader is currently not supported");
        }

        /// <summary>
        /// Gets the value of the specified column as a decimal object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override decimal GetDecimal(int ordinal)
        {
            return GetColumn<decimal>(GetCurrentDataReader().GetDecimal, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a double object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override double GetDouble(int ordinal)
        {
            return GetColumn<double>(GetCurrentDataReader().GetDouble, ordinal);
        }

        /// <summary>
        /// This method is currently not supported. Invoking the method will result in an exception.
        /// </summary>
        /// DEVNOTE VSTS 2202727: Right now this throws a NotSupportedException.  We need to create an object that handles the iteration properly.
        /// When we do the implementation we should consider sub-classing DbEnumerator.
        public override IEnumerator GetEnumerator()
        {
            throw new NotSupportedException("GetEnumerator is currently not supported");
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The data type of the specified column.</returns>
        public override Type GetFieldType(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<Type>(ordinal, typeof(string), GetCurrentDataReader().GetFieldType);
        }

        /// <summary>
        /// Synchronously gets the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">The Type to get the value of the column as.</typeparam>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override T GetFieldValue<T>(int ordinal)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();

            if (this.IsPseudoColumnReference(ordinal))
            {
                // It is a reference to our ShardName pseudo column.
                // In this case, only a string type is valid for our T parameter, so check that, and 
                // either return properly or throw an InvalidCast.
                //

                if (typeof(T) == typeof(string))
                {
                    string rVal = this.GetCurrentShardLabel();
                    return (T)Convert.ChangeType(rVal, typeof(T));
                }
                throw new InvalidCastException();
            }
            else
            {
                return this.GetCurrentDataReader().GetFieldValue<T>(ordinal);
            }
        }

        /// <summary>
        /// Asynchronously gets the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">The type of the value to be returned.</typeparam>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">
        /// The cancellation instruction, which propagates a notification that operations should be canceled. This does 
        /// not guarantee the cancellation. A setting of CancellationToken.None makes this method equivalent to 
        /// GetFieldValueAsync. The returned task must be marked as cancelled.
        /// </param>
        /// <returns>The value of the specified column.</returns>
        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            WaitForReaderOrThrow();

            if (this.IsPseudoColumnReference(ordinal))
            {
                return Task.FromResult<T>(GetFieldValue<T>(ordinal));
            }

            return this.GetCurrentDataReader().GetFieldValueAsync<T>(ordinal, cancellationToken);
        }

        /// <summary>
        /// Gets the value of the specified column as a single-precision floating point number.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override float GetFloat(int ordinal)
        {
            return GetColumn<float>(GetCurrentDataReader().GetFloat, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override Guid GetGuid(int ordinal)
        {
            return GetColumn<Guid>(GetCurrentDataReader().GetGuid, ordinal);
        }

        /// <summary>
        /// Serves as the default hash function that is useful for quick checks on equality.
        /// </summary>
        /// <returns>A hash code for the current object.</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        /// <summary>
        /// Gets the value of the specified column as a 16-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override short GetInt16(int ordinal)
        {
            return GetColumn<short>(GetCurrentDataReader().GetInt16, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a 32-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override int GetInt32(int ordinal)
        {
            return GetColumn<int>(GetCurrentDataReader().GetInt32, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a 64-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override long GetInt64(int ordinal)
        {
            return GetColumn<long>(GetCurrentDataReader().GetInt64, ordinal);
        }

        // Cannot override Object GetLifetimeService().  Should we use new?
        //

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The name of the specified column.</returns>
        public override string GetName(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference(ordinal, MultiShardDataReader.NameOfShardIdPseudoColumn, GetCurrentDataReader().GetName);
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        public override int GetOrdinal(string name)
        {
            WaitForReaderOrThrow();

            // TODO: May need to revisit our StringComparison logic here.
            //
            if (_hasShardIdPseudoColumn &&
                (string.Equals(name, MultiShardDataReader.NameOfShardIdPseudoColumn, StringComparison.OrdinalIgnoreCase)))
            {
                return _indexOfShardIdPseudoColumn;
            }
            else
            {
                return this.GetCurrentDataReader().GetOrdinal(name);
            }
        }

        /// <summary>
        /// Returns the provider-specific field type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The Type object that describes the data type of the specified column.</returns>
        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<Type>(ordinal, typeof(string), GetCurrentDataReader().GetProviderSpecificFieldType);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override object GetProviderSpecificValue(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<object>(ordinal, GetCurrentShardLabel(), GetCurrentDataReader().GetProviderSpecificValue);
        }

        /// <summary>
        /// Gets all provider-specific attribute columns in the collection for the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of Object in the array.</returns>
        public override int GetProviderSpecificValues(object[] values)
        {
            return HandleGetValuesCall(values, GetCurrentDataReader().GetProviderSpecificValues, GetProviderSpecificValue);
        }

        /// <summary>
        /// Returns a <see cref="DataTable"/> that describes the column metadata of the MultiShardDataReader.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> that describes the column metadata.</returns>
        public override DataTable GetSchemaTable()
        {
            return GetPropertyOrVariableWithStateCheck<DataTable>(_finalSchemaTable);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlBinary.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlBinary.</returns>
        public SqlBinary GetSqlBinary(int ordinal)
        {
            return GetColumn<SqlBinary>(GetCurrentDataReaderAsSqlDataReader().GetSqlBinary, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlBoolean.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlBoolean.</returns>
        public SqlBoolean GetSqlBoolean(int ordinal)
        {
            return GetColumn<SqlBoolean>(GetCurrentDataReaderAsSqlDataReader().GetSqlBoolean, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlByte.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlByte.</returns>
        public SqlByte GetSqlByte(int ordinal)
        {
            return GetColumn<SqlByte>(GetCurrentDataReaderAsSqlDataReader().GetSqlByte, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlBytes.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlBytes.</returns>
        public SqlBytes GetSqlBytes(int ordinal)
        {
            return GetColumn<SqlBytes>(GetCurrentDataReaderAsSqlDataReader().GetSqlBytes, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlChars.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlChars.</returns>
        public SqlChars GetSqlChars(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<SqlChars>(ordinal, new SqlChars(GetCurrentShardLabel()), GetCurrentDataReaderAsSqlDataReader().GetSqlChars);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlDateTime.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlDateTime.</returns>
        public SqlDateTime GetSqlDateTime(int ordinal)
        {
            return GetColumn<SqlDateTime>(GetCurrentDataReaderAsSqlDataReader().GetSqlDateTime, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlDecimal.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlDecimal.</returns>
        public SqlDecimal GetSqlDecimal(int ordinal)
        {
            return GetColumn<SqlDecimal>(GetCurrentDataReaderAsSqlDataReader().GetSqlDecimal, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlDouble.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlDouble.</returns>
        public SqlDouble GetSqlDouble(int ordinal)
        {
            return GetColumn<SqlDouble>(GetCurrentDataReaderAsSqlDataReader().GetSqlDouble, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlGuid.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlGuid.</returns>
        public SqlGuid GetSqlGuid(int ordinal)
        {
            return GetColumn<SqlGuid>(GetCurrentDataReaderAsSqlDataReader().GetSqlGuid, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlInt16.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlInt16.</returns>
        public SqlInt16 GetSqlInt16(int ordinal)
        {
            return GetColumn<SqlInt16>(GetCurrentDataReaderAsSqlDataReader().GetSqlInt16, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlInt32.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlInt32.</returns>
        public SqlInt32 GetSqlInt32(int ordinal)
        {
            return GetColumn<SqlInt32>(GetCurrentDataReaderAsSqlDataReader().GetSqlInt32, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlInt64.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlInt64.</returns>
        public SqlInt64 GetSqlInt64(int ordinal)
        {
            return GetColumn<SqlInt64>(GetCurrentDataReaderAsSqlDataReader().GetSqlInt64, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlMoney.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlMoney.</returns>
        public SqlMoney GetSqlMoney(int ordinal)
        {
            return GetColumn<SqlMoney>(GetCurrentDataReaderAsSqlDataReader().GetSqlMoney, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlSingle.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlSingle.</returns>
        public SqlSingle GetSqlSingle(int ordinal)
        {
            return GetColumn<SqlSingle>(GetCurrentDataReaderAsSqlDataReader().GetSqlSingle, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a SqlString.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlString.</returns>
        public SqlString GetSqlString(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<SqlString>(ordinal, new SqlString(GetCurrentShardLabel()), GetCurrentDataReaderAsSqlDataReader().GetSqlString);
        }

        /// <summary>
        /// Returns the data value in the specified column as a native SQL Server type. 
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the column expressed as a SqlDbType.</returns>
        public Object GetSqlValue(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<Object>(ordinal, new SqlString(GetCurrentShardLabel()), GetCurrentDataReaderAsSqlDataReader().GetSqlValue);
        }

        /// <summary>
        /// Fills an array of Object that contains the values for all the columns in the record, 
        /// expressed as native SQL Server types.
        /// </summary>
        /// <param name="values">
        /// An array of Object into which to copy the values. The column values are expressed as SQL Server types.
        /// </param>
        /// <returns>An integer indicating the number of columns copied.</returns>
        public int GetSqlValues(object[] values)
        {
            return HandleGetValuesCall(values, GetCurrentDataReaderAsSqlDataReader().GetSqlValues, GetSqlValue);
        }

        /// <summary>
        /// Gets the value of the specified column as an XML value.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>A SqlXml value that contains the XML stored within the corresponding field.</returns>
        public SqlXml GetSqlXml(int ordinal)
        {
            return GetColumn<SqlXml>(GetCurrentDataReaderAsSqlDataReader().GetSqlXml, ordinal);
        }

        /// <summary>
        /// Retrieves data as a Stream.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The returned object.</returns>
        /// Let the active DbDataReader handle it. It only supports Binary, VarBinary, Udt and Xml types
        public override Stream GetStream(int ordinal)
        {
            return GetColumn<Stream>(GetCurrentDataReader().GetStream, ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of String.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override string GetString(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<string>(ordinal, GetCurrentShardLabel(), GetCurrentDataReader().GetString);
        }

        // Suppression rationale: We will be returning the StringReader if it is a pseudo colkumn reference.  We don't want to dispose it.
        //
        /// <summary>
        /// Retrieves data as a TextReader.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The returned object.</returns>
        /// We let DbDataReader handle it which returns a TextReader to read the column
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public override TextReader GetTextReader(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<TextReader>(ordinal, new StringReader(GetCurrentShardLabel()), GetCurrentDataReader().GetTextReader);
        }

        /// <summary>
        /// Retrieves the value of the specified column as a TimeSpan object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public TimeSpan GetTimeSpan(int ordinal)
        {
            return GetColumn<TimeSpan>(GetCurrentDataReaderAsSqlDataReader().GetTimeSpan, ordinal);
        }

        // Cannot override GetType().
        //

        /// <summary>
        /// Gets the value of the specified column as an instance of Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override object GetValue(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<object>(ordinal, GetCurrentShardLabel(), GetCurrentDataReader().GetValue);
        }

        /// <summary>
        /// Populates an array of objects with the column values of the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        /// <returns>The number of instances of Object in the array.</returns>
        public override int GetValues(object[] values)
        {
            return HandleGetValuesCall(values, GetCurrentDataReader().GetValues, GetValue);
        }

        /// <summary>
        /// Retrieves data of type XML as an XmlReader.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The data as an XmlReader.</returns>
        public XmlReader GetXmlReader(int ordinal)
        {
            return GetColumn<XmlReader>(GetCurrentDataReaderAsSqlDataReader().GetXmlReader, ordinal);
        }

        /// <summary>
        /// This method is currently not supported. Invoking the method will result in an exception.
        /// </summary>
        public override object InitializeLifetimeService()
        {
            throw new NotSupportedException("InitializeLifetimeService is currently not supported");
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains nonexistent or missing values (NULL values).
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>True if the specified column is equivalent to DBNull; otherwise false.</returns>
        public override bool IsDBNull(int ordinal)
        {
            return ProcessPotentialPseudoColumnReference<bool>(ordinal, (null == GetCurrentShardLabel()), GetCurrentDataReader().IsDBNull);
        }

        // Cannot override IsDBNullAsync(int ordinal).  should we use new? 
        //

        /// <summary>
        /// An asynchronous version of IsDBNull, which gets a value that indicates whether the column contains 
        /// nonexistent or missing values (NULL values).
        /// </summary>
        /// <param name="ordinal">The zero-based column to be retrieved.</param>
        /// <param name="cancellationToken">
        /// The cancellation instruction, which propagates a notification that operations should be canceled. This does
        /// not guarantee the cancellation. A setting of CancellationToken.None makes this method equivalent to 
        /// IsDBNullAsync. The returned task must be marked as cancelled.
        /// </param>
        /// <returns>True if the specified column value is equivalent to DBNull otherwise false.</returns>
        /// <remarks>
        /// </remarks>
        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();
            if (this.IsPseudoColumnReference(ordinal))
            {
                return Task.FromResult<bool>(IsDBNull(ordinal));
            }
            else
            {
                return this.GetCurrentDataReader().IsDBNullAsync(ordinal, cancellationToken);
            }
        }

        // Cannot override Object MemberwiseClone().  Should we use new?
        //

        // Cannot override MarshalByRefObject MemberwiseClone(Boolean cloneIdentity).  Should we use new?
        //

        /// <summary>
        /// This method is currently not supported. Invoking the method will result in an exception.
        /// </summary>
        /// DEVNOTE (VSTS: 2202747): For now we are only supporting single result set.  Need to do some more work if we want to 
        /// handle the multiple result set case.  Especially if we attempt to move to a non "give me them all up front"
        /// approach. This comment applies to all the NextResult-related methods.
        public override bool NextResult()
        {
            return this.NextResultAsync().Result;
        }

        /// <summary>
        /// This method is currently not supported. Invoking the method will result in an exception.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
                {
                    InduceErrorIfClosed();

                    WaitForReaderOrThrow();

                    bool hasNextResult = await GetCurrentDataReader().NextResultAsync(cancellationToken).ConfigureAwait(false);

                    if (hasNextResult)
                    {
                        // Invalidate this instance of the MultiShardDataReader and throw an exception. 
                        // We currently do not support multiple result sets
                        this.Close();

                        throw new NotSupportedException("Commands with multiple result sets are currently not supported");
                    }

                    return false;
                });
        }

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns>True if there are more rows; otherwise false.</returns>
        public override bool Read()
        {
            try
            {
                return ReadAsync(CancellationToken.None).Result;
            }
            catch (AggregateException ex)
            {
                // When we synchronously wait on Task.Result, any exception thrown
                // inside the task is wrapped in an AggregateException. Catch the
                // parent AggregateException here and throw the actual exception to the
                // caller.
                throw ex.Flatten().InnerException;
            }
        }

        /// <summary>
        /// An asynchronous version of Read, which advances the MultiShardDataReader to the next record.
        ///
        /// The cancellation token can be used to request that the operation be abandoned before the command timeout elapses. 
        /// Exceptions will be reported via the returned Task object.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            InduceErrorIfClosed();

            // As a safety check, make sure that we have at least one data reader to work with. Throw an exception if we do not.
            //
            WaitForReaderOrThrow();

            Stopwatch stopwatch = Stopwatch.StartNew();

            // We can either do this call up front and repeat it in the loop (like it is written), or we can have only
            // one read call in the loop and make the loop stopping condition checking different.  I've chosen to put a
            // call out here since the common case is that we will usually have a row to return so we can skip the setup
            // and iteration through the loop.
            //
            if (await PerformReadToFillBufferAsync(cancellationToken).ConfigureAwait(false))
            {
                stopwatch.Stop();

                // We still have a valid row to fetch on the current reader, so just return true.
                //
                s_tracer.Verbose("MultiShardDataReader.Read.Complete; Duration: {0}; Shard: {1}", stopwatch.Elapsed,
                    GetCurrentShardLocation());
                return true;
            }

            // If we are here, then we hit the end of the current reader.
            // We need to figure out if we have another row ahead of us on a subsequent reader.
            // If so, then move to that row (closing this reader and any empty ones between here and that next row).
            // If not, then we need to stop on the last reader without closing it (we may already be there) which is why
            // the loop terminates on the "-1" condition.
            //
            while (_numReadersFinished < (_numReadersExpected - 1))
            {
                // We are not on the last reader, so we have more to check.
                // First close this reader (which will automatically advance our m_inputsCompletedReadingCount counter).
                //
                this.CloseCurrentDataReader();

                // Wait for the next input reader if it has yet to be added.
                //
                try
                {
                    WaitForReaderOrThrow();
                }
                catch (MultiShardDataReaderInternalException)
                {
                    // This should only happen if we were waiting on reader that was promised to us, but then while waiting
                    // we are told not to expect any more readers.
                    // In this case, we should just give up.
                    //
                    return false;
                }

                if (await PerformReadToFillBufferAsync(cancellationToken).ConfigureAwait(false))
                {
                    stopwatch.Stop();

                    s_tracer.Verbose("MultiShardDataReader.Read.Complete; Duration: {0}; Shard: {1}", stopwatch.Elapsed,
                        GetCurrentShardLocation());
                    return true;
                }

                // If we're here then the new reader we just advanced to had no rows, so loop again.
                //
            }

            // If we are here then we are sitting in the position after the last row (if any) on the last reader, so just
            // return false.
            return false;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>A string that represents the current object.</returns>
        /// We mimic DbDataReader and just call Object.ToString()
        /// Overriden to convey intent clearly.
        public override string ToString()
        {
            return base.ToString();
        }

        #endregion Public Methods

        #region Public Properties

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public override int Depth
        {
            get
            {
                return GetPropertyOrVariableWithStateCheck<int>(GetCurrentDataReader().Depth);
            }
        }

        /// <summary>
        /// Gets the number of columns in the current row. 
        /// </summary>
        public override int FieldCount
        {
            get
            {
                return GetFieldCountAdjustedForPseudoColumnWithStateCheck(GetCurrentDataReader().FieldCount);
            }
        }

        /// <summary>
        /// Gets a value that indicates whether this MultiShardDataReader contains one or more rows. 
        /// </summary>
        public override bool HasRows
        {
            get
            {
                return GetPropertyOrVariableWithStateCheck<bool>(_anyReaderHasRows);
            }
        }

        /// <summary>
        /// Gets a value indicating whether the specified MultiShardDataReader is closed.
        /// </summary>
        public override bool IsClosed
        {
            get
            {
                return _closed;
            }
        }

        /// <summary>
        /// Gets the value of the specified column in its native format as an instance of Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public override object this[int ordinal]
        {
            get
            {
                return this.GetValue(ordinal);
            }
        }

        /// <summary>
        /// Gets the value of the specified column in its native format as an instance of Object.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The value of the specified column.</returns>
        public override object this[string name]
        {
            get
            {
                return this.GetValue(this.GetOrdinal(name));
            }
        }

        /// <summary>
        /// This property is currently not supported. Accessing the property will result in an exception.
        /// </summary>
        /// However, from the DbDataReader source, it looks like the property is updated before the reader is closed
        /// and is initialized to -1 by default. So, we'll return -1 always since we only allow SELECT statements.
        public override int RecordsAffected { get { throw new NotSupportedException("RecordsAffected is currently not supported"); } }

        /// <summary>
        /// Gets the number of fields in the MultiShardDataReader that are not hidden.
        /// </summary>
        public override int VisibleFieldCount
        {
            get
            {
                return GetFieldCountAdjustedForPseudoColumnWithStateCheck(GetCurrentDataReader().VisibleFieldCount);
            }
        }

        #endregion Public Properties

        #region Internal Methods

        /// <summary>
        /// Cancels all the active commands executing for this reader.
        /// </summary>
        internal void Cancel()
        {
            lock (_addLabeledReaderLock)
            {
                lock (_cancelLock)
                {
                    // Cancel all the currently open readers.
                    foreach (LabeledDbDataReader currentReader in _labeledReaders)
                    {
                        if (!currentReader.DbDataReader.IsClosed)
                        {
                            currentReader.Command.Cancel();
                        }
                    }
                }
            }
        }

        #endregion Internal Methods

        #region Protected Methods

        /// <summary>
        /// Releases the managed resources used by the DbDataReader and optionally releases the unmanaged resources.
        /// </summary>
        /// <param name="disposing">
        /// true to release managed and unmanaged resources; false to release only unmanaged resources.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                // Also calls Close, so technically no need to call it again if this instance has been disposed.
                base.Dispose(disposing);

                // Close out our readers if they are not already closed.
                //
                if (!this.IsClosed)
                {
                    this.Close();
                }

                // Then dispose all the readers.
                //
                foreach (LabeledDbDataReader inputReader in _labeledReaders)
                {
                    inputReader.Dispose();
                }

                // Close any open connections
                // An extra safety-net just in-case
                // callers don't dispose failed sqlclient readers properly or
                // we failed to keep track of all active readers in m_inputReaders
                this.Connection.Close();

                if (_schemaComparisonTemplate != null)
                {
                    _schemaComparisonTemplate.Dispose();
                    _schemaComparisonTemplate = null;
                }

                if (_finalSchemaTable != null)
                {
                    _finalSchemaTable.Dispose();
                    _finalSchemaTable = null;
                }

                _disposed = true;
                s_tracer.Warning("MultiShardDataReader.Dispose; Reader was disposed");
            }
        }

        #endregion Protected Methods

        #region Internal Methods

        /// <summary>
        /// Method to add another DbDataReader to the set of underlying sources we are concatenating.
        /// </summary>
        /// <param name="toAdd">The DbDataReader to add.</param>
        /// <exception cref="ArgumentNullException">
        /// If the input DbDataReader is null.
        /// </exception>
        /// <exception cref="MultiShardDataReaderInternalException">
        /// If the input DbDataReader is malformed (e.g., null schema table) or if we are in an invalid state
        /// for adding a reader (e.g., Close, or MarkReaderSetComplete has already been called).
        /// </exception>
        /// <exception cref="MultiShardSchemaMismatchException">
        /// If the schema table underlying the data in the input DbDataReader does not match the schema table we are
        /// expecting for the DbDataReader objects underlying this MultiShardDataReader object.
        /// </exception>
        /// <returns>Null if we added successfully.  The encountered exception if we hit an error.</returns>
        /// 
        internal void AddReader(LabeledDbDataReader toAdd)
        {
            if (toAdd != null)
            {
                // Don't try to read from readers that encountered an exception 
                if (toAdd.Exception != null)
                {
                    // We tried adding a reader that we expected, but it was invalid. Let's expect one fewer inputReader as a result.
                    this.DecrementExpectedReaders();
                    _multiShardExceptions.Add(toAdd.Exception);
                    return;
                }

                // Skip adding closed readers.
                if (toAdd.DbDataReader != null && toAdd.DbDataReader.IsClosed)
                {
                    // We tried adding a reader that we expected, but it was invalid. Let's expect one fewer inputReader as a result.
                    this.DecrementExpectedReaders();

                    MultiShardDataReaderClosedException closedException = new MultiShardDataReaderClosedException(
                        String.Format(
                            "The reader for {0} was closed and could not be added.",
                            toAdd.ShardLocation));

                    MultiShardException reportedException = new MultiShardException(toAdd.ShardLocation, closedException);

                    _multiShardExceptions.Add(reportedException);

                    return;
                }

                // Skip adding readers with a null schema table
                if (toAdd.DbDataReader != null && toAdd.DbDataReader.GetSchemaTable() == null)
                {
                    // We tried adding a reader that we expected, but it was invalid. Let's expect one fewer inputReader as a result.
                    this.DecrementExpectedReaders();

                    // Validate schema for each reader, logging an exception if the schema is null.
                    //
                    ValidateNullSchematable(toAdd.ShardLocation);

                    toAdd.DbDataReader.Close();

                    return;
                }

                try
                {
                    AddReaderInternal(toAdd);
                }
                catch (MultiShardSchemaMismatchException smex)
                {
                    // We tried adding a reader that we expected, but it was invalid. Let's expect one fewer inputReader as a result.
                    //
                    this.DecrementExpectedReaders();

                    s_tracer.Error(
                        smex,
                        "MultiShardDataReader.Ctor.Failed to add reader; Execution Policy: {0}; Shard: {1}; Exception: {2}",
                        this.ExecutionPolicy,
                        toAdd.ShardLocation,
                        smex.ToString());

                    if (this.ExecutionPolicy == MultiShardExecutionPolicy.PartialResults)
                    {
                        // Perform cancellation in order to avoid flushing of large resultsets.
                        toAdd.Command.Cancel();

                        toAdd.DbDataReader.Close();

                        _multiShardExceptions.Add(smex);
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            else
            {
                // We tried adding a reader that we expected, but it was invalid. Let's expect one fewer inputReader as a result.
                // Is this the correct logic when we have complete results turned on?  I understand that it is null so we
                // don't know where it came from, but that seems like something that should cause us to throw anyway...
                // Is it correct when we have partial turned on? We may not want to throw, but we should probably log this 
                // somewhere.  
                // Likely related to VSTS 2616238.  Philip will be modifying logic/augmenting tests in this area.
                //
                this.DecrementExpectedReaders();
            }
        }

        /// <summary>
        /// Decrements the number of expected readers.
        /// </summary>
        private void DecrementExpectedReaders()
        {
            lock (_addLabeledReaderLock)
            {
                _numReadersExpected--;

                Monitor.Pulse(_addLabeledReaderLock);
            }
        }

        private void AddReaderInternal(LabeledDbDataReader toAdd)
        {
            if (null == toAdd)
            {
                throw new ArgumentNullException("toAdd");
            }

            // Make sure calls to Close and AddReader and ExpectNoMoreReaders are synchronized.
            // Any exceptions we throw here are expected, so it should _theoretically_ play nicely with the mutex.
            //
            lock (_addLabeledReaderLock)
            {
                if (_numReadersAdded >= _numReadersExpected)
                {
                    throw new MultiShardDataReaderInternalException("Cannot add a new reader after marking the reader set complete.");
                }

                if (this.IsClosed)
                {
                    throw new MultiShardDataReaderInternalException("Cannot add a new reader after marking the reader as closed.");
                }

                ValidateReaderSchema(toAdd);

                // If we are here, then the schema was OK, so add it to the ones we will process.
                // Also, check if this reader has any rows so that we can set the anyReaderHasRows property up-front.
                //
                _labeledReaders.Enqueue(toAdd);

                if (toAdd.DbDataReader.HasRows)
                {
                    _anyReaderHasRows = true;
                }

                Monitor.Pulse(_addLabeledReaderLock);

                _numReadersAdded++;
            }
        }

        internal void ExpectNoMoreReaders()
        {
            // Make sure calls to Close and AddReader and ExpectNoMoreReaders are synchronized.
            // Any exceptions we throw here are expected, so it should _theoretically_ play nicely with the mutex.
            //
            lock (_addLabeledReaderLock)
            {
                _numReadersExpected = _numReadersAdded;
                Monitor.Pulse(_addLabeledReaderLock);
            }
        }

        #endregion Internal Methods

        #region Private Methods

        /// <summary>
        /// Checks the schema of the passed in DbDataReader against the schema we are expecting for our 
        /// fan-out result set.
        /// </summary>
        /// <param name="labeledReader">The LabeledDbDataReader object to check against our expected schema.</param>
        private void ValidateReaderSchema(LabeledDbDataReader labeledReader)
        {
            Contract.Requires(null != labeledReader, "Unexpected null input to ValidateReaderSchema.");
            DbDataReader reader = labeledReader.DbDataReader;
            ShardLocation shard = labeledReader.ShardLocation;

            DataTable currentDataTable = reader.GetSchemaTable();
            if (null == currentDataTable)
            {
                throw new MultiShardDataReaderInternalException("Unexpected null SchemaTable encountered in ValidateReaderSchema.");
            }

            // Check if we encountered a reader with a null schema earlier
            //
            if (_foundNullSchemaReader)
            {
                throw new MultiShardDataReaderInternalException("Unexpected reader with null schema encountered among non-null readers");
            }

            // The SchemaTable holds 1 *row* for each *column* of the actual output returned.  So in order to compare 
            // column metadata for the inputDataReaders we need to compare row information contained in the schemaTable.
            //
            DataRowCollection currentRows = currentDataTable.Rows;
            if (null == currentRows)
            {
                throw new MultiShardDataReaderInternalException("Unexpected null DataRowCollection encountered in ValidateReaderSchema.");
            }

            if (null == _schemaComparisonTemplate)
            {
                // This is our first call to validate, so grab the table template off of this guy and use it as the 
                // expected schema for our results.  No need to validate since we are using this one as ground truth.
                //
                InitSchemaTemplate(reader);
                return;
            }

            // First let's make sure that the formats of the schema tables are the same.
            //
            ValidateSchemaTableStructure(shard, currentDataTable, _schemaComparisonTemplate);

            // Now Validate that the same columns exist with the same name and type in the same ordinal position
            // on the retruned result sets themsleves.
            //
            ValidateSchemaTableContents(shard, currentDataTable, _schemaComparisonTemplate);
        }

        /// <summary>
        /// Helper that compares the columns present in two different DataTables to ensure that they match.
        /// </summary>
        /// <param name="shardLocation">The shard being validated</param>
        /// <param name="toValidate">The DataTable to validate.</param>
        /// <param name="expected">The DataTable we expect to see.</param>
        /// <remarks>
        /// We should expect to see few (if any) errors actually encountered in here since these are
        /// Schema Table structure comparisons (i.e., comparisons in how the Schema information is reported out)
        /// and not comparisons on the schema of the returned result sets themselves (which is contained in
        /// the rows of these tables).
        /// </remarks>
        private static void ValidateSchemaTableStructure(ShardLocation shardLocation, DataTable toValidate, DataTable expected)
        {
            // Let's make sure that we have the same column count and the same column names, orderings,
            // types, etc...
            //

            // Here we check the column count.
            //
            if (toValidate.Columns.Count != expected.Columns.Count)
            {
                throw new MultiShardSchemaMismatchException(shardLocation, String.Format(
                    "Expected {0} columns on the schema table, but encountered {1} columns.",
                    expected.Columns.Count, toValidate.Columns.Count));
            }

            // Now we'll go column by column and check the information that we care about.
            //
            for (int i = 0; i < expected.Columns.Count; i++)
            {
                DataColumn dcExpected = expected.Columns[i];
                DataColumn dcToValidate = toValidate.Columns[i];

                if (!(dcExpected.ColumnName.Equals(dcToValidate.ColumnName)))
                {
                    throw new MultiShardSchemaMismatchException(shardLocation, String.Format
                        ("Expected schema column name {0}, but encounterd schema column name {1}.",
                        dcExpected.ColumnName, dcToValidate.ColumnName));
                }

                if (!(dcExpected.AllowDBNull == dcToValidate.AllowDBNull))
                {
                    throw new MultiShardSchemaMismatchException(shardLocation, String.Format(
                        "Mismatched AllowDBNull values for schema column {0}.",
                        dcExpected.ColumnName));
                }

                if (!(dcExpected.DataType.Equals(dcToValidate.DataType)))
                {
                    throw new MultiShardSchemaMismatchException(shardLocation, String.Format(
                        "Mismatched DataType values for schema column {0}.",
                        dcExpected.ColumnName));
                }

                if (!(dcExpected.MaxLength.Equals(dcToValidate.MaxLength)))
                {
                    throw new MultiShardSchemaMismatchException(shardLocation, String.Format(
                        "Mismatched MaxLength values for schema column {0}.",
                        dcExpected.ColumnName));
                }
            }
        }

        /// <summary>
        /// Checks all the column specifications (as encapsulated by a DataRow from a SchemaTable) for 
        /// compatibility with the expected column specification.
        /// </summary>
        /// <param name="shardLocation">The shard being validated</param>
        /// <param name="toValidate">The DataRow representing the column specification we wish to validate.</param>
        /// <param name="expected">The DataRow representing the expectd column specification.</param>
        /// <remarks>
        /// There are lot of opportunities in here for relaxed comparison semantics, but for now let's 
        /// just be super strict. 
        /// DEVNOTE (2244709): Need to tighten up our schema checking!
        /// </remarks>
        /// <exception cref="MultiShardSchemaMismatchException">
        /// If there is a mismatch on any column information in for any column of the table (i.e., if any pair
        /// of corresponding rows don't match exactly.)
        /// </exception>
        private static void ValidateSchemaTableContents(ShardLocation shardLocation, DataTable toValidate, DataTable expected)
        {
            DataColumnCollection dcc = expected.Columns;
            DataRowCollection rowsToValidate = toValidate.Rows;
            DataRowCollection rowsExpected = expected.Rows;

            // Eventually we may wish to be a bit more relaxed about the comparisons (e.g., if we expect bigint and we
            // see int that may be ok) but for now let's just be super-strict and make the behavior different and/or 
            // configurable later.
            //
            for (int curRowIndex = 0; curRowIndex < rowsExpected.Count; curRowIndex++)
            {
                DataRow rowToValidate = rowsToValidate[curRowIndex];
                DataRow rowTemplate = rowsExpected[curRowIndex];
                foreach (DataColumn col in dcc)
                {
                    if (!(rowToValidate[col.ColumnName].Equals(rowTemplate[col.ColumnName])))
                    {
                        throw new MultiShardSchemaMismatchException(shardLocation, String.Format(
                            "Expected a value of {0} for {1}, but encountered a value of {2} instead for column {3}.",
                            rowTemplate[col.ColumnName], col.ColumnName, rowToValidate[col.ColumnName],
                            rowTemplate["ColumnName"]));
                    }
                }
            }
        }

        /// <summary>
        /// Handles the case where a reader has a null schema table
        /// 
        /// Behavior-
        /// - Any exception will not be thrown if ALL readers have a null schema (regardless of execution
        /// policy). Otherwise, a <see cref="MultiShardDataReaderInternalException"/> will be thrown.
        /// </summary>
        /// <param name="shard">A shard that resulted in a reader with a null Schema table</param>
        private void ValidateNullSchematable(ShardLocation shard)
        {
            if (shard == null)
            {
                throw new ArgumentNullException("shard");
            }

            if (_numReadersAdded != 0)
            {
                throw new MultiShardDataReaderInternalException
                    ("Unexpected reader with null schema encountered among non-null readers");
            }

            _foundNullSchemaReader = true;
        }

        /// <summary>
        /// Closes the current DbDataReader and increments the current reader counter.
        /// </summary>
        private void CloseCurrentDataReader()
        {
            if (0 == _numReadersExpected)
            {
                // If we happen to be in the special case where we have 0 inputs, then we can throw.
                // We should have thrown already, but we'll throw here just in case.
                throw new MultiShardDataReaderInternalException("No input readers present.");
            }

            LabeledDbDataReader toClose = this.RemoveCurrentLabeledDbDataReader();

            Contract.Requires(null != toClose, "Null DbDataReader encountered in call to CloseDataReader.");

            if (!toClose.DbDataReader.IsClosed)
            {
                // Avoid the race condition resulting in dead-locks in SqlClient that might occur between Cancellation and Close.
                lock (_cancelLock)
                {
                    toClose.DbDataReader.Close();

                    // Close the connection associated with this reader as well
                    toClose.Connection.Close();
                }
            }
            else
            {
                if (this.IsClosed)
                {
                    // If we want to throw on duplicate closes, we would do that right here.  For now, let's just
                    // let it slide.
                }

                // It should never be closed already if the top-level reader is not closed, but in case it 
                // happens we don't want to completely freeze the reader with no way to move forward and
                // no way to close, so just ignore that it's already closed.
            }

            _numReadersFinished++;
        }

        /// <summary>
        /// Helper method to grab the current data reader based on the m_inputsCompletedReadingCount pointer into the m_inputReaders array.
        /// </summary>
        /// <returns>The current DbDataReader.</returns>
        private DbDataReader GetCurrentDataReader()
        {
            return PeekCurrentLabeledDbDataReader().DbDataReader;
        }

        /// <summary>
        /// Helper to grab the ShardLocation for the current reader.
        /// </summary>
        /// <returns>The current Shard Label.</returns>
        private ShardLocation GetCurrentShardLocation()
        {
            return PeekCurrentLabeledDbDataReader().ShardLocation;
        }

        /// <summary>
        /// Helper to grab the ShardLabel for the current reader.
        /// </summary>
        /// <returns>The current Shard Label.</returns>
        private string GetCurrentShardLabel()
        {
            return PeekCurrentLabeledDbDataReader().ShardLabel;
        }

        /// <summary>
        /// Helper to grab the active LabaeledDataReader without removing it from the collection.
        /// </summary>
        /// <returns>The current LabeledDbDataReader.</returns>
        private LabeledDbDataReader PeekCurrentLabeledDbDataReader()
        {
            LabeledDbDataReader outputDataReader;

            if (!_labeledReaders.TryPeek(out outputDataReader))
            {
                // Should never happen.
                throw new MultiShardDataReaderInternalException("Did not have a current LabeledDbDataReader.");
            }

            return outputDataReader;
        }

        /// <summary>
        /// Helper to grab the active LabaeledDataReader without removing it from the collection.
        /// </summary>
        /// <returns>The current LabeledDbDataReader.</returns>
        private LabeledDbDataReader RemoveCurrentLabeledDbDataReader()
        {
            LabeledDbDataReader outputDataReader;

            if (!_labeledReaders.TryDequeue(out outputDataReader))
            {
                // Should never happen.
                throw new MultiShardDataReaderInternalException("Did not have a current LabeledDbDataReader.");
            }

            return outputDataReader;
        }

        /// <summary>
        /// Helper method to grab the current data reader and cast it to a SqlDataReader.
        /// Useful for SqlDataReader specific calls.
        /// </summary>
        /// <returns>
        /// The current data reader cast as a SqlDataReader.
        /// </returns>
        /// <exception cref="InvalidCastException">
        /// If the cast failed.
        /// </exception>
        private SqlDataReader GetCurrentDataReaderAsSqlDataReader()
        {
            return GetCurrentDataReader() as SqlDataReader;
        }

        /// <summary>
        /// Helper method that sets up the SchemaTemplate to use as our "Ground Truth" for
        /// performing schema comparisons.  In addition to storing a copy of the schema 
        /// information, this method adds an additional row for the "ShardIdPseudoColumn".
        /// </summary>
        /// <param name="templateReader">
        /// The DbDataReader to use as the source for the ground truth schema information.
        /// </param>
        private void InitSchemaTemplate(DbDataReader templateReader)
        {
            _schemaComparisonTemplate = templateReader.GetSchemaTable().Copy();
            _finalSchemaTable = templateReader.GetSchemaTable().Copy();

            if (_hasShardIdPseudoColumn)
            {
                AddShardIdPseudoColumnRecordToSchemaTable();
            }
        }

        /// <summary>
        /// Helper method that throws an InvalidCastException for the case when we have detected
        /// a call to get the value for our ShardIdPseudoColumn for anything but the String data type.
        /// We need to do this because if we just pass the call down to the input reader we will get
        /// the wrong exception type (since the pseudo column does not exist on the input readers).
        /// </summary>
        /// <param name="ordinal">The ordinal of the column we are attempting to read from.</param>
        private void ThrowIfPseudoColumnReference(int ordinal)
        {
            if (IsPseudoColumnReference(ordinal))
            {
                InduceErrorIfClosed();
                throw new InvalidCastException();
            }
        }

        /// <summary>
        /// Helper method that checks whether the ordinal passed in matches the ordinal of the
        /// shard id pseudo column.
        /// </summary>
        /// <param name="ordinal">The ordinal to check.</param>
        /// <returns>True if the input matches the shard id pseudo column ordinal, False if not.</returns>
        private bool IsPseudoColumnReference(int ordinal)
        {
            return (_hasShardIdPseudoColumn && (ordinal == _indexOfShardIdPseudoColumn));
        }

        /// <summary>
        /// Helper to grab column data and perform necessary state and pseudo column reference checks.
        /// </summary>
        /// <typeparam name="T">The type of the column we wish to return.</typeparam>
        /// <param name="getterFunction">The function to use to pull the column value.</param>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// 
        /// <returns>The desired column value.</returns>
        private T GetColumn<T>(Func<int, T> getterFunction, int ordinal)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();
            ThrowIfPseudoColumnReference(ordinal);
            return getterFunction(ordinal);
        }

        /// <summary>
        /// Helper function that returns a property or variable after preceding it with a state check.
        /// </summary>
        /// <typeparam name="T">The type of the property or variable to return.</typeparam>
        /// <param name="value">The property or variable to return.</param>
        /// <returns>The desired property or variable.</returns>
        private T GetPropertyOrVariableWithStateCheck<T>(T value)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();
            return value;
        }

        /// <summary>
        /// Helper function that returns an int property or variable after preceding it with a state check
        /// and following it with a check that will increment it if this reader has a $ShardName pseudo column.
        /// </summary>
        /// <param name="value">The property or variable to return (potentially adjusted) after the state check.</param>
        /// <returns>The value parameter incremented, if necessary, to account for the pseudo column.</returns>
        private int GetFieldCountAdjustedForPseudoColumnWithStateCheck(int value)
        {
            int rawValue = GetPropertyOrVariableWithStateCheck<int>(value);
            if (_hasShardIdPseudoColumn)
            {
                // If we have a pseudo-column then we need to add 1 to our value.
                //
                rawValue++;
            }
            return rawValue;
        }

        /// <summary>
        /// Helper that centralizes the logic for processing references that could potentially refer to the $ShardName
        /// pseudo column.  All these functions follow the same basic pattern:
        ///   WaitForReaderOrThrow
        ///   Induce an error if we are closed
        ///   Check if this is a pseudo column reference.
        ///   If so:
        ///     Return some pseudo column specific value
        ///   If not:
        ///     Call some function that takes the column ordinal as a param.
        /// This function just templatizes that logic in one place.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        /// <param name="ordinal">The ordinal of the column of interest.</param>
        /// <param name="pseudoColumnValue">The value to return if the ordinal points at the pseudo column.</param>
        /// <param name="nonPseudoColumnValue">
        /// The function to invoke (with "ordinal" as an arg) if the ordinal does not point to the pseudo column.
        /// </param>
        /// <returns></returns>
        private T ProcessPotentialPseudoColumnReference<T>(int ordinal, T pseudoColumnValue, Func<int, T> nonPseudoColumnValue)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();
            if (IsPseudoColumnReference(ordinal))
            {
                return pseudoColumnValue;
            }
            else
            {
                return nonPseudoColumnValue(ordinal);
            }
        }

        /// <summary>
        /// Helper that centralizes the logic for the Get*Values methods.  The basic pattern of all of these is:
        ///   Throw if we have a state violation
        ///   Call the proper Get*Values method to fill the values array and get back the number of values entered.
        ///   If we have a PseudoColumn and there is enough room in the arry for its values, then:
        ///     Grab its value via the right Get*Value method
        ///     Put it into the values array in the right place
        ///     Increment the count of values entered into the array
        ///   Return the count of values entered into the array.
        /// </summary>
        /// <param name="values">The array to fill with values.</param>
        /// <param name="getValuesColl">The function to call to fill the array from the SqlDataReader.</param>
        /// <param name="getIndividualValue">The function to call to get the pseudo column value.</param>
        /// <returns></returns>
        private int HandleGetValuesCall(object[] values, Func<object[], int> getValuesColl, Func<int, object> getIndividualValue)
        {
            InduceErrorIfClosed();
            WaitForReaderOrThrow();
            int valuesObtained = getValuesColl(values);
            if (_hasShardIdPseudoColumn && (values.Length > _indexOfShardIdPseudoColumn))
            {
                // If there is enough room for the pseudo-column, then let's add
                // the pseudo column.
                //
                values[_indexOfShardIdPseudoColumn] = getIndividualValue(_indexOfShardIdPseudoColumn);
                valuesObtained++;
            }
            return valuesObtained;
        }

        /// <summary>
        /// Helper that throws a ReaderClosed exception if we are in a closed state.
        /// </summary>
        private void InduceErrorIfClosed()
        {
            if (this.IsClosed)
            {
                throw new MultiShardDataReaderClosedException();
            }
        }

        /// <summary>
        /// Helper that waits for a reader if one is expected to be added or throws if no readers remain.
        /// </summary>
        private void WaitForReaderOrThrow()
        {
            lock (_addLabeledReaderLock)
            {
                while (_labeledReaders.Count == 0)
                {
                    if (_numReadersAdded >= _numReadersExpected)
                    {
                        // If at any point, we find that we should not be expecting more rows, we also throw.
                        // Enterring this code block implies that we were told to stop expecting more readers.
                        throw new MultiShardDataReaderInternalException("This MultiShardDataReader object has no available readers.");
                    }

                    Monitor.Wait(_addLabeledReaderLock);
                }
            }
        }

        /// <summary>
        /// Helper that performs a read call on the current data reader.  It is done via ReadAsync()
        /// to ensure that we buffer the whole row on the client so that we don't have to deal
        /// with messy partial row read error cases.
        /// </summary>
        /// <param name="token">The cancellation instruction.</param>
        /// <returns>
        /// An async task to perform the read; when executed the task returns true 
        /// if we read another row from the current reader, false if we hit the end.
        /// </returns>
        private async Task<bool> PerformReadToFillBufferAsync(CancellationToken token)
        {
            // DEVNOTE: We could run this in either a try-catch or in a ContinueWith.
            // If we do any throwing, though, we want it to be on the main thread that called this function,
            // not on the async thread that the read is running on.  To throw on the main thread with ContinueWith
            // we would need to introduce another wrapper object to wrap either a) the result, or b) the exception
            // and then deal with the exception on the main thread (see the LabeledDataReader pattern).
            // To avoid intorducing that wrapper we will take the try-catch approach.  If we need to revisit later, we can.
            //
            try
            {
                return await this.GetCurrentDataReader().ReadAsync(token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Throw the exception only if a CompleteResults execution policy is in effect
                if (this.ExecutionPolicy == MultiShardExecutionPolicy.PartialResults)
                {
                    _multiShardExceptions.Add(
                        new MultiShardPartialReadException(
                            this.GetCurrentShardLocation(),
                            "Error encountered while reading from a shard.",
                            ex));

                    return false; // since we didn't get a new row
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Helper that adds the Shard Id Pseudo Column schema information to the schema table
        /// that we expose to end users.
        /// </summary>
        private void AddShardIdPseudoColumnRecordToSchemaTable()
        {
            _indexOfShardIdPseudoColumn = _finalSchemaTable.Rows.Count;

            DataRow theRow = _finalSchemaTable.NewRow();

            #region Shard Id Pseudo Column Schema Table information
            theRow[SchemaTableColumn.ColumnName] = MultiShardDataReader.NameOfShardIdPseudoColumn;
            theRow[SchemaTableColumn.ColumnOrdinal] = _indexOfShardIdPseudoColumn;
            theRow[SchemaTableColumn.ColumnSize] = (Int32)4000;
            theRow[SchemaTableColumn.NumericPrecision] = (Int16)255;
            theRow[SchemaTableColumn.NumericScale] = (Int16)255;
            theRow[SchemaTableColumn.IsUnique] = (Boolean)false;
            theRow[SchemaTableColumn.IsKey] = DBNull.Value; //Boolean 
            theRow[SchemaTableOptionalColumn.BaseServerName] = null; //string 
            theRow[SchemaTableOptionalColumn.BaseCatalogName] = null; //string 
            theRow[SchemaTableColumn.BaseColumnName] = MultiShardDataReader.NameOfShardIdPseudoColumn;
            theRow[SchemaTableColumn.BaseSchemaName] = null; //string 
            theRow[SchemaTableColumn.BaseTableName] = null; //string 
            theRow[SchemaTableColumn.DataType] = typeof(string); //System.Type
            theRow[SchemaTableColumn.AllowDBNull] = (Boolean)true;
            theRow[SchemaTableColumn.ProviderType] = (Int32)12;
            theRow[SchemaTableColumn.IsAliased] = DBNull.Value; //Boolean 
            theRow[SchemaTableColumn.IsExpression] = DBNull.Value; //Boolean 
            theRow["IsIdentity"] = (Boolean)false;
            theRow[SchemaTableOptionalColumn.IsAutoIncrement] = (Boolean)false;
            theRow[SchemaTableOptionalColumn.IsRowVersion] = (Boolean)false;
            theRow[SchemaTableOptionalColumn.IsHidden] = DBNull.Value; //Boolean 
            theRow[SchemaTableColumn.IsLong] = (Boolean)false;
            theRow[SchemaTableOptionalColumn.IsReadOnly] = (Boolean)false;
            theRow[SchemaTableOptionalColumn.ProviderSpecificDataType] = typeof(SqlString); //System.Type
            theRow["DataTypeName"] = "nvarchar"; //string
            theRow["XmlSchemaCollectionDatabase"] = null; //string 
            theRow["XmlSchemaCollectionOwningSchema"] = null; //string 
            theRow["XmlSchemaCollectionName"] = null; //string 
            theRow["UdtAssemblyQualifiedName"] = null; //string 
            theRow[SchemaTableColumn.NonVersionedProviderType] = (Int32)12;
            theRow["IsColumnSet"] = (Boolean)false;

            #endregion Shard Id Pseudo Column Schema Table information

            _finalSchemaTable.Rows.Add(theRow);
            _finalSchemaTable.AcceptChanges();
        }

        #endregion Private Methods
    }
}
