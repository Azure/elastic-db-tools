// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Shard key value. Wraps the type and value and allows normalization/denormalization
    /// for serialization.
    /// </summary>
    public sealed class ShardKey : IComparable<ShardKey>, IEquatable<ShardKey>
    {
        /// <summary>Size of Guid.</summary>
        private const int SizeOfGuid = 16;

        /// <summary>Size of Guid.</summary>
        private const int SizeOfDateTimeOffset = 16;

        /// <summary>Maximum size allowed for VarBytes keys.</summary>
        private const int MaximumVarBytesKeySize = 128;

        /// <summary>String representation of +ve infinity.</summary>
        private const string PositiveInfinity = "+inf";

        /// <summary>An empty array.</summary>
        private static readonly byte[] s_emptyArray = new byte[0];

        /// <summary>Mapping b/w CLR type and corresponding ShardKeyType.</summary>
        private static readonly Lazy<Dictionary<Type, ShardKeyType>> s_typeToShardKeyType = new Lazy<Dictionary<Type,ShardKeyType>>(() => 
            new Dictionary<Type, ShardKeyType>()
            {
                { typeof(int), ShardKeyType.Int32 },
                { typeof(long), ShardKeyType.Int64 },
                { typeof(Guid), ShardKeyType.Guid },
                { typeof(byte[]), ShardKeyType.Binary },
                { typeof(DateTime), ShardKeyType.DateTime },
                { typeof(TimeSpan), ShardKeyType.TimeSpan },
                { typeof(DateTimeOffset), ShardKeyType.DateTimeOffset },
            }, LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Mapping b/w ShardKeyType and corresponding CLR type.</summary>
        private static readonly Lazy<Dictionary<ShardKeyType, Type>> s_shardKeyTypeToType = new Lazy<Dictionary<ShardKeyType,Type>>(() => 
            new Dictionary<ShardKeyType, Type>()
            {
                { ShardKeyType.Int32, typeof(int) },
                { ShardKeyType.Int64, typeof(long) },
                { ShardKeyType.Guid, typeof(Guid) },
                { ShardKeyType.Binary, typeof(byte[]) },
                { ShardKeyType.DateTime, typeof(DateTime) },
                { ShardKeyType.TimeSpan, typeof(TimeSpan) },
                { ShardKeyType.DateTimeOffset, typeof(DateTimeOffset) },
            }, LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minInt32 = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Int32, Int32.MinValue), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxInt32 = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Int32, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minInt64 = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Int64, Int64.MinValue), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxInt64 = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Int64, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minGuid = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Guid, default(Guid)), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxGuid = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Guid, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minBinary = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Binary, ShardKey.s_emptyArray), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxBinary = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.Binary, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minDateTime = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.DateTime, DateTime.MinValue), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxDateTime = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.DateTime, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minTimeSpan = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.TimeSpan, TimeSpan.MinValue), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxTimeSpan = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.TimeSpan, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_minDateTimeOffset = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.DateTimeOffset, DateTimeOffset.MinValue), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        private static Lazy<ShardKey> s_maxDateTimeOffset = new Lazy<ShardKey>(() => new ShardKey(ShardKeyType.DateTimeOffset, null), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinInt32
        {
            get
            {
                return s_minInt32.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxInt32
        {
            get
            {
                return s_maxInt32.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinInt64
        {
            get
            {
                return s_minInt64.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxInt64
        {
            get
            {
                return s_maxInt64.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinGuid
        {
            get
            {
                return s_minGuid.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxGuid
        {
            get
            {
                return s_maxGuid.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinBinary
        {
            get
            {
                return s_minBinary.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxBinary
        {
            get
            {
                return s_maxBinary.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinDateTime
        {
            get
            {
                return s_minDateTime.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxDateTime
        {
            get
            {
                return s_maxDateTime.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinTimeSpan
        {
            get
            {
                return s_minTimeSpan.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxTimeSpan
        {
            get
            {
                return s_maxTimeSpan.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MinDateTimeOffset
        {
            get
            {
                return s_minDateTimeOffset.Value;
            }
        }

        /// <summary>Represents negative infinity.</summary>
        public static ShardKey MaxDateTimeOffset
        {
            get
            {
                return s_maxDateTimeOffset.Value;
            }
        }

        /// <summary>Type of shard key.</summary>
        private readonly ShardKeyType _keyType;

        /// <summary>
        /// Value as saved in persistent storage. Empty byte array represents the minimum value, 
        /// and a null value represents the maximum value. 
        /// </summary>
        private readonly byte[] _value;

        /// <summary>Hashcode for the shard key.</summary>
        private readonly int _hashCode;

        /// <summary>Constructs a shard key using 32-bit integer value.</summary>
        /// <param name="value">Input 32-bit integer.</param>
        public ShardKey(int value) : this(ShardKeyType.Int32, ShardKey.Normalize(value), false)
        {
        }

        /// <summary>Constructs a shard key using 64-bit integer value.</summary>
        /// <param name="value">Input 64-bit integer.</param>
        public ShardKey(long value) : this(ShardKeyType.Int64, ShardKey.Normalize(value), false)
        {
        }

        /// <summary>Constructs a shard key using a Guid.</summary>
        /// <param name="value">Input Guid.</param>
        public ShardKey(Guid value) : this(ShardKeyType.Guid, ShardKey.Normalize(value), false)
        {
        }

        /// <summary>Constructs a shard key using a byte array.</summary>
        /// <param name="value">Input byte array.</param>
        public ShardKey(byte[] value) : this(ShardKeyType.Binary, ShardKey.Normalize(value), true)
        {
        }

        /// <summary>Constructs a shard key using DateTime value.</summary>
        /// <param name="value">Input DateTime.</param>
        public ShardKey(DateTime value)
            : this(ShardKeyType.DateTime, ShardKey.Normalize(value), false)
        {
        }

        /// <summary>Constructs a shard key using TimeSpan value.</summary>
        /// <param name="value">Input TimeSpan.</param>
        public ShardKey(TimeSpan value)
            : this(ShardKeyType.TimeSpan, ShardKey.Normalize(value), false)
        {
        }

        /// <summary>Constructs a shard key using TimeSpan value.</summary>
        /// <param name="value">Input DateTimeOffset.</param>
        public ShardKey(DateTimeOffset value)
            : this(ShardKeyType.DateTimeOffset, ShardKey.Normalize(value), false)
        {
        }

        /// <summary>Constructs a shard key using given object.</summary>
        /// <param name="value">Input object.</param>
        public ShardKey(object value)
        {
            ExceptionUtils.DisallowNullArgument(value, "value");

            // We can't detect the type if value is null.
            if (DBNull.Value.Equals(value))
            {
                throw new ArgumentNullException("value");
            }

            ShardKey shardKey = value as ShardKey;

            if (shardKey != null)
            {
                _keyType = shardKey._keyType;
                _value = shardKey._value;
            }
            else
            {
                _keyType = ShardKey.DetectShardKeyType(value);
                _value = ShardKey.Normalize(_keyType, value);
            }

            _hashCode = this.CalculateHashCode();
        }

        /// <summary>
        /// Constructs a shard key using given object and keyType.
        /// </summary>
        /// <param name="keyType">The key type of value in object.</param>
        /// <param name="value">Input object.</param>
        public ShardKey(ShardKeyType keyType, object value)
        {
            if (keyType == ShardKeyType.None)
            {
                throw new ArgumentOutOfRangeException(
                    "keyType",
                    keyType,
                    Errors._ShardKey_UnsupportedShardKeyType);
            }

            _keyType = keyType;

            if (value != null && !DBNull.Value.Equals(value))
            {
                ShardKeyType detectedKeyType = ShardKey.DetectShardKeyType(value);

                if (_keyType != detectedKeyType)
                {
                    throw new ArgumentException(
                        StringUtils.FormatInvariant(
                            Errors._ShardKey_ValueDoesNotMatchShardKeyType,
                            "keyType"),
                        "value");
                }

                _value = ShardKey.Normalize(_keyType, value);
            }
            else
            {
                // Null represents positive infinity.
                _value = null;
            }

            _hashCode = this.CalculateHashCode();
        }

        /// <summary>
        /// Instantiates the key with given type and raw value and optionally validates
        /// the key type and raw representation of the value.
        /// </summary>
        /// <param name="keyType">Type of shard key.</param>
        /// <param name="rawValue">Raw value of the key.</param>
        /// <param name="validate">Whether to validate the key type and raw value.</param>
        private ShardKey(ShardKeyType keyType, byte[] rawValue, bool validate)
        {
            _keyType = keyType;
            _value = rawValue;
            _hashCode = this.CalculateHashCode();

            if (validate)
            {
                int expectedLength;

                switch (keyType)
                {
                    case ShardKeyType.Int32:
                        expectedLength = sizeof(int);
                        break;

                    case ShardKeyType.Int64:
                    case ShardKeyType.DateTime:
                    case ShardKeyType.TimeSpan:
                        expectedLength = sizeof(long);
                        break;

                    case ShardKeyType.Guid:
                        expectedLength = ShardKey.SizeOfGuid;
                        break;

                    case ShardKeyType.Binary:
                        expectedLength = ShardKey.MaximumVarBytesKeySize;
                        break;

                    case ShardKeyType.DateTimeOffset:
                        expectedLength = SizeOfDateTimeOffset;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException(
                            "keyType",
                            keyType,
                            Errors._ShardKey_UnsupportedShardKeyType);
                }

                // +ve & -ve infinity. Correct size provided.
                if (_value == null ||
                    _value.Length == 0 ||
                    _value.Length == expectedLength)
                {
                    return;
                }

                // Only allow byte[] values to be of different length than expected,
                // since there could be smaller values than 128 bytes. For anything
                // else any non-zero length should match the expected length.
                if (_keyType != ShardKeyType.Binary || _value.Length > expectedLength)
                {
                    throw new ArgumentOutOfRangeException(
                        "rawValue",
                        rawValue,
                        string.Format(CultureInfo.InvariantCulture,
                                        Errors._ShardKey_ValueLengthUnexpected,
                                        _value.Length,
                                        expectedLength,
                                        _keyType));
                }
            }
        }

        /// <summary>
        /// True if the key has a value; otherwise, false. Positive infinity returns false.
        /// </summary>
        public bool HasValue
        {
            get
            {
                return _value != null;
            }
        }

        /// <summary>
        /// Returns true if the key value is negative infinity; otherwise, false.
        /// </summary>
        public bool IsMin
        {
            get
            {
                return _value != null && _value.Length == 0;
            }
        }

        /// <summary>
        /// True if the key value is positive infinity; otherwise, false.
        /// </summary>
        public bool IsMax
        {
            get
            {
                return _value == null;
            }
        }

        /// <summary>
        /// Gets a byte array representing the key value.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays",
            Justification = "Necessary to return a copy of the byte array representing the key value")]
        public byte[] RawValue
        {
            get
            {
                if (_value == null)
                {
                    return null;
                }
                else
                {
                    byte[] rawValueCpy = new byte[_value.Length];
                    _value.CopyTo(rawValueCpy, 0);
                    return rawValueCpy;
                }
            }
        }

        /// <summary>
        /// Gets the denormalized value of the key.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1721:PropertyNamesShouldNotMatchGetMethods",
            Justification = "Necessary to return the un-typed value of the key")]
        public object Value
        {
            get
            {
                return ShardKey.DeNormalize(_keyType, _value);
            }
        }

        /// <summary>
        /// Gets the type of the shard key.
        /// </summary>
        public ShardKeyType KeyType
        {
            get
            {
                return _keyType;
            }
        }

        /// <summary>
        /// Gets the type of the value present in the object.
        /// </summary>
        public Type DataType
        {
            get
            {
                return ShardKey.s_shardKeyTypeToType.Value[_keyType];
            }
        }

        /// <summary>
        /// Instantiates a new shard key using the specified type and binary representation.
        /// </summary>
        /// <param name="keyType">Type of the shard key (Int32, Int64, Guid, byte[] etc.).</param>
        /// <param name="rawValue">Binary representation of the key.</param>
        /// <returns>A new shard key instance.</returns>
        public static ShardKey FromRawValue(ShardKeyType keyType, byte[] rawValue)
        {
            return new ShardKey(keyType, rawValue, true);
        }

        /// <summary>
        /// Gets the strongly typed value of the shard key.
        /// </summary>
        /// <typeparam name="T">Type of the key.</typeparam>
        /// <returns>Value of the key.</returns>
        public T GetValue<T>()
        {
            if (this.DataType != typeof(T))
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._ShardKey_RequestedTypeDoesNotMatchShardKeyType,
                        typeof(T),
                        this.KeyType));
            }

            if (this.IsMax)
            {
                throw new InvalidOperationException(Errors._ShardKey_MaxValueCannotBeRepresented);
            }

            return (T)this.Value;
        }

        /// <summary>
        /// Converts the object to its string representation.
        /// </summary>
        /// <returns>String representation of the object.</returns>
        public override string ToString()
        {
            if (this.IsMax)
            {
                return ShardKey.PositiveInfinity;
            }
            else
            {
                switch (_keyType)
                {
                    case ShardKeyType.Int32:
                    case ShardKeyType.Int64:
                    case ShardKeyType.Guid:
                    case ShardKeyType.DateTime:
                    case ShardKeyType.DateTimeOffset:
                    case ShardKeyType.TimeSpan:
                        return this.Value.ToString();
                    case ShardKeyType.Binary:
                        return StringUtils.ByteArrayToString(_value);
                    default:
                        Debug.Assert(_keyType == ShardKeyType.None);
                        Debug.Fail("Unexpected type for string representation.");
                        return String.Empty;
                }
            }
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return _hashCode;
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return this.Equals(obj as ShardKey);
        }

        /// <summary>
        /// Performs equality comparison with another given ShardKey.
        /// </summary>
        /// <param name="other">ShardKey to compare with.</param>
        /// <returns>True if same shard key, false otherwise.</returns>
        public bool Equals(ShardKey other)
        {
            if (other == null)
            {
                return false;
            }
            else
            {
                if (this.GetHashCode() != other.GetHashCode())
                {
                    return false;
                }
                else
                {
                    return this.CompareTo(other) == 0;
                }
            }
        }

        /// <summary>
        /// Compares between two <see cref="ShardKey"/> values.
        /// </summary>
        /// <param name="other">The <see cref="ShardKey"/> compared with this object.</param>
        /// <returns>0 for equality, &lt; -1 if this key is less than <paramref name="other"/>, &gt; 1 otherwise.</returns>
        public int CompareTo(ShardKey other)
        {
            if (other == null)
            {
                return 1;
            }

            // Handle the obvious case of same objects.
            if (object.ReferenceEquals(this, other))
            {
                return 0;
            }

            if (_keyType != other.KeyType)
            {
                throw new InvalidOperationException(
                    StringUtils.FormatInvariant(
                        Errors._ShardKey_ShardKeyTypesMustMatchForComparison,
                        _keyType,
                        other._keyType));
            }

            // Handle if any of the keys is MaxKey
            if (this.IsMax)
            {
                if (other.IsMax)
                {
                    return 0;
                }

                return 1;
            }

            if (other.IsMax)
            {
                return -1;
            }

            // If both values reference the same array, they are equal.
            if (object.ReferenceEquals(_value, other._value))
            {
                return 0;
            }

            // if it's DateTimeOffset we compare just the date part
            if (KeyType == ShardKeyType.DateTimeOffset)
            {
                byte[] rawThisValue = new byte[sizeof(long)];
                byte[] rawOtherValue = new byte[sizeof(long)];

                Buffer.BlockCopy(_value, 0, rawThisValue, 0, Buffer.ByteLength(rawThisValue));
                Buffer.BlockCopy(other._value, 0, rawOtherValue, 0, Buffer.ByteLength(rawOtherValue));

                ShardKey interimKeyThis = ShardKey.FromRawValue(ShardKeyType.DateTime, rawThisValue);
                ShardKey interimKeyOther = ShardKey.FromRawValue(ShardKeyType.DateTime, rawOtherValue);

                return interimKeyThis.CompareTo(interimKeyOther);
            }

            Int32 minLength = Math.Min(_value.Length, other._value.Length);

            Int32 differentByteIndex;

            for (differentByteIndex = 0; differentByteIndex < minLength; differentByteIndex++)
            {
                if (_value[differentByteIndex] != other._value[differentByteIndex])
                {
                    break;
                }
            }

            if (differentByteIndex == minLength)
            {
                // If all they bytes are same, then the key with the longer byte array is bigger. 
                // Note that we remove trailing 0's which are inert and could break this logic.
                return _value.Length.CompareTo(other._value.Length);
            }
            else
            {
                // Compare the most significant different byte.
                return _value[differentByteIndex].CompareTo(other._value[differentByteIndex]);
            }
        }

        #region Operators

        /// <summary>
        /// Compares two <see cref="ShardKey"/> using lexicographic order (less than).
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardKey"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardKey"/> of the operator.</param>
        /// <returns>True if lhs &lt; rhs</returns>
        public static bool operator <(ShardKey left, ShardKey right)
        {
            if (left == null)
            {
                return (right == null) ? false : true;
            }
            else
            {
                return (left.CompareTo(right) < 0);
            }
        }

        /// <summary>
        /// Compares two <see cref="ShardKey"/> using lexicographic order (greater than).
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardKey"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardKey"/> of the operator.</param>
        /// <returns>True if lhs &gt; rhs</returns>
        public static bool operator >(ShardKey left, ShardKey right)
        {
            return right < left;
        }

        /// <summary>
        /// Compares two <see cref="ShardKey"/> using lexicographic order (less or equal). 
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardKey"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardKey"/> of the operator.</param>
        /// <returns>True if lhs &lt;= rhs</returns>
        public static bool operator <=(ShardKey left, ShardKey right)
        {
            return !(left > right);
        }

        /// <summary>
        /// Compares two <see cref="ShardKey"/> using lexicographic order (greater or equal). 
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardKey"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardKey"/> of the operator.</param>
        /// <returns>True if lhs &gt;= rhs</returns>
        public static bool operator >=(ShardKey left, ShardKey right)
        {
            return !(left < right);
        }

        /// <summary>
        /// Equality operator.
        /// </summary>
        /// <param name="left">Left hand side</param>
        /// <param name="right">Right hand side</param>
        /// <returns>True if the two objects are equal, false in all other cases</returns>
        public static bool operator ==(ShardKey left, ShardKey right)
        {
            return object.Equals(left, right);
        }

        /// <summary>
        /// Inequality operator.
        /// </summary>
        /// <param name="left">Left hand side</param>
        /// <param name="right">Right hand side</param>
        /// <returns>True if the two objects are not equal, false in all other cases</returns>
        public static bool operator !=(ShardKey left, ShardKey right)
        {
            return !(left == right);
        }

        /// <summary>
        /// Gets the minimum of two shard keys.
        /// </summary>
        /// <param name="left">Left hand side.</param>
        /// <param name="right">Right hand side.</param>
        /// <returns>Minimum of two shard keys.</returns>
        public static ShardKey Min(ShardKey left, ShardKey right)
        {
            if (left < right)
                return left;
            else
                return right;
        }

        /// <summary>
        /// Gets the maximum of two shard keys.
        /// </summary>
        /// <param name="left">Left hand side.</param>
        /// <param name="right">Right hand side.</param>
        /// <returns>Maximum of two shard keys.</returns>
        public static ShardKey Max(ShardKey left, ShardKey right)
        {
            if (left > right)
                return left;
            else
                return right;
        }

        #endregion

        /// <summary>
        /// Given an object detect its ShardKeyType.
        /// </summary>
        /// <param name="value">Given value. Must be non-null.</param>
        /// <returns>Corresponding ShardKeyType.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static ShardKeyType DetectShardKeyType(object value)
        {
            ExceptionUtils.DisallowNullArgument(value, "value");

            ShardKeyType keyType;

            if (!ShardKey.s_typeToShardKeyType.Value.TryGetValue(value.GetType(), out keyType))
            {
                throw new ArgumentException(
                    StringUtils.FormatInvariant(
                        Errors._ShardKey_UnsupportedValue,
                        value.GetType()),
                    "value");
            }

            return keyType;
        }

        /// <summary>
        /// Checks whether the specified type is supported as ShardKey type.
        /// </summary>
        /// <param name="type">Input type.</param>
        /// <returns>True if supported, false otherwise.</returns>
        public static bool IsSupportedType(Type type)
        {
            return s_typeToShardKeyType.Value.ContainsKey(type);
        }

        /// <summary>
        /// Gets the CLR type corresponding to the specified ShardKeyType.
        /// </summary>
        /// <param name="keyType">Input ShardKeyType.</param>
        /// <returns>CLR type.</returns>
        public static Type TypeFromShardKeyType(ShardKeyType keyType)
        {
            if (keyType == ShardKeyType.None)
            {
                throw new ArgumentOutOfRangeException(
                    "keyType",
                    keyType,
                    Errors._ShardKey_UnsupportedShardKeyType);
            }

            return s_shardKeyTypeToType.Value[keyType];
        }

        /// <summary>
        /// Gets the ShardKeyType corresponding to CLR type.
        /// </summary>
        /// <param name="type">CLR type.</param>
        /// <returns>ShardKey type.</returns>
        public static ShardKeyType ShardKeyTypeFromType(Type type)
        {
            if (s_typeToShardKeyType.Value.ContainsKey(type))
            {
                return s_typeToShardKeyType.Value[type];
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    "type",
                    type,
                    Errors._ShardKey_UnsupportedType);
            }
        }

        /// <summary>
        /// Gets the next higher key
        /// </summary>
        /// <returns>Incremented newly constructed ShardKey</returns>
        /// <remarks>Returns a new ShardKey that is the numerical successor of this ShardKey (add a binary bit). 
        /// For example, if this ShardKey has the integer value 0, GetNextKey() returns a ShardKey
        /// with the value 1. Alternatively, if this ShardKey is a byte array with the value 0x1234,
        /// GetNextKey() returns a ShardKey with the value 0x1234...251 zeros....1
        /// </remarks>
        internal ShardKey GetNextKey()
        {
            if (this.IsMax)
            {
                throw new InvalidOperationException(Errors._ShardKey_MaxValueCannotBeIncremented);
            }
            else
            {
                int len = 0;

                switch (this.KeyType)
                {
                    case ShardKeyType.Int32:
                        len = sizeof(int);
                        break;

                    case ShardKeyType.Int64:
                    case ShardKeyType.DateTime:
                    case ShardKeyType.TimeSpan:
                        len = sizeof(long);
                        break;

                    case ShardKeyType.Guid:
                        len = ShardKey.SizeOfGuid;
                        break;

                    case ShardKeyType.Binary:
                        len = ShardKey.MaximumVarBytesKeySize;
                        break;

                    case ShardKeyType.DateTimeOffset:
                        byte[] denormalizedDtValue = new byte[sizeof(long)];

                        // essentially we do get next key of the date part (stored in utc) and 
                        // re-store that along with the original offset
                        Buffer.BlockCopy(_value, 0, denormalizedDtValue, 0, Buffer.ByteLength(denormalizedDtValue));
                        ShardKey interimKey = ShardKey.FromRawValue(ShardKeyType.DateTime, denormalizedDtValue);
                        ShardKey interimNextKey = interimKey.GetNextKey();
                        byte[] bRes = new byte[SizeOfDateTimeOffset];
                        Buffer.BlockCopy(interimNextKey.RawValue, 0, bRes, 0, Buffer.ByteLength(interimNextKey.RawValue));
                        Buffer.BlockCopy(_value, Buffer.ByteLength(interimNextKey.RawValue), bRes, Buffer.ByteLength(interimNextKey.RawValue), sizeof(long));

                        ShardKey resKey = ShardKey.FromRawValue(ShardKeyType.DateTimeOffset, bRes);
                        return resKey;

                    default:
                        Debug.Fail("Unexpected shard key kind.");
                        break;
                }

                byte[] b = new byte[len];
                _value.CopyTo(b, 0);

                // push carry forward, (per byte for now)
                while (--len >= 0 && ++b[len] == 0) ;

                // Overflow, the current key's value is the maximum in the key spectrum. Return +inf i.e. ShardKey with IsMax set to true.
                if (len < 0)
                {
                    return new ShardKey(this.KeyType, null);
                }
                else
                {
                    return ShardKey.FromRawValue(this.KeyType, b);
                }
            }
        }

        /// <summary>
        /// Mix up the hash key and add the specified value into it.
        /// </summary>
        /// <param name="hashKey">The previous value of the hash</param>
        /// <param name="value">The additional value to mix into the hash</param>
        /// <returns>The updated hash value</returns>
        internal static int QPHash(int hashKey, int value)
        {
            return hashKey ^ ((hashKey << 11) + (hashKey << 5) + (hashKey >> 2) + value);
        }

        /// <summary>
        /// Take an object and convert it to its normalized representation as a byte array.
        /// </summary>
        /// <param name="keyType">The type of the <see cref="ShardKey"/>.</param>
        /// <param name="value">The value</param>
        /// <returns>The normalized <see cref="ShardKey"/> information</returns>
        private static byte[] Normalize(ShardKeyType keyType, object value)
        {
            switch (keyType)
            {
                case ShardKeyType.Int32:
                    return ShardKey.Normalize((int)value);

                case ShardKeyType.Int64:
                    return ShardKey.Normalize((long)value);

                case ShardKeyType.Guid:
                    return ShardKey.Normalize((Guid)value);

                case ShardKeyType.DateTime:
                    return ShardKey.Normalize((DateTime)value);

                case ShardKeyType.TimeSpan:
                    return ShardKey.Normalize((TimeSpan)value);

                case ShardKeyType.DateTimeOffset:
                    return ShardKey.Normalize((DateTimeOffset)value);

                default:
                    Debug.Assert(keyType == ShardKeyType.Binary);
                    return ShardKey.Normalize((byte[])value);
            }
        }

        /// <summary>
        /// Takes a byte array and a shard key type and convert it to its native denormalized C# type.
        /// </summary>
        /// <returns>The denormalized object</returns>
        private static object DeNormalize(ShardKeyType keyType, byte[] value)
        {
            // Return null for positive infinity.
            if (value == null)
            {
                return null;
            }

            switch (keyType)
            {
                case ShardKeyType.Int32:
                    return ShardKey.DenormalizeInt32(value);

                case ShardKeyType.Int64:
                    return ShardKey.DenormalizeInt64(value);

                case ShardKeyType.Guid:
                    return ShardKey.DenormalizeGuid(value);

                case ShardKeyType.DateTime:
                    long dtTicks = ShardKey.DenormalizeInt64(value);
                    return new DateTime(dtTicks);

                case ShardKeyType.TimeSpan:
                    long tsTicks = ShardKey.DenormalizeInt64(value);
                    return new TimeSpan(tsTicks);

                case ShardKeyType.DateTimeOffset:
                    return DenormalizeDateTimeOffset(value);

                default:
                    // For varbinary type, we simply keep it as a VarBytes object
                    Debug.Assert(keyType == ShardKeyType.Binary);
                    return ShardKey.DenormalizeByteArray(value);
            }
        }

        /// <summary>
        /// Converts given 32-bit integer to normalized binary representation.
        /// </summary>
        /// <param name="value">Input 32-bit integer.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(int value)
        {
            if (value == Int32.MinValue)
            {
                return ShardKey.s_emptyArray;
            }
            else
            {
                byte[] normalized = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));

                // Maps Int32.Min - Int32.Max to UInt32.Min - UInt32.Max.
                normalized[0] ^= 0x80;

                return normalized;
            }
        }

        /// <summary>
        /// Converts given 64-bit integer to normalized binary representation.
        /// </summary>
        /// <param name="value">Input 64-bit integer.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(long value)
        {
            if (value == Int64.MinValue)
            {
                return ShardKey.s_emptyArray;
            }
            else
            {
                byte[] normalized = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));

                // Maps Int64.Min - Int64.Max to UInt64.Min - UInt64.Max.
                normalized[0] ^= 0x80;

                return normalized;
            }
        }

        /// <summary>
        /// Converts given GUID to normalized binary representation.
        /// </summary>
        /// <param name="value">Input GUID.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(Guid value)
        {
            if (value == default(Guid))
            {
                return ShardKey.s_emptyArray;
            }
            else
            {
                byte[] source = value.ToByteArray();

                // For normalization follow the pattern of SQL Server comparison.
                byte[] normalized = new byte[ShardKey.SizeOfGuid];

                // Last 6 bytes are the most significant (bytes 10 through 15)
                normalized[0] = source[10]; normalized[1] = source[11];
                normalized[2] = source[12]; normalized[3] = source[13];
                normalized[4] = source[14]; normalized[5] = source[15];

                // Then come bytes 8,9
                normalized[6] = source[8]; normalized[7] = source[9];

                // Then come bytes 6,7
                normalized[8] = source[6]; normalized[9] = source[7];

                // Then come bytes 4,5
                normalized[10] = source[4]; normalized[11] = source[5];

                // Then come the first 4 bytes  (bytes 0 through 4)
                normalized[12] = source[0]; normalized[13] = source[1];
                normalized[14] = source[2]; normalized[15] = source[3];

                return normalized;
            }
        }

        /// <summary>
        /// Converts given DateTime to normalized binary representation.
        /// </summary>
        /// <param name="value">Input DateTime value.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(DateTime value)
        {
            if (value == DateTime.MinValue)
            {
                return ShardKey.s_emptyArray;
            }
            else
            {
                return Normalize(value.Ticks);
            }
        }

        /// <summary>
        /// Converts given TimeSpan to normalized binary representation.
        /// </summary>
        /// <param name="value">Input TimeSpan value.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(TimeSpan value)
        {
            if (value == TimeSpan.MinValue)
            {
                return ShardKey.s_emptyArray;
            }
            else
            {
                return Normalize(value.Ticks);
            }
        }

        /// <summary>
        /// Converts given DateTimeOffset to normalized binary representation.
        /// </summary>
        /// <param name="value">Input DateTimeOffset value.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(DateTimeOffset value)
        {
            if (value == DateTimeOffset.MinValue)
            {
                return ShardKey.s_emptyArray;
            }
            else
            {
                // we store this as 2 parts: a date part and an offset part. 
                // the date part is the utc value of the input
                long storedDtValue = value.UtcTicks;
                long storedOffsetTicks = value.Offset.Ticks;

                byte[] normalizedDtValue = Normalize(storedDtValue);
                byte[] normalizedOffsetTicks = Normalize(storedOffsetTicks);

                byte[] result = new byte[SizeOfDateTimeOffset];
                Buffer.BlockCopy(normalizedDtValue, 0, result, 0, Buffer.ByteLength(normalizedDtValue));
                Buffer.BlockCopy(normalizedOffsetTicks, 0, result, Buffer.ByteLength(normalizedDtValue), Buffer.ByteLength(normalizedOffsetTicks));

                return result;
            }
        }

        /// <summary>
        /// Converts given byte array to normalized binary representation.
        /// </summary>
        /// <param name="value">Input byte array.</param>
        /// <returns>Normalized array of bytes.</returns>
        private static byte[] Normalize(byte[] value)
        {
            return TruncateTrailingZero(value);
        }

        private static int DenormalizeInt32(byte[] value)
        {
            if (value.Length == 0)
            {
                return Int32.MinValue;
            }
            else
            {
                // Make a copy of the normalized array
                byte[] denormalized = new byte[value.Length];

                value.CopyTo(denormalized, 0);

                // Flip the last bit and cast it to an integer
                denormalized[0] ^= 0x80;

                return System.Net.IPAddress.HostToNetworkOrder(BitConverter.ToInt32(denormalized, 0));
            }
        }

        private static long DenormalizeInt64(byte[] value)
        {
            if (value.Length == 0)
            {
                return Int64.MinValue;
            }
            else
            {
                // Make a copy of the normalized array
                byte[] denormalized = new byte[value.Length];

                value.CopyTo(denormalized, 0);

                // Flip the last bit and cast it to an integer
                denormalized[0] ^= 0x80;

                return System.Net.IPAddress.HostToNetworkOrder(BitConverter.ToInt64(denormalized, 0));
            }
        }

        private static Guid DenormalizeGuid(byte[] value)
        {
            if (value.Length == 0)
            {
                return default(Guid);
            }
            else
            {
                // Shuffle bytes to the denormalized form
                byte[] denormalized = new byte[ShardKey.SizeOfGuid];

                // Get the last 4 bytes first
                denormalized[0] = value[12]; denormalized[1] = value[13];
                denormalized[2] = value[14]; denormalized[3] = value[15];

                // Get every two bytes of the prev 6 bytes
                denormalized[4] = value[10]; denormalized[5] = value[11];

                denormalized[6] = value[8]; denormalized[7] = value[9];

                denormalized[8] = value[6]; denormalized[9] = value[7];

                // Copy the first 6 bytes
                denormalized[10] = value[0]; denormalized[11] = value[1];
                denormalized[12] = value[2]; denormalized[13] = value[3];
                denormalized[14] = value[4]; denormalized[15] = value[5];

                return new Guid(denormalized);
            }
        }

        private static DateTimeOffset DenormalizeDateTimeOffset(byte[] value)
        {
            // we stored the date and offset as 2 normalized Int64s. So split our input 
            // byte array and de-normalize the pieces
            byte[] denormalizedDtValue = new byte[sizeof(long)];
            byte[] denormalizedOffsetTicks = new byte[sizeof(long)];

            Buffer.BlockCopy(value, 0, denormalizedDtValue, 0, Buffer.ByteLength(denormalizedDtValue));
            Buffer.BlockCopy(value, Buffer.ByteLength(denormalizedDtValue), denormalizedOffsetTicks, 0, Buffer.ByteLength(denormalizedOffsetTicks));

            long datePart = DenormalizeInt64(denormalizedDtValue);
            long offsetPart = DenormalizeInt64(denormalizedOffsetTicks);

            TimeSpan offset = new TimeSpan(offsetPart);

            // we stored the date part as utc so convert back from utc by applying the offset
            DateTime date = new DateTime(datePart).Add(offset);
            DateTimeOffset result = new DateTimeOffset(date, offset);
            return result;
        }

        private static byte[] DenormalizeByteArray(byte[] value)
        {
            return value;
        }
        /// <summary>
        /// Truncate tailing zero of a byte array.
        /// </summary>
        /// <param name="a">The array from which truncate trailing zeros</param>
        /// <returns>a new byte array with non-zero tail</returns>
        private static byte[] TruncateTrailingZero(byte[] a)
        {
            if (a != null)
            {
                if (a.Length == 0)
                {
                    return ShardKey.s_emptyArray;
                }

                // Get the index of last byte with non-zero value
                int lastNonZeroIndex = a.Length;

                while (--lastNonZeroIndex >= 0 && a[lastNonZeroIndex] == 0)
                {
                }

                // If the index of the last non-zero byte is not the last index of the array, there are trailing zeros
                int countOfTrailingZero = a.Length - lastNonZeroIndex - 1;

                byte[] tmp = a;
                a = new byte[a.Length - countOfTrailingZero];
                for (int i = 0; i < a.Length; i++)
                {
                    // Copy byte by byte until the last non-zero byte
                    a[i] = tmp[i];
                }
            }

            return a;
        }

        /// <summary>
        /// Calculates the hash code for the object.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        private int CalculateHashCode()
        {
            int hash = (int)_keyType * 3137;

            if (null != _value)
            {
                byte[] tempArray = null;
                if (KeyType == ShardKeyType.DateTimeOffset && _value.Length == SizeOfDateTimeOffset)
                {
                    tempArray = new byte[sizeof(long)];
                    Buffer.BlockCopy(_value, 0, tempArray, 0, Buffer.ByteLength(tempArray));
                }
                else
                {
                    tempArray = new byte[_value.Length];
                    _value.CopyTo(tempArray, 0);
                }

                foreach (byte b in tempArray)
                {
                    hash = ShardKey.QPHash(hash, b);
                }
            }

            return hash;
        }
    }
}
