// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Threading;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// A range of shard keys between a low key and a high key.
    /// </summary>
    /// <remarks>
    /// The low key is inclusive (part of the range) while the high key is exclusive
    /// (not part of the range). The ShardRange class is immutable.
    /// </remarks>
    public sealed class ShardRange : IComparable<ShardRange>, IEquatable<ShardRange>
    {
        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeInt32 = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinInt32, ShardKey.MaxInt32), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeInt32
        {
            get
            {
                return s_fullRangeInt32.Value;
            }
        }

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeInt64 = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinInt64, ShardKey.MaxInt64), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeInt64
        {
            get
            {
                return s_fullRangeInt64.Value;
            }
        }

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeGuid = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinGuid, ShardKey.MaxGuid), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeGuid
        {
            get
            {
                return s_fullRangeGuid.Value;
            }
        }

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeBinary = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinBinary, ShardKey.MaxBinary), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeBinary
        {
            get
            {
                return s_fullRangeBinary.Value;
            }
        }

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeDateTime = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinDateTime, ShardKey.MaxDateTime), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeDateTime
        {
            get
            {
                return s_fullRangeDateTime.Value;
            }
        }

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeTimeSpan = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinTimeSpan, ShardKey.MaxTimeSpan), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeTimeSpan
        {
            get
            {
                return s_fullRangeTimeSpan.Value;
            }
        }

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        private static Lazy<ShardRange> s_fullRangeDateTimeOffset = new Lazy<ShardRange>(() => new ShardRange(ShardKey.MinDateTimeOffset, ShardKey.MaxDateTimeOffset), LazyThreadSafetyMode.PublicationOnly);

        /// <summary>Full range that starts from the min value for a key to the max value.</summary>
        public static ShardRange FullRangeDateTimeOffset
        {
            get
            {
                return s_fullRangeDateTimeOffset.Value;
            }
        }

        /// <summary>Hashcode for the shard range.</summary>
        private readonly int _hashCode;

        /// <summary>Constructs a shard range from low boundary (inclusive) to high high boundary (exclusive)</summary>
        /// <param name="low">Low boundary (inclusive)</param>
        /// <param name="high">High boundary (exclusive)</param>
        public ShardRange(ShardKey low, ShardKey high)
        {
            ExceptionUtils.DisallowNullArgument(low, "low");
            ExceptionUtils.DisallowNullArgument(high, "high");

            if (low >= high)
            {
                throw new ArgumentOutOfRangeException(
                    "low",
                    low,
                    string.Format(
                        Errors._ShardRange_LowGreaterThanOrEqualToHigh,
                        low,
                        high));
            }

            this.Low = low;
            this.High = high;
            this.KeyType = Low.KeyType;
            _hashCode = this.CalculateHashCode();
        }

        /// <summary>Accessor for low boundary (inclusive).</summary>
        public ShardKey Low
        {
            get;
            private set;
        }

        /// <summary>Accessor for high boundary (exclusive).</summary>
        public ShardKey High
        {
            get;
            private set;
        }

        /// <summary>Gets the key type of shard range.</summary>
        public ShardKeyType KeyType
        {
            get;
            private set;
        }

        /// <summary>
        /// Converts the object to its string representation.
        /// </summary>
        /// <returns>String representation of the object.</returns>
        public override string ToString()
        {
            return StringUtils.FormatInvariant("[{0}:{1})", this.Low.ToString(), this.High.ToString());
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
            return this.Equals(obj as ShardRange);
        }

        /// <summary>
        /// Performs equality comparison with another given ShardRange.
        /// </summary>
        /// <param name="other">ShardRange to compare with.</param>
        /// <returns>True if same shard range, false otherwise.</returns>
        public bool Equals(ShardRange other)
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

        /// <summary>Checks whether the specified key is inside the range.</summary>
        /// <param name="key">The key to check</param>
        /// <returns>True if inside, false otherwise</returns>
        public bool Contains(ShardKey key)
        {
            ExceptionUtils.DisallowNullArgument(key, "key");

            return (key >= Low) && (key < High);
        }

        /// <summary>Checks whether the range is inside the range.</summary>
        /// <param name="range">The range to check.</param>
        /// <returns>True if inside, false otherwise.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods",
            MessageId = "0")]
        public bool Contains(ShardRange range)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");

            return (range.Low >= Low) && (range.High <= High);
        }

        /// <summary>
        /// Performs comparison between two shard range values.
        /// </summary>
        /// <param name="other">The shard range compared with this object.</param>
        /// <returns>
        /// -1 : if this range's low boundary is less than the <paramref name="other"/>'s low boundary; 
        /// -1 : if the low boundary values match and the high boundary value of this range is less than the <paramref name="other"/>'s.
        ///  1 : if this range's high boundary is greater than the <paramref name="other"/>'s high boundary;
        ///  1 : if the low boundary value of this range is higher than <paramref name="other"/>'s low boundary and high boundary value of this range is less than or equal to <paramref name="other"/>'s high boundary .
        ///  0 : if this range has the same boundaries as <paramref name="other"/>.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public int CompareTo(ShardRange other)
        {
            ExceptionUtils.DisallowNullArgument(other, "other");

            if (this.Low < other.Low)
            {
                return -1;
            }

            if (this.High > other.High)
            {
                return 1;
            }

            if (this.Low == other.Low)
            {
                if (this.High == other.High)
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                Debug.Assert(this.Low > other.Low);
                Debug.Assert(this.High <= other.High);
                return 1;
            }
        }

        /// <summary>
        /// Compares two <see cref="ShardRange"/> using lexicographic order (less than).
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardRange"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardRange"/> of the operator.</param>
        /// <returns>True if lhs &lt; rhs</returns>
        public static bool operator <(ShardRange left, ShardRange right)
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
        /// Compares two <see cref="ShardRange"/> using lexicographic order (greater than).
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardRange"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardRange"/> of the operator.</param>
        /// <returns>True if lhs &gt; rhs</returns>
        public static bool operator >(ShardRange left, ShardRange right)
        {
            return right < left;
        }

        /// <summary>
        /// Compares two <see cref="ShardRange"/> using lexicographic order (less or equal). 
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardRange"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardRange"/> of the operator.</param>
        /// <returns>True if lhs &lt;= rhs</returns>
        public static bool operator <=(ShardRange left, ShardRange right)
        {
            return !(left > right);
        }

        /// <summary>
        /// Compares two <see cref="ShardRange"/> using lexicographic order (greater or equal). 
        /// </summary>
        /// <param name="left">Left hand side <see cref="ShardRange"/> of the operator.</param>
        /// <param name="right">Right hand side <see cref="ShardRange"/> of the operator.</param>
        /// <returns>True if lhs &gt;= rhs</returns>
        public static bool operator >=(ShardRange left, ShardRange right)
        {
            return !(left < right);
        }

        /// <summary>
        /// Equality operator.
        /// </summary>
        /// <param name="left">Left hand side</param>
        /// <param name="right">Right hand side</param>
        /// <returns>True if the two objects are equal, false in all other cases</returns>
        public static bool operator ==(ShardRange left, ShardRange right)
        {
            return object.Equals(left, right);
        }

        /// <summary>
        /// Inequality operator.
        /// </summary>
        /// <param name="left">Left hand side</param>
        /// <param name="right">Right hand side</param>
        /// <returns>True if the two objects are not equal, false in all other cases</returns>
        public static bool operator !=(ShardRange left, ShardRange right)
        {
            return !(left == right);
        }

        /// <summary>
        /// Gets a shard range corresponding to a specified key type.
        /// </summary>
        /// <param name="keyType">Type of key.</param>
        /// <returns>Full range for given key type.</returns>
        internal static ShardRange GetFullRange(ShardKeyType keyType)
        {
            Debug.Assert(keyType != ShardKeyType.None);

            switch (keyType)
            {
                case ShardKeyType.Int32:
                    return ShardRange.FullRangeInt32;
                case ShardKeyType.Int64:
                    return ShardRange.FullRangeInt64;
                case ShardKeyType.Guid:
                    return ShardRange.FullRangeGuid;
                case ShardKeyType.Binary:
                    return ShardRange.FullRangeBinary;
                case ShardKeyType.DateTime:
                    return ShardRange.FullRangeDateTime;
                case ShardKeyType.TimeSpan:
                    return ShardRange.FullRangeTimeSpan;
                case ShardKeyType.DateTimeOffset:
                    return ShardRange.FullRangeDateTimeOffset;
                default:
                    Debug.Fail("Unexpected ShardKeyType.");
                    return null;
            }
        }

        /// <summary>Checks whether the range intersects with the current range.</summary>
        /// <param name="range">The range to check.</param>
        /// <returns>True if it intersects, False otherwise.</returns>
        internal bool Intersects(ShardRange range)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");

            return (range.High > Low) && (range.Low < High);
        }

        /// <summary>Returns the intersection of two ranges.</summary>
        /// <param name="range">Range to intersect with.</param>
        /// <returns>
        /// The intersection of the current range and the specified range, null if ranges dont intersect.
        /// </returns>
        internal ShardRange Intersect(ShardRange range)
        {
            ExceptionUtils.DisallowNullArgument(range, "range");

            ShardKey intersectLow = ShardKey.Max(Low, range.Low);
            ShardKey intersectHigh = ShardKey.Min(High, range.High);

            if (intersectLow >= intersectHigh)
            {
                return null;
            }

            return new ShardRange(intersectLow, intersectHigh);
        }

        /// <summary>
        /// Calculates the hash code for the object.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        private int CalculateHashCode()
        {
            return ShardKey.QPHash(this.Low.GetHashCode(), this.High.GetHashCode());
        }
    }
}
