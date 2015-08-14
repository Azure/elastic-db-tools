// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>Represents a left-inclusive, right-exclusive range of values of type T.</summary>
    /// <typeparam name="TKey">Type of values.</typeparam>
    public sealed class Range<TKey> : IEquatable<Range<TKey>>
    {
        /// <summary>
        /// The shard range value corresponding to this value.
        /// </summary>
        private ShardRange _r;

        /// <summary>
        /// Constructs range based on its low and high boundary values.
        /// </summary>
        /// <param name="low">Low boundary value (inclusive).</param>
        /// <param name="high">High boundary value (exclusive).</param>
        public Range(TKey low, TKey high)
        {
            ShardKeyType k = ShardKey.ShardKeyTypeFromType(typeof(TKey));
            _r = new ShardRange(
                            new ShardKey(k, low),
                            new ShardKey(k, high));

            this.Low = low;
            this.High = high;
        }

        /// <summary>
        /// Constructs range based on its low boundary value. The low boundary value is
        /// set to the one specified in <paramref name="low"/> while the
        /// high boundary value is set to maximum possible value i.e. +infinity.
        /// </summary>
        /// <param name="low">Low boundary value (inclusive).</param>
        public Range(TKey low)
        {
            ShardKeyType k = ShardKey.ShardKeyTypeFromType(typeof(TKey));
            _r = new ShardRange(
                            new ShardKey(k, low),
                            new ShardKey(k, null));

            this.Low = low;
            this.HighIsMax = true;
        }

        /// <summary>
        /// Gets the low boundary value (inclusive).
        /// </summary>
        public TKey Low
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the high boundary value (exclusive).
        /// </summary>
        public TKey High
        {
            get;
            private set;
        }

        /// <summary>
        /// True if the high boundary value equals +infinity; otherwise, false.
        /// </summary>
        public bool HighIsMax
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
            return _r.ToString();
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return _r.GetHashCode();
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return this.Equals(obj as Range<TKey>);
        }

        /// <summary>
        /// Performs equality comparison with another Range.
        /// </summary>
        /// <param name="other">Range to compare with.</param>
        /// <returns>True if same Range, false otherwise.</returns>
        public bool Equals(Range<TKey> other)
        {
            if (other == null)
            {
                return false;
            }
            else
            {
                return _r.Equals(other._r);
            }
        }
    }
}
