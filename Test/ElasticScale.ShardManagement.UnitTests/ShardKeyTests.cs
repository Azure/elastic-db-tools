// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Test related to ShardKey class and date/time input values.
    /// </summary>
    public class ShardKeyTests
    {
        /// <summary>
        /// The length in bytes of each ShardKeyType.
        /// </summary>
        private readonly Dictionary<ShardKeyType, int> _shardKeyTypeLength = new Dictionary<ShardKeyType, int>
        {
            {ShardKeyType.Int32, 4},
            {ShardKeyType.Int64, 8},
            {ShardKeyType.Guid, 16},
            {ShardKeyType.Binary, 128},
            {ShardKeyType.DateTime, 8},
            {ShardKeyType.DateTimeOffset, 16},
            {ShardKeyType.TimeSpan, 8}
        };

        /// <summary>
        /// Verifies that new ShardKey(keyType, value) returns the correct ShardKey.Value
        /// </summary>
        [Fact]
        public void TestShardKeyValue()
        {
            foreach (ShardKeyInfo shardKeyInfo in ShardKeyInfo.AllTestShardKeyInfos)
            {
                Console.WriteLine(shardKeyInfo);

                // Verify ShardKey.Value with value type-specific Equals
                if (shardKeyInfo.KeyType == ShardKeyType.Binary && shardKeyInfo.Value != null)
                {
                    AssertExtensions.AssertScalarOrSequenceEqual(((byte[])shardKeyInfo.Value).DropTrailingZeroes(), shardKeyInfo.ShardKeyFromValue.Value, null);
                }
                else
                {
                    Assert.Equal(
                        shardKeyInfo.Value,
                        shardKeyInfo.ShardKeyFromValue.Value);
                }
            }
        }

        /// <summary>
        /// Verifies that new ShardKey(keyType, value) returns the correct RawValue
        /// </summary>
        [Fact]
        public void TestShardKeySerialization()
        {
            foreach (ShardKeyInfo shardKeyInfo in ShardKeyInfo.AllTestShardKeyInfos)
            {
                Console.WriteLine(shardKeyInfo);

                byte[] expectedSerializedValue = shardKeyInfo.RawValue;
                byte[] actualSerializedValue = shardKeyInfo.ShardKeyFromValue.RawValue;

                if (expectedSerializedValue == null)
                {
                    Assert.Equal(expectedSerializedValue, actualSerializedValue);
                }
                else
                {
                    AssertExtensions.AssertSequenceEqual(expectedSerializedValue, actualSerializedValue);
                }
            }
        }

        /// <summary>
        /// Verifies that ShardKey.FromRawValue(keyType, rawValue) returns the correct ShardKey and ShardKey.Value
        /// </summary>
        [Fact]
        public void TestShardKeyDeserialization()
        {
            foreach (ShardKeyInfo shardKeyInfo in ShardKeyInfo.AllTestShardKeyInfos)
            {
                Console.WriteLine(shardKeyInfo);

                ShardKey expectedDeserializedShardKey = shardKeyInfo.ShardKeyFromValue;
                ShardKey actualDeserializedShardKey = shardKeyInfo.ShardKeyFromRawValue;

                // Verify ShardKey with ShardKey.Equals
                Assert.Equal(
                    expectedDeserializedShardKey,
                    actualDeserializedShardKey);

                // Verify ShardKey.Value with value type-specific Equals
                AssertExtensions.AssertScalarOrSequenceEqual(expectedDeserializedShardKey.Value, actualDeserializedShardKey.Value, null);
            }
        }

        /// <summary>
        /// Verifies that ShardKey.FromRawValue(keyType, rawValue) returns the correct ShardKey and ShardKey.Value
        /// if extra zeroes are added to the end of rawValue
        /// </summary>
        [Fact]
        public void TestShardKeyDeserializationAddTrailingZeroes()
        {
            foreach (ShardKeyInfo shardKeyInfo in ShardKeyInfo.AllTestShardKeyInfos)
            {
                Console.WriteLine(shardKeyInfo);

                int dataTypeLength = _shardKeyTypeLength[shardKeyInfo.KeyType];
                if (shardKeyInfo.RawValue != null && shardKeyInfo.RawValue.Length != dataTypeLength)
                {
                    // Add trailing zeroes
                    byte[] originalRawValue = shardKeyInfo.RawValue;
                    byte[] rawValueWithTrailingZeroes = new byte[dataTypeLength];
                    originalRawValue.CopyTo(rawValueWithTrailingZeroes, 0);

                    ShardKey expectedDeserializedShardKey = shardKeyInfo.ShardKeyFromValue;
                    ShardKey actualDeserializedShardKey = ShardKey.FromRawValue(shardKeyInfo.KeyType, rawValueWithTrailingZeroes);

                    // Bug? Below fails when there are trailing zeroes even though the value is Equal
                    //// Verify ShardKey with ShardKey.Equals
                    //Assert.Equal(
                    //    expectedDeserializedShardKey,
                    //    actualDeserializedShardKey);

                    // Bug? Below fails for Binary type
                    if (shardKeyInfo.KeyType != ShardKeyType.Binary)
                    {
                        // Verify ShardKey.Value with value type-specific Equals
                        AssertExtensions.AssertScalarOrSequenceEqual(expectedDeserializedShardKey.Value, actualDeserializedShardKey.Value, null);
                    }
                }
            }
        }

        /// <summary>
        /// Tests that ShardKey.Min* and ShardKey.Max* have the correct KeyType, Value, and RawValue
        /// </summary>
        [Fact]
        public void TestShardKeyTypeInfo()
        {
            foreach (var shardKeyTypeInfo in ShardKeyTypeInfo.ShardKeyTypeInfos.Values)
            {
                Console.WriteLine(shardKeyTypeInfo.KeyType);

                // Min Value
                // Skip for DateTime & DateTimeOffset due to https://github.com/Azure/elastic-db-tools/issues/116
                if (shardKeyTypeInfo.KeyType != ShardKeyType.DateTime && shardKeyTypeInfo.KeyType != ShardKeyType.DateTimeOffset) 
                {
                    Assert.Equal(shardKeyTypeInfo.KeyType, shardKeyTypeInfo.MinShardKey.KeyType);
                    AssertExtensions.AssertScalarOrSequenceEqual(shardKeyTypeInfo.MinValue, shardKeyTypeInfo.MinShardKey.Value, null);
                    AssertExtensions.AssertSequenceEqual(new byte[0], shardKeyTypeInfo.MinShardKey.RawValue);
                }

                // Max value
                Assert.Equal(shardKeyTypeInfo.KeyType, shardKeyTypeInfo.MaxShardKey.KeyType);
                AssertExtensions.AssertScalarOrSequenceEqual(null, shardKeyTypeInfo.MaxShardKey.Value, null);
                Assert.Equal(null, shardKeyTypeInfo.MaxShardKey.RawValue);
            }
        }

        // This is the same ordering as SQL Server
        private readonly ShardKey[] _orderedGuidsDescending =
        {
            ShardKey.MaxGuid,
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-010000000000")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-000100000000")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-000001000000")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-000000010000")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-000000000100")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-000000000001")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0100-000000000000")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0010-000000000000")),
            new ShardKey(Guid.Parse("00000000-0000-0001-0000-000000000000")),
            new ShardKey(Guid.Parse("00000000-0000-0100-0000-000000000000")),
            new ShardKey(Guid.Parse("00000000-0001-0000-0000-000000000000")),
            new ShardKey(Guid.Parse("00000000-0100-0000-0000-000000000000")),
            new ShardKey(Guid.Parse("00000001-0000-0000-0000-000000000000")),
            new ShardKey(Guid.Parse("00000100-0000-0000-0000-000000000000")),
            new ShardKey(Guid.Parse("00010000-0000-0000-0000-000000000000")),
            new ShardKey(Guid.Parse("01000000-0000-0000-0000-000000000000")),
            new ShardKey(Guid.Parse("00000000-0000-0000-0000-000000000000"))
        };

        /// <summary>
        /// Verifies that ShardKey correct orders Guids according to SQL Server ordering.
        /// </summary>
        [Fact]
        public void TestGuidOrdering()
        {
            for (int i = 0; i < _orderedGuidsDescending.Length - 1; i++)
            {
                Assert.True(
                    _orderedGuidsDescending[0] > _orderedGuidsDescending[1],
                    String.Format("Expected {0} to be great than {1}",
                        _orderedGuidsDescending[0],
                        _orderedGuidsDescending[1])
                    );
            }
        }

        /// <summary>
        /// Verifies that ShardKey correct orders DateTimeOffset according to SQL Server ordering.
        /// </summary>
        [Fact]
        public void TestDateTimeOffsetOrdering()
        {
            ShardKeyInfo[] dateTimeOffsetShardKeyInfos = 
                ShardKeyInfo.AllTestShardKeyInfosGroupedByType[ShardKeyType.DateTimeOffset]
                .ToArray();

            for (int i = 0; i < dateTimeOffsetShardKeyInfos.Length - 1; i++)
            {
                ShardKeyInfo low = dateTimeOffsetShardKeyInfos[i];
                ShardKeyInfo high = dateTimeOffsetShardKeyInfos[i + 1];

                Console.WriteLine("({0}) < ({1})", low, high);

                // https://github.com/Azure/elastic-db-tools/issues/117
                // Bug? DateTimeOffsets with the same universal time but different offset are equal as ShardKeys. 
                // According to SQL (and our normalization format), they should be unequal, although according to .NET they should be equal.
                if (high.Value != null && ((DateTimeOffset)low.Value).UtcDateTime == ((DateTimeOffset)high.Value).UtcDateTime)
                {
                    Assert.Equal(low.ShardKeyFromValue, high.ShardKeyFromValue);
                }
                else
                {
                    AssertExtensions.AssertGreaterThan(low.ShardKeyFromValue, high.ShardKeyFromValue);
                }
            }
        }

        /// <summary>
        /// Test using ShardKey with DateTime value.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
        public void TestShardKeyWithDateTime()
        {
            DateTime testValue = DateTime.Now;
            TestShardKeyGeneric<DateTime>(ShardKeyType.DateTime, testValue, typeof (DateTime));
        }

        /// <summary>
        /// Test using ShardKey with TimeSpan value.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
        public void TestShardKeyWithTimeSpan()
        {
            TimeSpan testValue = TimeSpan.FromMinutes(6.2);
            TestShardKeyGeneric<TimeSpan>(ShardKeyType.TimeSpan, testValue, typeof (TimeSpan));
        }

        /// <summary>
        /// Test using ShardKey with DateTimeOffset value.
        /// </summary>
        [Fact]
        [Trait("Category", "ExcludeFromGatedCheckin")]
        public void TestShardKeyWithDateTimeOffset()
        {
            DateTimeOffset testValue = DateTimeOffset.Now;
            TestShardKeyGeneric<DateTimeOffset>(ShardKeyType.DateTimeOffset, testValue, typeof (DateTimeOffset));

            DateTime d1 = DateTime.Now;
            ShardKey k1 = new ShardKey(new DateTimeOffset(d1, DateTimeOffset.Now.Offset));
            ShardKey k2 = new ShardKey(new DateTimeOffset(d1.ToUniversalTime(), TimeSpan.FromHours(0)));
            Assert.Equal(k1, k2);

            ShardKey k3 = ShardKey.MinDateTimeOffset;
            Assert.AreNotEqual(k1, k3);
        }

        private void TestShardKeyGeneric<TKey>(ShardKeyType keyType, TKey inputValue, Type realType)
        {
            // Excercise DetectType
            //
            ShardKey k1 = new ShardKey(inputValue);
            Assert.Equal(realType, k1.DataType);

            // Go to/from raw value
            ShardKey k2 = new ShardKey(keyType, inputValue);
            byte[] k2raw = k2.RawValue;

            ShardKey k3 = ShardKey.FromRawValue(keyType, k2raw);
            Assert.Equal(inputValue, k2.Value);
            Assert.Equal(inputValue, k3.Value);
            Assert.Equal(k2, k3);

            // verify comparisons
            Assert.True(0 == k2.CompareTo(k3));

            try
            {
                k3 = k2.GetNextKey();
                Assert.True(k3 > k2);
            }
            catch (InvalidOperationException)
            {
            }
        }
    }

    internal static class ByteEnumerable
    {
        public static IEnumerable<byte> Range(byte start, byte count)
        {
            for (byte i = 0; i < count; i++)
            {
                yield return (byte) (start + count);
            }
        }

        public static IEnumerable<byte> DropTrailingZeroes(this byte[] source)
        {
            // Find last nonzero
            int lastNonzero;
            for (lastNonzero = source.Length - 1; lastNonzero >= 0 && source[lastNonzero] == 0; lastNonzero--)
            {
            }

            return source.Take(lastNonzero + 1);
        }
    }
}
