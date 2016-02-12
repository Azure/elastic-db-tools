// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Test related to ShardKey class and date/time input values.
    /// </summary>
    [TestClass]
    public class ShardKeyTests
    {
        /// <summary>
        /// A ShardKey and its corresponding RawValue in bytes.
        /// </summary>
        private struct ShardKeyAndRawValue
        {
            public ShardKey ShardKey { get; set; }
            public byte[] RawValue { get; set; }

            public override string ToString()
            {
                return string.Format("{0} <-> {1}", ToString(ShardKey), ToString(RawValue));
            }

            private static string ToString(ShardKey shardKey)
            {
                return string.Format("{0} {1}", shardKey.KeyType, shardKey);
            }

            private static string ToString(byte[] bytes)
            {
                if (bytes == null)
                {
                    return "(null)";
                }
                return "0x" + string.Join(string.Empty, bytes.Select(b => b.ToString("x2")));
            }
        }

        /// <summary>
        /// ShardKey and RawValue pairs to test for serialization/deserialization.
        /// DO NOT EDIT EXISTING ENTRIES IN THIS LIST TO MAKE THE TEST PASS!!!
        /// The binary serialization format must be consistent across different versions of EDCL.
        /// Any incompatible changes to this format is a major breaking change!!!
        /// </summary>
        private readonly ShardKeyAndRawValue[] _shardKeyAndRawValues =
        {
            #region Int32

            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MinInt32,
                RawValue = new byte[] {}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(int.MinValue),
                RawValue = new byte[] {}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(int.MinValue + 1),
                RawValue = new byte[] {0, 0, 0, 1},
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(-1),
                RawValue = new byte[] {0x7f, 0xff, 0xff, 0xff}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(0),
                RawValue = new byte[] {0x80, 0, 0, 0}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(1),
                RawValue = new byte[] {0x80, 0, 0, 1}
            },
            new ShardKeyAndRawValue
            {

                ShardKey = new ShardKey(int.MaxValue - 1),
                RawValue = new byte[] {0xff, 0xff, 0xff, 0xfe}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(int.MaxValue),
                RawValue = new byte[] {0xff, 0xff, 0xff, 0xff}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MaxInt32,
                RawValue = null
            },

            #endregion

            #region Int64

            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MinInt64,
                RawValue = new byte[] {}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(long.MinValue),
                RawValue = new byte[] {}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(long.MinValue + 1),
                RawValue = new byte[] {0, 0, 0, 0, 0, 0, 0, 1},
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey((long) -1),
                RawValue = new byte[] {0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey((long) 0),
                RawValue = new byte[] {0x80, 0, 0, 0, 0, 0, 0, 0}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey((long) 1),
                RawValue = new byte[] {0x80, 0, 0, 0, 0, 0, 0, 1}
            },
            new ShardKeyAndRawValue
            {

                ShardKey = new ShardKey(long.MaxValue - 1),
                RawValue = new byte[] {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(long.MaxValue),
                RawValue = new byte[] {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
            },
            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MaxInt64,
                RawValue = null
            },

            #endregion

            #region Guid

            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MinGuid,
                RawValue = new byte[] {}
            },

            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(Guid.Empty),
                RawValue = new byte[] {}
            },

            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(Guid.Parse("a0a1a2a3-a4a5-a6a7-a8a9-aaabacadaeaf")),
                RawValue = new byte[] {0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, /* - */ 0xa8, 0xa9, 
                                        /* - */ 0xa7, 0xa6, /* - */ 0xa5, 0xa4, /* - */ 0xa3, 0xa2, 0xa1, 0xa0}
            },

            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MaxGuid,
                RawValue = null
            },

            #endregion

            #region Binary

            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MinBinary,
                RawValue = new byte[] {}
            },

            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(new byte[] {}),
                RawValue = new byte[] {}
            },

            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(new byte[] {0}),
                RawValue = new byte[] {}
            },

            new ShardKeyAndRawValue
            {
                ShardKey = new ShardKey(ByteEnumerable.Range(0, 128).ToArray()),
                RawValue = ByteEnumerable.Range(0, 128).ToArray()
            },

            new ShardKeyAndRawValue
            {
                ShardKey = ShardKey.MaxBinary,
                RawValue = null
            },

            #endregion
        };

        /// <summary>
        /// The length in bytes of each ShardKeyType.
        /// </summary>
        private readonly Dictionary<ShardKeyType, int> _shardKeyTypeLength = new Dictionary<ShardKeyType, int>
        {
            {ShardKeyType.Int32, 4},
            {ShardKeyType.Int64, 8},
            {ShardKeyType.Guid, 16},
            {ShardKeyType.Binary, 128},
        };

        [TestMethod]
        public void TestShardKeySerialization()
        {
            foreach (ShardKeyAndRawValue shardKeyAndRawValue in _shardKeyAndRawValues)
            {
                Console.WriteLine(shardKeyAndRawValue);

                byte[] expectedSerializedValue = shardKeyAndRawValue.RawValue;
                byte[] actualSerializedValue = shardKeyAndRawValue.ShardKey.RawValue;

                if (expectedSerializedValue == null)
                {
                    Assert.AreEqual(expectedSerializedValue, actualSerializedValue);
                }
                else
                {
                    AssertExtensions.AssertSequenceEqual(expectedSerializedValue, actualSerializedValue);
                }
            }
        }

        [TestMethod]
        public void TestShardKeyDeserialization()
        {
            foreach (ShardKeyAndRawValue shardKeyAndRawValue in _shardKeyAndRawValues)
            {
                Console.WriteLine(shardKeyAndRawValue);

                ShardKey expectedDeserializedShardKey = shardKeyAndRawValue.ShardKey;
                ShardKey actualDeserializedShardKey = ShardKey.FromRawValue(expectedDeserializedShardKey.KeyType, shardKeyAndRawValue.RawValue);

                // Verify with ShardKey.Equals
                Assert.AreEqual(
                    expectedDeserializedShardKey,
                    actualDeserializedShardKey);

                // Verify with value type-specific Equals
                if (expectedDeserializedShardKey.KeyType == ShardKeyType.Binary && expectedDeserializedShardKey.Value != null)
                {
                    AssertExtensions.AssertSequenceEqual(
                        (byte[])expectedDeserializedShardKey.Value,
                        (byte[])actualDeserializedShardKey.Value);
                }
                else
                {
                    Assert.AreEqual(
                        expectedDeserializedShardKey.Value,
                        actualDeserializedShardKey.Value);
                }
            }
        }

        [TestMethod]
        public void TestShardKeyDeserializationAddTrailingZeroes()
        {
            foreach (ShardKeyAndRawValue shardKeyAndRawValue in _shardKeyAndRawValues)
            {
                Console.WriteLine(shardKeyAndRawValue);

                int dataTypeLength = _shardKeyTypeLength[shardKeyAndRawValue.ShardKey.KeyType];
                if (shardKeyAndRawValue.RawValue != null && shardKeyAndRawValue.RawValue.Length != dataTypeLength)
                {
                    ShardKey originalShardKey = shardKeyAndRawValue.ShardKey;
                    byte[] originalRawValue = shardKeyAndRawValue.RawValue;

                    // Add trailing zeroes
                    byte[] rawValueWithTrailingZeroes = new byte[dataTypeLength];
                    originalRawValue.CopyTo(rawValueWithTrailingZeroes, 0);

                    ShardKey actualDeserializedShardKey = ShardKey.FromRawValue(shardKeyAndRawValue.ShardKey.KeyType,
                        rawValueWithTrailingZeroes);

                    if (actualDeserializedShardKey.KeyType == ShardKeyType.Binary && actualDeserializedShardKey.Value != null)
                    {
                        // Unlike the other types that have fixed length, Binary values are sensitive to trailing zeroes.
                        // Since we added trailing zeroes, that means that actualDeserializedShardKey and its value
                        // inside should NOT equal originalShardKey ad its value

                        // Verify with ShardKey.Equals
                        Assert.AreNotEqual(
                            originalShardKey,
                            actualDeserializedShardKey);

                        // Verify with value type-specific Equals
                        // The deserialized ShardKey should have the same value as the byte[] that has extra zeroes
                        AssertExtensions.AssertSequenceEqual(
                            rawValueWithTrailingZeroes,
                            (byte[]) actualDeserializedShardKey.Value);
                    }
                    else
                    {
                        // Bug? Below fails when there are trailing zeroes even though the value is Equal
                        //// Verify with ShardKey.Equals
                        //Assert.AreEqual(
                        //    originalShardKey,
                        //    actualDeserializedValue);

                        // Verify with value type-specific Equals
                        Assert.AreEqual(
                            originalShardKey.Value,
                            actualDeserializedShardKey.Value);
                    }
                }
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

        [TestMethod]
        public void TestGuidOrdering()
        {
            for (int i = 0; i < _orderedGuidsDescending.Length - 1; i++)
            {
                Assert.IsTrue(
                    _orderedGuidsDescending[0] > _orderedGuidsDescending[1],
                    "Expected {0} to be great than {1}",
                    _orderedGuidsDescending[0],
                    _orderedGuidsDescending[1]);
            }
        }

        /// <summary>
        /// Test using ShardKey with DateTime value.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardKeyWithDateTime()
        {
            DateTime testValue = DateTime.Now;
            TestShardKeyGeneric<DateTime>(ShardKeyType.DateTime, testValue, typeof(DateTime));
        }

        /// <summary>
        /// Test using ShardKey with TimeSpan value.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardKeyWithTimeSpan()
        {
            TimeSpan testValue = TimeSpan.FromMinutes(6.2);
            TestShardKeyGeneric<TimeSpan>(ShardKeyType.TimeSpan, testValue, typeof(TimeSpan));
        }

        /// <summary>
        /// Test using ShardKey with DateTimeOffset value.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardKeyWithDateTimeOffset()
        {
            DateTimeOffset testValue = DateTimeOffset.Now;
            TestShardKeyGeneric<DateTimeOffset>(ShardKeyType.DateTimeOffset, testValue, typeof(DateTimeOffset));

            DateTime d1 = DateTime.Now;
            ShardKey k1 = new ShardKey(new DateTimeOffset(d1, DateTimeOffset.Now.Offset));
            ShardKey k2 = new ShardKey(new DateTimeOffset(d1.ToUniversalTime(), TimeSpan.FromHours(0)));
            Assert.AreEqual(k1, k2);

            ShardKey k3 = ShardKey.MinDateTimeOffset;
            Assert.AreNotEqual(k1, k3);
        }

        private void TestShardKeyGeneric<TKey>(ShardKeyType keyType, TKey inputValue, Type realType)
        {
            // Excercise DetectType
            //
            ShardKey k1 = new ShardKey(inputValue);
            Assert.AreEqual(realType, k1.DataType);

            // Go to/from raw value
            ShardKey k2 = new ShardKey(keyType, inputValue);
            byte[] k2raw = k2.RawValue;

            ShardKey k3 = ShardKey.FromRawValue(keyType, k2raw);
            Assert.AreEqual(inputValue, k2.Value);
            Assert.AreEqual(inputValue, k3.Value);
            Assert.AreEqual(k2, k3);

            // verify comparisons
            Assert.AreEqual(0, k2.CompareTo(k3));

            try
            {
                k3 = k2.GetNextKey();
                Assert.IsTrue(k3 > k2);
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
                yield return (byte)(start + count);
            }
        }
    }
}
