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
        };

        private Dictionary<ShardKeyType, int> _dataTypeLength = new Dictionary<ShardKeyType, int>
        {
            {ShardKeyType.Int32, 4},
            {ShardKeyType.Int64, 8}
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

                TestShardKeyDeserialization(shardKeyAndRawValue.ShardKey, shardKeyAndRawValue.RawValue);
            }
        }

        [TestMethod]
        public void TestShardKeyDeserializationAddTrailingZeroes()
        {
            foreach (ShardKeyAndRawValue shardKeyAndRawValue in _shardKeyAndRawValues)
            {
                Console.WriteLine(shardKeyAndRawValue);
                if (shardKeyAndRawValue.RawValue != null && shardKeyAndRawValue.RawValue.Length == 0)
                {
                    // If the length is zero, we are allowed to add exactly the right number of trailing zeroes for this data type
                    int dataTypeLength = _dataTypeLength[shardKeyAndRawValue.ShardKey.KeyType];
                    byte[] rawValueWithTrailingZeroes = new byte[dataTypeLength];
                    shardKeyAndRawValue.RawValue.CopyTo(rawValueWithTrailingZeroes, 0);

                    TestShardKeyDeserialization(shardKeyAndRawValue.ShardKey, rawValueWithTrailingZeroes);
                }
            }
        }

        private static void TestShardKeyDeserialization(ShardKey expectedDeserializedValue, byte[] rawValueToDeserialize)
        {
            ShardKey actualDeserializedValue = ShardKey.FromRawValue(expectedDeserializedValue.KeyType, rawValueToDeserialize);

            Assert.AreEqual(
                expectedDeserializedValue.Value,
                actualDeserializedValue.Value);
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

    internal static class EnumerableExtensions
    {
        public static IEnumerable<T> PadToLength<T>(this IEnumerable<T> source, int length)
        {
            using (IEnumerator<T> enumerator = source.GetEnumerator())
            {
                for (int i = 0; i < length; i++)
                {
                    if (enumerator.MoveNext())
                    {
                        yield return enumerator.Current;
                    }
                    else
                    {
                        yield return default(T);
                    }
                }

                if (enumerator.MoveNext())
                {
                    throw new InvalidOperationException(
                        string.Format("Cannot pad source to length {0} because source was longer than that length", length));
                }
            }
        }
    }
}
