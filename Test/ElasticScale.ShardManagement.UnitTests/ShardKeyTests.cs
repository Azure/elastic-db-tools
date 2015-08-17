// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Fakes;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Test related to ShardMapper class and it's methods.
    /// </summary>
    [TestClass]
    public class ShardKeyTests
    {
        /// <summary>
        /// Shard map type conversion between list and range.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardKeyWithDateTime()
        {
            DateTime testValue = DateTime.Now;
            TestShardKeyGeneric<DateTime>(ShardKeyType.DateTime, testValue, typeof(DateTime));
        }

        /// <summary>
        /// Shard map type conversion between list and range.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardKeyWithTimeSpan()
        {
            TimeSpan testValue = TimeSpan.FromMinutes(6.2);
            TestShardKeyGeneric<TimeSpan>(ShardKeyType.TimeSpan, testValue, typeof(TimeSpan));
        }

        /// <summary>
        /// Shard map type conversion between list and range.
        /// </summary>
        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void TestShardKeyWithDateTimeOffset()
        {
            DateTime dt = new DateTime(DateTime.Now.Ticks, DateTimeKind.Local);
            DateTimeOffset testValue = new DateTimeOffset(dt, TimeSpan.FromHours(-7));
            TestShardKeyGeneric<DateTimeOffset>(ShardKeyType.DateTimeOffset, testValue, typeof(DateTimeOffset));

            DateTime d1 = DateTime.Now;
            ShardKey k1 = new ShardKey(new DateTimeOffset(d1, TimeSpan.FromHours(-7)));
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
}
