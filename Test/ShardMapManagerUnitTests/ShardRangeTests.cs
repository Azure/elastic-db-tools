// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using Microsoft.Azure.SqlDatabase.ElasticScale.Test.Common;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Tests cover shard range tests
    /// </summary>
    [TestClass]
    public class ShardRangeTests
    {
        public const Int32 max32 = 0x7FFFFFFF;
        public const Int64 max64 = 0x7FFFFFFFFFFFFFFF;
        public ShardKey maxNonNullKey32 = new ShardKey(max32);
        public ShardKey maxNonNullKey64 = new ShardKey(max64);
        public Random rValGen = new Random();

        [TestMethod()]
        [TestCategory("ExcludeFromGatedCheckin")]
        public void ShardKeyTests()
        {
            ShardKey key = null;
            ShardKey result = null;
            byte[] array = null;
            byte[] arraymax = null;

            // Verify boundary conditions
            result = maxNonNullKey32.GetNextKey();
            Debug.Assert(result.IsMax);
            Debug.Assert(result == ShardKey.MaxInt32);

            result = maxNonNullKey64.GetNextKey();
            Debug.Assert(result.IsMax);
            Debug.Assert(result == ShardKey.MaxInt64);

            array = Enumerable.Repeat((byte)0xff, 16).ToArray();
            key = ShardKey.FromRawValue(ShardKeyType.Guid, array); // can not use other ctor because normalized representation differ
            result = key.GetNextKey();
            Debug.Assert(result.IsMax);
            Debug.Assert(result == ShardKey.MaxGuid);

            array = Enumerable.Repeat((byte)0xff, 128).ToArray();
            key = new ShardKey(array);
            result = key.GetNextKey();
            Debug.Assert(result.IsMax);
            Debug.Assert(result == ShardKey.MaxBinary);

            key = new ShardKey(max32 - 1);
            result = key.GetNextKey();
            Debug.Assert(result == maxNonNullKey32);

            key = new ShardKey(max64 - 1);
            result = key.GetNextKey();
            Debug.Assert(result == maxNonNullKey64);

            arraymax = Enumerable.Repeat((byte)0xff, 16).ToArray();
            array = Enumerable.Repeat((byte)0xff, 16).ToArray();
            array[15] = 0xfe;
            key = ShardKey.FromRawValue(ShardKeyType.Guid, array); // can not use other ctor because normalized representation differ
            result = key.GetNextKey();
            Debug.Assert(result == ShardKey.FromRawValue(ShardKeyType.Guid, arraymax));

            arraymax = Enumerable.Repeat((byte)0xff, 128).ToArray();
            array = Enumerable.Repeat((byte)0xff, 128).ToArray();
            array[127] = 0xfe;
            key = new ShardKey(array);
            result = key.GetNextKey();
            Debug.Assert(result == ShardKey.FromRawValue(ShardKeyType.Binary, arraymax));

            key = new ShardKey(ShardKeyType.Int32, null);
            AssertExtensions.AssertThrows<InvalidOperationException>(() => key.GetNextKey());

            key = new ShardKey(ShardKeyType.Int64, null);
            AssertExtensions.AssertThrows<InvalidOperationException>(() => key.GetNextKey());

            key = new ShardKey(ShardKeyType.Guid, null);
            AssertExtensions.AssertThrows<InvalidOperationException>(() => key.GetNextKey());

            key = new ShardKey(ShardKeyType.Binary, null);
            AssertExtensions.AssertThrows<InvalidOperationException>(() => key.GetNextKey());

            result = ShardKey.MinInt32.GetNextKey();
            Debug.Assert(result == new ShardKey(Int32.MinValue + 1));

            result = ShardKey.MinInt64.GetNextKey();
            Debug.Assert(result == new ShardKey(Int64.MinValue + 1));

            result = ShardKey.MinGuid.GetNextKey();
            array = new byte[16];
            array[15] = 0x01;
            key = ShardKey.FromRawValue(ShardKeyType.Guid, array);
            Debug.Assert(result == key);

            result = ShardKey.MinBinary.GetNextKey();
            array = new byte[128];
            array[127] = 0x01;
            key = ShardKey.FromRawValue(ShardKeyType.Binary, array);
            Debug.Assert(result == key);

            for (int i = 0; i < 10; i++)
            {
                Verify(ShardKeyType.Int32);
                Verify(ShardKeyType.Int64);
                Verify(ShardKeyType.Guid);
                Verify(ShardKeyType.Binary);
            }
        }

        private void Verify(ShardKeyType kind)
        {
            byte[] bytes = null;
            ShardKey key = null;
            ShardKey result = null;

            switch (kind)
            {
                case ShardKeyType.Int32:
                    bytes = new byte[sizeof(int)];
                    rValGen.NextBytes(bytes);
                    Int32 int32 = BitConverter.ToInt32(bytes, 0);
                    key = new ShardKey(int32);
                    result = key.GetNextKey();
                    Debug.Assert(result.IsMax || result == new ShardKey(int32 + 1));
                    break;

                case ShardKeyType.Int64:
                    bytes = new byte[sizeof(long)];
                    rValGen.NextBytes(bytes);
                    Int64 int64 = BitConverter.ToInt64(bytes, 0);
                    key = new ShardKey(int64);
                    result = key.GetNextKey();
                    Debug.Assert(result.IsMax || result == new ShardKey(int64 + 1));
                    break;

                case ShardKeyType.Guid:
                    Guid guid = Guid.NewGuid();
                    key = new ShardKey(guid);
                    result = key.GetNextKey();
                    // verify only the API call
                    break;

                case ShardKeyType.Binary:
                    bytes = new byte[128];
                    rValGen.NextBytes(bytes);
                    key = new ShardKey(bytes);
                    result = key.GetNextKey();
                    // verify only the API call
                    break;

                default:
                    throw new ArgumentOutOfRangeException(
                        "kind",
                        kind,
                        Errors._ShardKey_UnsupportedShardKeyType);
            }
        }
    }
}
