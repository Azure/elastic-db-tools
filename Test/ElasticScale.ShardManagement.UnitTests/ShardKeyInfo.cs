using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// A ShardKey and its corresponding RawValue in bytes.
    /// </summary>
    class ShardKeyInfo
    {
        public ShardKeyInfo(ShardKeyType keyType, object value, byte[] rawValue)
        {
            KeyType = keyType;
            Value = value;
            RawValue = rawValue;
        }

        public ShardKeyInfo(int value, byte[] rawValue) 
            : this(ShardKeyType.Int32, value, rawValue)
        {
        }

        public ShardKeyInfo(long value, byte[] rawValue)
            : this(ShardKeyType.Int64, value, rawValue)
        {
        }

        public ShardKeyInfo(Guid value, byte[] rawValue)
            : this(ShardKeyType.Guid, value, rawValue)
        {
        }

        public ShardKeyInfo(byte[] value)
            : this(ShardKeyType.Binary, value, value)
        {
        }

        public ShardKeyInfo(DateTime value, byte[] rawValue)
            : this(ShardKeyType.DateTime, value, rawValue)
        {
        }

        public ShardKeyInfo(DateTimeOffset value, byte[] rawValue)
            : this(ShardKeyType.DateTimeOffset, value, rawValue)
        {
        }

        public ShardKeyInfo(TimeSpan value, byte[] rawValue)
            : this(ShardKeyType.TimeSpan, value, rawValue)
        {
        }

        public ShardKeyType KeyType { get; set; }

        /// <summary>
        /// The original value that is provided to the ShardKey(keyType, value) constructor
        /// which should also exactly match ShardKey.Value [except that for binary type trailing zeroes are dropped]
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// The raw serialized value that is written to the database.
        /// </summary>
        public byte[] RawValue { get; set; }

        /// <summary>
        /// Gets the ShardKey using new ShardKey(keyType, value);
        /// </summary>
        public ShardKey ShardKeyFromValue
        {
            get { return new ShardKey(KeyType, Value); }
        }

        /// <summary>
        /// Gets the ShardKey using ShardKey.FromRawValue(keyType, rawValue);
        /// </summary>
        public ShardKey ShardKeyFromRawValue
        {
            get { return ShardKey.FromRawValue(KeyType, RawValue); }
        }

        public override string ToString()
        {
            return string.Format("[{0}] {1} <-> {2}", KeyType, Value, ToString(RawValue));
        }

        private static string ToString(byte[] bytes)
        {
            if (bytes == null)
            {
                return "(null)";
            }
            return "0x" + string.Join(string.Empty, bytes.Select(b => b.ToString("x2")));
        }

        /// <summary>
        /// ShardKey and RawValue pairs to test for serialization/deserialization.
        /// DO NOT EDIT EXISTING ENTRIES IN THIS LIST TO MAKE THE TEST PASS!!!
        /// The binary serialization format must be consistent across different versions of EDCL.
        /// Any incompatible changes to this format is a major breaking change!!!
        /// 
        /// The general strategy is to pick the following boundary values:
        /// * Min value
        /// * Min value + 1
        /// * -1 (if it's a numerical type)
        /// * 0 (if it's a numerical type), or some other value in the middle of the range
        /// * +1 (if it's a numerical type)
        /// * Max value - 1
        /// * Max value
        /// * +inf
        /// </summary>
        public static readonly ShardKeyInfo[] AllTestShardKeyInfos =
        {
            #region Int32

            new ShardKeyInfo(int.MinValue, new byte[] {}),
            new ShardKeyInfo(int.MinValue + 1, new byte[] {0, 0, 0, 1}),
            new ShardKeyInfo(-1, new byte[] {0x7f, 0xff, 0xff, 0xff}),
            new ShardKeyInfo(0, new byte[] {0x80, 0, 0, 0}),
            new ShardKeyInfo(1, new byte[] {0x80, 0, 0, 1}),
            new ShardKeyInfo(int.MaxValue - 1, new byte[] {0xff, 0xff, 0xff, 0xfe}),
            new ShardKeyInfo(int.MaxValue, new byte[] {0xff, 0xff, 0xff, 0xff}),
            new ShardKeyInfo(ShardKeyType.Int32, null, null),

            #endregion

            #region Int64

            new ShardKeyInfo(long.MinValue, new byte[] {}),
            new ShardKeyInfo(long.MinValue + 1, new byte[] {0, 0, 0, 0, 0, 0, 0, 1}),
            new ShardKeyInfo((long) -1, new byte[] {0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
            new ShardKeyInfo((long) 0, new byte[] {0x80, 0, 0, 0, 0, 0, 0, 0}),
            new ShardKeyInfo((long) 1, new byte[] {0x80, 0, 0, 0, 0, 0, 0, 1}),
            new ShardKeyInfo(long.MaxValue - 1, new byte[] {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}),
            new ShardKeyInfo(long.MaxValue, new byte[] {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
            new ShardKeyInfo(ShardKeyType.Int64, null, null),

            #endregion

            #region Guid

            new ShardKeyInfo(Guid.Empty, new byte[] {}),
            new ShardKeyInfo(
                Guid.Parse("a0a1a2a3-a4a5-a6a7-a8a9-aaabacadaeaf"),
                new byte[]
                {
                    0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, /* - */ 0xa8, 0xa9,
                    /* - */ 0xa7, 0xa6, /* - */ 0xa5, 0xa4, /* - */ 0xa3, 0xa2, 0xa1, 0xa0
                }),
            new ShardKeyInfo(ShardKeyType.Guid, null, null),

            #endregion

            #region Binary

            new ShardKeyInfo(new byte[] {}),
            new ShardKeyInfo(new byte[] {1}),
            new ShardKeyInfo(ByteEnumerable.Range(0, 128).ToArray()),
            new ShardKeyInfo(null),

            #endregion

            #region DateTime

            // https://github.com/Azure/elastic-db-tools/issues/116
            //new ShardKeyInfo(
            //    DateTime.MinValue,
            //    new byte[] {}),

            new ShardKeyInfo(new DateTime(ticks: 1), new byte[] {0x80, 0, 0, 0, 0, 0, 0, 1}),

            new ShardKeyInfo(
                new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                // new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks == 0x0851055320574000
                // 0x0851055320574000 + 0x8000000000000000 = 0x8851055320574000
                new byte[] {0x88, 0x51, 0x05, 0x53, 0x20, 0x57, 0x40, 0}),

            new ShardKeyInfo(
                DateTime.MaxValue,
                // DateTime.MaxValue.Ticks == 0x2bca2875f4373fff
                // 0x2bca2875f4373fff + 0x8000000000000000 = 0xabca2875f4373fff
                new byte[] {0xab, 0xca, 0x28, 0x75, 0xf4, 0x37, 0x3f, 0xff}),

            new ShardKeyInfo(ShardKeyType.DateTime, null, null),

            #endregion

            #region DateTimeOffset

            // https://github.com/Azure/elastic-db-tools/issues/116
            //new ShardKeyInfo(
            //    DateTimeOffset.MinValue,
            //    new byte[] {}),

            // https://github.com/Azure/elastic-db-tools/issues/116
            //new ShardKeyInfo(
            //    new DateTimeOffset(DateTime.MinValue, TimeSpan.Zero),
            //    new byte[] 
            //    {
            //        // DateTime part
            //        0x80, 0, 0, 0, 0, 0, 0, 0, 
            //         // Offset part
            //        0x80, 0, 0, 0, 0, 0, 0, 0
            //    }),

            new ShardKeyInfo(
                new DateTimeOffset(new DateTime(ticks: 1), TimeSpan.Zero),
                new byte[]
                {
                    // DateTime part
                    0x80, 0, 0, 0, 0, 0, 0, 1,
                    // Offset part
                    0x80, 0, 0, 0, 0, 0, 0, 0
                }),

            // BELOW ARE SORTED IN SQL SERVER ORDERING

            new ShardKeyInfo(
                new DateTimeOffset(1899, 12, 31, 23, 59, 0, TimeSpan.FromMinutes(-1)),
                new byte[]
                {
                    // DateTime part (note that 1899-12-31 23:59:00-00:01 is the same time as 1900-1-1 00:00:00Z)
                    // new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks == 0x0851055320574000
                    // 0x0851055320574000 + 0x8000000000000000 = 0x8851055320574000
                    0x88, 0x51, 5, 0x53, 0x20, 0x57, 0x40, 0,
                    // Offset part:
                    // TimeSpan.FromMinutes(1).Ticks == 600000000 == 0x23c34600
                    // 0x8000000000000000 - 0x23c34600 = 0x7fffffffdc3cba00
                    0x7f, 0xff, 0xff, 0xff, 0xdc, 0x3c, 0xba, 0
                }),

            new ShardKeyInfo(
                new DateTimeOffset(1900, 1, 1, 0, 0, 0, TimeSpan.Zero),
                // DateTime part (note that 1899-12-31 23:59:00-00:01 is the same time as 1900-1-1 00:00:00Z)
                // new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks == 0x0851055320574000
                // 0x0851055320574000 + 0x8000000000000000 = 0x8851055320574000
                new byte[]
                {
                    // DateTime part (note that 1899-12-31 23:59:00-00:01 is the same time as 1900-1-1 00:00:00Z)
                    // new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks == 0x0851055320574000
                    // 0x0851055320574000 + 0x8000000000000000 = 0x8851055320574000
                    0x88, 0x51, 5, 0x53, 0x20, 0x57, 0x40, 0,
                    // Offset part
                    0x80, 0, 0, 0, 0, 0, 0, 0
                }),

            new ShardKeyInfo(
                new DateTimeOffset(1900, 1, 1, 0, 1, 0, TimeSpan.FromMinutes(1)),
                new byte[]
                {
                    // DateTime part (note that 1899-12-31 23:59:00+00:01 is the same time as 1900-1-1 00:00:00Z)
                    // new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks == 0x0851055320574000
                    // 0x0851055320574000 + 0x8000000000000000 = 0x8851055320574000
                    0x88, 0x51, 5, 0x53, 0x20, 0x57, 0x40, 0,
                    // Offset part:
                    // TimeSpan.FromMinutes(1).Ticks == 600000000 == 0x23c34600
                    // 0x8000000000000000 + 0x23c34600 = 0x8000000023c34600
                    0x80, 0, 0, 0, 0x23, 0xC3, 0x46, 0
                }),

            new ShardKeyInfo(
                DateTimeOffset.MaxValue,
                new byte[]
                {
                    // DateTime part:
                    // DateTime.MaxValue.Ticks == 0x2bca2875f4373fff
                    // 0x2bca2875f4373fff + 0x8000000000000000 = 0xabca2875f4373fff
                    0xab, 0xca, 0x28, 0x75, 0xf4, 0x37, 0x3f, 0xff,
                    // Offset part:
                    0x80, 0, 0, 0, 0, 0, 0, 0
                }),

            new ShardKeyInfo(ShardKeyType.DateTimeOffset, null, null),

            #endregion

            #region TimeSpan

            new ShardKeyInfo(
                TimeSpan.FromTicks(long.MinValue),
                new byte[] {}),

            new ShardKeyInfo(
                TimeSpan.FromTicks(-1),
                new byte[] {0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
            

            new ShardKeyInfo(
                TimeSpan.Zero,
                new byte[] {0x80, 0, 0, 0, 0, 0, 0, 0}),
            

            new ShardKeyInfo(
                TimeSpan.FromTicks(1),
                new byte[] {0x80, 0, 0, 0, 0, 0, 0, 1}),
            

            new ShardKeyInfo(
                TimeSpan.FromTicks(long.MaxValue),
                new byte[] {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
            
            new ShardKeyInfo(ShardKeyType.TimeSpan, null, null),

            #endregion
        };

        public static readonly IReadOnlyList<object> AllTestShardKeyValues =
            AllTestShardKeyInfos.Select(i => i.Value).ToArray();

        public static readonly ILookup<ShardKeyType, ShardKeyInfo> AllTestShardKeyInfosGroupedByType =
            AllTestShardKeyInfos.ToLookup(
                keySelector: i => i.KeyType,
                elementSelector: i => i);
    }
}
