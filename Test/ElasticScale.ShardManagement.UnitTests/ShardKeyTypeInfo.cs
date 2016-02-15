using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.UnitTests
{
    /// <summary>
    /// Contains metadata for each ShardKeyType.
    /// </summary>
    public struct ShardKeyTypeInfo
    {
        public ShardKeyType KeyType { get; set; }
        public int Length { get; set; }
        public object MinValue { get; set; }
        public ShardKey MinShardKey { get; set; }
        public ShardKey MaxShardKey { get; set; }

        public static IDictionary<ShardKeyType, ShardKeyTypeInfo> ShardKeyTypeInfos = new[]
        {
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.Int32,
                Length = 4,
                MinValue = int.MinValue,
                MinShardKey = ShardKey.MinInt32,
                MaxShardKey = ShardKey.MaxInt32
            },
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.Int64,
                Length = 8,
                MinValue = long.MinValue,
                MinShardKey = ShardKey.MinInt64,
                MaxShardKey = ShardKey.MaxInt64
            },
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.Guid,
                Length = 16,
                MinValue = Guid.Empty,
                MinShardKey = ShardKey.MinGuid,
                MaxShardKey = ShardKey.MaxGuid
            },
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.Binary,
                Length = 128,
                MinValue = new byte[] {},
                MinShardKey = ShardKey.MinBinary,
                MaxShardKey = ShardKey.MaxBinary
            },
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.DateTime,
                Length = 8,
                MinValue = DateTime.MinValue,
                MinShardKey = ShardKey.MinDateTime,
                MaxShardKey = ShardKey.MaxDateTime
            },
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.DateTimeOffset,
                Length = 16,
                MinValue = DateTimeOffset.MinValue,
                MinShardKey = ShardKey.MinDateTimeOffset,
                MaxShardKey = ShardKey.MaxDateTimeOffset
            },
            new ShardKeyTypeInfo
            {
                KeyType = ShardKeyType.TimeSpan,
                Length = 8,
                MinValue = TimeSpan.MinValue,
                MinShardKey = ShardKey.MinTimeSpan,
                MaxShardKey = ShardKey.MaxTimeSpan
            },
        }.ToDictionary(keySelector: typeInfo => typeInfo.KeyType);
    }
}
