
namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Recovery
{
    /// <summary>
    /// Resolution strategy for resolving mapping differences.
    /// </summary>
    public enum MappingDifferenceResolution
    {
        /// <summary>
        /// Ignore the difference for now.
        /// </summary>
        Ignore,

        /// <summary>
        /// Use the mapping present in shard map.
        /// </summary>
        KeepShardMapMapping,

        /// <summary>
        /// Use the mapping in the shard.
        /// </summary>
        KeepShardMapping
    }

}
