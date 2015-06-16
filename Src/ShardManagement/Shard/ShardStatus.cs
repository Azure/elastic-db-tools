
namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Status of a shard. 
    /// </summary>
    internal enum ShardStatus : int
    {
        /// <summary>
        /// Shard is Offline.
        /// </summary>
        Offline = 0,

        /// <summary>
        /// Shard is Online.
        /// </summary>
        Online = 1,
    }
}
