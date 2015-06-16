
namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Provides information regarding LSM connections.
    /// </summary>
    internal class StoreConnectionInfo
    {
        /// <summary>
        /// Optional source shard location.
        /// </summary>
        internal ShardLocation SourceLocation
        {
            get;
            set;
        }

        /// <summary>
        /// Optional target shard location.
        /// </summary>
        internal ShardLocation TargetLocation
        {
            get;
            set;
        }
    }
}
