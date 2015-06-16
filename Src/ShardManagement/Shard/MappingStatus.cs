
namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Status of a mapping. 
    /// </summary>
    public enum MappingStatus : int
    {
        /// <summary>
        /// Mapping is Offline.
        /// </summary>
        Offline = 0,

        /// <summary>
        /// Mapping is Online.
        /// </summary>
        Online = 1,
    }
}
