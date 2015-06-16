
namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Types of transaction scopes used during store operations.
    /// </summary>
    internal enum StoreOperationTransactionScopeKind
    {
        /// <summary>
        /// Scope of GSM.
        /// </summary>
        Global,

        /// <summary>
        /// Scope of source LSM.
        /// </summary>
        LocalSource,

        /// <summary>
        /// Scope of target LSM.
        /// </summary>
        LocalTarget
    }
}
