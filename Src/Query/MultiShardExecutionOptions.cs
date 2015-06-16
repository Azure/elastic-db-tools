using System;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    /// <summary>
    /// Defines the available options when 
    /// executing commands against multiple shards
    /// </summary>
    /// <remarks>This enumeration has a flags attribute</remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"), FlagsAttribute]
    public enum MultiShardExecutionOptions
    {
        /// <summary>,
        /// Execute without any options enabled
        /// </summary>
        None,

        /// <summary>
        /// Whether the $ShardName pseudo column should be included
        /// in the result-sets.
        /// </summary>
        IncludeShardNameColumn
    };
}
