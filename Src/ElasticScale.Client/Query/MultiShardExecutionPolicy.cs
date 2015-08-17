// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Defines the possible query execution policies 

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// <summary>
    /// Defines the possible query execution policies
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi")]
    public enum MultiShardExecutionPolicy
    {
        /// <summary>
        /// With the complete results execution policy an unsuccessful 
        /// execution against any shard leads to all results being discarded
        /// and an exception being thrown either by the ExecuteReader method
        /// on the command or the Read method on the reader. 
        /// </summary>
        CompleteResults,

        /// <summary>
        /// A best-effort execution policy that, unlike CompleteResults, tolerates unsuccessful command execution 
        /// on some (but not all) shards and returns the results of the successful commands.  
        /// Any errors encountered are returned to the user along with the partial results.
        /// The caller can inspect exceptions encountered during execution through 
        /// the <see cref="MultiShardAggregateException"/> property of <see cref="MultiShardDataReader"/>. 
        /// </summary>
        PartialResults
    };
}
