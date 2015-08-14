// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Custom exception to throw when the schema from a DbDataReader from a given shard
// does not conform to the expected schema for the fanout query as a whole.
//
// Notes:

using System;
using System.Runtime.Serialization;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// <summary>
    /// Custom exception thrown when the schema on at least one of the shards 
    /// participating in the overall query does not conform to the expected schema 
    /// for the multi-shard query as a whole.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"), Serializable]
    public class MultiShardSchemaMismatchException : MultiShardException
    {
        #region Custom Constructors

        internal MultiShardSchemaMismatchException(ShardLocation shardLocation, string message)
            : base(shardLocation, message)
        {
        }

        #endregion Custom Constructors

        #region Standard Exception Constructors

        /// <summary>
        /// Initializes a new instance of the MultiShardSchemaMismatchException class with the specified error message and the 
        /// reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        /// <param name="innerException"> specifies the exception encountered at the shard.</param>
        public MultiShardSchemaMismatchException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardSchemaMismatchException class with the specified error message.
        /// </summary>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        public MultiShardSchemaMismatchException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardSchemaMismatchException class.
        /// </summary>
        public MultiShardSchemaMismatchException()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardSchemaMismatchException class with serialized data.
        /// </summary>
        /// <param name="info">
        /// The <see cref="SerializationInfo"/> see that holds the serialized object data about the exception being thrown.
        /// </param>
        /// <param name="context">
        /// The <see cref="StreamingContext"/> that contains contextual information about the source or destination.
        /// </param>
        protected MultiShardSchemaMismatchException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        #endregion Standard Exception Constructors
    }
}
