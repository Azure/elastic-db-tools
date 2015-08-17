// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Custom exception to throw when the MultiShardDataReader hits an exception
// during a Read() call to one of the underlying SqlDataReaders.  When that happens
// all we know is that we were not able to read all the results from that shard, so
// we need to notify the user somehow.
//
// Notes:

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// <summary>
    /// The <see cref="MultiShardDataReader"/> throws this exception when 
    /// an exception has been hit reading data from one of the underlying shards. 
    /// This indicates that not all rows have been successfully retrieved 
    /// from the targeted shard(s). Users can then take
    /// the steps necessary to decide whether to re-run the query, or whether 
    /// to continue working with the rows that have already been retrieved.
    /// </summary>
    /// <remarks>
    /// This exception is only thrown with the partial results policy.
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"), Serializable]
    public class MultiShardPartialReadException : MultiShardException
    {
        #region Custom Constructors

        internal MultiShardPartialReadException(ShardLocation shardLocation, string message, Exception inner)
            : base(shardLocation, message, inner)
        {
        }

        #endregion Custom Constructors

        #region Standard Exception Constructors

        /// <summary>
        /// Initializes a new instance of the MultiShardPartialReadException class with the specified error message and 
        /// reference to the inner exception causing the MultiShardPartialReadException.
        /// </summary>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        /// <param name="innerException"> specifies the exception encountered at the shard.</param>
        public MultiShardPartialReadException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardPartialReadException class with the specified error message.
        /// </summary>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        public MultiShardPartialReadException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardPartialReadException class.
        /// </summary>
        public MultiShardPartialReadException()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardPartialReadException class with serialized data.
        /// </summary>
        /// <param name="info">
        /// The <see cref="SerializationInfo"/> see that holds the serialized object data about the exception being thrown.
        /// </param>
        /// <param name="context">
        /// The <see cref="StreamingContext"/> that contains contextual information about the source or destination.
        /// </param>
        protected MultiShardPartialReadException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        #endregion Standard Exception Constructors
    }
}
