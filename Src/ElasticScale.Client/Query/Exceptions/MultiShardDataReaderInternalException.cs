// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Custom exception to throw when the MultiShardDataReader is in an invalid state.
// This error should not make it out to the user.
//
// Notes:

using System;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// <summary>
    /// Custom exception that is thrown when the <see cref="MultiShardDataReader"/> is in an invalid state.
    /// If you experience this exception repeatedly, please contact Microsoft Customer Support.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"), Serializable]
    public class MultiShardDataReaderInternalException : Exception
    {
        #region Standard Exception Constructors

        /// <summary>
        /// Initializes a new instance of the MultiShardDataReaderInternalException class.
        /// </summary>
        public MultiShardDataReaderInternalException()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardDataReaderInternalException class with a 
        /// specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public MultiShardDataReaderInternalException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardDataReaderInternalException class  
        /// with a message and an inner exception.
        /// </summary>
        /// <param name="message">The message to encapsulate in the exception.</param>
        /// <param name="innerException">The underlying exception that causes this exception.</param>
        public MultiShardDataReaderInternalException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardDataReaderInternalException class 
        /// with serialized data and context.
        /// </summary>
        /// <param name="info">
        /// The <see cref="SerializationInfo"/> holds the serialized object data about the exception being thrown.
        /// </param>
        /// <param name="context">
        /// The <see cref="StreamingContext"/> that contains contextual information about the source or destination.
        /// </param>
        protected MultiShardDataReaderInternalException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        #endregion Standard Exception Constructors
    }
}
