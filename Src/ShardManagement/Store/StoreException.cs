// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Globalization;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Representation of exceptions that occur during storage operations.
    /// </summary>
    [Serializable]
    public sealed class StoreException : Exception
    {
        /// <summary>
        /// Initializes a new instance with a specified error message. 
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors", Justification = "Clients of this library should not be able to instantiate this type.")]
        internal StoreException()
            : this("StoreException occured")
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified error message. 
        /// </summary>
        /// <param name="message">Error message.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors", Justification = "Clients of this library should not be able to instantiate this type.")]
        internal StoreException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified formatted error message. 
        /// </summary>
        /// <param name="format">The format message that describes the error</param>
        /// <param name="args">The arguments to the format string</param>
        internal StoreException(string format, params object[] args)
            : base(string.Format(CultureInfo.InvariantCulture, format, args))
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified error message and a reference to the inner exception 
        /// that is the cause of this exception.
        /// </summary>
        /// <param name="message">A message that describes the error</param>
        /// <param name="inner">The exception that is the cause of the current exception</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors", Justification = "Clients of this library should not be able to instantiate this type.")]
        internal StoreException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified formatted error message and a reference to the 
        /// inner exception that is the cause of this exception. 
        /// </summary>
        /// <param name="format">The format message that describes the error</param>
        /// <param name="inner">The exception that is the cause of the current exception</param>
        /// <param name="args">The arguments to the format string</param>
        internal StoreException(string format, Exception inner, params object[] args)
            : base(String.Format(CultureInfo.InvariantCulture, format, args), inner)
        {
        }

        /// <summary>
        /// Initializes a new instance with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data</param>
        /// <param name="context">The contextual information about the source or destination</param>
        private StoreException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
