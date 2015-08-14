// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.SqlDatabase.ElasticScale
{
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.Properties;
    using System;
    using System.Runtime.Serialization;

    internal partial class TransientFaultHandling
    {
        // Suppression rationale: We are intentionally keeping the class local to this package.
        //
        /// <summary>
        /// The special type of exception that provides managed exit from a retry loop. The user code can use this
        /// exception to notify the retry policy that no further retry attempts are required.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1064:ExceptionsShouldBePublic"),
         Obsolete("You should use cancellation tokens or other means of stopping the retry loop."), Serializable]
        internal sealed class RetryLimitExceededException : Exception
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="RetryLimitExceededException"/> class with a default error message.
            /// </summary>
            public RetryLimitExceededException()
                : this(Resources.RetryLimitExceeded)
            {
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="RetryLimitExceededException"/> class with a specified error message.
            /// </summary>
            /// <param name="message">The message that describes the error.</param>
            public RetryLimitExceededException(string message)
                : base(message)
            {
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="RetryLimitExceededException"/> class with a reference to the inner exception
            /// that is the cause of this exception.
            /// </summary>
            /// <param name="innerException">The exception that is the cause of the current exception.</param>
            public RetryLimitExceededException(Exception innerException)
                : base(innerException != null ? innerException.Message : Resources.RetryLimitExceeded, innerException)
            {
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="RetryLimitExceededException"/> class with a specified error message and inner exception.
            /// </summary>
            /// <param name="message">The message that describes the error.</param>
            /// <param name="innerException">The exception that is the cause of the current exception.</param>
            public RetryLimitExceededException(string message, Exception innerException)
                : base(message, innerException)
            {
            }
        }
    }
}