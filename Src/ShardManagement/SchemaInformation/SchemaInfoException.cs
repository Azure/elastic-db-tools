// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema
{
    /// <summary>
    /// The exception that is thrown when an error occurs during operations related to schema info collection.
    /// </summary>
    [Serializable]
    public sealed class SchemaInfoException : Exception
    {
        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public SchemaInfoException()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified error message. 
        /// </summary>
        /// <param name="message">Error message.</param>
        public SchemaInfoException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified formatted error message. 
        /// </summary>
        /// <param name="format">The format message that describes the error.</param>
        /// <param name="args">The arguments to the format string.</param>
        public SchemaInfoException(string format, params object[] args)
            : base(string.Format(CultureInfo.InvariantCulture, format, args))
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified error message and a reference to the inner exception 
        /// that caused this exception.
        /// </summary>
        /// <param name="message">A message that describes the error.</param>
        /// <param name="inner">The exception that is the cause of the current exception.</param>
        public SchemaInfoException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance with a specified formatted error message. 
        /// </summary>
        /// <param name="code">Error code.</param>
        /// <param name="format">The format message that describes the error</param>
        /// <param name="args">The arguments to the format string</param>
        public SchemaInfoException(SchemaInfoErrorCode code, string format, params object[] args)
            : base(string.Format(CultureInfo.InvariantCulture, format, args))
        {
            this.ErrorCode = code;
        }

        /// <summary>
        /// Initializes a new instance with a specified error message and a reference to the inner exception 
        /// that is the cause of this exception.
        /// </summary>
        /// <param name="code">Error code.</param>
        /// <param name="message">A message that describes the error</param>
        /// <param name="inner">The exception that is the cause of the current exception</param>
        public SchemaInfoException(SchemaInfoErrorCode code, string message, Exception inner)
            : base(message, inner)
        {
            this.ErrorCode = code;
        }

        /// <summary>
        /// Initializes a new instance with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data</param>
        /// <param name="context">The contextual information about the source or destination</param>
        private SchemaInfoException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.ErrorCode = (SchemaInfoErrorCode)info.GetValue("ErrorCode", typeof(ShardManagementErrorCode));
        }

        /// <summary>
        /// Populates a SerializationInfo with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info">The SerializationInfo to populate with data.</param>
        /// <param name="context">The destination (see StreamingContext) for this serialization.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info != null)
            {
                info.AddValue("ErrorCode", ErrorCode);
                base.GetObjectData(info, context);
            }
        }

        /// <summary>
        /// Error code.
        /// </summary>
        public SchemaInfoErrorCode ErrorCode
        {
            get;
            private set;
        }
    }
}
