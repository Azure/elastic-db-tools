// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Public type to communicate failures when performing operations against a shard

using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Globalization;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the spelling we want here.
    //
    /// DEVNOTE: Encapsulate SMM ShardLocation type for now since Shard isn't Serializable
    /// Support for serialization of ShardLocation is in the works.
    /// <summary>
    /// A MultiShardException represents an exception that occured when performing operations against a shard.
    /// It provides information about both the identity of the shard and the expection that occurred.
    /// Depending on the nature of the exception, one can try re-running the multi-shard query, 
    /// execute a separate query targeted directly at the shard(s) on that yielded the expection, 
    /// or lastly execute the query manually against the shard using a common tool such as SSMS.  
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"), Serializable]
    public class MultiShardException : Exception
    {
        private readonly ShardLocation _shardLocation;

        #region Custom Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardException"/> class with 
        /// the specified shard location.
        /// </summary>
        /// <param name="shardLocation"> specifies the location of the shard where the exception occurred.</param>
        public MultiShardException(ShardLocation shardLocation)
            : this(shardLocation, string.Format("Exception encountered on shard: {0}", shardLocation))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardException"/> class with
        /// the specified shard location and error message.
        /// </summary>
        /// <param name="shardLocation"> specifies the location of the shard where the exception occurred.</param>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        public MultiShardException(ShardLocation shardLocation, string message)
            : this(shardLocation, message, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardException"/> class with 
        /// the specified shard location and exception.
        /// </summary>
        /// <param name="shardLocation"> specifies the location of the shard where the exception occurred.</param>
        /// <param name="inner"> specifies the exception encountered at the shard.</param>
        public MultiShardException(ShardLocation shardLocation, Exception inner)
            : this(shardLocation, string.Format("Exception encountered on shard: {0}", shardLocation), inner)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardException"/> class with
        /// the specified shard location, error message and exception encountered.
        /// </summary>
        /// <param name="shardLocation"> specifies the location of the shard where the exception occurred.</param>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        /// <param name="inner"> specifies the exception encountered at the shard.</param>
        /// <exception cref="T:System.ArgumentNullException">The <paramref name="shardLocation"/> is null </exception>
        public MultiShardException(ShardLocation shardLocation, string message, Exception inner)
            : base(message, inner)
        {
            if (null == shardLocation)
            {
                throw new ArgumentNullException("shardLocation");
            }

            _shardLocation = shardLocation;
        }

        #endregion Custom Constructors

        #region Standard Exception Constructors

        /// <summary>
        /// Initializes a new instance of the MultiShardException class with the specified error message and the 
        /// reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="message"> specifices the message that explains the reason for the exception.</param>
        /// <param name="innerException"> specifies the exception encountered at the shard.</param>
        public MultiShardException(string message, Exception innerException)
            : this(DummyShardLocation(), message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardException class with the specified error message.
        /// </summary>
        /// <param name="message"> specifies the exception encountered at the shard.</param>
        public MultiShardException(string message)
            : this(DummyShardLocation(), message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardException class.
        /// </summary>
        public MultiShardException()
            : this(DummyShardLocation())
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardException class with serialized data.
        /// </summary>
        /// <param name="info">
        /// The <see cref="SerializationInfo"/> see that holds the serialized object data about the exception being thrown.
        /// </param>
        /// <param name="context">
        /// The <see cref="StreamingContext"/> that contains contextual information about the source or destination.
        /// </param>
        protected MultiShardException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            _shardLocation = (ShardLocation)(info.GetValue("ShardLocation", typeof(ShardLocation)));
        }

        #endregion Standard Exception Constructors

        #region Serialization Methods

        /// <summary>
        /// Populates the provided <see cref="SerializationInfo"/> parameter with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info"><see cref="SerializationInfo"/> object to populate with data.</param>
        /// <param name="context">The destination <see cref=" StreamingContext"/> object for this serialization.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("ShardLocation", _shardLocation);
        }

        #endregion Serialization Methods

        /// <summary>
        /// The shard associated with this exception
        /// </summary>
        public ShardLocation ShardLocation
        {
            get
            {
                return _shardLocation;
            }
        }

        /// <summary>
        /// Creates and returns a string representation of the current <see cref="MultiShardException"/>.
        /// </summary>
        /// <returns>String representation of the current exception.</returns>
        public override string ToString()
        {
            string text = base.ToString();
            return string.Format(CultureInfo.InvariantCulture,
                "MultiShardException encountered on shard: {0} {1} {2}", ShardLocation, Environment.NewLine, text);
        }

        private static ShardLocation DummyShardLocation()
        {
            return new ShardLocation("unknown", "unknown");
        }
    }
}
