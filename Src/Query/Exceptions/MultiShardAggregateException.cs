// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//
// Purpose:
// Public type that communicates errors that occured across multiple shards

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    // Suppression rationale: "Multi" is the correct spelling.
    //
    /// <summary>
    /// Represents one or more <see cref="Exception"/> errors that occured
    /// when executing a query across a shard set. The InnerExceptions field collects 
    /// these exceptions and one can iterate through the InnerExceptions 
    /// for further inspection or processing.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi"), Serializable]
    public class MultiShardAggregateException : Exception
    {
        private readonly ReadOnlyCollection<Exception> _innerExceptions;

        #region Standard Exception Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardAggregateException"/> class
        /// </summary>
        public MultiShardAggregateException()
            : this("One or more errors occured across shards")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardAggregateException"/> class
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception</param>
        public MultiShardAggregateException(string message)
            : base(message)
        {
            _innerExceptions = new ReadOnlyCollection<Exception>(new Exception[0]);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardAggregateException"/> class
        /// </summary>
        /// <param name="innerException">The <see cref="Exception"/> that caused the current exception</param>
        public MultiShardAggregateException(Exception innerException)
            : this(new Exception[] { innerException })
        {
        }

        /// <summary>
        /// Initializes a new instance of the MultiShardAggregateException class with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data.</param>
        /// <param name="context">The contextual information about the source or destination.</param>
        protected MultiShardAggregateException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            _innerExceptions = (ReadOnlyCollection<Exception>)(info.GetValue("InnerExceptions", typeof(ReadOnlyCollection<Exception>)));
        }

        #endregion Standard Exception Constructors

        #region Additional Constructors

        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public MultiShardAggregateException(string message, Exception innerException)
            : this(message, new Exception[] { innerException })
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardAggregateException"/> class
        /// </summary>
        /// <param name="innerExceptions">A list of <see cref="Exception"/> that caused the current exception</param>
        public MultiShardAggregateException(IEnumerable<Exception> innerExceptions)
            : this("One or more errors occured across shards", innerExceptions)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiShardAggregateException"/> class
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception</param>
        /// <param name="innerExceptions">A list of <see cref="Exception"/> that caused the current exception</param>
        /// <exception cref="T:System.ArgumentNullException">The <paramref name="innerExceptions"/> is null </exception>
        public MultiShardAggregateException(string message, IEnumerable<Exception> innerExceptions)
            : base(message, innerExceptions != null ? innerExceptions.FirstOrDefault() : null)
        {
            if (null == innerExceptions)
            {
                throw new ArgumentNullException("innerExceptions");
            }

            // Put them in a readonly collection
            List<Exception> exceptions = new List<Exception>();
            foreach (var exception in innerExceptions)
            {
                exceptions.Add(exception);
            }

            _innerExceptions = new ReadOnlyCollection<Exception>(exceptions);
        }

        #endregion Additional Constructors

        #region Serialization Support

        /// <summary>
        /// Populates a SerializationInfo with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info">The SerializationInfo to populate with data.</param>
        /// <param name="context">The destination (see StreamingContext) for this serialization.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("InnerExceptions", _innerExceptions);
        }

        #endregion Serialization Support

        /// <summary>
        /// Gets a read-only collection of the <see cref="Exception"/> instances 
        /// that caused the current exception.
        /// </summary>
        public ReadOnlyCollection<Exception> InnerExceptions
        {
            get
            {
                return _innerExceptions;
            }
        }

        /// <summary>
        /// Provides a string representation of this exception
        /// including its inner exceptions.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string text = base.ToString();

            for (int i = 0; i < _innerExceptions.Count; i++)
            {
                text = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0}{1}---> (Inner Exception #{2}) {3}{4}{5}",
                    text, Environment.NewLine, i, _innerExceptions[i].ToString(), "<---", Environment.NewLine);
            }

            return text;
        }
    }
}
