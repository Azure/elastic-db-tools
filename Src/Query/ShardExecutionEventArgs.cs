﻿using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    /// <summary>
    /// Input to be passed to per-shard event handlers.
    /// </summary>
    public class ShardExecutionEventArgs : EventArgs
    {
        /// <summary>
        /// The exception to process, if applicable. Null if no exception was thrown.
        /// </summary>
        public Exception Exception
        {
            get;
            internal set;
        }

        /// <summary>
        /// The location of the shard on which the MultiShardCommand is currently executing.
        /// </summary>
        public ShardLocation ShardLocation
        {
            get;
            internal set;
        }

        /// <summary>
        /// FOR INTERNAL USE ONLY:
        /// The returned input reader.
        /// </summary>
        internal LabeledDbDataReader Reader
        {
            get;
            set;
        }
    }
}
