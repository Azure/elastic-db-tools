// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Implementation of a store operation.
    /// </summary>
    internal class SqlLogEntry : IStoreLogEntry
    {
        /// <summary>
        /// Constructs an instance of IStoreLogEntry using parts of a row from SqlDataReader.
        /// Used for creating the store operation for Undo.
        /// </summary>
        /// <param name="reader">SqlDataReader whose row has operation information.</param>
        /// <param name="offset">Reader offset for column that begins operation information.</param>
        internal SqlLogEntry(SqlDataReader reader, int offset)
        {
            this.Id = reader.GetGuid(offset);
            this.OpCode = (StoreOperationCode)reader.GetInt32(offset + 1);
            this.Data = reader.GetSqlXml(offset + 2);
            this.UndoStartState = (StoreOperationState)reader.GetInt32(offset + 3);
            SqlGuid shardIdRemoves;
            shardIdRemoves = reader.GetSqlGuid(offset + 4);
            this.OriginalShardVersionRemoves = shardIdRemoves.IsNull ? default(Guid) : shardIdRemoves.Value;
            SqlGuid shardIdAdds;
            shardIdAdds = reader.GetSqlGuid(offset + 5);
            this.OriginalShardVersionAdds = shardIdAdds.IsNull ? default(Guid) : shardIdAdds.Value;
        }

        /// <summary>
        /// Identity of operation.
        /// </summary>
        public Guid Id
        {
            get;
            private set;
        }

        /// <summary>
        /// Operation code. Helps in deserialization during factory method.
        /// </summary>
        public StoreOperationCode OpCode
        {
            get;
            private set;
        }

        /// <summary>
        /// Serialized representation of the operation.
        /// </summary>
        public SqlXml Data
        {
            get;
            private set;
        }

        /// <summary>
        /// State from which Undo will start.
        /// </summary>
        public StoreOperationState UndoStartState
        {
            get;
            private set;
        }

        /// <summary>
        /// Original shard version for remove steps.
        /// </summary>
        public Guid OriginalShardVersionRemoves
        {
            get;
            private set;
        }

        /// <summary>
        /// Original shard version for add steps.
        /// </summary>
        public Guid OriginalShardVersionAdds
        {
            get;
            private set;
        }
    }
}
