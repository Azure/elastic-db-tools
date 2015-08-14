// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlTypes;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Represents a store operation.
    /// </summary>
    internal interface IStoreLogEntry
    {
        /// <summary>
        /// Identity of operation.
        /// </summary>
        Guid Id
        {
            get;
        }

        /// <summary>
        /// Operation code. Helps in deserialization during factory method.
        /// </summary>
        StoreOperationCode OpCode
        {
            get;
        }

        /// <summary>
        /// Serialized representation of the operation.
        /// </summary>
        SqlXml Data
        {
            get;
        }

        /// <summary>
        /// State from which Undo will start.
        /// </summary>
        StoreOperationState UndoStartState
        {
            get;
        }

        /// <summary>
        /// Original shard version for remove steps.
        /// </summary>
        Guid OriginalShardVersionRemoves
        {
            get;
        }

        /// <summary>
        /// Original shard version for add steps.
        /// </summary>
        Guid OriginalShardVersionAdds
        {
            get;
        }
    }
}
