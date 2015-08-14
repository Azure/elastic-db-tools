// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema
{
    /// <summary>
    /// Represents information about a single sharded table.
    /// </summary>
    [Serializable()]
    [DataContract(Name = "ShardedTableInfo", Namespace = "")]
    public class ShardedTableInfo : TableInfo, IEquatable<ShardedTableInfo>
    {
        /// <summary>
        /// Name of the shard key column.
        /// </summary>
        [DataMember()]
        public string KeyColumnName { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardedTableInfo"/> class.
        /// </summary>
        /// <param name="tableName">Sharded table name.</param>
        /// <param name="columnName">Shard key column name.</param>
        public ShardedTableInfo(string tableName, string columnName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");
            ExceptionUtils.DisallowNullOrEmptyStringArgument(columnName, "columnName");

            this.SchemaName = "dbo";
            this.TableName = tableName;
            this.KeyColumnName = columnName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ShardedTableInfo"/> class.
        /// </summary>
        /// <param name="schemaName">Schema name of the sharded table.</param>
        /// <param name="tableName">Sharded table name.</param>
        /// <param name="columnName">Shard key column name.</param>
        public ShardedTableInfo(string schemaName, string tableName, string columnName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(schemaName, "columnName");
            ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");
            ExceptionUtils.DisallowNullOrEmptyStringArgument(columnName, "columnName");

            this.SchemaName = schemaName;
            this.TableName = tableName;
            this.KeyColumnName = columnName;
        }

        /// <summary>
        /// Determines whether the specified ShardedTableInfo object is equal to the current object.
        /// </summary>
        /// <param name="other">The ShardedTableInfo object to compare with the current object.</param>
        /// <returns>true if the specified ShardedTableInfo object is equal to the current object; otherwise, false.</returns>
        public bool Equals(ShardedTableInfo other)
        {
            if (other == null)
            {
                return false;
            }

            if (Object.ReferenceEquals(this, other))
            {
                return true;
            }

            return this.SchemaName.Equals(other.SchemaName) &&
                this.TableName.Equals(other.TableName) &&
                this.KeyColumnName.Equals(other.KeyColumnName);
        }

        /// <summary>
        /// Overrides the Equals() method of Object class. Determines whether the specified object 
        /// is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current ShardedTableInfo object.</param>
        /// <returns>true if the specified object is equal to the current ShardedTableInfo object; otherwise, false.</returns>
        public override bool Equals(Object obj)
        {
            ShardedTableInfo shardedTableInfo = obj as ShardedTableInfo;
            return shardedTableInfo != null && this.Equals(shardedTableInfo);
        }

        /// <summary>
        /// Calculates the hash code for this instance.
        /// </summary>
        /// <returns>Hash code for the object.</returns>
        public override int GetHashCode()
        {
            return this.TableName.GetHashCode();
        }
    }
}
