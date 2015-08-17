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
    /// Represents information about a single reference table.
    /// </summary>
    [Serializable()]
    [DataContract(Name = "ReferenceTableInfo", Namespace = "")]
    public class ReferenceTableInfo : TableInfo, IEquatable<ReferenceTableInfo>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReferenceTableInfo"/> class.
        /// </summary>
        /// <param name="tableName">Reference table name.</param>
        public ReferenceTableInfo(string tableName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");

            this.SchemaName = "dbo";
            this.TableName = tableName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReferenceTableInfo"/> class.
        /// </summary>
        /// <param name="schemaName">Schema name of the reference table.</param>
        /// <param name="tableName">Reference table name.</param>
        public ReferenceTableInfo(string schemaName, string tableName)
        {
            ExceptionUtils.DisallowNullOrEmptyStringArgument(schemaName, "schemaName");
            ExceptionUtils.DisallowNullOrEmptyStringArgument(tableName, "tableName");

            this.SchemaName = schemaName;
            this.TableName = tableName;
        }

        /// <summary>
        /// Determines whether the specified ReferenceTableInfo object is equal to the current object.
        /// </summary>
        /// <param name="other">The ReferenceTableInfo object to compare with the current object.</param>
        /// <returns>true if the specified ReferenceTableInfo object is equal to the current object; otherwise, false.</returns>
        public bool Equals(ReferenceTableInfo other)
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
                this.TableName.Equals(other.TableName);
        }

        /// <summary>
        /// Overrides the Equals() method of Object class. Determines whether the specified object 
        /// is equal to the current object.
        /// </summary>
        /// <param name="obj">The object to compare with the current ReferenceTableInfo object.</param>
        /// <returns>true if the specified object is equal to the current ReferenceTableInfo object; otherwise, false.</returns>
        public override bool Equals(Object obj)
        {
            ReferenceTableInfo refTableInfo = obj as ReferenceTableInfo;
            return refTableInfo != null && this.Equals(refTableInfo);
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
