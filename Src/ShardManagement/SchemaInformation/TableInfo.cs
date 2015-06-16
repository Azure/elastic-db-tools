using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema
{
    /// <summary>
    /// Repesents a table in a database.
    /// </summary>
    [DataContract(Name = "TableInfo", Namespace = "")]
    public abstract class TableInfo
    {
        /// <summary>
        /// Table's schema name.
        /// </summary>
        [DataMember()]
        public string SchemaName { get; protected set; }

        /// <summary>
        /// Table name.
        /// </summary>
        [DataMember()]
        public string TableName { get; protected set; }
    }
}
