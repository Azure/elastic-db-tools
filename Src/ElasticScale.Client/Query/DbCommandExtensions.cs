using System;
using System.Data.Common;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query
{
    public static class DbCommandExtensions {

        /// <summary>
        /// Make best attempt to clone DbCommand
        /// </summary>
        /// <param name="from"></param>
        /// <returns></returns>
        public static DbCommand BestAttemptClone(this DbCommand from) {
            if(from is ICloneable) {
                return (DbCommand)(from as ICloneable).Clone();
            }

            DbCommand newcmd = null;

            if(from is SqlCommand) {
                // do some special magic for SqlCommand
                newcmd = new SqlCommand(from.CommandText, (from as SqlCommand).Connection, (from as SqlCommand).Transaction);
            } else {
                var mi = from.GetType().GetMethod("Clone");
                if(mi != null && mi.ReturnType != null && typeof(DbCommand).IsAssignableFrom(mi.ReturnType) && (mi.GetParameters()?.Count() ?? 0) == 0) {
                    return (DbCommand)mi.Invoke(from, null);
                } else {
                    newcmd = (DbCommand)Activator.CreateInstance(from.GetType());
                    newcmd.CommandText = from.CommandText;
                }
            }
            newcmd.CommandTimeout = from.CommandTimeout;
            newcmd.CommandType = from.CommandType;
            newcmd.DesignTimeVisible = from.DesignTimeVisible;
            newcmd.UpdatedRowSource = from.UpdatedRowSource;
            if(from.Parameters.Count > 0) {
                DbParameter[] p = new DbParameter[from.Parameters.Count];
                from.Parameters.CopyTo(p, 0);
                foreach(object current in p) {
                    newcmd.Parameters.Add(current);
                }
            }
            return newcmd;
        }

    }
}
