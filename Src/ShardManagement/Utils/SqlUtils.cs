﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Resources;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility properties and methods used for managing scripts and errors.
    /// </summary>
    internal static class SqlUtils
    {
        /// <summary>
        /// Regular expression for go tokens.
        /// </summary>
        private static readonly Regex GoTokenRegularExpression = new Regex(
                @"^\s*go\s*$",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        /// <summary>
        /// Regular expression for comment lines.
        /// </summary>
        private static readonly Regex CommentLineRegularExpression = new Regex(
            @"^\s*--",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        /// <summary>
        /// Special version number representing first step in upgrade script.
        /// </summary>
        private const int MajorNumberForInitialUpgradeStep = 0;
        
        /// <summary>
        /// Special version number representing last step in upgrade script.
        /// <remarks>Keep this number in sync with upgrade t-sql scripts.</remarks>
        /// </summary>
        private const int MajorNumberForFinalUpgradeStep = 1000;

        /// <summary>
        /// structure to hold upgrade command batches along with the starting version to apply the upgrade step.
        /// </summary>
        internal struct UpgradeSteps
        {
            /// <summary>
            /// Major version to apply this upgrade step.
            /// </summary>
            public int InitialMajorVersion
            {
                get;
                private set;
            }

            /// <summary>
            /// Minor version to apply this upgrade step.
            /// </summary>
            public int InitialMinorVersion
            {
                get;
                private set;
            }

            /// <summary>
            /// Commands in this upgrade step batch. These will be executed only when store is at (this.InitialMajorVersion, this.InitialMinorVersion).
            /// </summary>
            public StringBuilder Commands
            {
                get;
                private set;
            }

            /// <summary>
            /// Construct upgrade steps.
            /// </summary>
            /// <param name="initialMajorVersion">Expected major version of store to run this upgrade step.</param>
            /// <param name="initialMinorVersion">Expected minor version of store to run this upgrade step.</param>
            /// <param name="commands">Commands to execute as part of this upgrade step.</param>
            public UpgradeSteps(int initialMajorVersion, int initialMinorVersion, StringBuilder commands)
                : this()
            {
                this.InitialMajorVersion = initialMajorVersion;
                this.InitialMinorVersion = initialMinorVersion;
                this.Commands = commands;
            }
        };
       
        /// <summary>
        /// Parsed representation of GSM existence check script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> checkIfExistsGlobalScript = SqlUtils.SplitScriptCommands(Scripts.CheckShardMapManagerGlobal);

        /// <summary>
        /// Parsed representation of GSM creation script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> createGlobalScript = SqlUtils.SplitScriptCommands(Scripts.CreateShardMapManagerGlobal);

        /// <summary>
        /// Parsed representation of GSM drop script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> dropGlobalScript = SqlUtils.SplitScriptCommands(Scripts.DropShardMapManagerGlobal);

        /// <summary>
        /// Parsed representation of GSM upgrade script.
        /// </summary>
        private static readonly IEnumerable<UpgradeSteps> upgradeGlobalScript = SqlUtils.ParseUpgradeScripts(parseLocal: false);

        /// <summary>
        /// Parsed representation of LSM existence check script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> checkIfExistsLocalScript = SqlUtils.SplitScriptCommands(Scripts.CheckShardMapManagerLocal);

        /// <summary>
        /// Parsed representation of LSM creation script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> createLocalScript = SqlUtils.SplitScriptCommands(Scripts.CreateShardMapManagerLocal);

        /// <summary>
        /// Parsed representation of LSM drop script.
        /// </summary>
        private static readonly IEnumerable<StringBuilder> dropLocalScript = SqlUtils.SplitScriptCommands(Scripts.DropShardMapManagerLocal);

        /// <summary>
        /// Parsed representation of LSM upgrade script.
        /// </summary>
        private static readonly IEnumerable<UpgradeSteps> upgradeLocalScript = SqlUtils.ParseUpgradeScripts(parseLocal: true);

        /// <summary>
        /// SQL transient fault detection strategy.
        /// </summary>
        private static TransientFaultHandling.SqlDatabaseTransientErrorDetectionStrategy sqlTransientErrorDetector = new TransientFaultHandling.SqlDatabaseTransientErrorDetectionStrategy();

        /// <summary>
        /// Transient failure detector function.
        /// </summary>
        private static Func<Exception, bool> transientErrorDetector = (e) =>
        {
            ShardManagementException smmException = null;
            StoreException storeException = null;
            SqlException sqlException = null;

            smmException = e as ShardManagementException;

            if (smmException != null)
            {
                storeException = smmException.InnerException as StoreException;
            }
            else
            {
                storeException = e as StoreException;
            }

            if (storeException != null)
            {
                sqlException = storeException.InnerException as SqlException;
            }
            else
            {
                sqlException = e as SqlException;
            }

            return sqlTransientErrorDetector.IsTransient(sqlException ?? e);
        };

        /// <summary>
        /// Transient failure detector function.
        /// </summary>
        internal static Func<Exception, bool> TransientErrorDetector
        {
            get
            {
                return SqlUtils.transientErrorDetector;
            }
        }

        /// <summary>
        /// Parsed representation of GSM existence check script.
        /// </summary>
        internal static IEnumerable<StringBuilder> CheckIfExistsGlobalScript
        {
            get
            {
                return SqlUtils.checkIfExistsGlobalScript;
            }
        }

        /// <summary>
        /// Parsed representation of GSM creation script.
        /// </summary>
        internal static IEnumerable<StringBuilder> CreateGlobalScript
        {
            get
            {
                return SqlUtils.createGlobalScript;
            }
        }

        /// <summary>
        /// Parsed representation of GSM drop script.
        /// </summary>
        internal static IEnumerable<StringBuilder> DropGlobalScript
        {
            get
            {
                return SqlUtils.dropGlobalScript;
            }
        }

        /// <summary>
        /// Parsed representation of GSM upgrade script.
        /// </summary>
        internal static IEnumerable<UpgradeSteps> UpgradeGlobalScript
        {
            get
            {
                return SqlUtils.upgradeGlobalScript;
            }
        }

        /// <summary>
        /// Parsed representation of LSM existence check script.
        /// </summary>
        internal static IEnumerable<StringBuilder> CheckIfExistsLocalScript
        {
            get
            {
                return SqlUtils.checkIfExistsLocalScript;
            }
        }

        /// <summary>
        /// Parsed representation of LSM creation script.
        /// </summary>
        internal static IEnumerable<StringBuilder> CreateLocalScript
        {
            get
            {
                return SqlUtils.createLocalScript;
            }
        }

        /// <summary>
        /// Parsed representation of LSM drop script.
        /// </summary>
        internal static IEnumerable<StringBuilder> DropLocalScript
        {
            get
            {
                return SqlUtils.dropLocalScript;
            }
        }

        /// <summary>
        /// Parsed representation of LSM upgrade script.
        /// </summary>
        internal static IEnumerable<UpgradeSteps> UpgradeLocalScript
        {
            get
            {
                return SqlUtils.upgradeLocalScript;
            }
        }

        /// <summary>
        /// Reads a varbinary column from the given reader.
        /// </summary>
        /// <param name="reader">Input reader.</param>
        /// <param name="colIndex">Index of the column.</param>
        /// <returns>Buffer representing the data value.</returns>
        internal static byte[] ReadSqlBytes(SqlDataReader reader, int colIndex)
        {
            Debug.Assert(reader != null);

            SqlBytes data = reader.GetSqlBytes(colIndex);

            if (data.IsNull)
            {
                return null;
            }
            else
            {
                byte[] buffer = new byte[data.Length];

                data.Read(0, buffer, 0, (int)data.Length);

                return buffer;
            }
        }

        /// <summary>
        /// Adds parameter to given command.
        /// </summary>
        /// <param name="cmd">Command to add parameter to.</param>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter type.</param>
        /// <param name="direction">Parameter direction.</param>
        /// <param name="size">Size of parameter, useful for variable length types only.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns>Parameter object this created.</returns>
        internal static SqlParameter AddCommandParameter(
            SqlCommand cmd,
            string parameterName,
            SqlDbType dbType,
            ParameterDirection direction,
            int size,
            object value)
        {
            SqlParameter p = new SqlParameter(parameterName, dbType)
            {
                Direction = direction,
                Value = value ?? DBNull.Value
            };

            if ((dbType == SqlDbType.NVarChar) || (dbType == SqlDbType.VarBinary))
            {
                p.Size = size;
            }

            cmd.Parameters.Add(p);

            return p;
        }

        /// <summary>
        /// Executes the code with SqlException handling.
        /// </summary>
        /// <param name="operation">Operation to execute.</param>
        internal static void WithSqlExceptionHandling(Action operation)
        {
            try
            {
                operation();
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors._Store_StoreException,
                    se);
            }
        }

        /// <summary>
        /// Executes the code asynchronously with SqlException handling.
        /// </summary>
        /// <param name="operation">Operation to execute.</param>
        /// <returns>Task to await sql exception handling completion</returns>
        internal static async Task WithSqlExceptionHandlingAsync(Func<Task> operation)
        {
            try
            {
                await operation();
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors._Store_StoreException,
                    se);
            }
        }

        /// <summary>
        /// Executes the code with SqlException handling.
        /// </summary>
        /// <typeparam name="TResult">Type of result.</typeparam>
        /// <param name="operation">Operation to execute.</param>
        /// <returns>Result of the operation.</returns>
        internal static TResult WithSqlExceptionHandling<TResult>(Func<TResult> operation)
        {
            try
            {
                return operation();
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors._Store_StoreException,
                    se);
            }
        }

        /// <summary>
        /// Asynchronously executes the code with SqlException handling.
        /// </summary>
        /// <typeparam name="TResult">Type of result.</typeparam>
        /// <param name="operation">Operation to execute.</param>
        /// <returns>Task encapsulating the result of the operation.</returns>
        internal async static Task<TResult> WithSqlExceptionHandlingAsync<TResult>(Func<Task<TResult>> operation)
        {
            try
            {
                return await operation();
            }
            catch (SqlException se)
            {
                throw new StoreException(
                    Errors._Store_StoreException,
                    se);
            }
        }

        /// <summary>
        /// Filters collection of upgrade steps based on the specified target version of store.
        /// </summary>
        /// <param name="commandList">Collection of upgrade steps.</param>
        /// <param name="targetVersion">Target version of store.</param>
        /// <param name="currentVersion">Current version of store.</param>
        /// <returns>Collection of string builder that represent batches of commands to upgrade store to specified target version.</returns>
        internal static IEnumerable<StringBuilder> FilterUpgradeCommands(IEnumerable<UpgradeSteps> commandList, Version targetVersion, Version currentVersion = null)
        {
            List<StringBuilder> list = new List<StringBuilder>();

            foreach (UpgradeSteps s in commandList)
            {
                // For every upgrade step, add it to the output list if its initial version satisfy one of the 3 criteria below:
                // 1. If it is part of initial upgrade step (from version 0.0 to 1.0) which acquires SCH-M lock on ShardMapManagerGlobal
                // 2. If initial version is greater than current store version and less than target version requested
                // 3. If it is part of final upgrade step which releases SCH-M lock on ShardMapManagerGlobal

                if ((s.InitialMajorVersion == MajorNumberForInitialUpgradeStep) ||
                    ((currentVersion == null || s.InitialMajorVersion > currentVersion.Major || (s.InitialMajorVersion == currentVersion.Major && s.InitialMinorVersion >= currentVersion.Minor)) &&
                     (s.InitialMajorVersion < targetVersion.Major  || (s.InitialMajorVersion == targetVersion.Major  && s.InitialMinorVersion < targetVersion.Minor))) ||
                    (s.InitialMajorVersion == MajorNumberForFinalUpgradeStep)
                    )
                {
                    list.Add(s.Commands);
                }
            }

            return list;
        }

        /// <summary>
        /// Splits the input script into batches of individual commands, the go token is
        /// considered the separation boundary. Also skips comment lines.
        /// </summary>
        /// <param name="script">Input script.</param>
        /// <returns>Collection of string builder that represent batches of commands.</returns>
        private static IEnumerable<StringBuilder> SplitScriptCommands(string script)
        {
            List<StringBuilder> batches = new List<StringBuilder>();

            using (StringReader sr = new StringReader(script))
            {
                StringBuilder current = new StringBuilder();
                string currentLine;

                while ((currentLine = sr.ReadLine()) != null)
                {
                    // Break at the go token boundary.
                    if (SqlUtils.GoTokenRegularExpression.IsMatch(currentLine))
                    {
                        batches.Add(current);
                        current = new StringBuilder();
                    }
                    else if (!SqlUtils.CommentLineRegularExpression.IsMatch(currentLine))
                    {
                        // Add the line to the batch if it is not a comment.
                        current.AppendLine(currentLine);
                    }
                }
            }

            return batches;
        }

        /// <summary>
        /// Split upgrade scripts into batches of upgrade steps, the go token is 
        /// considered as separation boundary of batches.
        /// </summary>
        /// <param name="parseLocal">Whether to parse ShardMapManagerLocal upgrade scripts, default = false</param>
        /// <returns></returns>
        private static IEnumerable<UpgradeSteps> ParseUpgradeScripts(bool parseLocal = false)
        {
            List<UpgradeSteps> upgradeSteps = new List<UpgradeSteps>();

            ResourceSet rs = Scripts.ResourceManager.GetResourceSet(System.Globalization.CultureInfo.CurrentCulture, true, true);

            string upgradeFileNameFilter = @"^UpgradeShardMapManagerGlobalFrom(\d*).(\d*)";

            if (parseLocal)
            {
                upgradeFileNameFilter = upgradeFileNameFilter.Replace("Global", "Local");
            }
            
            Regex fileNameRegEx = new Regex(
                upgradeFileNameFilter,
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

            // Filter upgrade scripts based on file name and order by initial Major.Minor version
            var upgradeScriptObjects = from r in rs.Cast<DictionaryEntry>()
                    let m = fileNameRegEx.Match(r.Key.ToString())
                    where 
                        m.Success
                    orderby new Version(Convert.ToInt32(m.Groups[1].Value), Convert.ToInt32(m.Groups[2].Value))
                    select new
                    {
                        Key = r.Key,
                        Value = r.Value,
                        initialMajorVersion = Convert.ToInt32(m.Groups[1].Value),
                        initialMinorVersion = Convert.ToInt32(m.Groups[2].Value)
                    };

            foreach (var entry in upgradeScriptObjects)
            {
                foreach (StringBuilder cmd in SplitScriptCommands(entry.Value.ToString()))
                {
                    upgradeSteps.Add(new UpgradeSteps(entry.initialMajorVersion, entry.initialMinorVersion, cmd));
                }
            }

            return upgradeSteps;
        }
    }
}
