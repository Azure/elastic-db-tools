// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using System;
using System.Diagnostics;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ClientTestCommon;

/// <summary>
/// Manage SQL Authentication login for testing.
/// </summary>
public class SqlAuthenticationLogin
{
    /// <summary>
    /// A counter to ensure each username is unique (to allow for concurrent unit test execution)
    /// </summary>
    private static int usernameUniquifier = 1;

    /// <summary>
    /// The connection string.
    /// </summary>
    private readonly string connectionString;

    /// <summary>
    /// The s_test password.
    /// </summary>
    private readonly string testPassword;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlAuthenticationLogin"/> class.
    /// </summary>
    /// <param name="connectionString">
    /// The connection string.
    /// </param>
    /// <param name="username">
    /// The username.
    /// </param>
    /// <param name="password">
    /// The password.
    /// </param>
    public SqlAuthenticationLogin(string connectionString, string username, string password)
    {
        this.connectionString = connectionString;
        UniquifiedUserName = username + "_" + usernameUniquifier++;
        testPassword = password;
    }

    /// <summary>
    /// The uniquified user name (to allow for concurrent unit test execution)
    /// </summary>
    public string UniquifiedUserName { get; }

    /// <summary>
    /// Create test login
    /// </summary>
    /// <returns>
    /// <see cref="bool"/> to indicate success.
    /// </returns>
    public bool Create()
    {
        try
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            conn.ChangeDatabase("master");
            using var cmd = new SqlCommand();
            cmd.Connection = conn;
            cmd.CommandText = $@"
if exists (select name from syslogins where name = '{UniquifiedUserName}')
begin
	drop login {UniquifiedUserName}
end
create login {UniquifiedUserName} with password = '{testPassword}'";
            _ = cmd.ExecuteNonQuery();

            cmd.CommandText = $"SP_ADDSRVROLEMEMBER  '{UniquifiedUserName}', 'sysadmin'";
            _ = cmd.ExecuteNonQuery();

            return true;
        }
        catch (Exception e)
        {
            Trace.WriteLine($"Exception caught in CreateTestLogin(): {e}");
        }

        return false;
    }

    /// <summary>
    /// Drop test login.
    /// </summary>
    public void Drop()
    {
        try
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            conn.ChangeDatabase("master");
            using var cmd = new SqlCommand();
            cmd.Connection = conn;
            cmd.CommandText = $@"
if exists (select name from syslogins where name = '{UniquifiedUserName}')
begin
	drop login {UniquifiedUserName}
end";
            _ = cmd.ExecuteNonQuery();
        }
        catch (Exception e)
        {
            Trace.WriteLine($"Exception caught in DropTestLogin(): {e}");
        }
    }
}
