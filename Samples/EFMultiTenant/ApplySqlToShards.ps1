# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

# Params you must supply
$server = ""
$shardmapmgr = ""
$username = ""
$password = ""
$sqlfile = "EnableRLS.sql"

# Get shards and execute sqlfile
$sqldir = Split-Path -Parent $MyInvocation.MyCommand.Path
$sqlpath = Join-Path -Path $sqldir -ChildPath $sqlfile
$query = "SELECT ServerName, DatabaseName FROM __ShardManagement.ShardsGlobal;"
$shards = Invoke-Sqlcmd -Query $query -ServerInstance $server -Database $shardmapmgr -Username $username -Password $password

foreach ($shard in $shards) {
    # Assume all shards have same username/password as shard map manager
    "Executing... " + $shard["DatabaseName"]
    Invoke-Sqlcmd -InputFile $sqlpath -ServerInstance $shard["ServerName"] -Database $shard["DatabaseName"] -Username $username -Password $password
}