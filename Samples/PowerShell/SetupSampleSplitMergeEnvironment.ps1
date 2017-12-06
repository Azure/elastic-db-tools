<#
/********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    Creates a sample environment (shard map manager and shards) that
    can be used with the Split-Merge service.

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 8/22/2014

.EXAMPLES
    .\SetupSampleSplitMergeEnvironment.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Int32'

    .\SetupSampleSplitMergeEnvironment.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Int64'

    .\SetupSampleSplitMergeEnvironment.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Guid' `
        -SplitRangeLow '00000000-0000-0000-0000-000000000000' `
        -SplitValue '10000000-0000-0000-0000-000000000000' `
        -SplitRangeHigh '20000000-0000-0000-0000-000000000000'

    .\SetupSampleSplitMergeEnvironment.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Binary' `
        -SplitRangeLow '0x00' `
        -SplitValue '0x64' `
        -SplitRangeHigh '0xc8'

    .\SetupSampleSplitMergeEnvironment.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Datetime' `
        -SplitRangeLow '2010-3-21 12:00:00' `
        -SplitValue '2015-7-1 12:00:00' `
        -SplitRangeHigh '2018-9-24 12:00:00'
#>

param (
    [parameter(Mandatory=$true)][string]$UserName,
    [parameter(Mandatory=$true)][string]$Password,
    [parameter(Mandatory=$true)][string]$ShardMapManagerServerName,

    [string]$ShardMapManagerDatabaseName = 'SplitMergeShardManagement',
    [string]$ShardServerName1 = $ShardMapManagerServerName,
    [string]$ShardDatabaseName1 = 'ShardDb1',
    [string]$ShardServerName2 = $ShardMapManagerServerName,
    [string]$ShardDatabaseName2 = 'ShardDb2',

    $ShardMapName = 'MyTestShardMap',

    $ShardKeyType = 'Int32', # Other accepted values are 'Int64', 'Guid', 'Binary', or 'Datetime'

    # Below values must be convertible to KeyType
    $SplitRangeLow = 0,
    $SplitValue = 100,
    $SplitRangeHigh = 200,

    $ShardedTableName = 'MyShardedTable',
    $ShardedTableKeyColumnName = 'MyKeyColumn',
    $ReferenceTableName = 'MyReferenceTable',
    $ReferenceTableDataColumnName = 'MyDataColumn',

    $SqlDatabaseEdition = 'STANDARD'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Import modules
$ScriptDir = Split-Path -parent $MyInvocation.MyCommand.Path
Import-Module $ScriptDir\SqlDatabaseHelpers -Force
Import-Module $ScriptDir\ShardManagement -Force

# Create databases, if they don't already exist
$jobs = @()
foreach ($DbName in $ShardDatabaseName1, $ShardDatabaseName2, $ShardMapManagerDatabaseName)
{
    if (!$(Test-SqlDatabase -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $DbName))
    {
        $jobs += Start-Job `
            -ScriptBlock {
                param (
                    $ScriptDir,
                    $UserName,
                    $Password,
                    $ShardMapManagerServerName,
                    $DbName,
                    $SqlDatabaseEdition
                )
                Import-Module $ScriptDir\SqlDatabaseHelpers -Force
                New-SqlDatabase -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $DbName -Edition $SqlDatabaseEdition } `
            -ArgumentList @($ScriptDir, $UserName, $Password, $ShardMapManagerServerName, $DbName, $SqlDatabaseEdition)
    }
}

if ($jobs.Count -gt 0)
{
    Receive-Job $jobs -Wait
    Remove-Job $jobs
}

# Create new (or replace existing) shard map manager
$ShardMapManager = New-ShardMapManager -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $ShardMapManagerDatabaseName -ReplaceExisting $true

# Parse key type string
switch ($ShardKeyType)
{
    'Int32'    { $TKey = $([int]);    $SqlKeyType = 'INT';              break }
    'Int64'    { $TKey = $([long]);   $SqlKeyType = 'BIGINT';           break }
    'Guid'     { $TKey = $([Guid]);   $SqlKeyType = 'UNIQUEIDENTIFIER'; break }
    'Binary'   { $Tkey = $([byte[]]); $SqlKeyType = 'BINARY';           break }
    'Datetime' { $Tkey = $([datetime]); $SqlKeyType = 'DATETIME';     break }
    default    { throw "Invalid ShardKeyType $ShardKeyType. Accepted values are Int32, Int64, Guid, and Binary" }
}

# Converts a hex string like 0x1234 to a byte[]
function HexStringToByteArray
{
    param(
        [string]$hexString
    )

    if (-not $hexString.StartsWith("0x", [StringComparison]::OrdinalIgnoreCase) -or
        $hexString.Length % 2 -ne 0)
    {
        throw "Invalid hex string $hexString"
    }

    # Get rid of the leading '0x'
    $hexString = $hexString.Remove(0, 2);
    $numBytes = $hexString.Length / 2;
    $hexBytes = New-Object byte[] $numBytes;

    for ($i = 0; $i -lt $numBytes; $i++){
        $hexBytes[$i] = [System.Byte]::Parse($hexString.Substring($i * 2, 2), [System.Globalization.NumberStyles]::HexNumber, [CultureInfo]::InvariantCulture);
    }

    return $hexBytes;
}

if ($ShardKeyType -eq 'BINARY')
{
    # For binary keys specified in hex format; convert into byte array.
    $LowKey = HexStringToByteArray $SplitRangeLow;
    if ($SplitRangeHigh)
    {
        $HighKey = HexStringToByteArray $SplitRangeHigh;
    }
    else
    {
        $HighKey = $SplitRangeHigh;
    }
}
else
{
    $LowKey = $SplitRangeLow;
    $HighKey = $SplitRangeHigh;
}

# Create shard map
$ShardMap = New-RangeShardMap -KeyType $TKey -ShardMapManager $ShardMapManager -RangeShardMapName $ShardMapName

# Add shards
foreach ($DbName in $ShardDatabaseName1, $ShardDatabaseName2)
{
    Add-Shard -ShardMap $ShardMap -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $DbName
}

# Create mapping on the first shard
Add-RangeMapping -KeyType $TKey -RangeShardMap $ShardMap -RangeLow $LowKey -RangeHigh $HighKey -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $ShardDatabaseName1

# Create SchemaInfo for our tables
$SchemaInfo = New-Object Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema.SchemaInfo
$SchemaInfo.Add($(New-Object Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema.ShardedTableInfo($ShardedTableName, $ShardedTableKeyColumnName)))
$SchemaInfo.Add($(New-Object Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema.ReferenceTableInfo($ReferenceTableName)))
$SchemaInfoCollection = $ShardMapManager.GetSchemaInfoCollection()

# Add the SchemaInfo for this Shard Map to the Schema Info Collection
if ($($SchemaInfoCollection | Where Key -eq $ShardMapName) -eq $null)
{
    $SchemaInfoCollection.Add($ShardMapName, $SchemaInfo)
}
else
{
    $SchemaInfoCollection.Replace($ShardMapName, $SchemaInfo)
}

# Create table in both shards
foreach ($DbName in $ShardDatabaseName1, $ShardDatabaseName2)
{
    $CreateTablesQuery = @"
        IF (OBJECT_ID('$ShardedTableName') IS NOT NULL)
            DROP TABLE [$ShardedTableName]
        CREATE TABLE $ShardedTableName ($ShardedTableKeyColumnName $SqlKeyType PRIMARY KEY)

        IF (OBJECT_ID('$ReferenceTableName') IS NOT NULL)
            DROP TABLE [$ReferenceTableName]
        CREATE TABLE $ReferenceTableName ($ReferenceTableDataColumnName $SqlKeyType PRIMARY KEY)
"@
    Invoke-SqlScalar -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $DbName -DbQuery $CreateTablesQuery
}

# Popluate tables with sample data
if ($ShardKeyType -eq 'Int32' -or $ShardKeyType -eq 'Int64')
{
    $SampleKeys = ($LowKey .. $($HighKey - 1))
}
elseif ($ShardKeyType -eq 'Binary')
{
    $SampleKeys = @("$SplitRangeLow", "$SplitValue", "$SplitRangeHigh")
}
else
{
    $SampleKeys = @("'$SplitRangeLow'", "'$SplitValue'", "'$SplitRangeHigh'")
}

foreach ($i in $SampleKeys)
{
    # Populate sharded table in the first shard
    $InsertIntoShardedTableQuery = "INSERT INTO $ShardedTableName ($ShardedTableKeyColumnName) VALUES ($i)"
    Invoke-SqlScalar -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $ShardDatabaseName1 -DbQuery $InsertIntoShardedTableQuery

    # Populate reference table in the first shard
    # The Split-Merge service can copy reference tables, but only if they are empty in the target.
    $InsertIntoReferenceTableQuery = "INSERT INTO $ReferenceTableName ($ReferenceTableDataColumnName) VALUES ($i)"
    Invoke-SqlScalar -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $ShardDatabaseName1 -DbQuery $InsertIntoReferenceTableQuery
}

Write-Host
Write-Host -ForegroundColor Cyan "Sample Split-Merge Environment has been created."
Write-Host -ForegroundColor Cyan "  ShardMapManager Server: $ShardMapManagerServerName Database: $ShardMapManagerDatabaseName "
Write-Host -ForegroundColor Cyan "  ShardMapName: $ShardMapName ShardKeyType: $ShardKeyType"
Write-Host
Write-Host -ForegroundColor Cyan "To view the current shard mappings, execute:"
Write-Host -ForegroundColor Cyan "  .\GetMappings.ps1 -UserName $UserName -Password $Password -ShardMapManagerServerName $ShardMapManagerServerName -ShardMapManagerDatabaseName $ShardMapManagerDatabaseName -ShardMapName $ShardMapName"
Write-Host
