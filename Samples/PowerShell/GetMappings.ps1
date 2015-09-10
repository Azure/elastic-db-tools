<#
/********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    For a given Sharding setup, this script outputs the Mapping 
    of the shards.

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 7/30/2015

.EXAMPLES
    .\GetMappings.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardMapManagerDatabaseName 'MyShardMapManagerDB'
        -ShardMapName 'CustomerIdShardMap'

#>

param (
    [parameter(Mandatory=$true)][string]$UserName,
    [parameter(Mandatory=$true)][string]$Password,
    [parameter(Mandatory=$true)][string]$ShardMapManagerServerName,
    [parameter(Mandatory=$true)][string]$ShardMapManagerDatabaseName,
    [parameter(Mandatory=$true)][string]$ShardMapName
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Import modules
$ScriptDir = Split-Path -parent $MyInvocation.MyCommand.Path
Import-Module $ScriptDir\ShardManagement -Force

# Create new (or replace existing) shard map manager 
$ShardMapManager = Get-ShardMapManager -UserName $UserName -Password $Password -SqlServerName $ShardMapManagerServerName -SqlDatabaseName $ShardMapManagerDatabaseName

$ShardMap = $ShardMapManager.GetShardMap($ShardMapName);

# Get shard map
If ($ShardMap.MapType.Equals([Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardMapType]::List))
{
    $ShardMap = Get-ListShardMap -KeyType $ShardMap.KeyType.ToString() -ShardMapManager $ShardMapManager -ListShardMapName $ShardMapName
}
ElseIf ($ShardMap.MapType.Equals([Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardMapType]::Range))
{
    $ShardMap = Get-RangeShardMap -KeyType $ShardMap.KeyType.ToString() -ShardMapManager $ShardMapManager -RangeShardMapName $ShardMapName
}
Else
{
    Write-Error "Invalid Shard Map Type"
}

# Get mappings
return Get-Mappings -ShardMap $ShardMap | Format-List

