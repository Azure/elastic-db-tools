<#
/********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    For a given Sharding setup, this script outputs the list of Shards.

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 8/13/2015

.EXAMPLES
    .\GetShards.ps1 `
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

$ShardMap = $ShardMapManager.GetShardMap($ShardMapName)

# Get mappings
return Get-Shards -ShardMap $ShardMap | Format-List

