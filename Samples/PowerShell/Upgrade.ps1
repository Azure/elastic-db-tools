<#
/*********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    Upgrades a Shard Map Manager and all its shards to the current version.

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 9/5/2014
#>
param(
    [Parameter(Mandatory = $true)]
    [string]
    $ShardMapManagerServerName="", 
    [Parameter(Mandatory = $true)]
    [string]
    $ShardMapManagerDatabaseName="", 
    [Parameter(Mandatory = $true)]
    [string]
    $UserName="", 
    [Parameter(Mandatory = $true)]
    [string]
    $Password=""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Import modules
$ScriptDir = Split-Path -parent $MyInvocation.MyCommand.Path
Import-Module $ScriptDir\ShardManagement -Force

# Get Shard Map Manager 

$SmmConnectionString = New-Object System.Data.SqlClient.SqlConnectionStringBuilder("Server=$ShardMapManagerServerName; Initial Catalog=$ShardMapManagerDatabaseName; User ID=$UserName; Password=$Password;")
$LoadPolicy = [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardMapManagerLoadPolicy]::Lazy

$smm = [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardMapManagerFactory]::GetSqlShardMapManager($SmmConnectionString, $LoadPolicy)

# Upgrade GSM
Write-Host "Upgrading GSM"
$smm.UpgradeGlobalStore();

# Get distinct locations in this shard map manager
$Locations = $smm.GetDistinctShardLocations();

# Upgrade all shards
foreach ($Location in $Locations)
{
    Write-Host "Upgrading " $Location.ToString()
    $smm.UpgradeLocalStore($Location);
}
