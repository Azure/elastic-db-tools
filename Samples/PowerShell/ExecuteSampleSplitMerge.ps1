<#
/********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    Sends sample split and merge requests to the Split-Merge service
    and writes the request status to the console output.

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 8/22/2014

.EXAMPLES
    .\ExecuteSampleSplitMerge.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Int32' `
        -SplitMergeServiceEndpoint 'https://mysplitmergeservice.cloudapp.net' `
        -CertificateThumbprint '0123456789abcdef0123456789abcdef01234567'

    .\ExecuteSampleSplitMerge.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Int64' `
        -SplitMergeServiceEndpoint 'https://mysplitmergeservice.cloudapp.net' `
        -CertificateThumbprint '0123456789abcdef0123456789abcdef01234567'

    .\ExecuteSampleSplitMerge.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Guid' `
        -SplitRangeLow '00000000-0000-0000-0000-000000000000' `
        -SplitValue '10000000-0000-0000-0000-000000000000' `
        -SplitRangeHigh '20000000-0000-0000-0000-000000000000' `
        -SplitMergeServiceEndpoint 'https://mysplitmergeservice.cloudapp.net' `
        -CertificateThumbprint '0123456789abcdef0123456789abcdef01234567'

    .\ExecuteSampleSplitMerge.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Binary' `
        -SplitRangeLow '0x00' `
        -SplitValue '0x64' `
        -SplitRangeHigh '0xc8' `
        -SplitMergeServiceEndpoint 'https://mysplitmergeservice.cloudapp.net' `
        -CertificateThumbprint '0123456789abcdef0123456789abcdef01234567'

    .\ExecuteSampleSplitMerge.ps1 `
        -UserName 'mysqluser' `
        -Password 'MySqlPassw0rd' `
        -ShardMapManagerServerName 'abcdefghij.database.windows.net' `
        -ShardKeyType 'Datetime' `
        -SplitRangeLow '2010-3-21 12:00:00' `
        -SplitValue '2015-7-1 12:00:00' `
        -SplitRangeHigh '2018-9-24 12:00:00' `
        -SplitMergeServiceEndpoint 'https://mysplitmergeservice.cloudapp.net' `
        -CertificateThumbprint '0123456789abcdef0123456789abcdef01234567'
#>

[CmdletBinding()]
param (
    [parameter(Mandatory=$true)][string]$UserName,
    [parameter(Mandatory=$true)][string]$Password,
    [parameter(Mandatory=$true)][string]$ShardMapManagerServerName,
    [parameter(Mandatory=$true)][string]$SplitMergeServiceEndpoint,

    [string]$ShardMapManagerDatabaseName = 'SplitMergeShardManagement',
    [string]$ShardServerName1 = $ShardMapManagerServerName,
    [string]$ShardDatabaseName1 = 'ShardDb1',
    [string]$ShardServerName2 = $ShardMapManagerServerName,
    [string]$ShardDatabaseName2 = 'ShardDb2',

    [string]$ShardMapName = "MyTestShardMap",

    $ShardKeyType = 'Int32', # Other accepted values are 'Int64', 'Guid', 'Binary', or 'Datetime'

    # Below values must be convertible to ShardKeyType
    $SplitRangeLow = 0,
    $SplitValue = 100,
    $SplitRangeHigh = 200,

    # The thumbprint of the client certificate to be used for authentication.
    # Do not specify if client certificate authentication is not enabled in the service.
    [string]$CertificateThumbprint = $null
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Import SplitMerge module
$ScriptDir = Split-Path -parent $MyInvocation.MyCommand.Path
Import-Module $ScriptDir\SplitMerge -Force

# Send split request - split the high end of the range to the second shard
Write-Output 'Sending split request'
$splitOperationId = Submit-SplitRequest `
    -SplitMergeServiceEndpoint $SplitMergeServiceEndpoint `
    -ShardMapManagerServerName $ShardMapManagerServerName `
    -ShardMapManagerDatabaseName $ShardMapManagerDatabaseName `
    -TargetServerName $ShardServerName2 `
    -TargetDatabaseName $ShardDatabaseName2 `
    -UserName $UserName `
    -Password $Password `
    -ShardMapName $ShardMapName `
    -ShardKeyType $ShardKeyType `
    -SplitRangeLowKey $SplitRangeLow `
    -SplitValue $SplitValue `
    -SplitRangeHighKey $SplitRangeHigh `
    -CertificateThumbprint $CertificateThumbprint

Write-Output "Began split operation with id $splitOperationId"

# Get split request output
Wait-SplitMergeRequest -SplitMergeServiceEndpoint $SplitMergeServiceEndpoint -OperationId $splitOperationId -CertificateThumbprint $CertificateThumbprint

# Send merge request - merge the high end of the range back where the low end of the range is (i.e. the first shard)
Write-Output 'Sending merge request'
$mergeOperationId = Submit-MergeRequest `
    -SplitMergeServiceEndpoint $SplitMergeServiceEndpoint `
    -ShardMapManagerServerName $ShardMapManagerServerName `
    -ShardMapManagerDatabaseName $ShardMapManagerDatabaseName `
    -UserName $UserName `
    -Password $Password `
    -ShardMapName $ShardMapName `
    -ShardKeyType $ShardKeyType `
    -SourceRangeLowKey $SplitValue `
    -SourceRangeHighKey $SplitRangeHigh `
    -TargetRangeLowKey $SplitRangeLow `
    -TargetRangeHighKey $SplitValue `
    -CertificateThumbprint $CertificateThumbprint

Write-Output "Began merge operation with id $mergeOperationId"

# Get merge request output
Wait-SplitMergeRequest -SplitMergeServiceEndpoint $SplitMergeServiceEndpoint -OperationId $mergeOperationId -CertificateThumbprint $CertificateThumbprint

