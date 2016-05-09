<#
/********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    This script creates performance counter catagory and adds
    specified user to 'Performance Monitor Users' local group.

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 5/6/2016

.EXAMPLES
    .\CreatePerformanceCounterCategory.ps1 `
        -UserName 'domain\username'
#>

param(
	[Parameter(Mandatory = $true)]
	[string]
	$UserName=""
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Import modules
$ScriptDir = Split-Path -parent $MyInvocation.MyCommand.Path
Import-Module $ScriptDir\ShardManagement -Force

if (([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "Creating performance counter category 'Elastic Database: Shard Management' ..."
    [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardMapManagerFactory]::CreatePerformanceCategoryAndCounters()
    Write-Host "Performance counter category 'Elastic Database: Shard Management' created successfully."
    Write-Host "Adding specified user to 'Performance Monitor Users' group ..."
    net localgroup "Performance Monitor Users" $UserName /ADD
    if($?) {
        Write-Host "User $UserName is now part of 'Performance Monitor Users' group. 'Performance Monitor Users' group membership is required for accessing performance counters."
    } else {
        Write-Host "Failed to add User $UserName to 'Performance Monitor Users' group, please add $UserName to this group to enable performance counters access."
    }
} else {
    Write-Host "Performance counter category creation needs Administrator privileges, please rerun script as Administrator."
}
