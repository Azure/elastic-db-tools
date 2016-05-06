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
$scriptPath = split-path -parent $MyInvocation.MyCommand.Path
$dllNameWithPath = $scriptPath + "\Microsoft.Azure.SqlDatabase.ElasticScale.Client.dll";

if (([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    if (Test-Path $dllNameWithPath) {
        [system.reflection.assembly]::LoadFile($dllNameWithPath)
        Write-Host "Creating performance counter category 'Elastic Database: Shard Management' ..."
        [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardMapManagerFactory]::CreatePerformanceCategoryAndCounters()
        Write-Host "Performance counter category 'Elastic Database: Shard Management' created successfully."
        Write-Host "Adding specified user to 'Performance Monitor Users' group ..."
        net localgroup "Performance Monitor Users" $UserName /ADD
        Write-Host "User" $UserName "is now part of 'Performance Monitor Users' group"
    } else {
            Write-Host "Please copy Microsoft.Azure.SqlDatabase.ElasticScale.Client.dll in this folder and rerun the script."
    }
} else {
    Write-Host "Performance counter catagory creation needs Administrator privileges, please rerun script as Administrator."
}
