<#
/********************************************************
*                                                        *
*   © Microsoft. All rights reserved.                    *
*                                                        *
*********************************************************/

.SYNOPSIS
    Gets the history of Split-Merge operations from a 
    Split-Merge service's status database. 

.NOTES
    Author: Microsoft SQL Elastic Scale team
    Last Updated: 11/4/2015   
#>

param (
    [Parameter(Mandatory)][string]$SplitMergeStatusDbConnectionString
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Import modules
$ScriptDir = Split-Path -parent $MyInvocation.MyCommand.Path
Import-Module $ScriptDir\ShardManagement -Force

$conn = New-Object -TypeName "System.Data.SqlClient.SqlConnection"
$conn.ConnectionString = $SplitMergeStatusDbConnectionString
try
{
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = @"
select 
create_time as CreateTime,
case OperationType
	when 1 then 'Move'
	when 2 then 'Split'
	when 3 then 'Merge'
end as OperationType,
State,
MovedShardletsLowKey as MovedLowKey,
MovedShardletsHighKey as MovedHighKey,
KeyType,
SourceDataSourceName as SrcServer,
SourceDatabaseName as SrcDb,
TargetDataSourceName as TargetServer,
TargetDatabaseName as TargetDb,
ShardingSchemaInfo as TablesMoved
from SplitMergeFsm
order by CreateTime
"@
    try
    {
        $reader = $cmd.ExecuteReader()
        $columns = $reader.GetSchemaTable().Rows
        while ($reader.Read())
        {
            # Create a PSObject and add properties to it from the reader's columns
            $splitMergeHistoryItem = New-Object -TypeName PSObject
            foreach ($column in $columns)
            {
                $splitMergeHistoryItem | Add-Member -MemberType NoteProperty -Name $column['ColumnName'] -Value $reader.GetValue($column['ColumnOrdinal'])
            }
            
            # Convert KeyType from an integer value to a human-readable enum value
            $splitMergeHistoryItem.KeyType = [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKeyType]$splitMergeHistoryItem.KeyType
            
            # Convert moved key values from binary format to human-readable value 
            $splitMergeHistoryItem.MovedLowKey = [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey]::FromRawValue($splitMergeHistoryItem.KeyType, $splitMergeHistoryItem.MovedLowKey)
            $splitMergeHistoryItem.MovedHighKey = [Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.ShardKey]::FromRawValue($splitMergeHistoryItem.KeyType, $splitMergeHistoryItem.MovedHighKey)

            # Get the sharded tables that were moved as two-part table names
            $tablesMovedXml = [xml]$splitMergeHistoryItem.TablesMoved
            $splitMergeHistoryItem.TablesMoved = Select-Xml -Xml $tablesMovedXml -Xpath '//ShardedTableInfo' | % { "$($_.Node.SchemaName).$($_.Node.TableName)" }

            # Write the result
            $splitMergeHistoryItem
        }
    }
    finally
    {
        if ($reader -ne $null) 
        {
            $reader.Dispose()
        }
    }
}
finally
{
    $conn.Dispose()
}