-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

-- Remove RLS policies and functions
IF ( EXISTS(SELECT * FROM sys.security_policies WHERE name = 'tenantAccessPolicy') )
	DROP SECURITY POLICY rls.tenantAccessPolicy
go

IF ( EXISTS(SELECT * FROM sys.objects WHERE name = 'fn_tenantAccessPredicate') )
	DROP FUNCTION rls.fn_tenantAccessPredicate
go

IF ( EXISTS(SELECT * FROM sys.objects WHERE name = 'fn_tenantAccessPredicateWithSuperUser') )
	DROP FUNCTION rls.fn_tenantAccessPredicateWithSuperUser
go

IF ( EXISTS(SELECT * FROM sys.schemas WHERE name = 'rls') )
	DROP SCHEMA rls
go
