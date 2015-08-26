-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

-- Remove RLS and check constraints
IF ( EXISTS(SELECT * FROM sys.security_policies WHERE name = 'tenantAccessPolicy') )
	DROP SECURITY POLICY rls.tenantAccessPolicy
go

IF ( EXISTS(SELECT * FROM sys.objects WHERE name = 'fn_tenantAccessPredicate') )
	DROP FUNCTION rls.fn_tenantAccessPredicate
go

IF ( EXISTS(SELECT * FROM sys.check_constraints WHERE name = 'chk_blocking_Blogs') )
	ALTER TABLE Blogs DROP CONSTRAINT chk_blocking_Blogs
go

IF ( EXISTS(SELECT * FROM sys.check_constraints WHERE name = 'chk_blocking_Posts') )
	ALTER TABLE Posts DROP CONSTRAINT chk_blocking_Posts
go

IF ( EXISTS(SELECT * FROM sys.objects WHERE name = 'fn_tenantAccessPredicateScalar') )
	DROP FUNCTION rls.fn_tenantAccessPredicateScalar
go

IF ( EXISTS(SELECT * FROM sys.schemas WHERE name = 'rls') )
	DROP SCHEMA rls
go
