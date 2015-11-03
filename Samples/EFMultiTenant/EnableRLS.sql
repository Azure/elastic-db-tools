-- Copyright (c) Microsoft. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

-- Enable RLS
CREATE SCHEMA rls -- separate schema to organize RLS objects
GO

CREATE FUNCTION rls.fn_tenantAccessPredicate(@TenantId int)
    RETURNS TABLE
    WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS fn_accessResult 
        WHERE DATABASE_PRINCIPAL_ID() = DATABASE_PRINCIPAL_ID('dbo') -- the user in your application’s connection string (dbo is only for demo purposes!)
		AND CAST(SESSION_CONTEXT(N'TenantId') AS int) = @TenantId
GO

CREATE SECURITY POLICY rls.tenantAccessPolicy
    ADD FILTER PREDICATE rls.fn_tenantAccessPredicate(TenantId) ON dbo.Blogs,
	ADD BLOCK PREDICATE rls.fn_tenantAccessPredicate(TenantId) ON dbo.Blogs,
    ADD FILTER PREDICATE rls.fn_tenantAccessPredicate(TenantId) ON dbo.Posts,
	ADD BLOCK PREDICATE rls.fn_tenantAccessPredicate(TenantId) ON dbo.Posts
GO

-- Default constraint example
-- Note: You should remove TenantId from the Entity Framework data model (DataClasses.cs)
-- before creating default constraints to prevent EF from automatically supplying default values
--ALTER TABLE Blogs
--    ADD CONSTRAINT df_TenantId_Blogs 
--	DEFAULT CAST(SESSION_CONTEXT(N'TenantId') AS int) FOR TenantId
--GO
--ALTER TABLE Posts
--    ADD CONSTRAINT df_TenantId_Posts 
--	DEFAULT CAST(SESSION_CONTEXT(N'TenantId') AS int) FOR TenantId
--GO

-- Example of altering the security policy to allow a "superuser" to access all rows
-- Note: You should create a new function with the new logic, and then "swap" it out with 
-- the existing predicates on the Blogs and Posts tables
--CREATE FUNCTION rls.fn_tenantAccessPredicateWithSuperUser(@TenantId int)
--    RETURNS TABLE
--    WITH SCHEMABINDING
--AS
--    RETURN SELECT 1 AS fn_accessResult 
--        WHERE 
--		(
--			DATABASE_PRINCIPAL_ID() = DATABASE_PRINCIPAL_ID('dbo') -- note, should not be dbo!
--			AND CAST(SESSION_CONTEXT(N'TenantId') AS int) = @TenantId
--		) 
--		OR
--		(
--			DATABASE_PRINCIPAL_ID() = DATABASE_PRINCIPAL_ID('superuser')
--		)
--GO

--ALTER SECURITY POLICY rls.tenantAccessPolicy
--	ALTER FILTER PREDICATE rls.fn_tenantAccessPredicateWithSuperUser(TenantId) ON dbo.Blogs,
--  ALTER BLOCK PREDICATE rls.fn_tenantAccessPredicateWithSuperUser(TenantId) ON dbo.Blogs,
--	ALTER FILTER PREDICATE rls.fn_tenantAccessPredicateWithSuperUser(TenantId) ON dbo.Posts,
--	ALTER BLOCK PREDICATE rls.fn_tenantAccessPredicateWithSuperUser(TenantId) ON dbo.Posts
--GO