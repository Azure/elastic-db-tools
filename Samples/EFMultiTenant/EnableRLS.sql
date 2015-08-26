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
        AND CONVERT(int, CONVERT(varbinary(4), CONTEXT_INFO())) = @TenantId -- @TenantId (int) is 4 bytes
GO

CREATE SECURITY POLICY rls.tenantAccessPolicy
    ADD FILTER PREDICATE rls.fn_tenantAccessPredicate(TenantId) ON dbo.Blogs,
    ADD FILTER PREDICATE rls.fn_tenantAccessPredicate(TenantId) ON dbo.Posts
GO

-- Create a scalar version of the predicate function for use in check constraints
CREATE FUNCTION rls.fn_tenantAccessPredicateScalar(@TenantId int)
    RETURNS bit
AS
    BEGIN
    IF EXISTS( SELECT 1 FROM rls.fn_tenantAccessPredicate(@TenantId) )
        RETURN 1
    RETURN 0
END
GO

-- Add the function as a check constraint on all sharded tables
ALTER TABLE Blogs
	WITH NOCHECK -- don't check data already in table
	ADD CONSTRAINT chk_blocking_Blogs -- needs a unique name
	CHECK( rls.fn_tenantAccessPredicateScalar(TenantId) = 1 )
GO
ALTER TABLE Posts
	WITH NOCHECK
	ADD CONSTRAINT chk_blocking_Posts
	CHECK( rls.fn_tenantAccessPredicateScalar(TenantId) = 1 )
GO

-- Default constraint example
-- Note: You should remove TenantId from the Entity Framework data model (DataClasses.cs)
-- before creating default constraints to prevent EF from automatically supplying default values
--ALTER TABLE Blogs
--    ADD CONSTRAINT df_TenantId_Blogs 
--	DEFAULT CONVERT(int, CONVERT(varbinary(4), CONTEXT_INFO())) FOR TenantId
--GO
--ALTER TABLE Posts
--    ADD CONSTRAINT df_TenantId_Posts 
--	DEFAULT CONVERT(int, CONVERT(varbinary(4), CONTEXT_INFO())) FOR TenantId
--GO

-- Example of altering the security policy to allow a "superuser" to access all rows
-- Note: You should create a new function with the new logic, and then "swap" it out with 
-- the existing predicate on the Blogs and Posts tables
--CREATE FUNCTION rls.fn_tenantAccessPredicateWithSuperUser(@TenantId int)
--    RETURNS TABLE
--    WITH SCHEMABINDING
--AS
--    RETURN SELECT 1 AS fn_accessResult 
--        WHERE 
--		(
--			DATABASE_PRINCIPAL_ID() = DATABASE_PRINCIPAL_ID('dbo') -- note, should not be dbo!
--			AND CONVERT(int, CONVERT(varbinary(4), CONTEXT_INFO())) = @TenantId
--		) 
--		OR
--		(
--			DATABASE_PRINCIPAL_ID() = DATABASE_PRINCIPAL_ID('superuser')
--		)
--GO

--ALTER SECURITY POLICY rls.tenantAccessPolicy
--	ALTER FILTER PREDICATE rls.fn_tenantAccessPredicateWithSuperUser(TenantId) ON dbo.Blogs,
--	ALTER FILTER PREDICATE rls.fn_tenantAccessPredicateWithSuperUser(TenantId) ON dbo.Posts
--GO