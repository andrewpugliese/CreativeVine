use Master
go
declare @dbName varchar(24)
set @dbName = 'FamilyView'

-- if database exists, kill processes and drop database
if exists (select null from sys.databases where name = @dbName)
	begin
		declare @SPID int, @LOGIN nvarchar(50), @DB nvarchar(50), @CMD nvarchar(256)

		declare cUsers Cursor for
		select spid, loginame, d.[name] 
		from sysprocesses p 
		inner join sysdatabases d  on d.dbid = p.dbid 
		and upper(d.name) = upper(@dbName)

		OPEN cUsers
		FETCH NEXT FROM cUsers
		INTO @SPID, @LOGIN, @DB
		WHILE @@FETCH_STATUS = 0
		BEGIN
			PRINT 'Killing SPID: ' + cast(@SPID as nvarchar(50)) + ' LOGIN: ' 
				+ @LOGIN + ' IN DB: ' + @DB
			SET @CMD = 'KILL ' + cast(@SPID as nvarchar(20))
			EXEC sp_executesql @CMD
			FETCH NEXT FROM cUsers
			INTO @SPID, @LOGIN, @DB
		END

		CLOSE cUsers
		DEALLOCATE cUsers
		drop database FamilyView

	end

go
-- FamilyView database 
-- primary filegroup
-- should only contain the system objects
-- no user objects should be defined here
-- All CVCore objects will be created in a seperate filegroup
create database FamilyView on  primary 
( name = N'FamilyView'
, filename = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLSERVER2008\MSSQL\DATA\FamilyView.mdf' 
, SIZE = 3048KB 
, MAXSIZE = UNLIMITED
, FILEGROWTH = 1024KB )

-- The filegroup for the CVCore (non index) objects
, filegroup FamilyViewData
( name = N'FamilyViewData'
, filename = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLSERVER2008\MSSQL\DATA\FamilyView_Data.ndf' 
, SIZE = 2048KB 
, MAXSIZE = UNLIMITED
, FILEGROWTH = 1024KB )

-- The filegroup for the Family View Indexes
, filegroup FamilyViewIdx 
( name = N'FamilyViewIdx'
, filename = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLSERVER2008\MSSQL\DATA\FamilyView_Idx.ndf' 
, SIZE = 2048KB 
, MAXSIZE = UNLIMITED
, FILEGROWTH = 1024KB )

-- The filegroup for the CVCore (non index) objects
, filegroup CVCore
( name = N'CVCore'
, filename = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLSERVER2008\MSSQL\DATA\FamilyView_CVCore.ndf' 
, SIZE = 2048KB 
, MAXSIZE = UNLIMITED
, FILEGROWTH = 1024KB )

-- The filegroup for the Core Indexes
, filegroup CVCoreIdx 
( name = N'CVCoreIdx'
, filename = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLSERVER2008\MSSQL\DATA\FamilyView_CVCoreIdx.ndf' 
, SIZE = 2048KB 
, MAXSIZE = UNLIMITED
, FILEGROWTH = 1024KB )

-- define a Log file
 LOG on 
( name = N'FamilyView_log'
, filename = N'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLSERVER2008\MSSQL\DATA\FamilyView_log.ldf' 
, SIZE = 1024KB 
, MAXSIZE = 2048GB 
, FILEGROWTH = 10%)
go
--
-- create a B1 Schema
USE FamilyView
go
create schema CV authorization dbo
go
create schema FV authorization dbo

-- Logins may already exist on the server, so we will check
--		if they are not, create them.
--
go
if not exists (select * from sys.server_principals where name = N'owner')
	create login owner with password = N'owner!'
	, default_database = FamilyView
	, default_language = us_english
	, check_expiration = off
	, check_policy = off

go
-- Create Database Users and match them to their Server logins and default Schemas
--
use  FamilyView
create user owner for login owner with default_schema = FV

--
-- Add server roles
--
go
sp_addrolemember 'db_ddladmin','owner'
go
sp_addrolemember 'db_datareader','owner'
go
sp_addrolemember 'db_datawriter','owner'
go
use master
go