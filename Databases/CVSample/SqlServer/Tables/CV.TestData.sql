--
-- This table is used for testing and demoing the functionality
-- of the DatabaseMgr class and the UniqueIds table and stored procedure.
--  
CREATE TABLE CV.TestData(
  AppSequenceId BIGINT NOT NULL,	-- unique id generated by DatabaseMgr
  DbSequenceId BIGINT IDENTITY(1,1) NOT NULL, -- unique id generated by database 
  AppSynchTime DATETIME NOT NULL, -- application-database synchronized time kept by app
  AppLocalTime DATETIME NOT NULL, -- local time of application
  DbServerTime DATETIME DEFAULT(GETUTCDATE()) NOT NULL, -- database server time (universal time)
  AppSequenceName NVARCHAR(32) not null, -- sample name field
  Remarks NVARCHAR(100), -- sample comments
  ExtraData NVARCHAR(MAX) -- sample large data field
 CONSTRAINT TestData_PK_AppSequenceId PRIMARY KEY(AppSequenceId)
) ON CVCore
GO

CREATE UNIQUE INDEX TestData_UX_DbSequenceId_AppSequenceId ON
	CV.TestData( DbSequenceId, AppSequenceId ) ON CVCoreIdx
GO

CREATE UNIQUE INDEX TestData_UX_AppSequenceName_AppSequenceId ON
CV.TestData( AppSequenceName, AppSequenceId ) ON CVCoreIdx
GO
