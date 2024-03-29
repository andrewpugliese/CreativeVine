--
-- Application Sessions
--
-- Used to maintain the status of the application sessions.
-- An application can ONLY have 1 session at a time for a
-- single unique AppCode or AppId if it is going to be part
-- of the Task Processing Grid.
--
-- An application can have multiple instances running, the 
-- only caveat is that each instance be assigned a unique
-- code and id that is defined in the AppMaster table
-- and placed in the individual configuration file.
-- 
CREATE TABLE CV.AppSessions
(
	AppCode					INT NOT NULL, -- Unique App Numeric Code
	AppId					NVARCHAR(32) NOT NULL, -- Unique App String Id
	AppProduct				NVARCHAR(64) NOT NULL, -- Found in Assembly.cs
	AppVersion				NVARCHAR(24) NOT NULL, -- Found in Assembly.cs
	MultipleSessionCode		BIGINT NOT NULL DEFAULT(0), -- A unique code when application can have multiple instances
	MachineName				NVARCHAR(64) NOT NULL, -- Name of server app resides
	ProcessId				BIGINT NOT NULL, -- OS Process Id
	StartDateTime			DATETIME NOT NULL DEFAULT (GETUTCDATE()), -- Time app started
	StatusDateTime			DATETIME NOT NULL DEFAULT (GETUTCDATE()), -- Time of last status update
	EnvironmentSettings		NVARCHAR(512) NOT NULL, -- Various environment settings (ie. OS version, etc)
	ConfigSettings			NVARCHAR(512) NOT NULL, -- Various configuration settings of the app.
	StatusMessage			VARCHAR(512) NOT NULL, 
	TpeEndpointAddress		NVARCHAR(256) NULL, 
	CONSTRAINT AppSessions_PK PRIMARY KEY (AppCode, MultipleSessionCode) 
) ON CVCore

GO

CREATE UNIQUE INDEX AppSessions_UX_AppId
ON CV.AppSessions(AppId, MultipleSessionCode)
ON CVCoreIdx

GO

ALTER TABLE CV.AppSessions
ADD CONSTRAINT AppSessions_FK_AppMaster_Code
FOREIGN KEY (AppCode)
REFERENCES CV.AppMaster(AppCode)

GO

ALTER TABLE CV.AppSessions
ADD CONSTRAINT AppSessions_FK_AppMaster_Id
FOREIGN KEY (AppId)
REFERENCES CV.AppMaster(AppId)

GO