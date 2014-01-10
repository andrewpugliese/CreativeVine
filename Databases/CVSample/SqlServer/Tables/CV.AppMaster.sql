CREATE TABLE CV.AppMaster
(
	AppCode					INT NOT NULL, -- Unique Numeric Code
	AppId					NVARCHAR(32) NOT NULL, -- Unique String Identifier
	AllowMultipleSessions	BIT NOT NULL DEFAULT(1), -- Indicates if the same appCode/appId
													-- can have multiple active app sessions
													-- TaskProcessingEngine Apps CANNOT have multiple sessions
	IsTaskProcessingHost	BIT NOT NULL DEFAULT(0), -- Indicates if the application will be used to host
													-- asynchronsou task processing 
	Remarks					NVARCHAR(512) NOT NULL, -- Description, comments
	LastModifiedUserCode	INT,	-- Last User to Modify the record
	LastModifiedDateTime	DATETIME, -- Date/Time of the last modification
	CONSTRAINT AppMaster_PK PRIMARY KEY (AppCode) 
) ON CVCore

GO

CREATE UNIQUE INDEX AppMaster_UX_AppId
ON CV.AppMaster(AppId)
ON CVCoreIdx

GO

ALTER TABLE CV.AppMaster
ADD CONSTRAINT AppMaster_FK_UserMaster
FOREIGN KEY (LastModifiedUserCode)
REFERENCES CV.UserMaster(UserCode)

GO
