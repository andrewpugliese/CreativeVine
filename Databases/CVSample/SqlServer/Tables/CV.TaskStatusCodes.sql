-- 
-- Task Status Codes
--
-- Contains the various task processing status code definations
-- -- 0: NotQueued; 32: Queued; 64: InProcess; 128: Failed; 255: Succeeded
--
CREATE TABLE CV.TaskStatusCodes
(
	StatusCode					TINYINT NOT NULL,		-- unique numeric identifier
	StatusName					NVARCHAR(48) NOT NULL,	-- unique string identifier
	LastModifiedUserCode		INT NULL,
	LastModifiedDateTime		DATETIME NULL,
	CONSTRAINT TaskStatusCodes_PK_StatusCode PRIMARY KEY (StatusCode)
) ON CVCore

GO

CREATE UNIQUE INDEX TaskStatusCodes_UX_StatusName
ON CV.TaskStatusCodes(StatusName) ON CVCoreIdx

GO


ALTER TABLE CV.TaskStatusCodes
ADD CONSTRAINT TaslStatisCpdes_FK_UserMaster
FOREIGN KEY (LastModifiedUserCode)
REFERENCES CV.UserMaster(UserCode)
