--
-- Task Configurations
--
-- Each record contains a different configuration description that
-- can be associated with a Task.  This forces a task to only be processed
-- by an Engine that is running in an environment that supports that configuration.
-- For example, it could describe specific hardware (for cryptography).
--
CREATE TABLE CV.TaskConfigurations
(
	ConfigId					NVARCHAR(32) NOT NULL,	-- Unique string identifier for the configuration description
	ConfigurationDescription	NVARCHAR(512) NOT NULL,	-- details specific to this configuration (e.g. memory, hardware, etc)
	LastModifiedUserCode		INT NULL,
	LastModifiedDateTime		DATETIME NULL,
	CONSTRAINT TaskConfigurations_PK_ConfigId PRIMARY KEY (ConfigId)
) ON CVCore

GO

ALTER TABLE CV.TaskConfigurations
ADD CONSTRAINT TaskConfigurations_FK_UserMaster_Code
FOREIGN KEY (LastModifiedUserCode)
REFERENCES CV.UserMaster(UserCode)

GO
