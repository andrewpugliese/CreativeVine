--
-- Access Control Groups
--
-- It is used to define access control groups
-- Gropus will then be assigned specific 
-- rules (permissions) to different User Interface objects
-- In addition, 

CREATE TABLE CV.AccessControlGroups
(
	AccessControlGroupCode  INT NOT NULL,	-- unique numeric identifier
	AccessControlGroupName  NVARCHAR(64) NOT NULL, -- unique string identifier
	DefaultAccessDenied		BIT DEFAULT (0), -- indicates whether the default access is denied or allowed
	Remarks					NVARCHAR(512),
	CONSTRAINT AccessControlGroups_PK PRIMARY KEY (AccessControlGroupCode) 
) ON CVCore

GO

CREATE UNIQUE INDEX AccessControlGroups_UX_GroupName
ON CV.AccessControlGroups(AccessControlGroupName) 
ON CVCoreIdx


GO