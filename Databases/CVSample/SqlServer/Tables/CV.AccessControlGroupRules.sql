--
-- Access Control Group Rules
--
-- Maintains the AccessControlGroup's rules (Permissions)
-- for the given User Interface (UI) control code.
-- The values listed in this table override the default
-- access control rule defined at the group level
--
CREATE TABLE CV.AccessControlGroupRules
(
	AccessControlGroupCode  INT NOT NULL, -- Identifies the AccessGroup
	UIControlCode			INT NOT NULL, -- Identifies the UI Control
	AccessDenied			BIT NOT NULL DEFAULT 0, -- Indicates whether access is denied for the UI Control
	LastModifiedUserCode	INT,
	LastModifiedDateTime	DATETIME,
	CONSTRAINT AccessControlGroupRules_PK PRIMARY KEY (AccessControlGroupCode, UIControlCode) 
) ON CVCore

GO

ALTER TABLE CV.AccessControlGroupRules
ADD CONSTRAINT AccessControlGroupRules_FK_UIControls
FOREIGN KEY (UIControlCode)
REFERENCES CV.UIControls(UIControlCode)

GO

ALTER TABLE CV.AccessControlGroupRules
ADD CONSTRAINT AccessControlGroupRules_FK_UserMaster
FOREIGN KEY (LastModifiedUserCode)
REFERENCES CV.UserMaster(UserCode)

GO