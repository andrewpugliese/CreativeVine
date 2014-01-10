CREATE TABLE CV.UIControls
(
	UIControlCode			INT NOT NULL,
	UIControlURI			NVARCHAR(128) NOT NULL,
	Description				NVARCHAR(512),
	LastModifiedUserCode	INT,
	LastModifiedDateTime	DATETIME,
	CONSTRAINT UIControls_PK_UIControlCode PRIMARY KEY (UIControlCode)
)
ON CVCore

GO

CREATE UNIQUE INDEX UIControls_UX_UIControlURI 
ON CV.UIControls (UIControlURI)
ON CVCoreIdx

GO

ALTER TABLE CV.UIControls
ADD CONSTRAINT UIControls_FK_UserMaster
FOREIGN KEY (LastModifiedUserCode)
REFERENCES CV.UserMaster(UserCode)

GO