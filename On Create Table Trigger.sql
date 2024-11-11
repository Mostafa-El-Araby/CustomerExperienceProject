USE [CustomerProfile-DB]
GO

/****** Object:  DdlTrigger [OnCreateTable]    Script Date: 11/11/2024 8:43:10 AM ******/
CREATE OR ALTER TRIGGER [OnCreateTable]
ON DATABASE
FOR CREATE_TABLE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventData XML;
    SET @EventData = EVENTDATA();

    -- Insert information into UserTablesAudit table
    INSERT INTO UserTablesAudit (TableName, CreatedBy, CreatedDate)
    VALUES (
        @EventData.value('(/EVENT_INSTANCE/ObjectName)[1]', 'NVARCHAR(255)'),
        @EventData.value('(/EVENT_INSTANCE/LoginName)[1]', 'NVARCHAR(100)'),
        GETDATE()
    );
END;
GO

ENABLE TRIGGER [OnCreateTable] ON DATABASE
GO