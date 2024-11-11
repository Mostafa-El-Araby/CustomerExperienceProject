USE [CustomerProfile-DB]
GO

/****** Object:  Trigger [dbo].[tr_Delta_Insert]    Script Date: 11/11/2024 8:47:35 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   TRIGGER [dbo].[tr_Delta_Insert]
ON
    [dbo].[CustomerExperience]
AFTER INSERT
AS
BEGIN
    UPDATE ce
    SET ce.[Delta_Timestamp] = GETDATE()
    FROM [dbo].[CustomerExperience] ce
    INNER JOIN inserted i ON ce.[SurrogateKeyS] = i.[SurrogateKeyS];
END;
GO

ALTER TABLE [dbo].[CustomerExperience] ENABLE TRIGGER [tr_Delta_Insert]
GO