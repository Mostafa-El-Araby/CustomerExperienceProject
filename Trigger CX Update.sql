USE [CustomerProfile-DB]
GO

/****** Object:  Trigger [dbo].[tr_Cx_Update]    Script Date: 11/11/2024 8:46:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   TRIGGER [dbo].[tr_Cx_Update]
ON [dbo].[CustomerExperience]
AFTER UPDATE, DELETE
AS
BEGIN
    INSERT INTO [dbo].[MyTable_History]
    (
        [Username], [Action], [Key], [Reference Sap IDs], [names], [title], [Gender], [Region], [Date_Of_Birth], [Address], [Mobile_Number], [Total_Amount],
        [SalesQuantity], [SalesValue], [Maintenance], [installation], [Complaint], [Category1], [Brand1], [WorkOrder1], [Warrantydate1], [Category2], [Brand2],
        [WorkOrder2], [Warrantydate2], [Category3], [Brand3], [WorkOrder3], [Warrantydate3], [Category4], [Brand4], [WorkOrder4], [Warrantydate4],
        [Category5], [Brand5], [WorkOrder5], [Warrantydate5], [Category6], [Brand6], [WorkOrder6], [Warrantydate6], [Category7], [Brand7],
        [WorkOrder7], [Warrantydate7], [Category8], [Brand8], [WorkOrder8], [Warrantydate8], [Category9], [Brand9], [WorkOrder9], [Warrantydate9],
        [Category10], [Brand10], [WorkOrder10], [Warrantydate10], [CreatedOn], [Similarity], [Gr], [Region_Clean], [Gender_EN], [Area], [Area_Name], [Region_EN],
        [Title_EN],[Status],[NewSapId], [SapGUID], [ModifiedDate]
    )
    SELECT SYSTEM_USER,
    CASE 
         WHEN EXISTS(SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted) THEN 'Update'
         WHEN EXISTS(SELECT * FROM inserted) THEN 'Insert'
         WHEN EXISTS(SELECT * FROM deleted) THEN 'Delete'
          ELSE 'No Change' 
    END,
    d.[Key], d.[Reference Sap IDs], d.[names], d.[title], d.[Gender], d.[Region], d.[Date_Of_Birth], d.[Address], d.[Mobile_Number], d.[Total_Amount],
    d.[SalesQuantity], d.[SalesValue], d.[Maintenance], d.[installation], d.[Complaint], d.[Category1], d.[Brand1], d.[WorkOrder1], d.[Warrantydate1], d.[Category2],d.[Brand2],
    d.[WorkOrder2], d.[Warrantydate2], d.[Category3], d.[Brand3], d.[WorkOrder3], d.[Warrantydate3], d.[Category4], d.[Brand4], d.[WorkOrder4], d.[Warrantydate4],
    d.[Category5], d.[Brand5], d.[WorkOrder5], d.[Warrantydate5], d.[Category6], d.[Brand6], d.[WorkOrder6], d.[Warrantydate6], d.[Category7], d.[Brand7],
    d.[WorkOrder7], d.[Warrantydate7], d.[Category8], d.[Brand8], d.[WorkOrder8], d.[Warrantydate8], d.[Category9], d.[Brand9], d.[WorkOrder9], d.[Warrantydate9],
    d.[Category10], d.[Brand10], d.[WorkOrder10], d.[Warrantydate10], d.[CreatedOn], d.[Similarity], d.[Gr], d.[Region_Clean], d.[Gender_EN], d.[Area], d.[Area_Name], d.[Region_EN],
    d.[Title_EN], d.[Status], d.[NewSapId], d.[SapGUID], GETDATE()
    FROM deleted d;
END;
GO

ALTER TABLE [dbo].[CustomerExperience] ENABLE TRIGGER [tr_Cx_Update]
GO