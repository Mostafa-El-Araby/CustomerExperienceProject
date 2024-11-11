USE [CustomerProfile-DB]
GO

/****** Object:  Trigger [dbo].[tr_Cx_Insert]    Script Date: 11/11/2024 8:45:24 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   TRIGGER [dbo].[tr_Cx_Insert]
ON [dbo].[CustomerExperience]
AFTER INSERT
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
            [Title_EN], [Status], [NewSapId], [SapGUID], [ModifiedDate]
        )
        SELECT SYSTEM_USER,
        CASE 
             WHEN EXISTS(SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted) THEN 'Update'
             WHEN EXISTS(SELECT * FROM inserted) THEN 'Insert'
             WHEN EXISTS(SELECT * FROM deleted) THEN 'Delete'
              ELSE 'No Change' 
         END,
        i.[Key], i.[Reference Sap IDs], i.[names], i.[title], i.[Gender], i.[Region], i.[Date_Of_Birth], i.[Address], i.[Mobile_Number], i.[Total_Amount],
        i.[SalesQuantity], i.[SalesValue], i.[Maintenance], i.[installation], i.[Complaint], i.[Category1], i.[Brand1], i.[WorkOrder1], i.[Warrantydate1], i.[Category2], i.[Brand2],
        i.[WorkOrder2], i.[Warrantydate2], i.[Category3], i.[Brand3], i.[WorkOrder3], i.[Warrantydate3], i.[Category4], i.[Brand4], i.[WorkOrder4], i.[Warrantydate4],
        i.[Category5], i.[Brand5], i.[WorkOrder5], i.[Warrantydate5], i.[Category6], i.[Brand6], i.[WorkOrder6], i.[Warrantydate6], i.[Category7], i.[Brand7],
        i.[WorkOrder7], i.[Warrantydate7], i.[Category8], i.[Brand8], i.[WorkOrder8], i.[Warrantydate8], i.[Category9], i.[Brand9], i.[WorkOrder9], i.[Warrantydate9],
        i.[Category10], i.[Brand10], i.[WorkOrder10], i.[Warrantydate10], i.[CreatedOn], i.[Similarity], i.[Gr], i.[Region_Clean], i.[Gender_EN], i.[Area], i.[Area_Name], i.[Region_EN],
        i.[Title_EN], i.[Status], i.[NewSapId], i.[SapGUID], GETDATE()
        FROM inserted i;
END;
GO

ALTER TABLE [dbo].[CustomerExperience] DISABLE TRIGGER [tr_Cx_Insert]
GO