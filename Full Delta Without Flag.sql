USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[FullDeltaWithoutFlag]    Script Date: 11/10/2024 10:38:01 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER  PROCEDURE [dbo].[FullDeltaWithoutFlag] @RowsAffected INT OUTPUT
/*
-This proc is used to merge CRM and Sales Data in CustomerExperience
-We have 4 source tabls:
CRM tables ( the CRM row data is in [dbo].[D365_cases] table which is cascaded to the following 2 tables):
    1) [CustProfile365]  ---> old CRM data before 2023 - as it is old data , we only update data from this table to CX 
	2) [CustProfile2023] ---> new CRM data from 2023 - as it is new data , we update and insert data from this table to CX
Sales Tables 
	1) [S4HanaSales2023] ---> aggregated new sales data from 2023 - as it is new data , we update and insert data from this table to CX
    2) the sales row data is in [dbo].[0-SalesDetailed] table which is cascaded to [0-SalesDetailed-Cascade] table , so we use [0-SalesDetailed-Cascade]
	in our procedure where data is cascaded - we only update data from this table to CX

VIP note : We take the master and aggregated sales data from [S4HanaSales2023] and then update this data with its sales details 
from [0-SalesDetailed-Cascade] table , that is why we use this table only for update

-Let's summarize :
[CustProfile365] ---> update 
[CustProfile2023] ---> update, insert
[S4HanaSales2023] ---> update, insert
[0-SalesDetailed-Cascade] ---> update
---------------------------------------------------------------------------------------------------------------------------
-To handle logs that monitor the transfer process between source tables and CustomerExperience , we decided to use Cursor and Try..Catch
-We have a cursor for each source table , so 4 source tables = 4 cursors
-In each update statement , we have 'modified_on' column that we assign to GETDATE() to mark the rows updated in cx for this day
-In each insert statement , we have 'inserted_on' column that we assign to GETDATE() to mark the rows inserted in cx for this day
*/
---------------------------------------------------------------------------------------------------------------------------
AS
BEGIN
SET @RowsAffected = 0  --initialize the output parameter to 0 to increment after each operation
--Declare a variable for each column you need to update from CustProfile365 table with the same data type 
DECLARE @is_oldcustomersapid NVARCHAR(450), @names NVARCHAR(MAX), @Gender NVARCHAR(350),
		@Region NVARCHAR(MAX), @Address NVARCHAR(MAX), @CustomerCreatedOn datetime2(7),
		@Mobile_Number NVARCHAR(MAX), @Date_Of_Birth datetime2(7), @Total_Amount decimal(38, 18),
		@National_ID NVARCHAR(MAX), @installation INT, @Mantinance INT, @Complaint INT,
		@Category1 NVARCHAR(MAX), @Brand1 NVARCHAR(MAX), @WorkOrder1 NVARCHAR(12), @Warrantydate1 datetime2(7),
		@Category2 NVARCHAR(MAX), @Brand2 NVARCHAR(MAX), @WorkOrder2 NVARCHAR(12), @Warrantydate2 datetime2(7),
		@Category3 NVARCHAR(MAX), @Brand3 NVARCHAR(MAX), @WorkOrder3 NVARCHAR(12), @Warrantydate3 datetime2(7),
		@Category4 NVARCHAR(MAX), @Brand4 NVARCHAR(MAX), @WorkOrder4 NVARCHAR(12), @Warrantydate4 datetime2(7),
		@Category5 NVARCHAR(MAX), @Brand5 NVARCHAR(MAX), @WorkOrder5 NVARCHAR(12), @Warrantydate5 datetime2(7),
		@Category6 NVARCHAR(MAX), @Brand6 NVARCHAR(MAX), @WorkOrder6 NVARCHAR(12), @Warrantydate6 datetime2(7),
		@Category7 NVARCHAR(MAX), @Brand7 NVARCHAR(MAX), @WorkOrder7 NVARCHAR(12), @Warrantydate7 datetime2(7),
		@Category8 NVARCHAR(MAX), @Brand8 NVARCHAR(MAX), @WorkOrder8 NVARCHAR(12), @Warrantydate8 datetime2(7),
		@Category9 NVARCHAR(MAX), @Brand9 NVARCHAR(MAX), @WorkOrder9 NVARCHAR(12), @Warrantydate9 datetime2(7),
		@Category10 NVARCHAR(MAX), @Brand10 NVARCHAR(MAX), @WorkOrder10 NVARCHAR(12), @Warrantydate10 datetime2(7),
		@CentralTelephone NVARCHAR(450), @Category11flag NVARCHAR(450), 
		@uold INT = 0  -- this variable for counting the number of rows affected to insert in DeltaStatistics table

DECLARE OldCRMCursor CURSOR FOR
/*
The cursor select statement in which we join [CustomerExperience] and CustProfile365 for comparison and detect 
different values that need to be updated 
*/
SELECT   
	CRM.[is_oldcustomersapid],
	CRM.[names],
	CRM.[Gender],
	CRM.[Region],
	CRM.[Address],
	CRM.[CustomerCreatedOn],
	CRM.[Mobile_Number],
	CRM.[Date_Of_Birth],
	CRM.[Total_Amount],
	CRM.[National_ID],
	CRM.[installation],
	CRM.[Mantinance],
	CRM.[Complaint],
	CRM.[Category1],
	CRM.[Brand1],
	CRM.[WorkOrder1],
	CRM.[Warrantydate1],
	CRM.[Category2],
	CRM.[Brand2],
	CRM.[WorkOrder2],
	CRM.[Warrantydate2],
	CRM.[Category3],
	CRM.[Brand3],
	CRM.[WorkOrder3],
	CRM.[Warrantydate3],
	CRM.[Category4],
	CRM.[Brand4],
	CRM.[WorkOrder4],
	CRM.[Warrantydate4],
	CRM.[Category5],
	CRM.[Brand5],
	CRM.[WorkOrder5],
	CRM.[Warrantydate5],
	CRM.[Category6],
	CRM.[Brand6],
	CRM.[WorkOrder6],
	CRM.[Warrantydate6],
	CRM.[Category7],
	CRM.[Brand7],
	CRM.[WorkOrder7],
	CRM.[Warrantydate7],
	CRM.[Category8],
	CRM.[Brand8],
	CRM.[WorkOrder8],
	CRM.[Warrantydate8],
	CRM.[Category9],
	CRM.[Brand9],
	CRM.[WorkOrder9],
	CRM.[Warrantydate9],
	CRM.[Category10],
	CRM.[Brand10],
	CRM.[WorkOrder10],
	CRM.[Warrantydate10],
	CRM.[CentralTelephone],
	CRM.[Category11flag]
FROM
	[dbo].[CustProfile365] AS CRM INNER JOIN [dbo].[CustomerExperience] AS FV
ON
	FV.[Key] = CRM.[is_oldcustomersapid] AND COALESCE(FV.[Gr], '') != 'G' 
WHERE
	COALESCE(FV.[Maintenance], 0) <> CRM.[Mantinance]
	OR
	COALESCE(FV.[Complaint], 0) <> CRM.[Complaint] 
	OR
	COALESCE (FV.[installation], 0) <> CRM.[installation]
	OR
	COALESCE(FV.[Category1], '') <> CRM.[Category1]
	OR
	COALESCE(FV.[Category2], '') <> CRM.[Category2]
	OR
	COALESCE(FV.[Category3], '') <> CRM.[Category3]
	OR
	COALESCE(FV.[Category4], '') <> CRM.[Category4]
	OR
	COALESCE(FV.[Category5], '') <> CRM.[Category5]
	OR
	COALESCE(FV.[Category6], '') <> CRM.[Category6]
	OR
	COALESCE(FV.[Category7], '') <> CRM.[Category7]
	OR
	COALESCE(FV.[Category8], '') <> CRM.[Category8]
	OR
	COALESCE(FV.[Category9], '') <> CRM.[Category9]
	OR
	COALESCE(FV.[Category10], '') <> CRM.[Category10]
	OR
	COALESCE([CRMCaterory11Flag], '') <> CRM.[Category11flag]
	OR
	COALESCE([centraltelephonename], '') <> CRM.[CentralTelephone] 
FOR UPDATE
OPEN OldCRMCursor
--Cursor FETCH statement 
FETCH OldCRMCursor INTO
	@is_oldcustomersapid,
	@names,
	@Gender,
	@Region,
	@Address,
	@CustomerCreatedOn,
	@Mobile_Number,
	@Date_Of_Birth,
	@Total_Amount,
	@National_ID,
	@installation,
	@Mantinance,
	@Complaint,
	@Category1,
	@Brand1,
	@WorkOrder1,
	@Warrantydate1,
	@Category2,
	@Brand2,
	@WorkOrder2,
	@Warrantydate2,
	@Category3,
	@Brand3,
	@WorkOrder3,
	@Warrantydate3,
	@Category4,
	@Brand4,
	@WorkOrder4,
	@Warrantydate4,
	@Category5,
	@Brand5,
	@WorkOrder5,
	@Warrantydate5,
	@Category6,
	@Brand6,
	@WorkOrder6,
	@Warrantydate6,
	@Category7,
	@Brand7,
	@WorkOrder7,
	@Warrantydate7,
	@Category8,
	@Brand8,
	@WorkOrder8,
	@Warrantydate8,
	@Category9,
	@Brand9,
	@WorkOrder9,
	@Warrantydate9,
	@Category10,
	@Brand10,
	@WorkOrder10,
	@Warrantydate10,
	@CentralTelephone,
	@Category11flag
WHILE @@FETCH_STATUS = 0
    BEGIN
/*
In the try block we do the update process by assigning the variables to the columns and then insert into the MDelatLog table (
each row data and its success status(which is 1 in try block))
*/
	BEGIN TRY
	UPDATE [dbo].[CustomerExperience]
	SET
		[Total_Amount] = @Total_Amount,
		[installation] = @installation,
		[Maintenance] = @Mantinance,
		[Complaint] = @Complaint,
		[Category1] = @Category1,
		[Brand1] = @Brand1,
		[WorkOrder1] = @WorkOrder1,
		[Warrantydate1] = @Warrantydate1,
		[Category2] = @Category2,
		[Brand2] = @Brand2,
		[WorkOrder2] = @WorkOrder2,
		[Warrantydate2] = @Warrantydate2,
		[Category3] = @Category3,
		[Brand3] = @Brand3,
		[WorkOrder3] = @WorkOrder3,
		[Warrantydate3] = @Warrantydate3,
		[Category4] = @Category4,
		[Brand4] = @Brand4,
		[WorkOrder4] = @WorkOrder4,
		[Warrantydate4] = @Warrantydate4,
		[Category5] = @Category5,
		[Brand5] = @Brand5,
		[WorkOrder5] = @WorkOrder5,
		[Warrantydate5] = @Warrantydate5,
		[Category6] = @Category6,
		[Brand6] = @Brand6,
		[WorkOrder6] = @WorkOrder6,
		[Warrantydate6] = @Warrantydate6,
		[Category7] = @Category7,
		[Brand7] = @Brand7,
		[WorkOrder7] = @WorkOrder7,
		[Warrantydate7] = @Warrantydate7,
		[Category8] = @Category8,
		[Brand8] = @Brand8,
		[WorkOrder8] = @WorkOrder8,
		[Warrantydate8] = @Warrantydate8,
		[Category9] = @Category9,
		[Brand9] = @Brand9,
		[WorkOrder9] = @WorkOrder9,
		[Warrantydate9] = @Warrantydate9,
		[Category10] = @Category10,
		[Brand10] = @Brand10,
		[WorkOrder10] = @WorkOrder10,
		[Warrantydate10] = @Warrantydate10,
		[CRMCaterory11Flag] = @Category11flag,
		[Modified_On] = GETDATE(),
		[centraltelephonename] = @CentralTelephone
	WHERE
		[Key] = @is_oldcustomersapid AND COALESCE([Gr], '') != 'G'   --Delta doesn't affect any G
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT
	SET @uold = @uold + 1  --Look! for each row updated , we increment this variable

	INSERT INTO [dbo].[MDeltaLog]
	(
		[Key], [Statement], [Success], [SourceTable], [LogTime], [Name], [Gender], [Region],
		[Date_Of_Birth], [Address], [Mobile_Number], [CreatedOn], [Total_Amount],
		[Maintenance], [installation], [Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10], [WorkOrder10], [Warrantydate10],
		[CentralTelephone]
	)
	VALUES
	(
		@is_oldcustomersapid, 'Update CustProfile365', 1, 'CustProfile365', GETDATE(),
		@names, @Gender, @Region, @Date_Of_Birth, @Address, @Mobile_Number, @CustomerCreatedOn, @Total_Amount,
		@Mantinance, @installation, @Complaint,
		@Category1, @Brand1, @WorkOrder1, @Warrantydate1,
		@Category2, @Brand2, @WorkOrder2, @Warrantydate2,
		@Category3, @Brand3, @WorkOrder3, @Warrantydate3,
		@Category4, @Brand4, @WorkOrder4, @Warrantydate4,
		@Category5, @Brand5, @WorkOrder5, @Warrantydate5,
		@Category6, @Brand6, @WorkOrder6, @Warrantydate6,
		@Category7, @Brand7, @WorkOrder7, @Warrantydate7,
		@Category8, @Brand8, @WorkOrder8, @Warrantydate8,
		@Category9, @Brand9, @WorkOrder9, @Warrantydate9,
		@Category10, @Brand10, @WorkOrder10, @Warrantydate10,
		@CentralTelephone
	)
	END TRY
	BEGIN CATCH
--In the catch block we repeat the code in the try block but here the success column will be 0 if there is any failure in the process

	INSERT INTO [dbo].[MDeltaLog]
	(
		[Key], [Statement], [Success], [SourceTable], [LogTime], [Name], [Gender], [Region],
		[Date_Of_Birth], [Address], [Mobile_Number], [CreatedOn], [Total_Amount],
		[Maintenance], [installation], [Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10], [WorkOrder10], [Warrantydate10],
		[CentralTelephone]
	)
    VALUES
	(
		@is_oldcustomersapid, 'Update CustProfile365', 0, 'CustProfile365', GETDATE(),
		@names, @Gender, @Region, @Date_Of_Birth, @Address, @Mobile_Number, @CustomerCreatedOn, @Total_Amount,
		@Mantinance, @installation, @Complaint,
		@Category1, @Brand1, @WorkOrder1, @Warrantydate1,
		@Category2, @Brand2, @WorkOrder2, @Warrantydate2,
		@Category3, @Brand3, @WorkOrder3, @Warrantydate3,
		@Category4, @Brand4, @WorkOrder4, @Warrantydate4,
		@Category5, @Brand5, @WorkOrder5, @Warrantydate5,
		@Category6, @Brand6, @WorkOrder6, @Warrantydate6,
		@Category7, @Brand7, @WorkOrder7, @Warrantydate7,
		@Category8, @Brand8, @WorkOrder8, @Warrantydate8,
		@Category9, @Brand9, @WorkOrder9, @Warrantydate9,
		@Category10, @Brand10, @WorkOrder10, @Warrantydate10,
		@CentralTelephone
	)
	END CATCH
	FETCH OldCRMCursor INTO
		@is_oldcustomersapid, @names, @Gender, @Region, @Address, @CustomerCreatedOn, @Mobile_Number,
		@Date_Of_Birth, @Total_Amount, @National_ID, @installation, @Mantinance, @Complaint,
		@Category1, @Brand1, @WorkOrder1, @Warrantydate1,
		@Category2, @Brand2, @WorkOrder2, @Warrantydate2,
		@Category3, @Brand3, @WorkOrder3, @Warrantydate3,
		@Category4, @Brand4, @WorkOrder4, @Warrantydate4,
		@Category5, @Brand5, @WorkOrder5, @Warrantydate5,
		@Category6, @Brand6, @WorkOrder6, @Warrantydate6,
		@Category7, @Brand7, @WorkOrder7, @Warrantydate7,
		@Category8, @Brand8, @WorkOrder8, @Warrantydate8,
		@Category9, @Brand9, @WorkOrder9, @Warrantydate9,
		@Category10, @Brand10, @WorkOrder10, @Warrantydate10,
		@CentralTelephone, @Category11flag
	END
	CLOSE OldCRMCursor;
    DEALLOCATE OldCRMCursor;
	INSERT INTO [dbo].[DeltaStatistics]  -- Here , we insert the number of rows updated carried by @uold var in [DeltaStatistics] table
	(
		[Action], [sourcetable], [Count], [Date]
	)
    SELECT
		'Update', 'CustProfile365', @uold, GETDATE()
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
--Declare a variable for each column you need to be updated or inserted from CustProfile2023 table with the same data type 

DECLARE @ldv_customersapid NVARCHAR(350), @newnames NVARCHAR(MAX), @newGender NVARCHAR(350),
		@newRegion NVARCHAR(MAX), @newAddress NVARCHAR(MAX), @newCustomerCreatedOn datetime2(7),
		@newMobile_Number NVARCHAR(MAX), @newDate_Of_Birth datetime2(7),
		@newTotal_Amount decimal(38, 18), @newNational_ID NVARCHAR(MAX),
		@newinstallation INT, @newMantinance INT, @newComplaint INT,
		@newCategory1 NVARCHAR(MAX), @newBrand1 NVARCHAR(MAX), @newWorkOrder1 NVARCHAR(12), @newWarrantydate1 datetime2(7),
		@newCategory2 NVARCHAR(MAX), @newBrand2 NVARCHAR(MAX), @newWorkOrder2 NVARCHAR(12), @newWarrantydate2 datetime2(7),
		@newCategory3 NVARCHAR(MAX), @newBrand3 NVARCHAR(MAX), @newWorkOrder3 NVARCHAR(12), @newWarrantydate3 datetime2(7),
		@newCategory4 NVARCHAR(MAX), @newBrand4 NVARCHAR(MAX), @newWorkOrder4 NVARCHAR(12), @newWarrantydate4 datetime2(7),
		@newCategory5 NVARCHAR(MAX), @newBrand5 NVARCHAR(MAX), @newWorkOrder5 NVARCHAR(12), @newWarrantydate5 datetime2(7),
		@newCategory6 NVARCHAR(MAX), @newBrand6 NVARCHAR(MAX), @newWorkOrder6 NVARCHAR(12), @newWarrantydate6 datetime2(7),
		@newCategory7 NVARCHAR(MAX), @newBrand7 NVARCHAR(MAX), @newWorkOrder7 NVARCHAR(12), @newWarrantydate7 datetime2(7),
		@newCategory8 NVARCHAR(MAX), @newBrand8 NVARCHAR(MAX), @newWorkOrder8 NVARCHAR(12), @newWarrantydate8 datetime2(7),
		@newCategory9 NVARCHAR(MAX), @newBrand9 NVARCHAR(MAX), @newWorkOrder9 NVARCHAR(12), @newWarrantydate9 datetime2(7),
		@newCategory10 NVARCHAR(MAX), @newBrand10 NVARCHAR(MAX), @newWorkOrder10 NVARCHAR(12), @newWarrantydate10 datetime2(7),
		@newCentralTelephone NVARCHAR(450), @newCategory11flag NVARCHAR(450), 
		@Unew INT = 0,  -- this variable for counting the number of rows updated to insert in DeltaStatistics table
		@Inew INT = 0  -- this variable for counting the number of rows inserted to insert in DeltaStatistics table

DECLARE NEWCRMCursor CURSOR FOR
--The cursor select statement 
SELECT
	[ldv_customersapid], [names], [Gender], [Region], [Address], [CustomerCreatedOn], [Mobile_Number],
	[Date_Of_Birth], [Total_Amount], [National_ID],
	[installation], [Mantinance], [Complaint],
	[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
	[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
	[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
	[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
	[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
	[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
	[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
	[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
	[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
	[Category10], [Brand10], [WorkOrder10], [Warrantydate10],
	[CentralTelephone], [Category11flag]
FROM [dbo].[CustProfile2023] 

FOR UPDATE

OPEN NEWCRMCursor
--Cursor FETCH statement 
FETCH NEWCRMCursor INTO
	@ldv_customersapid, @newnames, @newGender, @newRegion, @newAddress, @newCustomerCreatedOn,
	@newMobile_Number, @newDate_Of_Birth, @newTotal_Amount, @newNational_ID,
	@newinstallation, @newMantinance, @newComplaint,
	@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
	@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
	@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3,
	@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
	@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
	@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
	@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
	@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
	@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
	@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10,
	@newCentralTelephone, @newCategory11flag
WHILE @@FETCH_STATUS = 0
--As we update and insert from this table we use if exists to detect rows for update and if not exists to detect rows for insert
    BEGIN
	IF EXISTS --if rows existed in [CustProfile2023] and there are differences between them and cx, we need to update them
	(
		SELECT
			[NewSapId]
		FROM [dbo].[CustomerExperience]
		WHERE [NewSapId] = @ldv_customersapid
		AND COALESCE([Gr], '') != 'G' 
		AND
		(
			COALESCE([Maintenance], 0) <> @newMantinance
			OR
			COALESCE([Complaint], 0) <> @newComplaint
			OR
			COALESCE([installation], 0) <> @newinstallation
			OR
			COALESCE([Category1], '') <> @newCategory1
			OR
			COALESCE([Category2], '') <> @newCategory2
			OR
			COALESCE([Category3], '') <> @newCategory3
			OR
			COALESCE([Category4], '') <> @newCategory4
			OR
			COALESCE([Category5], '') <> @newCategory5
			OR
			COALESCE([Category6], '') <> @newCategory6
			OR
			COALESCE([Category7], '') <> @newCategory7
			OR
			COALESCE([Category8], '') <> @newCategory8
			OR
			COALESCE([Category9], '') <> @newCategory9
			OR
			COALESCE([Category10], '') <> @newCategory10
			OR
			COALESCE([centraltelephonename], '') <> @newCentralTelephone
			OR
			COALESCE([CRMCaterory11Flag], '') <> @newCategory11flag
		)
	)
	BEGIN
	BEGIN TRY
/*
In the try block we do the update process by assigning the variables to the columns and then insert into the MDelatLog table (
each row data and its success status(which is 1 in try block))
*/
	UPDATE [dbo].[CustomerExperience]
	SET
		[Total_Amount] = @newTotal_Amount,
		[installation] = @newinstallation, [Maintenance] = @newMantinance, [Complaint] = @newComplaint,
		[Category1] = @newCategory1, [Brand1] = @newBrand1, [WorkOrder1] = @newWorkOrder1, [Warrantydate1] = @newWarrantydate1,
		[Category2] = @newCategory2, [Brand2] = @newBrand2, [WorkOrder2] = @newWorkOrder2, [Warrantydate2] = @newWarrantydate2,
		[Category3] = @newCategory3, [Brand3] = @newBrand3, [WorkOrder3] = @newWorkOrder3, [Warrantydate3] = @newWarrantydate3,
		[Category4] = @newCategory4, [Brand4] = @newBrand4, [WorkOrder4] = @newWorkOrder4, [Warrantydate4] = @newWarrantydate4,
		[Category5] = @newCategory5, [Brand5] = @newBrand5, [WorkOrder5] = @newWorkOrder5, [Warrantydate5] = @newWarrantydate5,
		[Category6] = @newCategory6, [Brand6] = @newBrand6, [WorkOrder6] = @newWorkOrder6, [Warrantydate6] = @newWarrantydate6,
		[Category7] = @newCategory7, [Brand7] = @newBrand7, [WorkOrder7] = @newWorkOrder7, [Warrantydate7] = @newWarrantydate7,
		[Category8] = @newCategory8, [Brand8] = @newBrand8, [WorkOrder8] = @newWorkOrder8, [Warrantydate8] = @newWarrantydate8,
		[Category9] = @newCategory9, [Brand9] = @newBrand9, [WorkOrder9] = @newWorkOrder9, [Warrantydate9] = @newWarrantydate9,
		[Category10] = @newCategory10, [Brand10] = @newBrand10, [WorkOrder10] = @newWorkOrder10, [Warrantydate10] = @newWarrantydate10,
		[centraltelephonename] = @newCentralTelephone, [CRMCaterory11Flag] = @newCategory11flag, [Modified_On] = GETDATE()
		WHERE
			[NewSapId] = @ldv_customersapid
			AND COALESCE([Gr], '') != 'G'   --Delta doesn't affect any G

	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

	SET @Unew = @Unew + 1   --Look! for each row updated , we increment this variable
		
	INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [SourceTable], [LogTime], [Name], [Gender], [Region], [Date_Of_Birth],
		[Address], [Mobile_Number], [CreatedOn], [Total_Amount],
		[Maintenance], [installation], [Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10], [WorkOrder10], [Warrantydate10], [CentralTelephone]
	)
	VALUES
	(
		@ldv_customersapid, 'Update CustProfile2023', 1, 'CustProfile2023', GETDATE(), @newnames, @newGender, @newRegion,
		@newDate_Of_Birth, @newAddress, @newMobile_Number, @newCustomerCreatedOn, @newTotal_Amount, @newMantinance, @newinstallation, @newComplaint,
		@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
		@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
		@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3,
		@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
		@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
		@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
		@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
		@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
		@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
		@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10,
		@newCentralTelephone
	)
	END TRY
	BEGIN CATCH
--In the catch block we repeat the code in the try block but here the success column will be 0 if there is any failure in the process

	INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [SourceTable], [ErrorMessage], [LogTime], [Name], [Gender], [Region], [Date_Of_Birth],
		[Address], [Mobile_Number], [CreatedOn], [Total_Amount], [Maintenance], [installation], [Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10], [WorkOrder10], [Warrantydate10], [CentralTelephone]
	)
	VALUES
	(
		@ldv_customersapid, 'Update CustProfile2023', 0, 'CustProfile2023', ERROR_MESSAGE(), GETDATE(), @newnames, @newGender, @newRegion,
		@newDate_Of_Birth, @newAddress, @newMobile_Number, @newCustomerCreatedOn, @newTotal_Amount, @newMantinance, @newinstallation, @newComplaint,
		@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
		@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
		@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3,
		@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
		@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
		@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
		@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
		@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
		@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
		@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10,
		@newCentralTelephone
	)
	END CATCH
	END
	IF NOT EXISTS
--if rows don't exist in cx, we need to insert them
	(
		SELECT
			[NewSapId]
		FROM [dbo].[CustomerExperience]
		WHERE
			[NewSapId] = @ldv_customersapid
	)
	BEGIN
	BEGIN TRY
/*
In the try block we do the insert process by assigning the variables to the columns and then insert into the MDelatLog table (
each row data and its success status(which is 1 in try block))
*/
	INSERT INTO [dbo].[CustomerExperience]
	(
		[NewSapId], [names], [Gender], [Region], [Address], [CreatedOn], [Mobile_Number],
		[Date_Of_Birth], [Total_Amount], [installation], [Maintenance], [Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10],[WorkOrder10], [Warrantydate10], [centraltelephonename],
		[CRMCaterory11Flag], [Modified_On], [Inserted_On]
	)
	VALUES
	(
		@ldv_customersapid, @newnames, @newGender, @newRegion, @newAddress, @newCustomerCreatedOn, @newMobile_Number, @newDate_Of_Birth,
		@newTotal_Amount, @newinstallation, @newMantinance, @newComplaint,
		@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
		@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
		@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3,
		@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
		@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
		@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
		@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
		@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
		@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
		@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10, @newCentralTelephone,
		@newCategory11flag, GETDATE(), GETDATE()
	)

	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

    SET @Inew = @Inew + 1    --Look! for each row inserted , we increment this variable
    INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [SourceTable], [LogTime], [Name], [Gender], [Region], [Date_Of_Birth],
		[Address], [Mobile_Number], [CreatedOn], [Total_Amount], [Maintenance], [installation], [Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10], [WorkOrder10], [Warrantydate10], [CentralTelephone]
	)
    VALUES
	(
		@ldv_customersapid, 'Insert CustProfile2023', 1, 'CustProfile2023', GETDATE(), @newnames, @newGender, @newRegion, @newDate_Of_Birth,
		@newAddress, @newMobile_Number, @newCustomerCreatedOn, @newTotal_Amount, @newMantinance, @newinstallation, @newComplaint,
		@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
		@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
		@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3,
		@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
		@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
		@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
		@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
		@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
		@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
		@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10, @newCentralTelephone
	)
	END TRY
	BEGIN CATCH
--In the catch block we repeat the code in the try block but here the success column will be 0 if there is any failure in the process
	INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [SourceTable], [ErrorMessage], [LogTime], [Name], [Gender], [Region], [Date_Of_Birth],
		[Address], [Mobile_Number],[CreatedOn],[Total_Amount],[Maintenance],[installation],[Complaint],
		[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
		[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
		[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
		[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
		[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
		[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
		[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
		[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
		[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
		[Category10], [Brand10], [WorkOrder10], [Warrantydate10], [CentralTelephone]
	)
    VALUES
	(
		@ldv_customersapid, 'Insert CustProfile2023', 0, 'CustProfile2023', ERROR_MESSAGE(), GETDATE(), @newnames, @newGender, @newRegion, @newDate_Of_Birth,
		@newAddress, @newMobile_Number, @newCustomerCreatedOn, @newTotal_Amount, @newMantinance, @newinstallation, @newComplaint,
		@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
		@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
		@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3,
		@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
		@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
		@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
		@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
		@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
		@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
		@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10, @newCentralTelephone
	)
	END CATCH
	END
	FETCH NEWCRMCursor INTO
		@ldv_customersapid, @newnames, @newGender, @newRegion, @newAddress, @newCustomerCreatedOn,
		@newMobile_Number, @newDate_Of_Birth, @newTotal_Amount, @newNational_ID, @newinstallation, @newMantinance, @newComplaint,
		@newCategory1, @newBrand1, @newWorkOrder1, @newWarrantydate1,
		@newCategory2, @newBrand2, @newWorkOrder2, @newWarrantydate2,
		@newCategory3, @newBrand3, @newWorkOrder3, @newWarrantydate3, 
		@newCategory4, @newBrand4, @newWorkOrder4, @newWarrantydate4,
		@newCategory5, @newBrand5, @newWorkOrder5, @newWarrantydate5,
		@newCategory6, @newBrand6, @newWorkOrder6, @newWarrantydate6,
		@newCategory7, @newBrand7, @newWorkOrder7, @newWarrantydate7,
		@newCategory8, @newBrand8, @newWorkOrder8, @newWarrantydate8,
		@newCategory9, @newBrand9, @newWorkOrder9, @newWarrantydate9,
		@newCategory10, @newBrand10, @newWorkOrder10, @newWarrantydate10,
		@newCentralTelephone, @newCategory11flag
	END
	CLOSE NEWCRMCursor;
    DEALLOCATE NEWCRMCursor;
	INSERT INTO [dbo].[DeltaStatistics]   -- Here , we insert the number of rows updated carried by @Unew var in [DeltaStatistics] table
	(
		[Action], [sourcetable], [Count], [Date]
	)
    SELECT
		'Update', 'CustProfile2023', @Unew, GETDATE()
	INSERT INTO [dbo].[DeltaStatistics]   -- Here , we insert the number of rows inserted carried by @Inew var in [DeltaStatistics] table
	(
		[Action], [sourcetable], [Count], [Date]
	)
    SELECT
		'Insert', 'CustProfile2023', @Inew, GETDATE()
------------------------------------------------------------------------
------------------------------------------------------------------------
--Declare a variable for each column you need to be updated or inserted from [S4HanaSales2023] table with the same data type 
DECLARE @CurrentCustomerID NVARCHAR(MAX), @Name NVARCHAR(MAX), @Phone NVARCHAR(MAX), @City NVARCHAR(MAX),
		@SAPAddress NVARCHAR(MAX), @CreateDate NVARCHAR(MAX), @SalesQuantity DECIMAL(38, 18), @SalesValue DECIMAL(38, 18),
		@u INT = 0,    -- this variable for counting the number of rows updated to insert in DeltaStatistics table
	    @i INT = 0;    -- this variable for counting the number of rows inserted to insert in DeltaStatistics table
	
DECLARE CustomerCursor CURSOR FOR
--The cursor select statement 
SELECT
	[CustomerSapID],
	[Name],
	[Phone],
	[City],
	[Address],
	[CreateDate],
	[SalesQuantity],
	[SalesValue]
FROM
	[dbo].[S4HanaSales2023]
FOR UPDATE

OPEN CustomerCursor;
--Cursor FETCH statement 
FETCH CustomerCursor INTO
	@CurrentCustomerID, @Name, @Phone, @City, @SAPAddress, @CreateDate, @SalesQuantity, @SalesValue;
WHILE @@FETCH_STATUS = 0
--As we update and insert from this table we use if exists to detect rows for update and if not exists to detect rows for insert

    BEGIN
	IF EXISTS  --if rows existed in [S4HanaSales2023] and there are differences between them and cx, we need to update them
	(
		SELECT
			[NewSapId]
		FROM [dbo].[CustomerExperience]
		WHERE [NewSapId] = @CurrentCustomerID
		AND COALESCE([Gr], '') != 'G'    
		AND [Exclude_This] IS NULL
		AND
		(
			COALESCE([SalesQuantity], 0) <> CONVERT(INT, @SalesQuantity)
			OR
			COALESCE([SalesValue], 0) <> CONVERT(decimal(38, 2), @SalesValue)
		)
	)
	BEGIN
    BEGIN TRY
/*
In the try block we do the update process by assigning the variables to the columns and then insert into the MDelatLog table (
each row data and its success status(which is 1 in try block))
*/
    UPDATE [dbo].[CustomerExperience]
	SET
		[SalesQuantity] = CONVERT(INT, @SalesQuantity),
		[SalesValue] = CONVERT(DECIMAL(38, 2), @SalesValue),
		[Modified_On] = GETDATE()
    WHERE
		[NewSapId] = @CurrentCustomerID
	AND COALESCE([Gr], '') != 'G'    --Delta doesn't affect any G
	AND [Exclude_This] IS NULL

	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

	SET @u = @u + 1   --Look! for each row updated , we increment this variable

    INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [LogTime], [SourceTable], [Name], [Mobile_Number], [Region],
		[Address], [CreatedOn], [SalesQuantity], [SalesValue]
	)
	VALUES
	(
		@CurrentCustomerID, 'Update SAP2023', 1, GETDATE(), 'S4HanaSales2023', @Name, @Phone, @City, @SAPAddress,
		TRY_CONVERT(DATE, @CreateDate, 104), CONVERT(INT, @SalesQuantity), CONVERT(DECIMAL(38, 2), @SalesValue)
	)		
	END TRY
    BEGIN CATCH
--In the catch block we repeat the code in the try block but here the success column will be 0 if there is any failure in the process
    INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [ErrorMessage], [LogTime], [SourceTable], [Name], [Mobile_Number], [Region],
		[Address], [CreatedOn], [SalesQuantity], [SalesValue]
	)
	VALUES
	(
		@CurrentCustomerID, 'Update SAP2023', 0, ERROR_MESSAGE(), GETDATE(), 'S4HanaSales2023', @Name, @Phone, @City, @SAPAddress,
		TRY_CONVERT(DATE, @CreateDate, 104), CONVERT(INT, @SalesQuantity), CONVERT(DECIMAL(38, 2), @SalesValue)
	);
	END CATCH;
	END
	IF NOT EXISTS     --if rows don't exist in cx, we need to insert them
	(
		SELECT
			[NewSapId]
		FROM [dbo].[CustomerExperience]
		WHERE
			[NewSapId] = @CurrentCustomerID
	)
	BEGIN
	BEGIN TRY
/*
In the try block we do the insert process by assigning the variables to the columns and then insert into the MDelatLog table (
each row data and its success status(which is 1 in try block))
*/
    INSERT INTO [dbo].[CustomerExperience]
	(
		[NewSapId], [names], [Mobile_Number], [Region], [Address], [Createdon], [SalesQuantity], [SalesValue], [Modified_On], [Inserted_On]
	)
	VALUES
	(
		@CurrentCustomerID, @Name, @Phone, @City, @SAPAddress, TRY_CONVERT(DATE, @CreateDate, 104),
		CONVERT(INT, @SalesQuantity), CONVERT(DECIMAL(38, 2), @SalesValue),GETDATE(), GETDATE()
	)
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

	SET @i = @i + 1   --Look! for each row inserted , we increment this variable

	INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [LogTime], [SourceTable], [Name], [Mobile_Number], [Region],
		[Address], [CreatedOn], [SalesQuantity], [SalesValue]
	)
	VALUES
	(
		@CurrentCustomerID, 'Insert SAP2023', 1, GETDATE(), 'S4HanaSales2023',
		@Name, @Phone, @City, @SAPAddress, TRY_CONVERT(DATE, @CreateDate, 104), CONVERT(INT, @SalesQuantity), CONVERT(DECIMAL(38, 2), @SalesValue)
	)
	END TRY
    BEGIN CATCH
--In the catch block we repeat the code in the try block but here the success column will be 0 if there is any failure in the process
    INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [ErrorMessage], [LogTime], [SourceTable], [Name], [Mobile_Number], [Region],
		[Address], [CreatedOn], [SalesQuantity] ,[SalesValue]
	)
    VALUES
	(
		@CurrentCustomerID, 'Insert SAP2023', 0, ERROR_MESSAGE(), GETDATE(), 'S4HanaSales2023', @Name, @Phone, @City, @SAPAddress,
		TRY_CONVERT(DATE, @CreateDate, 104), CONVERT(INT, @SalesQuantity), CONVERT(DECIMAL(38, 2), @SalesValue)
	)
	END CATCH
	END
    FETCH CustomerCursor INTO
		@CurrentCustomerID, @Name, @Phone, @City, @SAPAddress, @CreateDate, @SalesQuantity, @SalesValue;
    END;
    CLOSE CustomerCursor;
    DEALLOCATE CustomerCursor;
	INSERT INTO [dbo].[DeltaStatistics]
	(
		[Action], [sourcetable], [Count], [Date]   -- Here , we insert the number of rows updated carried by @u var in [DeltaStatistics] table
	)
    SELECT
		'Update', 'S4HanaSales2023', @u, GETDATE()
	INSERT INTO [dbo].[DeltaStatistics]
	(
		[Action], [sourcetable], [Count], [Date]    -- Here , we insert the number of rows inserted carried by @i var in [DeltaStatistics] table
	)
    SELECT
		'Insert', 'S4HanaSales2023', @i, GETDATE()
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--Declare a variable for each column you need to be updated from [0-SalesDetailed-Cascade] table with the same data type 
DECLARE @SCustomerID NVARCHAR(450), @Snames NVARCHAR(MAX), @SSalesQuantity DECIMAL(38, 18), @SSalesValue DECIMAL(38,18),
	@SMaterial_ID1 NVARCHAR(MAX), @SMaterial_Type1 NVARCHAR(MAX), @SCategory1 NVARCHAR(MAX), @SCat_Group1 NVARCHAR(MAX), @SBrand1 NVARCHAR(MAX), @SSumQuantity1 DECIMAL(38, 18), @SSumValue1 DECIMAL(38,18),
	@SInvoice1 NVARCHAR(MAX), @SInvoiceDate1 DATE, @SDist_Channel1 NVARCHAR(MAX),
	@SMaterial_ID2 NVARCHAR(MAX), @SMaterial_Type2 NVARCHAR(MAX), @SCategory2 NVARCHAR(MAX), @SCat_Group2 NVARCHAR(MAX), @SBrand2 NVARCHAR(MAX), @SSumQuantity2 DECIMAL(38, 18), @SSumValue2 DECIMAL(38, 18),
	@SInvoice2 NVARCHAR(MAX), @SInvoiceDate2 DATE, @SDist_Channel2 NVARCHAR(MAX),
	@SMaterial_ID3 NVARCHAR(MAX), @SMaterial_Type3 NVARCHAR(MAX), @SCategory3 NVARCHAR(MAX), @SCat_Group3 NVARCHAR(MAX), @SBrand3 NVARCHAR(MAX), @SSumQuantity3 DECIMAL(38, 18), @SSumValue3 DECIMAL(38, 18),
	@SInvoice3 NVARCHAR(MAX), @SInvoiceDate3 DATE, @SDist_Channel3 NVARCHAR(MAX),
	@SMaterial_ID4 NVARCHAR(MAX), @SMaterial_Type4 NVARCHAR(MAX), @SCategory4 NVARCHAR(MAX), @SCat_Group4 NVARCHAR(MAX), @SBrand4 NVARCHAR(MAX), @SSumQuantity4 DECIMAL(38, 18), @SSumValue4 DECIMAL(38, 18),
	@SInvoice4 NVARCHAR(MAX), @SInvoiceDate4 DATE, @SDist_Channel4 NVARCHAR(MAX),
	@SMaterial_ID5 NVARCHAR(MAX), @SMaterial_Type5 NVARCHAR(MAX), @SCategory5 NVARCHAR(MAX), @SCat_Group5 NVARCHAR(MAX), @SBrand5 NVARCHAR(MAX), @SSumQuantity5 DECIMAL(38, 18), @SSumValue5 DECIMAL(38, 18),
	@SInvoice5 NVARCHAR(MAX), @SInvoiceDate5 DATE, @SDist_Channel5 NVARCHAR(MAX),
	@SMaterial_ID6 NVARCHAR(MAX), @SMaterial_Type6 NVARCHAR(MAX), @SCategory6 NVARCHAR(MAX), @SCat_Group6 NVARCHAR(MAX), @SBrand6 NVARCHAR(MAX), @SSumQuantity6 DECIMAL(38, 18), @SSumValue6 DECIMAL(38, 18),
	@SInvoice6 NVARCHAR(MAX), @SInvoiceDate6 DATE, @SDist_Channel6 NVARCHAR(MAX),
	@SMaterial_ID7 NVARCHAR(MAX), @SMaterial_Type7 NVARCHAR(MAX), @SCategory7 NVARCHAR(MAX), @SCat_Group7 NVARCHAR(MAX), @SBrand7 NVARCHAR(MAX), @SSumQuantity7 DECIMAL(38, 18), @SSumValue7 DECIMAL(38, 18),
	@SInvoice7 NVARCHAR(MAX), @SInvoiceDate7 DATE, @SDist_Channel7 NVARCHAR(MAX),
	@SMaterial_ID8 NVARCHAR(MAX), @SMaterial_Type8 NVARCHAR(MAX), @SCategory8 NVARCHAR(MAX), @SCat_Group8 NVARCHAR(MAX), @SBrand8 NVARCHAR(MAX), @SSumQuantity8 DECIMAL(38, 18), @SSumValue8 DECIMAL(38, 18),
	@SInvoice8 NVARCHAR(MAX), @SInvoiceDate8 DATE, @SDist_Channel8 NVARCHAR(MAX),
	@SMaterial_ID9 NVARCHAR(MAX), @SMaterial_Type9 NVARCHAR(MAX), @SCategory9 NVARCHAR(MAX), @SCat_Group9 NVARCHAR(MAX), @SBrand9 NVARCHAR(MAX), @SSumQuantity9 DECIMAL(38, 18), @SSumValue9 DECIMAL(38, 18),
	@SInvoice9 NVARCHAR(MAX), @SInvoiceDate9 DATE, @SDist_Channel9 NVARCHAR(MAX),
	@SMaterial_ID10 NVARCHAR(MAX), @SMaterial_Type10 NVARCHAR(MAX), @SCategory10 NVARCHAR(MAX), @SCat_Group10 NVARCHAR(MAX), @SBrand10 NVARCHAR(MAX), @SSumQuantity10 DECIMAL(38, 18), @SSumValue10 DECIMAL(38, 18),
	@SInvoice10 NVARCHAR(MAX), @SInvoiceDate10 DATE, @SDist_Channel10 NVARCHAR(MAX),@flag INT, 
	@Su INT = 0     -- this variable for counting the number of rows updated to insert in DeltaStatistics table
DECLARE SalesCascadeCursor CURSOR FOR
/*
The cursor select statement in which we join [CustomerExperience] and [0-SalesDetailed-Cascade] for comparison and detect 
different values that need to be updated 
*/
SELECT
	SC.[CustomerSAPID], SC.[names], SC.[SalesQuantity], SC.[SalesValue],
	SC.[Material_ID1], SC.[Material_Type1], SC.[Category1], SC.[Cat_Group1], SC.[Brand1], SC.[SumQuantity1], SC.[SumValue1], SC.[Invoice1], SC.[InvoiceDate1], SC.[Dist_Channel1],
	SC.[Material_ID2], SC.[Material_Type2], SC.[Category2], SC.[Cat_Group2], SC.[Brand2], SC.[SumQuantity2], SC.[SumValue2], SC.[Invoice2], SC.[InvoiceDate2], SC.[Dist_Channel2],
	SC.[Material_ID3], SC.[Material_Type3], SC.[Category3], SC.[Cat_Group3], SC.[Brand3], SC.[SumQuantity3], SC.[SumValue3], SC.[Invoice3], SC.[InvoiceDate3], SC.[Dist_Channel3],
	SC.[Material_ID4], SC.[Material_Type4], SC.[Category4], SC.[Cat_Group4], SC.[Brand4], SC.[SumQuantity4], SC.[SumValue4], SC.[Invoice4], SC.[InvoiceDate4], SC.[Dist_Channel4],
	SC.[Material_ID5], SC.[Material_Type5], SC.[Category5], SC.[Cat_Group5], SC.[Brand5], SC.[SumQuantity5], SC.[SumValue5], SC.[Invoice5], SC.[InvoiceDate5], SC.[Dist_Channel5],
	SC.[Material_ID6], SC.[Material_Type6], SC.[Category6], SC.[Cat_Group6], SC.[Brand6], SC.[SumQuantity6], SC.[SumValue6], SC.[Invoice6], SC.[InvoiceDate6], SC.[Dist_Channel6],
	SC.[Material_ID7], SC.[Material_Type7], SC.[Category7], SC.[Cat_Group7], SC.[Brand7], SC.[SumQuantity7], SC.[SumValue7], SC.[Invoice7], SC.[InvoiceDate7], SC.[Dist_Channel7],
	SC.[Material_ID8], SC.[Material_Type8], SC.[Category8], SC.[Cat_Group8], SC.[Brand8], SC.[SumQuantity8], SC.[SumValue8], SC.[Invoice8], SC.[InvoiceDate8], SC.[Dist_Channel8],
	SC.[Material_ID9], SC.[Material_Type9], SC.[Category9], SC.[Cat_Group9], SC.[Brand9], SC.[SumQuantity9], SC.[SumValue9], SC.[Invoice9], SC.[InvoiceDate9], SC.[Dist_Channel9],
	SC.[Material_ID10], SC.[Material_Type10], SC.[Category10], SC.[Cat_Group10], SC.[Brand10], SC.[SumQuantity10], SC.[SumValue10], SC.[Invoice10], SC.[InvoiceDate10], SC.[Dist_Channel10], SC.[Flag]
FROM
	[dbo].[0-SalesDetailed-Cascade] AS SC INNER JOIN [dbo].[CustomerExperience] AS cx
ON
	SC.[CustomerSAPID] = cx.[NewSapId]
AND COALESCE(cx.[Gr], '') != 'G'
AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate1], 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate2], 104), '') <= GETDATE() 
AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate3], 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate4], 104), '') <= GETDATE() 
AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate5], 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate6], 104), '') <= GETDATE() 
AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate7], 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate8], 104), '') <= GETDATE() 
AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate9], 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, SC.[InvoiceDate10], 104), '') <= GETDATE() 
WHERE
	COALESCE(cx.[Material_ID1], '') <> SC.[Material_ID1]
	OR
	COALESCE(cx.[Material_Type1], '') <> SC.[Material_Type1]
	OR
	COALESCE(cx.[Category_1], '') <> SC.[Category1]
	OR
	COALESCE(cx.[Cat_Group1], '') <> SC.[Cat_Group1]
	OR
	COALESCE(cx.[Brand_1], '') <> SC.[Brand1]
	OR
	COALESCE(cx.[SumQuantity1], '') <> SC.[SumQuantity1] 
	OR
	COALESCE(cx.[SumValue1], '') <> SC.[SumValue1]
	OR
	COALESCE(cx.[Invoice1], '') <> SC.[Invoice1]
	OR
	COALESCE(cx.[InvoiceDate1], '') <> SC.[InvoiceDate1]
	OR
	COALESCE(cx.[Dist_Channel1], '') <> SC.[Dist_Channel1]
	OR
	COALESCE(cx.[Material_ID2], '') <> SC.[Material_ID2]
	OR
	COALESCE(cx.[Material_Type2], '') <> SC.[Material_Type2]
	OR
	COALESCE(cx.[Category_2], '') <> SC.[Category2]
	OR
	COALESCE(cx.[Cat_Group2], '') <> SC.[Cat_Group2]
	OR
	COALESCE(cx.[Brand_2], '') <> SC.[Brand2]
	OR
	COALESCE(cx.[SumQuantity2], '') <> SC.[SumQuantity2] 
	OR
	COALESCE(cx.[SumValue2], '') <> SC.[SumValue2]
	OR
	COALESCE(cx.[Invoice2], '') <> SC.[Invoice2]
	OR
	COALESCE(cx.[InvoiceDate2], '') <> SC.[InvoiceDate2]
	OR
	COALESCE(cx.[Dist_Channel2], '') <> SC.[Dist_Channel2]
	OR
	COALESCE(cx.[Material_ID3], '') <> SC.[Material_ID3]
	OR
	COALESCE(cx.[Material_Type3], '') <> SC.[Material_Type3]
	OR
	COALESCE(cx.[Category_3], '') <> SC.[Category3]
	OR
	COALESCE(cx.[Cat_Group3], '') <> SC.[Cat_Group3]
	OR
	COALESCE(cx.[Brand_3], '') <> SC.[Brand3]
	OR
	COALESCE(cx.[SumQuantity3], '') <> SC.[SumQuantity3] 
	OR
	COALESCE(cx.[SumValue3], '') <> SC.[SumValue3]
	OR
	COALESCE(cx.[Invoice3], '') <> SC.[Invoice3]
	OR
	COALESCE(cx.[InvoiceDate3], '') <> SC.[InvoiceDate3]
	OR
	COALESCE(cx.[Dist_Channel3], '') <> SC.[Dist_Channel3]
	OR
	COALESCE(cx.[Material_ID4], '') <> SC.[Material_ID4]
	OR
	COALESCE(cx.[Material_Type4], '') <> SC.[Material_Type4]
	OR
	COALESCE(cx.[Category_4], '') <> SC.[Category4]
	OR
	COALESCE(cx.[Cat_Group4], '') <> SC.[Cat_Group4]
	OR
	COALESCE(cx.[Brand_4], '') <> SC.[Brand4]
	OR
	COALESCE(cx.[SumQuantity4], '') <> SC.[SumQuantity4] 
	OR
	COALESCE(cx.[SumValue4], '') <> SC.[SumValue4]
	OR
	COALESCE(cx.[Invoice4], '') <> SC.[Invoice4]
	OR
	COALESCE(cx.[InvoiceDate4], '') <> SC.[InvoiceDate4]
	OR
	COALESCE(cx.[Dist_Channel4], '') <> SC.[Dist_Channel4]
	OR
	COALESCE(cx.[Material_ID5], '') <> SC.[Material_ID5]
	OR
	COALESCE(cx.[Material_Type5], '') <> SC.[Material_Type5]
	OR
	COALESCE(cx.[Category_5], '') <> SC.[Category5]
	OR
	COALESCE(cx.[Cat_Group5], '') <> SC.[Cat_Group5]
	OR
	COALESCE(cx.[Brand_5], '') <> SC.[Brand5]
	OR
	COALESCE(cx.[SumQuantity5], '') <> SC.[SumQuantity5] 
	OR
	COALESCE(cx.[SumValue5], '') <> SC.[SumValue5]
	OR
	COALESCE(cx.[Invoice5], '') <> SC.[Invoice5]
	OR
	COALESCE(cx.[InvoiceDate5], '') <> SC.[InvoiceDate5]
	OR
	COALESCE(cx.[Dist_Channel5], '') <> SC.[Dist_Channel5]
	OR
	COALESCE(cx.[Material_ID6], '') <> SC.[Material_ID6]
	OR
	COALESCE(cx.[Material_Type6], '') <> SC.[Material_Type6]
	OR
	COALESCE(cx.[Category_6], '') <> SC.[Category6]
	OR
	COALESCE(cx.[Cat_Group6], '') <> SC.[Cat_Group6]
	OR
	COALESCE(cx.[Brand_6], '') <> SC.[Brand6]
	OR
	COALESCE(cx.[SumQuantity6], '') <> SC.[SumQuantity6] 
	OR
	COALESCE(cx.[SumValue6], '') <> SC.[SumValue6]
	OR
	COALESCE(cx.[Invoice6], '') <> SC.[Invoice6]
	OR
	COALESCE(cx.[InvoiceDate6], '') <> SC.[InvoiceDate6]
	OR
	COALESCE(cx.[Dist_Channel6], '') <> SC.[Dist_Channel6]
	OR
	COALESCE(cx.[Material_ID7], '') <> SC.[Material_ID7]
	OR
	COALESCE(cx.[Material_Type7], '') <> SC.[Material_Type7]
	OR
	COALESCE(cx.[Category_7], '') <> SC.[Category7]
	OR
	COALESCE(cx.[Cat_Group7], '') <> SC.[Cat_Group7]
	OR
	COALESCE(cx.[Brand_7], '') <> SC.[Brand7]
	OR
	COALESCE(cx.[SumQuantity7], '') <> SC.[SumQuantity7] 
	OR
	COALESCE(cx.[SumValue7], '') <> SC.[SumValue7]
	OR
	COALESCE(cx.[Invoice7], '') <> SC.[Invoice7]
	OR
	COALESCE(cx.[InvoiceDate7], '') <> SC.[InvoiceDate7]
	OR
	COALESCE(cx.[Dist_Channel7], '') <> SC.[Dist_Channel7]
	OR
	COALESCE(cx.[Material_ID8], '') <> SC.[Material_ID8]
	OR
	COALESCE(cx.[Material_Type8], '') <> SC.[Material_Type8]
	OR
	COALESCE(cx.[Category_8], '') <> SC.[Category8]
	OR
	COALESCE(cx.[Cat_Group8], '') <> SC.[Cat_Group8]
	OR
	COALESCE(cx.[Brand_8], '') <> SC.[Brand8]
	OR
	COALESCE(cx.[SumQuantity8], '') <> SC.[SumQuantity8] 
	OR
	COALESCE(cx.[SumValue8], '') <> SC.[SumValue8]
	OR
	COALESCE(cx.[Invoice8], '') <> SC.[Invoice8]
	OR
	COALESCE(cx.[InvoiceDate8], '') <> SC.[InvoiceDate8]
	OR
	COALESCE(cx.[Dist_Channel8], '') <> SC.[Dist_Channel8]
	OR
	COALESCE(cx.[Material_ID9], '') <> SC.[Material_ID9]
	OR
	COALESCE(cx.[Material_Type9], '') <> SC.[Material_Type9]
	OR
	COALESCE(cx.[Category_9], '') <> SC.[Category9]
	OR
	COALESCE(cx.[Cat_Group9], '') <> SC.[Cat_Group9]
	OR
	COALESCE(cx.[Brand_9], '') <> SC.[Brand9]
	OR
	COALESCE(cx.[SumQuantity9], '') <> SC.[SumQuantity9] 
	OR
	COALESCE(cx.[SumValue9], '') <> SC.[SumValue9]
	OR
	COALESCE(cx.[Invoice9], '') <> SC.[Invoice9]
	OR
	COALESCE(cx.[InvoiceDate9], '') <> SC.[InvoiceDate9]
	OR
	COALESCE(cx.[Dist_Channel9], '') <> SC.[Dist_Channel9]
	OR
	COALESCE(cx.[Material_ID10], '') <> SC.[Material_ID10]
	OR
	COALESCE(cx.[Material_Type10], '') <> SC.[Material_Type10]
	OR
	COALESCE(cx.[Category_10], '') <> SC.[Category10]
	OR
	COALESCE(cx.[Cat_Group10], '') <> SC.[Cat_Group10]
	OR
	COALESCE(cx.[Brand_10], '') <> SC.[Brand10]
	OR
	COALESCE(cx.[SumQuantity10], '') <> SC.[SumQuantity10] 
	OR
	COALESCE(cx.[SumValue10], '') <> SC.[SumValue10]
	OR
	COALESCE(cx.[Invoice10], '') <> SC.[Invoice10]
	OR
	COALESCE(cx.[InvoiceDate10], '') <> SC.[InvoiceDate10]
	OR
	COALESCE(cx.[Dist_Channel10], '') <> SC.[Dist_Channel10] 
	OR
	COALESCE(cx.[Flag_SalesCategory11], '') <> SC.Flag 

FOR UPDATE

OPEN SalesCascadeCursor;
--Cursor FETCH statement 
FETCH SalesCascadeCursor INTO
	@SCustomerID, @Snames, @SSalesQuantity, @SSalesValue,
	@SMaterial_ID1, @SMaterial_Type1, @SCategory1, @SCat_Group1, @SBrand1, @SSumQuantity1, @SSumValue1, @SInvoice1, @SInvoiceDate1, @SDist_Channel1,
	@SMaterial_ID2, @SMaterial_Type2, @SCategory2, @SCat_Group2, @SBrand2, @SSumQuantity2, @SSumValue2, @SInvoice2, @SInvoiceDate2, @SDist_Channel2,
	@SMaterial_ID3, @SMaterial_Type3, @SCategory3, @SCat_Group3, @SBrand3, @SSumQuantity3, @SSumValue3, @SInvoice3, @SInvoiceDate3, @SDist_Channel3,
	@SMaterial_ID4, @SMaterial_Type4, @SCategory4, @SCat_Group4, @SBrand4, @SSumQuantity4, @SSumValue4, @SInvoice4, @SInvoiceDate4, @SDist_Channel4,
	@SMaterial_ID5, @SMaterial_Type5, @SCategory5, @SCat_Group5, @SBrand5, @SSumQuantity5, @SSumValue5, @SInvoice5, @SInvoiceDate5, @SDist_Channel5,
	@SMaterial_ID6, @SMaterial_Type6, @SCategory6, @SCat_Group6, @SBrand6, @SSumQuantity6, @SSumValue6, @SInvoice6, @SInvoiceDate6, @SDist_Channel6,
	@SMaterial_ID7, @SMaterial_Type7, @SCategory7, @SCat_Group7, @SBrand7, @SSumQuantity7, @SSumValue7, @SInvoice7, @SInvoiceDate7, @SDist_Channel7,
	@SMaterial_ID8, @SMaterial_Type8, @SCategory8, @SCat_Group8, @SBrand8, @SSumQuantity8, @SSumValue8, @SInvoice8, @SInvoiceDate8, @SDist_Channel8,
	@SMaterial_ID9, @SMaterial_Type9, @SCategory9, @SCat_Group9, @SBrand9, @SSumQuantity9, @SSumValue9, @SInvoice9, @SInvoiceDate9, @SDist_Channel9,
	@SMaterial_ID10, @SMaterial_Type10, @SCategory10, @SCat_Group10, @SBrand10, @SSumQuantity10, @SSumValue10,@SInvoice10, @SInvoiceDate10, @SDist_Channel10, @flag
WHILE @@FETCH_STATUS = 0
    BEGIN
	BEGIN TRY
/*
In the try block we do the update process by assigning the variables to the columns and then insert into the MDelatLog table (
each row data and its success status(which is 1 in try block))
*/
	UPDATE [dbo].[CustomerExperience]
	SET
		[Material_ID1] = @SMaterial_ID1, [Material_Type1] = @SMaterial_Type1, [Category_1] = @SCategory1, [Cat_Group1] = @SCat_Group1, [Brand_1] = @SBrand1, [SumQuantity1] = @SSumQuantity1, [SumValue1] = @SSumValue1, [Invoice1] = @SInvoice1, [InvoiceDate1] = @SInvoiceDate1, [Dist_Channel1] = @SDist_Channel1,
		[Material_ID2] = @SMaterial_ID2, [Material_Type2] = @SMaterial_Type2, [Category_2] = @SCategory2, [Cat_Group2] = @SCat_Group2, [Brand_2] = @SBrand2, [SumQuantity2] = @SSumQuantity2, [SumValue2] = @SSumValue2, [Invoice2] = @SInvoice2, [InvoiceDate2] = @SInvoiceDate2, [Dist_Channel2] = @SDist_Channel2,
		[Material_ID3] = @SMaterial_ID3, [Material_Type3] = @SMaterial_Type3, [Category_3] = @SCategory3, [Cat_Group3] = @SCat_Group3, [Brand_3] = @SBrand3, [SumQuantity3] = @SSumQuantity3, [SumValue3] = @SSumValue3, [Invoice3] = @SInvoice3, [InvoiceDate3] = @SInvoiceDate3, [Dist_Channel3] = @SDist_Channel3,
		[Material_ID4] = @SMaterial_ID4, [Material_Type4] = @SMaterial_Type4, [Category_4] = @SCategory4, [Cat_Group4] = @SCat_Group4, [Brand_4] = @SBrand4, [SumQuantity4] = @SSumQuantity4, [SumValue4] = @SSumValue4, [Invoice4] = @SInvoice4, [InvoiceDate4] = @SInvoiceDate4, [Dist_Channel4] = @SDist_Channel4,
		[Material_ID5] = @SMaterial_ID5, [Material_Type5] = @SMaterial_Type5, [Category_5] = @SCategory5, [Cat_Group5] = @SCat_Group5, [Brand_5] = @SBrand5, [SumQuantity5] = @SSumQuantity5, [SumValue5] = @SSumValue5, [Invoice5] = @SInvoice5, [InvoiceDate5] = @SInvoiceDate5, [Dist_Channel5] = @SDist_Channel5,
		[Material_ID6] = @SMaterial_ID6, [Material_Type6] = @SMaterial_Type6, [Category_6] = @SCategory6, [Cat_Group6] = @SCat_Group6, [Brand_6] = @SBrand6, [SumQuantity6] = @SSumQuantity6, [SumValue6] = @SSumValue6, [Invoice6] = @SInvoice6, [InvoiceDate6] = @SInvoiceDate6, [Dist_Channel6] = @SDist_Channel6,
		[Material_ID7] = @SMaterial_ID7, [Material_Type7] = @SMaterial_Type7, [Category_7] = @SCategory7, [Cat_Group7] = @SCat_Group7, [Brand_7] = @SBrand7, [SumQuantity7] = @SSumQuantity7, [SumValue7] = @SSumValue7, [Invoice7] = @SInvoice7, [InvoiceDate7] = @SInvoiceDate7, [Dist_Channel7] = @SDist_Channel7,
		[Material_ID8] = @SMaterial_ID8, [Material_Type8] = @SMaterial_Type8, [Category_8] = @SCategory8, [Cat_Group8] = @SCat_Group8, [Brand_8] = @SBrand8, [SumQuantity8] = @SSumQuantity8, [SumValue8] = @SSumValue8, [Invoice8] = @SInvoice8, [InvoiceDate8] = @SInvoiceDate8, [Dist_Channel8] = @SDist_Channel8,
		[Material_ID9] = @SMaterial_ID9, [Material_Type9] = @SMaterial_Type9, [Category_9] = @SCategory9, [Cat_Group9] = @SCat_Group9, [Brand_9] = @SBrand9, [SumQuantity9] = @SSumQuantity9, [SumValue9] = @SSumValue9, [Invoice9] = @SInvoice9, [InvoiceDate9] = @SInvoiceDate9, [Dist_Channel9] = @SDist_Channel9,
		[Material_ID10] = @SMaterial_ID10, [Material_Type10] = @SMaterial_Type10, [Category_10] = @SCategory10, [Cat_Group10] = @SCat_Group10, [Brand_10] = @SBrand10, [SumQuantity10] = @SSumQuantity10, [SumValue10] = @SSumValue10, [Invoice10] = @SInvoice10, [InvoiceDate10] = @SInvoiceDate10, [Dist_Channel10] = @SDist_Channel10,
		[Flag_SalesCategory11] = @flag, [Modified_On] = GETDATE()
	WHERE
		[NewSapId] = @SCustomerID
	AND COALESCE([Gr], '') != 'G'   --Delta doesn't affect any G
	AND COALESCE(TRY_CONVERT(date, @SInvoiceDate1, 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, @SInvoiceDate2, 104), '') <= GETDATE() 
	AND COALESCE(TRY_CONVERT(date, @SInvoiceDate3, 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, @SInvoiceDate4, 104), '') <= GETDATE() 
	AND COALESCE(TRY_CONVERT(date, @SInvoiceDate5, 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, @SInvoiceDate6, 104), '') <= GETDATE() 
	AND COALESCE(TRY_CONVERT(date, @SInvoiceDate7, 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, @SInvoiceDate8, 104), '') <= GETDATE() 
	AND COALESCE(TRY_CONVERT(date, @SInvoiceDate9, 104), '') <= GETDATE() AND COALESCE(TRY_CONVERT(date, @SInvoiceDate10, 104), '') <= GETDATE() 

	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT


    SET @Su = @Su + 1   --Look! for each row updated , we increment this variable

    INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [LogTime], [SourceTable], [Name],
		[Category_1], [Cat_Group1], [Brand_1], [SumQuantity1], [SumValue1], [Invoice1], [InvoiceDate1],
		[Category2], [Cat_Group2], [Brand2], [SumQuantity2],[SumValue2], [Invoice2], [InvoiceDate2],
		[Category3], [Cat_Group3], [Brand3], [SumQuantity3],[SumValue3], [Invoice3], [InvoiceDate3],
		[Category4], [Cat_Group4], [Brand4], [SumQuantity4],[SumValue4], [Invoice4], [InvoiceDate4],
		[Category5], [Cat_Group5], [Brand5], [SumQuantity5],[SumValue5], [Invoice5], [InvoiceDate5],
		[Category6], [Cat_Group6], [Brand6], [SumQuantity6],[SumValue6], [Invoice6], [InvoiceDate6],
		[Category7], [Cat_Group7], [Brand7], [SumQuantity7],[SumValue7], [Invoice7], [InvoiceDate7],
		[Category8], [Cat_Group8], [Brand8], [SumQuantity8],[SumValue8], [Invoice8], [InvoiceDate8],
		[Category9], [Cat_Group9], [Brand9], [SumQuantity9],[SumValue9], [Invoice9], [InvoiceDate9],
		[Category10], [Cat_Group10], [Brand10], [SumQuantity10],[SumValue10], [Invoice10], [InvoiceDate10]
	)
    VALUES
	(
		@SCustomerID, 'Update SalesCascade2023', 1, GETDATE(), 'SalesCascade2023', @Snames,
		@SCategory1, @SCat_Group1, @SBrand1, @SSumQuantity1, @SSumValue1, @SInvoice1, @SInvoiceDate1,
		@SCategory2, @SCat_Group2, @SBrand2, @SSumQuantity2, @SSumValue2, @SInvoice2, @SInvoiceDate2,
		@SCategory3, @SCat_Group3, @SBrand3, @SSumQuantity3, @SSumValue3, @SInvoice3, @SInvoiceDate3,
		@SCategory4, @SCat_Group4, @SBrand4, @SSumQuantity4, @SSumValue4, @SInvoice4, @SInvoiceDate4,
		@SCategory5, @SCat_Group5, @SBrand5, @SSumQuantity5, @SSumValue5, @SInvoice5, @SInvoiceDate5,
		@SCategory6, @SCat_Group6, @SBrand6, @SSumQuantity6, @SSumValue6, @SInvoice6, @SInvoiceDate6,
		@SCategory7, @SCat_Group7, @SBrand7, @SSumQuantity7, @SSumValue7, @SInvoice7, @SInvoiceDate7,
		@SCategory8, @SCat_Group8, @SBrand8, @SSumQuantity8, @SSumValue8, @SInvoice8, @SInvoiceDate8,
		@SCategory9, @SCat_Group9, @SBrand9, @SSumQuantity9, @SSumValue9, @SInvoice9, @SInvoiceDate9,
		@SCategory10, @SCat_Group10, @SBrand10, @SSumQuantity10, @SSumValue10, @SInvoice10, @SInvoiceDate10
	)
    END TRY
    BEGIN CATCH
--In the catch block we repeat the code in the try block but here the success column will be 0 if there is any failure in the process
    INSERT INTO [dbo].[MDeltaLog]
	(
		[NewSapId], [Statement], [Success], [ErrorMessage], [LogTime], [SourceTable], [Name],
		[Category_1], [Cat_Group1], [Brand_1], [SumQuantity1], [SumValue1], [Invoice1], [InvoiceDate1],
		[Category2], [Cat_Group2], [Brand2], [SumQuantity2], [SumValue2], [Invoice2], [InvoiceDate2],
		[Category3], [Cat_Group3], [Brand3], [SumQuantity3], [SumValue3], [Invoice3], [InvoiceDate3],
		[Category4], [Cat_Group4], [Brand4], [SumQuantity4], [SumValue4], [Invoice4], [InvoiceDate4],
		[Category5], [Cat_Group5], [Brand5], [SumQuantity5], [SumValue5], [Invoice5], [InvoiceDate5],
		[Category6], [Cat_Group6], [Brand6], [SumQuantity6], [SumValue6], [Invoice6], [InvoiceDate6],
		[Category7], [Cat_Group7], [Brand7], [SumQuantity7], [SumValue7], [Invoice7], [InvoiceDate7],
		[Category8], [Cat_Group8], [Brand8], [SumQuantity8], [SumValue8], [Invoice8], [InvoiceDate8],
		[Category9], [Cat_Group9], [Brand9], [SumQuantity9], [SumValue9], [Invoice9], [InvoiceDate9],
		[Category10], [Cat_Group10], [Brand10], [SumQuantity10], [SumValue10], [Invoice10], [InvoiceDate10]
	)
    VALUES
	(
		@SCustomerID, 'Update SalesCascade2023', 0, ERROR_MESSAGE(), GETDATE(), 'SalesCascade2023', @Snames,
		@SCategory1, @SCat_Group1, @SBrand1, @SSumQuantity1, @SSumValue1, @SInvoice1, @SInvoiceDate1,
		@SCategory2, @SCat_Group2, @SBrand2, @SSumQuantity2, @SSumValue2, @SInvoice2, @SInvoiceDate2,
		@SCategory3, @SCat_Group3, @SBrand3, @SSumQuantity3, @SSumValue3, @SInvoice3, @SInvoiceDate3,
		@SCategory4, @SCat_Group4, @SBrand4, @SSumQuantity4, @SSumValue4, @SInvoice4, @SInvoiceDate4,
		@SCategory5, @SCat_Group5, @SBrand5, @SSumQuantity5, @SSumValue5, @SInvoice5, @SInvoiceDate5,
		@SCategory6, @SCat_Group6, @SBrand6, @SSumQuantity6, @SSumValue6, @SInvoice6, @SInvoiceDate6,
		@SCategory7, @SCat_Group7, @SBrand7, @SSumQuantity7, @SSumValue7, @SInvoice7, @SInvoiceDate7,
		@SCategory8, @SCat_Group8, @SBrand8, @SSumQuantity8, @SSumValue8, @SInvoice8, @SInvoiceDate8,
		@SCategory9, @SCat_Group9, @SBrand9, @SSumQuantity9, @SSumValue9, @SInvoice9, @SInvoiceDate9,
		@SCategory10, @SCat_Group10, @SBrand10, @SSumQuantity10, @SSumValue10, @SInvoice10, @SInvoiceDate10
	)
    END CATCH
    FETCH SalesCascadeCursor INTO
		@SCustomerID, @Snames , @SSalesQuantity ,@SSalesValue ,
		@SMaterial_ID1, @SMaterial_Type1, @SCategory1, @SCat_Group1, @SBrand1, @SSumQuantity1, @SSumValue1, @SInvoice1, @SInvoiceDate1, @SDist_Channel1,
		@SMaterial_ID2, @SMaterial_Type2, @SCategory2, @SCat_Group2, @SBrand2, @SSumQuantity2, @SSumValue2, @SInvoice2, @SInvoiceDate2, @SDist_Channel2,
		@SMaterial_ID3, @SMaterial_Type3, @SCategory3, @SCat_Group3, @SBrand3, @SSumQuantity3, @SSumValue3, @SInvoice3, @SInvoiceDate3, @SDist_Channel3,
		@SMaterial_ID4, @SMaterial_Type4, @SCategory4, @SCat_Group4, @SBrand4, @SSumQuantity4, @SSumValue4, @SInvoice4, @SInvoiceDate4, @SDist_Channel4,
		@SMaterial_ID5, @SMaterial_Type5, @SCategory5, @SCat_Group5, @SBrand5, @SSumQuantity5, @SSumValue5, @SInvoice5, @SInvoiceDate5, @SDist_Channel5,
		@SMaterial_ID6, @SMaterial_Type6, @SCategory6, @SCat_Group6, @SBrand6, @SSumQuantity6, @SSumValue6, @SInvoice6, @SInvoiceDate6, @SDist_Channel6,
		@SMaterial_ID7, @SMaterial_Type7, @SCategory7, @SCat_Group7, @SBrand7, @SSumQuantity7, @SSumValue7, @SInvoice7, @SInvoiceDate7, @SDist_Channel7,
		@SMaterial_ID8, @SMaterial_Type8, @SCategory8, @SCat_Group8, @SBrand8, @SSumQuantity8, @SSumValue8, @SInvoice8, @SInvoiceDate8, @SDist_Channel8,
		@SMaterial_ID9, @SMaterial_Type9, @SCategory9, @SCat_Group9, @SBrand9, @SSumQuantity9, @SSumValue9, @SInvoice9, @SInvoiceDate9, @SDist_Channel9,
		@SMaterial_ID10, @SMaterial_Type10, @SCategory10, @SCat_Group10, @SBrand10, @SSumQuantity10, @SSumValue10, @SInvoice10, @SInvoiceDate10, @SDist_Channel10, @flag
	END
	CLOSE SalesCascadeCursor
	DEALLOCATE SalesCascadeCursor
	INSERT INTO [dbo].[DeltaStatistics]
	(
		[Action], [sourcetable], [Count], [Date]  -- Here , we insert the number of rows updated carried by @Su var in [DeltaStatistics] table
	)
    SELECT
		'Update', 'SalesCascade2023', @Su, GETDATE()

END;