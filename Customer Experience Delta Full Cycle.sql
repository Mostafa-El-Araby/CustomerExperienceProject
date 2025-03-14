USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[AUTO_NUMBERS]    Script Date: 9/29/2024 10:34:44 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[AUTO_NUMBERS]   
/*
-This proc includes all the procedures of the delta cycle
-Each proc has try and catch block to handle errors, so it is called two times 
-Each proc has output parameter for number of rows affected 
-The logs of the run saved in [AutomationTimes] table that contains [ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
and in case of failure the catch block insert the [ErrorMessage] in [AutomationTimes] table
*/ 
AS
BEGIN
DECLARE @StartTime datetime, @EndTime datetime, @count INT   --variables we need for each procedure info
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Cleansing] @RowsAffected = @count OUTPUT     --Cleansing City and Address in S4HanaSales2023 table to replace '#' with null
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Cleansing IN SALES', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Cleansing] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO AutomationTimes
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Cleansing IN SALES', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
/*Cleansing Mobile Numbers in source tables by [UpdateColumn] procedure (parametrized procedure takes 2 parameters : Column name , table name)including:
-Remove leading +2
-Remove English characters
-Remove Arabic characters
-Remove special characters
-Concat missed leading zeros 
-Set mobile numbers like '00', '000', '0000','00000', '000000', '0000000', '00000000','000000000', '0000000000' and so on to NULL
-Set Mobile Numbers to NULL where [Mobile_Numbers] = ''
*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[UpdateColumn] 'Mobile_Number', 'Custprofile365', @RowsAffected = @count OUTPUT   --Cleansing Mobile Number column in Custprofile365
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Mobiles Cleansing in Old CRM', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[UpdateColumn] 'Mobile_Number', 'Custprofile365', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Mobiles Cleansing in Old CRM', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
----------------------------------------------------------------------------------
BEGIN TRY
	SET @StartTime = GETDATE() 
	EXEC [dbo].[UpdateColumn] 'Mobile_Number', 'Custprofile2023', @RowsAffected = @count OUTPUT   --Cleansing Mobile Number column in Custprofile2023
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Mobiles Cleansing in New CRM', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[UpdateColumn] 'Mobile_Number', 'Custprofile2023', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Mobiles Cleansing in New CRM', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
----------------------------------------------------------------------------------
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[UpdateColumn] 'Phone', 'S4HanaSales2023', @RowsAffected = @count OUTPUT    --Cleansing Mobile Number column in S4HanaSales2023
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Mobiles Cleansing in Sales', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[UpdateColumn] 'Phone', 'S4HanaSales2023', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Mobiles Cleansing in Sales', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
----------------------------------------------------------------------------------
/*Cleansing Names in source tables by [CleansingNames] procedure (parametrized procedure takes 2 parameters : Column name , table name )including:
Remove special characters at the beginning of the name 
*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[CleansingNames] 'names', 'Custprofile365', @RowsAffected = @count OUTPUT   --Names cleansing in Custprofile365
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Names Cleansing in Old CRM', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[CleansingNames] 'names', 'Custprofile365', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Names Cleansing in Old CRM', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[CleansingNames] 'names', 'Custprofile2023', @RowsAffected = @count OUTPUT   --Names cleansing in Custprofile2023
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Names Cleansing in New CRM', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[CleansingNames] 'names', 'Custprofile2023', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Names Cleansing in New CRM', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[CleansingNames] 'Name', 'S4HanaSales2023', @RowsAffected = @count OUTPUT   --Names cleansing in S4HanaSales2023
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Names Cleansing in Sales', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[CleansingNames] 'Name', 'S4HanaSales2023', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Names Cleansing in Sales', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
/*Cleansing Date of Birth in source tables by [DOB_Cleansing] procedure (parametrized procedure takes 2 parameters : Column name , table name )including:
Set Date of Birth to NULL for customers under 16 years old or over 100 years old 
We have Date of Birth only in CRM data
*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[DOB_Cleansing] 'Date_Of_Birth', 'Custprofile365', @RowsAffected = @count OUTPUT   --Date of Birth cleansing in Custprofile365
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Date_Of_Birth Cleansing in Old CRM ', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[DOB_Cleansing] 'Date_Of_Birth', 'Custprofile365', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Date_Of_Birth Cleansing in Old CRM', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[DOB_Cleansing] 'Date_Of_Birth', 'Custprofile2023', @RowsAffected = @count OUTPUT   --Date of Birth cleansing in Custprofile2023
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Date_Of_Birth Cleansing in New CRM', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[DOB_Cleansing] 'Date_Of_Birth', 'Custprofile2023', @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Date_Of_Birth Cleansing in New CRM', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
/*Cleansing Region in source tables by [Region_Cleansing_Before] procedure including:
-Replace 0 with Null in address & region columns in Custprofile365,Custprofile2023
-Replace 0 , # with Null in address & city columns in S4HanaSales2023
*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Region_Cleansing_Before] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Region Cleansing in source table', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Region_Cleansing_Before] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Region Cleansing in source table', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
/*Run Delta(Merge CRM and Sales Data in CustomerExperience) including:
-logs to monitor the transfer process between source tables and destination inserted into MDeltalog table 
-DeltaStatistics table to show the number of updated and inserted rows from each source table
*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[FullDeltaWithoutFlag] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Delta', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[FullDeltaWithoutFlag] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Delta', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Freeze invalid mobile numbers(don't begin with 01 , not equal 11 digits , null mobile numbers ) in CustomerExperience
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[FREEZED_MOB] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Freezed Mobile Numbers', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
    EXEC [dbo].[FREEZED_MOB] @RowsAffected = @count OUTPUT	
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Freezed Mobile Numbers', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
/*Used to set [Gender_Update_Flag] column to 1 for customers whose gender is company from source 
If the gender in source table is company , we don't modify it , we assume that it is right and these customers are excluded from the detect gender proc*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Companies_From_Source] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Companies From Source', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Companies_From_Source] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Companies From Source', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Detect Companies in CustomerExperience via joining with [Company] lookup
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Companies] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Detect Companies', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Companies] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Detect Companies', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Detect titles in CustomerExperience via joining with [Titles_Lookup] table
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Titles] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Detect Titles', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Titles] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Detect Titles', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Detect gender for customers with titles in CustomerExperience
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Gender_For_Customers_With_Title] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Detect Gender for Titles', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Gender_For_Customers_With_Title] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Detect Gender for Titles', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
/*Detect gender in CustomerExperience via joining with [Gender_Lookup] table and use [Gender_Update_Flag] column 
to mark the affected rows(customers whose gender was detected)*/
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Gender] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Detect Gender', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Detect_Gender] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Detect Gender', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Freeze customers whose gender couldn't be detected in the Detect gender procedure ([Gender_Update_Flag] column is null here)
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[FREEZED_GENDER] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Freeze Gender', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[FREEZED_GENDER] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Freeze Gender', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Detect Region in CustomerExperience via joining with [AreaGovernmentCentral] table and [region] Lookup
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Region_Cleansing_After] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date], [RowCount]
	)
	SELECT
		'Region Cleansing in CX', @StartTime, @EndTime, 'Success', GETDATE(), @count
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Region_Cleansing_After] @RowsAffected = @count OUTPUT
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date], [RowCount]
	)
	SELECT
		'Region Cleansing in CX', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE(), @count
END CATCH
-----------------------------------------------------------------------------------
--Golden record in CustomerExperience
BEGIN TRY
	SET @StartTime = GETDATE()
	EXEC [dbo].[Golden_Records_Delta]
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [Date]
	)
	SELECT
		'Golden Record in CX', @StartTime, @EndTime, 'Success', GETDATE()
END TRY
BEGIN CATCH
	SET @StartTime = GETDATE()
	EXEC [dbo].[Golden_Records_Delta]
	SET @EndTime = GETDATE()
	INSERT INTO [dbo].[AutomationTimes]
	(
		[ProcName], [StartTime], [EndTime], [Status], [ErrorMessage], [Date]
	)
	SELECT
		'Golden Record in CX', @StartTime, @EndTime, 'Fail', ERROR_MESSAGE(), GETDATE()
END CATCH
-----------------------------------------------------------------------------------
END;