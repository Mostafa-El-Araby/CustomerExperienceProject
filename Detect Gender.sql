USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Detect_Gender]    Script Date: 11/10/2024 10:58:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Detect_Gender] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
AS
BEGIN
/*
-Detect gender in CustomerExperience via joining with [Gender_Lookup] table and use [Gender_Update_Flag] column 
to mark the affected rows(customers whose gender was detected)
-We exclude company customers in each update statement
-we set [Gender_Update_Flag] to 1 in each update statement
-we use Inserted_On in each update statement to work on rows inserted today
*/
	UPDATE cx
	SET cx.[Gender] = GL.[gender], cx.[Gender_Update_Flag] = 1
	FROM [dbo].[CustomerExperience] cx
	INNER JOIN [dbo].[Gender_Lookup] GL
	ON TRIM(cx.[names]) COLLATE Latin1_General_BIN2 LIKE GL.[name] + N' %'
	WHERE COALESCE (cx.[Gender], '') != 'Company' AND cx.[Gender_Update_Flag] IS NULL AND cx.[Freezed] IS NULL AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);
	SElECT @RowsAffected = @@ROWCOUNT   --Variable carries the number of rows affected

	UPDATE [dbo].[CustomerExperience]
	SET [Gender] = 'Male', [Gender_Update_Flag] = 1
	WHERE [names] LIKE N'عبد%'
	AND COALESCE ([Gender], '') != 'Company' AND [Gender_Update_Flag] IS NULL AND [Freezed] IS NULL AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT   --Variable carries the number of rows affected


	UPDATE [dbo].[CustomerExperience]
	SET [Gender] = 'Male', [Gender_Update_Flag] = 1
	WHERE [names] LIKE N'Abd%'
	AND COALESCE ([Gender], '') != 'Company' AND [Gender_Update_Flag] IS NULL AND [Freezed] IS NULL AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT  --Variable carries the number of rows affected


	UPDATE [dbo].[CustomerExperience]
	SET [Gender] = 'Male', [Gender_Update_Flag] = 1
	WHERE [names] LIKE N'أبو%'
	AND COALESCE ([Gender], '') != 'Company' AND [Gender_Update_Flag] IS NULL AND [Freezed] IS NULL AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT   --Variable carries the number of rows affected


	UPDATE [dbo].[CustomerExperience]
	SET [Gender] = 'Male', [Gender_Update_Flag] = 1
	WHERE [names] LIKE N'ابو%'
	AND COALESCE ([Gender], '') != 'Company' AND [Gender_Update_Flag] IS NULL AND [Freezed] IS NULL AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT   --Variable carries the number of rows affected


	UPDATE [dbo].[CustomerExperience]
	SET [Gender] = 'Male', [Gender_Update_Flag] = 1
	WHERE [names] LIKE N'Abu%'
	AND COALESCE ([Gender], '') != 'Company' AND [Gender_Update_Flag] IS NULL AND [Freezed] IS NULL AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT  --Variable carries the number of rows affected
END;