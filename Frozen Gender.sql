USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[FREEZED_GENDER]    Script Date: 11/10/2024 11:10:33 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[FREEZED_GENDER] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
AS
BEGIN
--Freeze customers whose gender couldn't be detected in the Detect gender procedure ([Gender_Update_Flag] column is null here)
UPDATE [dbo].[CustomerExperience]
	SET [Freezed] = 1
	WHERE [Gender_Update_Flag] IS NULL AND [Freezed] IS NULL
	AND COALESCE ([Inserted_On], '') = TRY_CONVERT(DATE, GETDATE(), 104);   --To work on rows inserted today

	SElECT @RowsAffected = @@ROWCOUNT   --Variable carries the number of rows affected
END