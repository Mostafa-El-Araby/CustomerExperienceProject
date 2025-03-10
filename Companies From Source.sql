USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Companies_From_Source]    Script Date: 11/10/2024 10:41:02 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[Companies_From_Source] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
AS
BEGIN
/*Used to set [Gender_Update_Flag] column to 1 for customers whose gender is company from source 
If the gender in source table is company , we don't modify it , we assume that it is right and these customers 
are excluded from the detect gender proc*/
UPDATE [dbo].[CustomerExperience]
	SET [Gender_Update_Flag] = 1
	WHERE
		[Gender] = 'Company'
	AND [Gender_Update_Flag] IS NULL
	AND [Freezed] IS NULL
	AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);    --To work on the rows inserted today

	SElECT @RowsAffected = @@ROWCOUNT   --Variable carries the number of rows affected

END