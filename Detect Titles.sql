USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Detect_Titles]    Script Date: 11/10/2024 10:54:20 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Detect_Titles] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
AS
BEGIN
--Detect titles in CustomerExperience via joining with [Titles_Lookup] table
	UPDATE cx
	SET cx.[title] = titles.[title]
	FROM
		[dbo].[CustomerExperience] AS cx INNER JOIN [dbo].[Titles_Lookup] AS titles
	ON cx.[names] LIKE titles.[word] + N'%'
	WHERE
		COALESCE(cx.[Gender], '') != 'Company'
	AND cx.[title] IS NULL
	AND cx.[Freezed] IS NULL
	AND cx.[Gender_Update_Flag] IS NULL 
	AND cx.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104)   --To work on rows inserted today

	SElECT @RowsAffected = @@ROWCOUNT   --Variable carries the number of rows affected

END;
