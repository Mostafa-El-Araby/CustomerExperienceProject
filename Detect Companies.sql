USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Detect_Companies]    Script Date: 11/10/2024 10:53:06 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Detect_Companies] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
AS
BEGIN
--Detect Companies in CustomerExperience via joining with [Company] lookup
	UPDATE cx
	SET cx.[Gender] = 'Company', cx.[Gender_Update_Flag] = 1
	FROM
		[dbo].[CustomerExperience] AS cx INNER JOIN [dbo].[Company] AS comp
	ON cx.[names] LIKE N'%' + comp.[word] + N'%'
	AND COALESCE (cx.[Gender], '') != 'Company'
	--AND cx.[Gender_Update_Flag] IS NULL
	AND cx.[Freezed] IS NULL
	AND cx.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104)   --To work on rows inserted today
	
	SElECT @RowsAffected = @@ROWCOUNT   --Variable carries the number of rows affected

	/*UPDATE [dbo].[CustomerExperience]
	SET [Gender] = 'Company' 
	WHERE LEFT([Key], 2) NOT IN ('13', '14', '15', '24')
	AND [Gender] != 'Company'
	AND [Gender_Update_Flag] IS NULL;*/	
END;



	