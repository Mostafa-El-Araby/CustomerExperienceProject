USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[FREEZED_MOB]    Script Date: 11/10/2024 10:39:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[FREEZED_MOB] @RowsAffected INT OUTPUT   --output parameter for number of rows affected 
AS
BEGIN
--Freeze invalid mobile numbers(don't begin with 01 , not equal 11 digits , null mobile numbers ) in CustomerExperience
	UPDATE [dbo].[CustomerExperience] 
	SET [Freezed] = 1
	WHERE
	(
	    LEN([Mobile_Number]) != 11
		OR
		[Mobile_Number] IS NULL
		OR
		(
			CHARINDEX('01', [Mobile_Number]) = 0
			AND LEN([Mobile_Number]) = 11
		)
	)
	AND [Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104)  
	--To freeze the new inserted mobiles , we use [Inserted_On] flag to avoid affecting all data

	SElECT @RowsAffected = @@ROWCOUNT  --Variable carries the number of rows affected
END;

