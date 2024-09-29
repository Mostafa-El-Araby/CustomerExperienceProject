USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Cleansing]    Script Date: 9/30/2024 12:29:29 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[Cleansing] @RowsAffected INT OUTPUT  --output parameter for number of rows affected 
AS
BEGIN
--Cleansing City and Address in S4HanaSales2023 table to replace '#' with null

	UPDATE [dbo].[S4HanaSales2023]
	SET [City] = NULL
	WHERE [City] = '#';

	SElECT
		@RowsAffected = @@ROWCOUNT;

	UPDATE [dbo].[S4HanaSales2023]
	SET [Address] = NULL
	WHERE [Address] = '#';

	SElECT
		@RowsAffected = @RowsAffected + @@ROWCOUNT;   --Variable carries the number of rows affected
END;