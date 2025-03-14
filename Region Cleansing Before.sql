USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Region_Cleansing_Before]    Script Date: 11/10/2024 10:28:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   procedure [dbo].[Region_Cleansing_Before] @RowsAffected INT OUTPUT
AS
BEGIN
--replace 0 with Null values in address column from Custprofile365
UPDATE [dbo].[CustProfile365]
SET [Address] = NULL
WHERE [Address] LIKE '%0%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [Address]) = 0 AND PATINDEX('%[a-zA-Z]%', [Address]) = 0;

SElECT @RowsAffected = @@ROWCOUNT

--replace 0 with Null values in region column from Custprofile365
UPDATE [dbo].[CustProfile365]
SET [Region] = NULL
WHERE [Region] LIKE '%0%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [Region]) = 0 AND PATINDEX('%[a-zA-Z]%', [Region]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

--replace 0 with Null values in address column from Custprofile2023
UPDATE [dbo].[CustProfile2023]
SET [Address] = NULL
WHERE [Address] LIKE '%0%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [Address]) = 0 AND PATINDEX('%[a-zA-Z]%', [Address]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

--replace 0 with Null values in Region column from Custprofile2023
UPDATE [dbo].[CustProfile2023]
SET [Region] = NULL
WHERE [Region] LIKE '%0%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [Region]) = 0 AND PATINDEX('%[a-zA-Z]%', [Region]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT
  
  --replace 0 with Null values in address column from Sales4Hana
UPDATE [dbo].[S4HanaSales2023]
SET [Address] = NULL
WHERE [Address] LIKE '%0%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [Address]) = 0 AND PATINDEX('%[a-zA-Z]%', [Address]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

  --replace 0 with Null values in City column from Sales4Hana
UPDATE [dbo].[S4HanaSales2023]
SET [City] = NULL
WHERE [City] LIKE '%0%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [City]) = 0 AND PATINDEX('%[a-zA-Z]%', [City]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

  --replace # with Null values in address column from Sales4Hana
UPDATE [dbo].[S4HanaSales2023]
SET [Address] = NULL
WHERE [Address] LIKE '%#%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [Address]) = 0 AND PATINDEX('%[a-zA-Z]%', [Address]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

--replace # with Null values in City column from Sales4Hana
UPDATE [dbo].[S4HanaSales2023]
SET [City] = NULL
WHERE [City] LIKE '%#%' AND PATINDEX('%[' + NCHAR(1569) + '-' + NCHAR(1610) + ']%', [City]) = 0 AND PATINDEX('%[a-zA-Z]%', [City]) = 0;

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT
END;