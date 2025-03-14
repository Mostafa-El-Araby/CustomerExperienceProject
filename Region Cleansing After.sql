USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Region_Cleansing_After]    Script Date: 11/10/2024 11:13:00 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE  [dbo].[Region_Cleansing_After] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
/*
-Detect Region in CustomerExperience via joining with [AreaGovernmentCentral] table and [region] Lookup
-In each statement , we use the Modified_On and Inserted_On columns assigned to GETDATE() to work on rows updated or inserted today
*/
AS
BEGIN
--Update From Central to Central
UPDATE CX
SET CX.[Region_test] = AC.[ldv_name_ar_en]
FROM [dbo].[CustomerExperience] CX INNER JOIN [dbo].[AreaGovernmentCentral] AC
ON TRIM(CX.[centraltelephonename]) = TRIM(AC.[ldv_centraltelephonename])
WHERE ([Modified_On] = TRY_CONVERT(DATE, GETDATE(), 104) AND CX.[centraltelephonename] IS NOT NULL) OR (CX.[region_test] IS NULL AND CX.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104));

SElECT @RowsAffected = @@ROWCOUNT   --Variable carries the number of rows affected


--Update from region to Central
UPDATE CX
SET CX.[Region_test] = AC.[ldv_name_ar_en]
FROM [dbo].[CustomerExperience] CX INNER JOIN [dbo].[AreaGovernmentCentral] AC
ON TRIM (CX.[Region]) = TRIM (AC.[ldv_centraltelephonename])
WHERE ([Modified_On] = TRY_CONVERT(DATE, GETDATE(), 104) AND CX.[centraltelephonename] IS NOT NULL) OR (CX.[region_test] IS NULL AND CX.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104));

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT  --Variable carries the number of rows affected

-- Update from Address to Central
UPDATE CX
SET CX.[Region_test] = AC.[ldv_name_ar_en]
FROM [dbo].[CustomerExperience] CX INNER JOIN [dbo].[AreaGovernmentCentral] AC
ON TRIM (CX.[Address]) = TRIM (AC.[ldv_centraltelephonename])
WHERE ([Modified_On] = TRY_CONVERT(DATE, GETDATE(), 104) AND CX.[centraltelephonename] IS NOT NULL) OR (CX.[region_test] IS NULL AND CX.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104));

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT   --Variable carries the number of rows affected

--Update from Region to lookup table
UPDATE CX
SET CX.[Region_test] = RE.[now]
FROM [dbo].[CustomerExperience] CX INNER JOIN [dbo].[region] RE
ON TRIM (CX.[region]) = TRIM(RE.[Region])
WHERE ([Modified_On] = TRY_CONVERT(DATE, GETDATE(), 104) AND CX.[centraltelephonename] IS NOT NULL) OR (CX.[region_test] IS NULL AND CX.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104));

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT    --Variable carries the number of rows affected

-- Update from address to lookup table
UPDATE CX
SET CX.[Region_test] = RE.[now]
FROM [dbo].[CustomerExperience] CX INNER JOIN [dbo].[region] RE
ON TRIM (CX.[Address]) = TRIM(RE.[Region])
WHERE ([Modified_On] = TRY_CONVERT(DATE, GETDATE(), 104) AND CX.[centraltelephonename] IS NOT NULL) OR (CX.[region_test] IS NULL AND CX.[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104));

SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT   --Variable carries the number of rows affected

END;