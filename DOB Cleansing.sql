USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[DOB_Cleansing]    Script Date: 11/10/2024 10:26:01 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   proc [dbo].[DOB_Cleansing]    --Set Date of Birth to NULL for customers under 16 years old or over 100 years old 
@DataColumnName NVARCHAR(MAX),
@DataTableName NVARCHAR(MAX),
@RowsAffected INT OUTPUT
AS
BEGIN
DECLARE @sql NVARCHAR(MAX);
SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + ' SET ' + QUOTENAME(@DataColumnName) + ' = NULL' + ' WHERE DATEDIFF(year, ' + QUOTENAME(@DataColumnName) + ', GETDATE()) < 16 OR ' +
'DATEDIFF(year, ' + QUOTENAME(@DataColumnName) + ', GETDATE()) > 100';
EXEC sp_executesql @sql;
SElECT @RowsAffected=@@ROWCOUNT   ----Variable carries the number of rows affected 
END;