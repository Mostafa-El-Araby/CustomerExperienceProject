USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[UpdateColumn]    Script Date: 9/30/2024 12:34:45 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[UpdateColumn]
    @DataColumnName NVARCHAR(MAX),
    @DataTableName NVARCHAR(MAX),
	@RowsAffected INT OUTPUT
AS
BEGIN
	EXEC [dbo].[Mapping] @DataColumnName, @DataTableName;
	SElECT @RowsAffected = @@ROWCOUNT

    DECLARE @sql NVARCHAR(MAX);

	-- Remove leading +2
	SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + ' SET ' + QUOTENAME(@DataColumnName) + ' = REPLACE(' + QUOTENAME(@DataColumnName) + ', ''+2'', '''')' +
        ' WHERE LEFT(' + QUOTENAME(@DataColumnName) + ', 2) = ''+2''';
    EXEC sp_executesql @sql;
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

		
		-- Remove English characters
	SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + ' SET ' + QUOTENAME(@DataColumnName) + ' = REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						REPLACE(
						UPPER(' + QUOTENAME(@DataColumnName) + '),
					''A'', ''''),
					''B'', ''''),
					''C'', ''''),
					''D'', ''''),
					''E'', ''''),
					''F'', ''''),
					''G'', ''''),
					''H'', ''''),
					''I'', ''''),
					''J'', ''''),
					''K'', ''''),
					''L'', ''''),
					''M'', ''''),
					''N'', ''''),
					''O'', ''''),
					''P'', ''''),
					''Q'', ''''),
					''R'', ''''),
					''S'', ''''),
					''T'', ''''),
					''U'', ''''),
					''V'', ''''),
					''W'', ''''),
					''X'', ''''),
					''Y'', ''''),
					''Z'', '''') WHERE PATINDEX(''%[a-zA-Z]%'', ' + QUOTENAME(@DataColumnName) + ') > 0';
	EXEC sp_executesql @sql;
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT


	SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + ' SET ' + QUOTENAME(@DataColumnName) + ' = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(' +
	QUOTENAME(@DataColumnName) + ',
    NCHAR(1569), ''''),
	NCHAR(42295), ''''),
	NCHAR(0x0649), ''''),
	NCHAR(1570), ''''),
    NCHAR(1571), ''''),
    NCHAR(1572), ''''),
    NCHAR(0x0648), ''''),
	NCHAR(1611), ''''),
	NCHAR(1575), ''''),
	NCHAR(1574), ''''),
	NCHAR(1576), ''''),
	NCHAR(1577), ''''),
	NCHAR(1578), ''''),
	NCHAR(1579), ''''),
	NCHAR(1580), ''''),
	NCHAR(1581), ''''),
	NCHAR(1582), ''''),
	NCHAR(1583), ''''),
	NCHAR(1584), ''''),
	NCHAR(1585), ''''),
	NCHAR(1586), ''''),
	NCHAR(1587), ''''),
	NCHAR(1588), ''''),
	NCHAR(1589), ''''),
	NCHAR(1590), ''''),
	NCHAR(1591), ''''),
	NCHAR(1592), ''''),
	NCHAR(1593), ''''),
	NCHAR(1594), ''''),
	NCHAR(1601), ''''),
	NCHAR(1602), ''''),
    NCHAR(1603), ''''),
	NCHAR(1604), ''''),
	NCHAR(1605), ''''),
	NCHAR(1606), ''''),
	NCHAR(0x0625), ''''),
	NCHAR(0x0647), ''''),
	NCHAR(1610), '''') WHERE PATINDEX(''%['' + NCHAR(1569) + ''-'' + NCHAR(1610) + '']%'', ' + QUOTENAME(@DataColumnName) + ') > 0';
    EXEC sp_executesql @sql;
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT


	-- Remove special characters
	SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + ' SET ' + QUOTENAME(@DataColumnName) + ' = REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(
					REPLACE(' + QUOTENAME(@DataColumnName) + ',
					''!'', ''''),
					''"'', ''''),
					''#'', ''''),
					''$'', ''''),
					''%'', ''''),
					''&'', ''''),
					'''', ''''),
					''('', ''''),
					'')'', ''''),
					''*'', ''''),
					''@'', ''''),
					''+'', ''''),
					'','', ''''),
					''-'', ''''),
					''.'', ''''),
					''/'', ''''),
					'':'', ''''),
					'';'', ''''),
					''<'', ''''),
					''='', ''''),
					''>'', ''''),
					''?'', ''''),
					''['', ''''),
					''\'', ''''),
					'']'', ''''),
					''_'', ''''),
					''`'', ''''),
					''{'', ''''),
					''|'', ''''),
					''}'', ''''),
					''~'', ''''),
					'' '', ''''),
					''ـ'', ''''),
					''^'', ''''),
					N'' '', ''''),
					NCHAR(9), ''''),
					NCHAR(160), '''') WHERE PATINDEX(''%['' + NCHAR(1569) + ''-'' + NCHAR(1610) + '']%'',' + QUOTENAME(@DataColumnName) + ') = 0 AND '
					+ QUOTENAME(@DataColumnName) + ' LIKE ''%[^0-9]%''
 AND PATINDEX(N''%[٠١٢٣٤٥٦٧٨٩]%'', ' + QUOTENAME(@DataColumnName) + ') = 0
 AND PATINDEX(''%[a-zA-Z]%'', ' + QUOTENAME(@DataColumnName) + ') = 0 ';
	EXEC sp_executesql @sql;
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT

	
	SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + 
				' SET ' + QUOTENAME(@DataColumnName) + ' = CONCAT(' + '0' + ' , ' + QUOTENAME(@DataColumnName) +
				') WHERE CHARINDEX(''1'' , ' + QUOTENAME(@DataColumnName) + ') = 1 AND LEN(' + QUOTENAME(@DataColumnName) + ') = 10';
	EXEC sp_executesql @sql;
	SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT


	 SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) + ' SET ' + QUOTENAME(@DataColumnName) + ' = NULL WHERE ' + QUOTENAME(@DataColumnName)
	 + 'IN(''0'', ''00'', ''000'', ''0000'',''00000'', ''000000'', ''0000000'', ''00000000'',''000000000'', ''0000000000'', ''00000000000'', ''000000000000'',
		''0000000000000'', ''00000000000000'', ''000000000000000'', ''0000000000000000'',
		''000000003'', ''00000001'', ''000000125'', ''00000020'',
		''00064646'', ''01'', ''0100000000'', ''01000000000'')';
	 EXEC sp_executesql @sql;
	 SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT


	 SET @sql = N'UPDATE ' + QUOTENAME(@DataTableName) +
	' SET ' + QUOTENAME(@DataColumnName) + ' = NULL
	 WHERE ' + QUOTENAME(@DataColumnName) + ' = ''''';
	 EXEC sp_executesql @sql;
	 SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT
END;