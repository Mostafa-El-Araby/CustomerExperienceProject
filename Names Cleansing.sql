USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[CleansingNames]    Script Date: 11/10/2024 10:16:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[CleansingNames]   
/*please consider the the changing of tables names here (the tables in if else conditions)
if the tables names such as Custprofile365,Custprofile2023,S4HanaSales2023 were changed for any reason , don't forget to modify them here
*/

    @ColumnName NVARCHAR(MAX),
    @TableName NVARCHAR(MAX),
	@RowsAffected INT OUTPUT
AS
BEGIN
    DECLARE @sql_update NVARCHAR(MAX);
    SET @sql_update = N'UPDATE ' + QUOTENAME(@TableName) + ' SET ' + QUOTENAME(@ColumnName) + ' = SUBSTRING(' + QUOTENAME(@ColumnName) + ', 2, LEN(' + QUOTENAME(@ColumnName) + ')) WHERE LEFT(' + QUOTENAME(@ColumnName) + ', 1) = NCHAR(42295)';
    EXEC sp_executesql @sql_update;

	SElECT @RowsAffected=@@ROWCOUNT

    DECLARE @count INT;
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'SELECT @count = COUNT(*) FROM ' + QUOTENAME(@TableName) +
    N' WHERE LEFT(' + QUOTENAME(@ColumnName) + ', 1) IN
    (
        N''!'', N''"'', N''#'', N''$'', N''%'',
        N''&'', N'''', N''('', N'')'', N''*'',
        N''+'', N'','', N''-'', N''.'', N''/'',
        N'':'', N'';'', N''<'', N''='', N''>'',
        N''?'', N''@'', N''['', N''\\'', N'']'',
        N''_'', N''`'', N''{'', N''|'', N''}'',
        N''~'', N'' '', NCHAR(9), NCHAR(160),''\''
    )';
    EXEC sp_executesql @sql, N'@count INT OUTPUT', @count = @count OUTPUT;

    IF @count > 0
        BEGIN
            DECLARE @pscript NVARCHAR(MAX);
            SET @pscript = N'
import pandas as pd
import re
import sys

lst = [''names'', ''Name'']
try:
    if InputDataSet is None or len(InputDataSet) == 0:
        df = pd.DataFrame()
    else:
        df = InputDataSet
    if len(df) > 0:
        for col in df.columns:
            if col in lst:
                pattern = r''^\W+''
                df[col] = df[col].str.replace(pattern, "",regex=True)
                df[col] = df[col].apply(lambda x: re.sub(pattern, "", str(x).strip()))
                pattern = r''^ـ+''
                df[col] = df[col].str.replace(pattern, "", regex=True)
                df[col] = df[col].str.strip(''_'')
    OutputDataSet = df
except Exception as e:
	print(''Error:'', e, file=sys.stderr)
	sys.exit(1)';
        DECLARE @sqlscript NVARCHAR(MAX);
        IF @TableName = 'CustomerExperience'
            BEGIN
                SET @sqlscript = N'
                SELECT [Key], [NewSapId], [names] FROM CustomerExperience WHERE LEFT([names], 1) IN (N''!'', N''"'', N''#'', N''$'', N''%'', N''&'', N'''', N''('', N'')'', N''*'', N''+'', N'','', N''-'', N''.'', N''/'', N'':'', N'';'', N''<'', N''='', N''>'', N''?'', N''@'', N''['', N''\\'', N'']'', N''_'', N''`'', N''{'', N''|'', N''}'', N''~'', N'' '', NCHAR(9), NCHAR(160),''\'')
                '
                DECLARE @CXTEST_temp_table TABLE
                (
                    [Key] NVARCHAR(450),
                    [NewSapId] NVARCHAR(450),
                    [names] NVARCHAR(450)
                );
                INSERT INTO @CXTEST_temp_table
                EXEC sp_execute_external_script
                    @language = N'Python',
                    @script = @pscript,
                    @input_data_1 = @sqlscript;
                UPDATE Cx
                SET Cx.[names] = tt.[names]
                FROM CustomerExperience Cx INNER JOIN @CXTEST_temp_table tt
                ON (Cx.[Key] = tt.[Key] AND Cx.[Key] IS NOT NULL AND tt.[Key] IS NOT NULL AND Cx.[NewSapId] IS NULL AND tt.[NewSapId] IS NULL)
                OR
                (Cx.[NewSapId] = tt.[NewSapId] AND Cx.[NewSapId] IS NOT NULL AND tt.[NewSapId] IS NOT NULL AND Cx.[Key] IS NULL AND tt.[Key] IS NULL);
				SElECT @RowsAffected=@RowsAffected+@@ROWCOUNT

            END
        ELSE IF @TableName = 'Custprofile365'
            BEGIN
                SET @sqlscript = N'
                SELECT [is_oldcustomersapid], [names] FROM Custprofile365 WHERE LEFT([names], 1) IN (N''!'', N''"'', N''#'', N''$'', N''%'', N''&'', N'''', N''('', N'')'', N''*'', N''+'', N'','', N''-'', N''.'', N''/'', N'':'', N'';'', N''<'', N''='', N''>'', N''?'', N''@'', N''['', N''\\'', N'']'', N''_'', N''`'', N''{'', N''|'', N''}'', N''~'', N'' '', NCHAR(9), NCHAR(160),''\'')
                '
                DECLARE @Custprofile365TEST_temp_table TABLE
                (
                    [is_oldcustomersapid] NVARCHAR(450),
                    [names] NVARCHAR(MAX)
                );
                INSERT INTO @Custprofile365TEST_temp_table
                EXEC sp_execute_external_script
                    @language = N'Python',
                    @script = @pscript,
                    @input_data_1 = @sqlscript;
                UPDATE old_crm
                SET old_crm.[names] = tt.[names]
                FROM [dbo].Custprofile365 old_crm INNER JOIN @Custprofile365TEST_temp_table tt
                ON old_crm.[is_oldcustomersapid] = tt.[is_oldcustomersapid];
				SElECT @RowsAffected=@RowsAffected+@@ROWCOUNT
            END
        ELSE IF @TableName = 'Custprofile2023'
            BEGIN
                SET @sqlscript = N'
                SELECT [ldv_customersapid], [names] FROM Custprofile2023 WHERE LEFT([names], 1) IN (N''!'', N''"'', N''#'', N''$'', N''%'', N''&'', N'''', N''('', N'')'', N''*'', N''+'', N'','', N''-'', N''.'', N''/'', N'':'', N'';'', N''<'', N''='', N''>'', N''?'', N''@'', N''['', N''\\'', N'']'', N''_'', N''`'', N''{'', N''|'', N''}'', N''~'', N'' '', NCHAR(9), NCHAR(160),''\'')
                '
                DECLARE @Custprofile2023TEST_temp_table TABLE
                (
                    [ldv_customersapid] NVARCHAR(450),
                    [names] NVARCHAR(MAX)
                );
                INSERT INTO @Custprofile2023TEST_temp_table
                EXEC sp_execute_external_script
                    @language = N'Python',
                    @script = @pscript,
                    @input_data_1 = @sqlscript;
                UPDATE new_crm
                SET new_crm.[names] = tt.[names]
                FROM [dbo].Custprofile2023 new_crm INNER JOIN @Custprofile2023TEST_temp_table tt
                ON new_crm.[ldv_customersapid] = tt.[ldv_customersapid];
				SElECT @RowsAffected=@RowsAffected+@@ROWCOUNT

            END
        ELSE IF @TableName = 'S4HanaSales2023'
            BEGIN
                SET @sqlscript = N'
                SELECT [CustomerSapID], [Name] FROM [S4HanaSales2023] WHERE LEFT([Name], 1) IN (N''!'', N''"'', N''#'', N''$'', N''%'', N''&'', N'''', N''('', N'')'', N''*'', N''+'', N'','', N''-'', N''.'', N''/'', N'':'', N'';'', N''<'', N''='', N''>'', N''?'', N''@'', N''['', N''\\'', N'']'', N''_'', N''`'', N''{'', N''|'', N''}'', N''~'', N'' '', NCHAR(9), NCHAR(160),''\'')
                '
                DECLARE @Sales4hanaTEST_temp_table TABLE
                (
                    [CustomerSapID] NVARCHAR(MAX),
                    [Name] NVARCHAR(MAX)
                );
                INSERT INTO @Sales4hanaTEST_temp_table
                EXEC sp_execute_external_script
                    @language = N'Python',
                    @script = @pscript,
                    @input_data_1 = @sqlscript;
                UPDATE new_sales_agg
                SET new_sales_agg.[Name] = tt.[Name]
                FROM [dbo].S4HanaSales2023 new_sales_agg INNER JOIN @Sales4hanaTEST_temp_table tt
                ON new_sales_agg.[CustomerSapID] = tt.[CustomerSapID];
				SElECT @RowsAffected=@RowsAffected+@@ROWCOUNT
            END
        ELSE IF @TableName = '0-SalesDetailed-Cascade'
            BEGIN
                SET @sqlscript = N'
                SELECT [CustomerSAPID], [names] FROM 0-SalesDetailed-Cascade WHERE LEFT([names], 1) IN (N''!'', N''"'', N''#'', N''$'', N''%'', N''&'', N'''', N''('', N'')'', N''*'', N''+'', N'','', N''-'', N''.'', N''/'', N'':'', N'';'', N''<'', N''='', N''>'', N''?'', N''@'', N''['', N''\\'', N'']'', N''_'', N''`'', N''{'', N''|'', N''}'', N''~'', N'' '', NCHAR(9), NCHAR(160),''\'')
                '
                DECLARE @Sales2023Cascade_temp_table TABLE
                (
                    [CustomerID] NVARCHAR(MAX),
                    [names] NVARCHAR(MAX)
                );
                INSERT INTO @Sales2023Cascade_temp_table
                EXEC sp_execute_external_script
                    @language = N'Python',
                    @script = @pscript,
                    @input_data_1 = @sqlscript;
                UPDATE new_sales_cascade
                SET new_sales_cascade.[names] = tt.[names]
                FROM [dbo].[0-SalesDetailed-Cascade] new_sales_cascade INNER JOIN @Sales2023Cascade_temp_table tt
                ON new_sales_cascade.[CustomerSAPID] = tt.[CustomerID];
				SElECT @RowsAffected=@RowsAffected+@@ROWCOUNT
            END
    END
    ELSE
        BEGIN
            DECLARE @empty_table TABLE([Key] NVARCHAR(450), [NewSapId] NVARCHAR(450), [names] NVARCHAR(MAX));
            SELECT * FROM @empty_table;
            DELETE @empty_table;
        END
END;