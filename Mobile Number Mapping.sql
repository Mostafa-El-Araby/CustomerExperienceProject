USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Mapping]    Script Date: 9/30/2024 12:43:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Mapping]
    @ColumnName NVARCHAR(MAX),
    @TableName NVARCHAR(MAX)
AS
BEGIN
    DECLARE @count INT;
    DECLARE @sql NVARCHAR(MAX);

    SET @sql = N'SELECT @count = COUNT(*) FROM ' + QUOTENAME(@TableName) +
               N' WHERE PATINDEX(N''%[٠١٢٣٤٥٦٧٨٩]%'', ' + QUOTENAME(@ColumnName) + ') > 0';

    EXEC sp_executesql @sql, N'@count INT OUTPUT', @count = @count OUTPUT;

    IF @count > 0
    BEGIN
        DECLARE @pscript NVARCHAR(MAX);
		SET @pscript = N'
import pandas as pd
import sys
numbers = {
    ''٠'': ''0'',
    ''١'': ''1'',
    ''٢'': ''2'',
    ''٣'': ''3'',
    ''٤'': ''4'',
    ''٥'': ''5'',
    ''٦'': ''6'',
    ''٧'': ''7'',
    ''٨'': ''8'',
    ''٩'': ''9''
	}
try:
	if InputDataSet is None or len(InputDataSet) == 0:
		df = pd.DataFrame()
	else:
		df = InputDataSet
	def replace_arabic_numbers(text):
		result = ''''
		for char in text:
			if char in numbers.keys():
				result += numbers[char]
			else:
				result += char
		return result
	df["Mobile_Number_Cleaned"] = df.iloc[:, 0].apply(replace_arabic_numbers)
	OutputDataSet = df
except Exception as e:
	print(''Error:'', e, file=sys.stderr)
	sys.exit(1)
';
DECLARE @sqlscript NVARCHAR(MAX);
SET @sqlscript = N'
SELECT ' + QUOTENAME(@ColumnName) + ' FROM ' + QUOTENAME(@TableName) + N' WHERE PATINDEX(N''%[٠١٢٣٤٥٦٧٨٩]%'', ' + QUOTENAME(@ColumnName) + ') > 0';
CREATE TABLE #temp
(
	[Mobile_Number] NVARCHAR(450),
    [Mobile_Number_Cleaned] NVARCHAR(450)
);
INSERT INTO #temp
EXEC sp_execute_external_script
	@language = N'Python',
    @script = @pscript,
	@input_data_1 = @sqlscript;
DECLARE @sql_update_query NVARCHAR(MAX);
    SET @sql_update_query = N'UPDATE t
                 SET t.' + QUOTENAME(@ColumnName) + N' = temp.[Mobile_Number_Cleaned]
                 FROM [dbo].' + QUOTENAME(@TableName) + N' t
                 INNER JOIN #temp temp ON temp.[Mobile_Number] = t.' + QUOTENAME(@ColumnName);
    EXEC sp_executesql @sql_update_query;
    END
    ELSE
    BEGIN
        DECLARE @empty_table TABLE([Mobile_Number] NVARCHAR(450), [Mobile_Number_Cleaned] NVARCHAR(450));
        SELECT * FROM @empty_table;
    END;
END;