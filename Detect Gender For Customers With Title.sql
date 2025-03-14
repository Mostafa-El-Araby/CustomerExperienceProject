USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Detect_Gender_For_Customers_With_Title]    Script Date: 11/10/2024 10:55:41 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Detect_Gender_For_Customers_With_Title] @RowsAffected INT OUTPUT   --output parameter for number of rows affected
AS
BEGIN
	DECLARE @count INT;
	SELECT @count = COUNT(*) FROM [dbo].[CustomerExperience] WHERE [Gender_Update_Flag] IS NULL AND [title] IS NOT NULL  AND [Freezed] IS NULL 
	AND	[Inserted_On] = TRY_CONVERT(DATE, GETDATE(), 104);  --To work on rows inserted today
	IF @count > 0
	BEGIN
		DECLARE @pscript NVARCHAR(MAX);
		SET @pscript = N'
import pandas as pd
import re
import sys

def get_first_name(full_name):
   if not isinstance(full_name, str):
       return None
   words = re.findall(r''\b\w+\b'', full_name)
   if len(words) < 2:
       return None
   else:
       return words[1].strip()

try:
	if InputDataSet is None or len(InputDataSet) == 0:
		df = pd.DataFrame()
	else:
		df = InputDataSet
	df[''First_name''] = df[''names''].apply(lambda x: get_first_name(x))
	OutputDataSet = df
except Exception as e:
	print(''Error:'', e, file=sys.stderr)
	sys.exit(1)
';
DECLARE @sqlscript NVARCHAR(MAX);
SET @sqlscript = N'
SELECT [Key], [NewSapId], [names] FROM [dbo].[CustomerExperience] WHERE [Gender_Update_Flag] IS NULL AND [title] IS NOT NULL AND [Freezed] IS NULL
AND Inserted_On=TRY_CONVERT(DATE,GETDATE(),104);
'
DECLARE @temp_table TABLE
(
	[Key] NVARCHAR(450),
	[NewSapId] NVARCHAR(450),
	[names] NVARCHAR(450),
	[First_name] NVARCHAR(450)
);

INSERT INTO @temp_table
EXEC sp_execute_external_script
	@language = N'Python',
    @script = @pscript,
	@input_data_1 = @sqlscript;
UPDATE cx
SET cx.[Gender] = CTE.[Gender], cx.[Gender_Update_Flag] = 1
FROM [dbo].[CustomerExperience] cx INNER JOIN
(
	SELECT tt.[Key], tt.[NewSapId], tt.[names], tt.[First_name], GL.[gender] AS 'Gender'
	FROM @temp_table tt
	INNER JOIN [dbo].[Gender_Lookup] GL
	ON tt.[First_name] COLLATE Latin1_General_BIN2 = GL.[name]
) CTE
ON cx.[Key] = CTE.[Key]
SElECT @RowsAffected = @@ROWCOUNT    --Variable carries the number of rows affected

UPDATE cx
SET cx.[Gender] = CTE.[Gender], cx.[Gender_Update_Flag] = 1
FROM [dbo].[CustomerExperience] cx INNER JOIN
(
	SELECT tt.[Key], tt.[NewSapId], tt.[names], tt.[First_name], GL.[gender] AS 'Gender'
	FROM @temp_table tt
	INNER JOIN [dbo].[Gender_Lookup] GL
	ON tt.[First_name] COLLATE Latin1_General_BIN2 = GL.[name]
) CTE
ON cx.[NewSapId] = CTE.[NewSapId];
SElECT @RowsAffected = @RowsAffected + @@ROWCOUNT   --Variable carries the number of rows affected

	END
	ELSE
	BEGIN
		DECLARE @empty_table TABLE([Key] NVARCHAR(450), [NewSapId] NVARCHAR(450), [names] NVARCHAR(450));
        SELECT * FROM @empty_table;
    END;
END;