USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Golden_Records_Delta]    Script Date: 9/29/2024 8:12:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Golden_Records_Delta]
AS
BEGIN
	DECLARE @count INT;
	SELECT @count = COUNT(*)
	FROM [dbo].[CustomerExperience]
	WHERE
	(
		(COALESCE([Gr], '') IN('O', 'N', 'D') OR [Gr] IS NULL)
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM [dbo].[CustomerExperience]
			WHERE [Modified_On] = CONVERT(date, GETDATE())
		)
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM [dbo].[CustomerExperience]
			WHERE [Modified_On] IS NULL
			OR [Modified_On] != CONVERT(date, GETDATE())
		)
	)
	OR
	(
		[Gr] = 'D'
		AND [Modified_On] = CONVERT(date, GETDATE())
	);
	IF @count > 0
	BEGIN
		DECLARE @similarity_python_script NVARCHAR(MAX);
		SET @similarity_python_script = N'
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from itertools import combinations
import re
import sys
def is_arabic(string):
    reg = re.compile(r''[a-zA-Z]'')
    if not reg.match(string):
        return True
    else:
        return False

def is_english(string):
    english_pattern = re.compile("[a-zA-Z]+")
    return english_pattern.fullmatch(string) is not None

def all_arabic(lst):
    for item in lst:
        if not is_arabic(item):
            return False
    return True

def all_english(lst):
    for item in lst:
        if not is_english(item):
            return False
    return True

def group_by_first_element(data):
    grouped_lists = []
    prev, rest = data[0], data[1:]
    subgroup = [prev]
    for item in rest:
        if item[0] != prev[0]:
            grouped_lists.append(subgroup)
            subgroup = []
        subgroup.append(item)
        prev = item
    grouped_lists.append(subgroup)
    return grouped_lists
try:
	if InputDataSet is None or len(InputDataSet) == 0:
		df = pd.DataFrame()
	else:
		df = InputDataSet
	data = df.values.tolist()
	grouped_lists = group_by_first_element(data)
	final_data = []
	for lst in grouped_lists:
		comb = [[x, y, fuzz.ratio(x[3], y[3]), None] for x, y in combinations(lst, 2)]
		mini_similarity = min([item[-2] for item in comb])
		dictionary = {tuple(item[0]): item[1] for item in comb}
		dictionary.update({tuple(item[1]): item[0] for item in comb})
		for k in dictionary.keys():
			if 5 <= mini_similarity <= 12 or mini_similarity >= 50:
				final_data.append([k[0], k[1], k[2], k[3], mini_similarity, ''D''])
			else:
				final_data.append([k[0], k[1], k[2], k[3], mini_similarity, ''N''])
	grouped_lists = group_by_first_element(final_data)
	for lst in grouped_lists:
		if lst[0][-2] in range(5, 12) and all_arabic([item[3] for item in lst]):
			for i in range(len(lst)):
				lst[i][-1] = ''N''
		elif lst[0][-2] in range(5, 12) and all_english([item[3] for item in lst]):
			lst[i][-1] = ''N''
	Final = [item for sublist in grouped_lists for item in sublist]
	df = pd.DataFrame(data=Final, columns=[''Mobile_Number'', ''Key'', ''NewSapId'', ''names'', ''Similarity'', ''Gr''])
	OutputDataSet = df
except Exception as e:
	print(''Error:'', e, file=sys.stderr)
	sys.exit(1)';
		DECLARE @sqlscript NVARCHAR(MAX);
		SET @sqlscript = N'
	SELECT
		[Mobile_Number],
		[Key],
		[NewSapId],
		[names]
	FROM [dbo].[CustomerExperience]
	WHERE 
	(
		(COALESCE([Gr], '''') IN(''O'', ''N'', ''D'') OR [Gr] IS NULL)
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM [dbo].[CustomerExperience]
			WHERE [Modified_On] = CONVERT(date, GETDATE())
		)
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM [dbo].[CustomerExperience]
			WHERE [Modified_On] IS NULL
			OR [Modified_On] != CONVERT(date, GETDATE())
		)
	)
	OR
	(
		[Gr] = ''D''
		AND [Modified_On] = CONVERT(date, GETDATE())
	)
	ORDER BY [Mobile_Number];';
		DECLARE @temp_table TABLE
		(
			[Mobile_Number] NVARCHAR(450),
			[Key] NVARCHAR(450),
			[NewSapId] NVARCHAR(450),
			[names] NVARCHAR(450),
			[Similarity] INT,
			[Gr] CHAR(1)
		);
		INSERT INTO @temp_table
		EXEC sp_execute_external_script
			@language = N'Python',
			@script = @similarity_python_script,
			@input_data_1 = @sqlscript;
        -------------------------------------------------------------------------------------------------------
		UPDATE cx
		SET cx.[Similarity] = tt.[Similarity], cx.[Gr] = tt.[Gr]
		FROM [dbo].[CustomerExperience] cx
		INNER JOIN @temp_table tt
		ON cx.[Mobile_Number] = tt.[Mobile_Number]
		AND (cx.[Gr] != 'G' OR cx.[Gr] IS NULL);
		-------------------------------------------------------------------------------------------------------
		UPDATE [dbo].[CustomerExperience]
		SET [Freezed] = NULL
		WHERE 
	    (
            [Gr] = 'N'
            AND [Mobile_Number] IN
            (
                SELECT [Mobile_Number]
                FROM [dbo].[CustomerExperience]
                WHERE [Modified_On] = CONVERT(date, GETDATE())
            )
            AND [Mobile_Number] IN
            (
                SELECT [Mobile_Number]
                FROM [dbo].[CustomerExperience]
                WHERE [Modified_On] IS NULL
                OR [Modified_On] != CONVERT(date, GETDATE())
            )
        )
        OR
        (
            [Gr] = 'N'
            AND [Modified_On] = CONVERT(date, GETDATE())
        );
		-------------------------------------------------------------------------------------------------------
		DELETE FROM [dbo].[CustomerExperience]
		WHERE [Gr] = 'G'
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM @temp_table
		);
		-------------------------------------------------------------------------------------------------------
		DECLARE @sql_query NVARCHAR(MAX);
		SET @sql_query = N'SELECT [Mobile_Number], [Key], [names], [Gender], [Address], [region_test],
    TRY_CONVERT(date, [Date_Of_Birth], 23) AS Date_Of_Birth,
    CAST(COALESCE([Total_Amount], 0) AS float) AS Total_Amount,
    COALESCE([SalesQuantity], 0) AS SalesQuantity,
    CAST(COALESCE([SalesValue], 0) AS float) AS SalesValue,
    COALESCE([Maintenance], 0) AS Maintenance,
    COALESCE([installation], 0) AS installation,
    COALESCE([Complaint], 0) AS Complaint,
    [Category1], [Brand1], [Workorder1], TRY_CONVERT(date, [Warrantydate1], 23) AS Warrantydate1,
    [Category2], [Brand2], [Workorder2], TRY_CONVERT(date, [Warrantydate2], 23) AS Warrantydate2,
    [Category3], [Brand3], [Workorder3], TRY_CONVERT(date, [Warrantydate3], 23) AS Warrantydate3,
    [Category4], [Brand4], [Workorder4], TRY_CONVERT(date, [Warrantydate4], 23) AS Warrantydate4,
    [Category5], [Brand5], [Workorder5], TRY_CONVERT(date, [Warrantydate5], 23) AS Warrantydate5,
    [Category6], [Brand6], [Workorder6], TRY_CONVERT(date, [Warrantydate6], 23) AS Warrantydate6,
    [Category7], [Brand7], [Workorder7], TRY_CONVERT(date, [Warrantydate7], 23) AS Warrantydate7,
    [Category8], [Brand8], [Workorder8], TRY_CONVERT(date, [Warrantydate8], 23) AS Warrantydate8,
    [Category9], [Brand9], [Workorder9], TRY_CONVERT(date, [Warrantydate9], 23) AS Warrantydate9,
    [Category10], [Brand10], [Workorder10], TRY_CONVERT(date, [Warrantydate10], 23) AS Warrantydate10,
    TRY_CONVERT(date, COALESCE([CreatedOn], ''1990-01-01''), 23) AS CreatedOn,
    [NewSapId], [Similarity], [Gr]
FROM [dbo].[CustomerExperience]
WHERE [Gr] = ''D''
    AND
    (
		[Mobile_Number] IN
		(
			SELECT
                [Mobile_Number]
            FROM [dbo].[CustomerExperience]
            WHERE CONVERT(date, [Modified_On]) = CONVERT(date, GETDATE())
		)
        AND [Mobile_Number] IN
		(
			SELECT
                [Mobile_Number]
            FROM [dbo].[CustomerExperience]
            WHERE (CONVERT(date, [Modified_On]) != CONVERT(date, GETDATE()) OR [Modified_On] IS NULL)
		)
	)
    OR [Mobile_Number] IN
	(
		SELECT
            [Mobile_Number]
        FROM [dbo].[CustomerExperience]
        WHERE [Modified_On] = CONVERT(date, GETDATE()) AND [Gr] = ''D''
	)
    ORDER BY [Mobile_Number] ASC;';
		DECLARE @Golden_CRM_python_script NVARCHAR(MAX);
		SET @Golden_CRM_python_script = N'
import numpy as np
import pandas as pd
import re
from datetime import datetime, date

def chosen(lst):
    max_arabic_len = 0
    max_arabic_sublist = None
    max_english_len = 0
    max_english_sublist = None
    for item in lst:
        if any(c for c in item[2] if ''\u0600'' <= c <= ''\u06FF''):
            if len(item[2]) > max_arabic_len:
                max_arabic_len = len(item[2])
                max_arabic_sublist = item
        else:
            if len(item[2]) > max_english_len:
                max_english_len = len(item[2])
                max_english_sublist = item
    if max_arabic_sublist is not None:
        return max_arabic_sublist
    else:
        return max_english_sublist
def custom_sort(sublist):
    if all(value is None for value in sublist):
        return 1
    elif all(value is None for value in sublist[:4]):
        return 2
    else:
        return 0
df = InputDataSet
df[''Mobile_Number''] = df[''Mobile_Number''].astype(str)
df[''Warrantydate1''] = pd.to_datetime(df[''Warrantydate1''], errors=''coerce'')
df[''Warrantydate2''] = pd.to_datetime(df[''Warrantydate2''], errors=''coerce'')
df[''Warrantydate3''] = pd.to_datetime(df[''Warrantydate3''], errors=''coerce'')
df[''Warrantydate4''] = pd.to_datetime(df[''Warrantydate4''], errors=''coerce'')
df[''Warrantydate5''] = pd.to_datetime(df[''Warrantydate5''], errors=''coerce'')
df[''Warrantydate6''] = pd.to_datetime(df[''Warrantydate6''], errors=''coerce'')
df[''Warrantydate7''] = pd.to_datetime(df[''Warrantydate7''], errors=''coerce'')
df[''Warrantydate8''] = pd.to_datetime(df[''Warrantydate8''], errors=''coerce'')
df[''Warrantydate9''] = pd.to_datetime(df[''Warrantydate9''], errors=''coerce'')
df[''Warrantydate10''] = pd.to_datetime(df[''Warrantydate10''], errors=''coerce'')
df.replace({pd.NaT: None}, inplace=True)
data = []
for i, row in df.iterrows():
    data.append(row.tolist())
grouped_lists = []
prev, rest = data[0], data[1:]
subgroup = [prev]
for item in rest:
    if item[0] != prev[0]:
        grouped_lists.append(subgroup)
        subgroup = []
    subgroup.append(item)
    prev = item
grouped_lists.append(subgroup)
sample, golden_records = [], []
for lst in grouped_lists:
    if lst[0][-1] == ''D'':
        old_customers = [item for item in lst if item[1] is not None]
        new_customers = [item for item in lst if item[54] is not None]
        old_sap_id = str(min(old_customers, key=lambda x: x[53])[1]).split(''.'')[0] if len(old_customers) > 0 else None
        new_sap_id = str(min(new_customers, key=lambda x: x[53])[54]).split(''.'')[0] if len(new_customers) > 0 else None
        old_customers = [str(item[1]) for item in lst if item[1] is not None]
        new_customers = [''s'' + str(item[54]).split(''.'')[0] for item in lst if item[54] is not None]
        old_ids = ''''
        new_ids = ''''
        ids_concat = ''''
        if len(old_customers) > 0 and len(new_customers):
            for i in range(len(old_customers)):
                if i == len(old_customers) - 1:
                    old_ids += str(old_customers[i]).split(''.'')[0]
                else:
                    old_ids += str(old_customers[i]).split(''.'')[0] + '' - ''

            for i in range(len(new_customers)):
                if i == len(new_customers) - 1:
                    new_ids += str(new_customers[i]).split(''.'')[0]
                else:
                    new_ids += str(new_customers[i]).split(''.'')[0] + '' - ''
            ids_concat = old_ids + '' - '' + new_ids
        elif len(old_customers) > 0 and len(new_customers) == 0:
            for i in range(len(old_customers)):
                if i == len(old_customers) - 1:
                    old_ids += str(old_customers[i]).split(''.'')[0]
                else:
                    old_ids += str(old_customers[i]).split(''.'')[0] + '' - ''
            ids_concat = old_ids
        else:
            for i in range(len(new_customers)):
                if i == len(new_customers) - 1:
                    new_ids += str(new_customers[i]).split(''.'')[0]
                else:
                    new_ids += str(new_customers[i]).split(''.'')[0] + '' - ''
            ids_concat = new_ids
        name = chosen(lst)[2]
        gender = chosen(lst)[3]
        for item in lst:
            if item[3] == ''Company'':
                gender = ''Company''
                break
        mobile_number = str(chosen(lst)[0])
        addresses_list = [item[4] for item in lst if item[4] is not None]
        addresses = ''''
        for i in range(len(addresses_list)):
            if i == len(addresses_list) - 1:
                addresses += str(f''العنوان {i + 1}: {addresses_list[i]}'')
            else:
                addresses += str(f''العنوان {i + 1}: {addresses_list[i]}'') + ''\n''
        region = max(lst, key=lambda x: x[53])[5]
        if region is None:
            max_sublist = None
            max_date = min([date for date in [item[53] for item in lst]])
            for item in lst:
                if item[53] >= max_date and item[5] is not None:
                    max_sublist = item
            if type(max_sublist) == list:
                region = max_sublist[5]
        date_of_birth = chosen(lst)[6]
        created_on = min([date for date in [item[53] for item in lst]])
        similarity = chosen(lst)[55]
        gr = chosen(lst)[56]
        sample = [old_sap_id, ids_concat, name, gender, mobile_number, addresses, region, date_of_birth,
                  sum([item[7] for item in lst]), int(sum([item[8] for item in lst])), sum([item[9] for item in lst]),
                  int(sum([item[10] for item in lst])), int(sum([item[11] for item in lst])),
                  int(sum([item[12] for item in lst]))]
        current_client = []
        for item in lst:
            current_client.extend(item[13:-4])
        i = 0
        subs = []
        while i < len(current_client):
            subs.append(current_client[i:i + 4])
            i += 4
        sorted_lst = sorted(subs, key=custom_sort)
        flat_list = [item for sublist in sorted_lst for item in sublist]
        flat_list = flat_list[:40]
        sample.extend(flat_list)
        sample.append(created_on)
        sample.append(new_sap_id)
        sample.append(similarity)
        sample.append(''G'')
        golden_records.append(sample)
headers = [''Key'', ''Reference Sap IDs'', ''names'', ''Gender'', ''Mobile_Number'', ''Address'', ''Region'', ''Date_Of_Birth'',
           ''Total_Amount'', ''SalesQuantity'', ''SalesValue'', ''Maintenance'', ''installation'', ''Complaint'',
           ''Category1'', ''Brand1'', ''Workorder1'', ''Warrantydate1'',
           ''Category2'', ''Brand2'', ''Workorder2'', ''Warrantydate2'',
           ''Category3'', ''Brand3'', ''Workorder3'', ''Warrantydate3'',
           ''Category4'', ''Brand4'', ''Workorder4'', ''Warrantydate4'',
           ''Category5'', ''Brand5'', ''Workorder5'', ''Warrantydate5'',
           ''Category6'', ''Brand6'', ''Workorder6'', ''Warrantydate6'',
           ''Category7'', ''Brand7'', ''Workorder7'', ''Warrantydate7'',
           ''Category8'', ''Brand8'', ''Workorder8'', ''Warrantydate8'',
           ''Category9'', ''Brand9'', ''Workorder9'', ''Warrantydate9'',
           ''Category10'', ''Brand10'', ''Workorder10'', ''Warrantydate10'',
           ''CreatedOn'', ''NewSapId'', ''Similarity'', ''Gr'']
dataframe = pd.DataFrame(data=golden_records, columns=headers)
OutputDataSet = dataframe';
		DECLARE @logic_table TABLE
		(
			[Key] NVARCHAR(450),
			[Reference Sap IDs] NVARCHAR(MAX),
			[names] NVARCHAR(450),
			[Gender] NVARCHAR(350),
			[Mobile_Number] NVARCHAR(450),
			[Address] NVARCHAR(MAX),
			[Region] NVARCHAR(MAX),
			[Date_Of_Birth] DATE,
			[Total_Amount] DECIMAL(18, 2),
			[SalesQuantity] INT,
			[SalesValue] DECIMAL(18, 2),
			[Maintenance] INT,
			[installation] INT,
			[Complaint] INT,
			[Category1] NVARCHAR(MAX),
			[Brand1] NVARCHAR(MAX),
			[Workorder1] VARCHAR(12),
			[Warrantydate1] DATE,
			[Category2] NVARCHAR(MAX),
			[Brand2] NVARCHAR(MAX),
			[Workorder2] VARCHAR(12),
			[Warrantydate2] DATE,
			[Category3] NVARCHAR(MAX),
			[Brand3] NVARCHAR(MAX),
			[Workorder3] VARCHAR(12),
			[Warrantydate3] DATE,
			[Category4] NVARCHAR(MAX),
			[Brand4] NVARCHAR(MAX),
			[Workorder4] VARCHAR(12),
			[Warrantydate4] DATE,
			[Category5] NVARCHAR(MAX),
			[Brand5] NVARCHAR(MAX),
			[Workorder5] VARCHAR(12),
			[Warrantydate5] DATE,
			[Category6] NVARCHAR(MAX),
			[Brand6] NVARCHAR(MAX),
			[Workorder6] VARCHAR(12),
			[Warrantydate6] DATE,
			[Category7] NVARCHAR(MAX),
			[Brand7] NVARCHAR(MAX),
			[Workorder7] VARCHAR(12),
			[Warrantydate7] DATE,
			[Category8] NVARCHAR(MAX),
			[Brand8] NVARCHAR(MAX),
			[Workorder8] VARCHAR(12),
			[Warrantydate8] DATE,
			[Category9] NVARCHAR(MAX),
			[Brand9] NVARCHAR(MAX),
			[Workorder9] VARCHAR(12),
			[Warrantydate9] DATE,
			[Category10] NVARCHAR(MAX),
			[Brand10] NVARCHAR(MAX),
			[Workorder10] VARCHAR(12),
			[Warrantydate10] DATE,
			[CreatedOn] DATE,
			[NewSapId] NVARCHAR(450),
			[Similarity] INT,
			[Gr] CHAR(1)
		);
		INSERT INTO @logic_table
		EXEC sp_execute_external_script
			@language = N'Python',
			@script = @Golden_CRM_python_script,
			@input_data_1 = @sql_query;
		DECLARE @sales_sql_script NVARCHAR(MAX);
		SET @sales_sql_script = N'
	SELECT
        [Mobile_Number], [Key], [names], [Gender], [Address], [Region_Test],
        TRY_CONVERT(date, [Date_Of_Birth], 23) AS Date_Of_Birth, [NewSapId],
        TRY_CONVERT(date, COALESCE([CreatedOn], ''1990-01-01''), 23) AS CreatedOn,
		[Flag_SalesCategory11],
        [Material_ID1], [Material_Type1], [Category_1], [Cat_Group1], [Brand_1], [SumQuantity1], [SumValue1], [Invoice1], [InvoiceDate1], [Dist_Channel1],
        [Material_ID2], [Material_Type2], [Category_2], [Cat_Group2], [Brand_2], [SumQuantity2], [SumValue2], [Invoice2], [InvoiceDate2], [Dist_Channel2],
        [Material_ID3], [Material_Type3], [Category_3], [Cat_Group3], [Brand_3], [SumQuantity3], [SumValue3], [Invoice3], [InvoiceDate3], [Dist_Channel3],
        [Material_ID4], [Material_Type4], [Category_4], [Cat_Group4], [Brand_4], [SumQuantity4], [SumValue4], [Invoice4], [InvoiceDate4], [Dist_Channel4],
        [Material_ID5], [Material_Type5], [Category_5], [Cat_Group5], [Brand_5], [SumQuantity5], [SumValue5], [Invoice5], [InvoiceDate5], [Dist_Channel5],
        [Material_ID6], [Material_Type6], [Category_6], [Cat_Group6], [Brand_6], [SumQuantity6], [SumValue6], [Invoice6], [InvoiceDate6], [Dist_Channel6],
        [Material_ID7], [Material_Type7], [Category_7], [Cat_Group7], [Brand_7], [SumQuantity7], [SumValue7], [Invoice7], [InvoiceDate7], [Dist_Channel7],
        [Material_ID8], [Material_Type8], [Category_8], [Cat_Group8], [Brand_8], [SumQuantity8], [SumValue8], [Invoice8], [InvoiceDate8], [Dist_Channel8],
        [Material_ID9], [Material_Type9], [Category_9], [Cat_Group9], [Brand_9], [SumQuantity9], [SumValue9], [Invoice9], [InvoiceDate9], [Dist_Channel9],
        [Material_ID10], [Material_Type10], [Category_10], [Cat_Group10], [Brand_10], [SumQuantity10], [SumValue10], [Invoice10], [InvoiceDate10], [Dist_Channel10],
        [Similarity], [Gr]
	FROM [dbo].[CustomerExperience]
	WHERE 
	(
		(COALESCE([Gr], '''') IN(''O'', ''N'', ''D'') OR [Gr] IS NULL)
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM [dbo].[CustomerExperience]
			WHERE [Modified_On] = CONVERT(date, GETDATE())
		)
		AND [Mobile_Number] IN
		(
			SELECT [Mobile_Number]
			FROM [dbo].[CustomerExperience]
			WHERE [Modified_On] IS NULL
			OR [Modified_On] != CONVERT(date, GETDATE())
		)
	)
	OR
	(
		[Gr] = ''D''
		AND [Modified_On] = CONVERT(date, GETDATE())
	)
	ORDER BY [Mobile_Number];';
		DECLARE @sales_python_script NVARCHAR(MAX);
		SET @sales_python_script = N'
import numpy as np
import pandas as pd
import re
from datetime import datetime

df = InputDataSet
def contains_same_char(s):
    return all(char == str(s)[0] for char in str(s))
def chosen(lst):
    max_arabic_len = 0
    max_arabic_sublist = None
    max_english_len = 0
    max_english_sublist = None
    for item in lst:
        if any(c for c in item[2] if ''\u0600'' <= c <= ''\u06FF''):
            if len(item[2]) > max_arabic_len:
                max_arabic_len = len(item[2])
                max_arabic_sublist = item
        else:
            if len(item[2]) > max_english_len:
                max_english_len = len(item[2])
                max_english_sublist = item
    if max_arabic_sublist is not None:
        return max_arabic_sublist
    else:
        return max_english_sublist
def custom_sort(sublist):
    if all(value is None for value in sublist):
        return 1
    elif all(value is None for value in sublist[:10]):
        return 2
    else:
        return 0
df = InputDataSet
df[[''SumQuantity1'', ''SumValue1'', ''SumQuantity2'', ''SumValue2'', ''SumQuantity3'', ''SumValue3'', ''SumQuantity4'', ''SumValue4'', ''SumQuantity5'', ''SumValue5'', ''SumQuantity6'', ''SumValue6'', ''SumQuantity7'', ''SumValue7'', ''SumQuantity8'', ''SumValue8'', ''SumQuantity9'', ''SumValue9'', ''SumQuantity10'', ''SumValue10'']] = df[[''SumQuantity1'', ''SumValue1'', ''SumQuantity2'', ''SumValue2'', ''SumQuantity3'', ''SumValue3'', ''SumQuantity4'', ''SumValue4'', ''SumQuantity5'', ''SumValue5'', ''SumQuantity6'', ''SumValue6'', ''SumQuantity7'', ''SumValue7'', ''SumQuantity8'', ''SumValue8'', ''SumQuantity9'', ''SumValue9'', ''SumQuantity10'', ''SumValue10'']].fillna(value=0)
df.replace({pd.NaT: None}, inplace=True)
data = []
for i, row in df.iterrows():
    data.append(row.tolist())
grouped_lists = []
prev, rest = data[0], data[1:]
subgroup = [prev]
for item in rest:
    if item[0] != prev[0]:
        grouped_lists.append(subgroup)
        subgroup = []
    subgroup.append(item)
    prev = item
grouped_lists.append(subgroup)
sample, golden_records = [], []
for lst in grouped_lists:
    if lst[0][-1] == ''D'':
        old_customers = [item for item in lst if item[1] is not None]
        new_customers = [item for item in lst if item[7] is not None]
        old_sap_id = str(min(old_customers, key=lambda x: x[8])[1]).split(''.'')[0] if len(old_customers) > 0 else None
        new_sap_id = str(min(new_customers, key=lambda x: x[8])[7]).split(''.'')[0] if len(new_customers) > 0 else None
        old_customers = [str(item[1]) for item in lst if item[1] is not None]
        new_customers = [''s'' + str(item[7]).split(''.'')[0] for item in lst if item[7] is not None]
        old_ids = ''''
        new_ids = ''''
        ids_concat = ''''
        if len(old_customers) > 0 and len(new_customers):
            for i in range(len(old_customers)):
                if i == len(old_customers) - 1:
                    old_ids += str(old_customers[i]).split(''.'')[0]
                else:
                    old_ids += str(old_customers[i]).split(''.'')[0] + '' - ''

            for i in range(len(new_customers)):
                if i == len(new_customers) - 1:
                    new_ids += str(new_customers[i]).split(''.'')[0]
                else:
                    new_ids += str(new_customers[i]).split(''.'')[0] + '' - ''
            ids_concat = old_ids + '' - '' + new_ids
        elif len(old_customers) > 0 and len(new_customers) == 0:
            for i in range(len(old_customers)):
                if i == len(old_customers) - 1:
                    old_ids += str(old_customers[i]).split(''.'')[0]
                else:
                    old_ids += str(old_customers[i]).split(''.'')[0] + '' - ''
            ids_concat = old_ids
        else:
            for i in range(len(new_customers)):
                if i == len(new_customers) - 1:
                    new_ids += str(new_customers[i]).split(''.'')[0]
                else:
                    new_ids += str(new_customers[i]).split(''.'')[0] + '' - ''
            ids_concat = new_ids
        name = chosen(lst)[2]
        gender = chosen(lst)[3]
        for item in lst:
            if item[3] == ''Company'':
                gender = ''Company''
                break
        mobile_number = str(chosen(lst)[0])
        addresses_list = [item[4] for item in lst if item[4] is not None]
        addresses = ''''
        for i in range(len(addresses_list)):
            if i == len(addresses_list) - 1:
                addresses += str(f''العنوان {i + 1}: {addresses_list[i]}'')
            else:
                addresses += str(f''العنوان {i + 1}: {addresses_list[i]}'') + ''\n''
        region = max(lst, key=lambda x: x[8])[5]
        max_sublist = None
        if region is None:
            max_date = min([date for date in [item[8] for item in lst]])
            for item in lst:
                if item[8] >= max_date and item[5] is not None:
                    max_sublist = item
            if type(max_sublist) == list:
                region = max_sublist[5]
        date_of_birth = chosen(lst)[6]
        created_on = min([date for date in [item[8] for item in lst]])
        similarity = chosen(lst)[-2]
        gr = chosen(lst)[-1]
        sample = [old_sap_id, ids_concat, name, gender, mobile_number, addresses, region, date_of_birth]
        current_client = []
        for item in lst:
            current_client.extend(item[10:-2])
        i = 0
        subs = []
        while i < len(current_client):
            subs.append(current_client[i:i + 10])
            i += 10
        subs = [[None if value == 0 else value for value in sublist] for sublist in subs]
        sorted_lst = sorted(subs, key=custom_sort)
        flat_list = [item for sublist in sorted_lst for item in sublist]
        sales_flag = 0
        if flat_list[101] is not None:
            sales_flag = 1
        flags = [item[9] for item in lst if item is not None]
        if 1 in flags:
            sales_flag = 1
        flat_list = flat_list[:100]
        sample.extend(flat_list)
        sample.append(created_on)
        sample.append(new_sap_id)
        sample.append(similarity)
        sample.append(''G'')
        sample.append(sales_flag)
        golden_records.append(sample)
headers = [''Key'', ''Reference Sap IDs'', ''names'', ''Gender'', ''Mobile_Number'', ''Address'', ''Region'', ''Date_Of_Birth'',
           ''Material_ID1'', ''Material_Type1'', ''Category_1'', ''Cat_Group1'', ''Brand_1'', ''SumQuantity1'', ''SumValue1'', ''Invoice1'', ''InvoiceDate1'', ''Dist_Channel1'',
           ''Material_ID2'', ''Material_Type2'', ''Category_2'', ''Cat_Group2'', ''Brand_2'', ''SumQuantity2'', ''SumValue2'', ''Invoice2'', ''InvoiceDate2'', ''Dist_Channel2'',
           ''Material_ID3'', ''Material_Type3'', ''Category_3'', ''Cat_Group3'', ''Brand_3'', ''SumQuantity3'', ''SumValue3'', ''Invoice3'', ''InvoiceDate3'', ''Dist_Channel3'',
           ''Material_ID4'', ''Material_Type4'', ''Category_4'', ''Cat_Group4'', ''Brand_4'', ''SumQuantity4'', ''SumValue4'', ''Invoice4'', ''InvoiceDate4'', ''Dist_Channel4'',
           ''Material_ID5'', ''Material_Type5'', ''Category_5'', ''Cat_Group5'', ''Brand_5'', ''SumQuantity5'', ''SumValue5'', ''Invoice5'', ''InvoiceDate5'', ''Dist_Channel5'',
           ''Material_ID6'', ''Material_Type6'', ''Category_6'', ''Cat_Group6'', ''Brand_6'', ''SumQuantity6'', ''SumValue6'', ''Invoice6'', ''InvoiceDate6'', ''Dist_Channel6'',
           ''Material_ID7'', ''Material_Type7'', ''Category_7'', ''Cat_Group7'', ''Brand_7'', ''SumQuantity7'', ''SumValue7'', ''Invoice7'', ''InvoiceDate7'', ''Dist_Channel7'',
           ''Material_ID8'', ''Material_Type8'', ''Category_8'', ''Cat_Group8'', ''Brand_8'', ''SumQuantity8'', ''SumValue8'', ''Invoice8'', ''InvoiceDate8'', ''Dist_Channel8'',
           ''Material_ID9'', ''Material_Type9'', ''Category_9'', ''Cat_Group9'', ''Brand_9'', ''SumQuantity9'', ''SumValue9'', ''Invoice9'', ''InvoiceDate9'', ''Dist_Channel9'',
           ''Material_ID10'', ''Material_Type10'', ''Category_10'', ''Cat_Group10'', ''Brand_10'', ''SumQuantity10'', ''SumValue10'', ''Invoice10'', ''InvoiceDate10'', ''Dist_Channel10'',
           ''CreatedOn'', ''NewSapId'', ''Similarity'', ''Gr'', ''Sales_Flag'']
dataframe = pd.DataFrame(data=golden_records, columns=headers)
OutputDataSet = dataframe';
DECLARE @sales_logic_table TABLE
(
        [Key] NVARCHAR(450),
        [Reference Sap IDs] NVARCHAR(MAX),
        [names] NVARCHAR(450),
        [Gender] NVARCHAR(350),
        [Mobile_Number] NVARCHAR(450),
        [Address] NVARCHAR(MAX),
        [Region] NVARCHAR(MAX),
        [Date_Of_Birth] DATE,
		[Material_ID1] NVARCHAR(MAX),
		[Material_Type1] NVARCHAR(MAX),
		[Category_1] NVARCHAR(450),
        [Cat_Group1] NVARCHAR(MAX),
        [Brand_1] NVARCHAR(450),
        [SumQuantity1] FLOAT,
        [SumValue1] FLOAT,
        [Invoice1] NVARCHAR(MAX),
        [InvoiceDate1] DATE,
		[Dist_Channel1] NVARCHAR(MAX),
		[Material_ID2] NVARCHAR(MAX),
		[Material_Type2] NVARCHAR(MAX),
        [Category_2] NVARCHAR(450),
        [Cat_Group2] NVARCHAR(MAX),
        [Brand_2] NVARCHAR(450),
        [SumQuantity2] FLOAT,
        [SumValue2] FLOAT,
        [Invoice2] NVARCHAR(MAX),
        [InvoiceDate2] DATE,
		[Dist_Channel2] NVARCHAR(MAX),
		[Material_ID3] NVARCHAR(MAX),
		[Material_Type3] NVARCHAR(MAX),
        [Category_3] NVARCHAR(450),
        [Cat_Group3] NVARCHAR(MAX),
        [Brand_3] NVARCHAR(450),
        [SumQuantity3] FLOAT,
        [SumValue3] FLOAT,
        [Invoice3] NVARCHAR(MAX),
        [InvoiceDate3] DATE,
		[Dist_Channel3] NVARCHAR(MAX),
		[Material_ID4] NVARCHAR(MAX),
		[Material_Type4] NVARCHAR(MAX),
        [Category_4] NVARCHAR(450),
        [Cat_Group4] NVARCHAR(MAX),
        [Brand_4] NVARCHAR(450),
        [SumQuantity4] FLOAT,
        [SumValue4] FLOAT,
        [Invoice4] NVARCHAR(MAX),
        [InvoiceDate4] DATE,
		[Dist_Channel4] NVARCHAR(MAX),
		[Material_ID5] NVARCHAR(MAX),
		[Material_Type5] NVARCHAR(MAX),
        [Category_5] NVARCHAR(450),
        [Cat_Group5] NVARCHAR(MAX),
        [Brand_5] NVARCHAR(450),
        [SumQuantity5] FLOAT,
        [SumValue5] FLOAT,
        [Invoice5] NVARCHAR(MAX),
        [InvoiceDate5] DATE,
		[Dist_Channel5] NVARCHAR(MAX),
		[Material_ID6] NVARCHAR(MAX),
		[Material_Type6] NVARCHAR(MAX),
        [Category_6] NVARCHAR(450),
        [Cat_Group6] NVARCHAR(MAX),
        [Brand_6] NVARCHAR(450),
        [SumQuantity6] FLOAT,
        [SumValue6] FLOAT,
        [Invoice6] NVARCHAR(MAX),
        [InvoiceDate6] DATE,
		[Dist_Channel6] NVARCHAR(MAX),
		[Material_ID7] NVARCHAR(MAX),
		[Material_Type7] NVARCHAR(MAX),
        [Category_7] NVARCHAR(450),
        [Cat_Group7] NVARCHAR(MAX),
        [Brand_7] NVARCHAR(450),
        [SumQuantity7] FLOAT,
        [SumValue7] FLOAT,
        [Invoice7] NVARCHAR(MAX),
        [InvoiceDate7] DATE,
		[Dist_Channel7] NVARCHAR(MAX),
		[Material_ID8] NVARCHAR(MAX),
		[Material_Type8] NVARCHAR(MAX),
        [Category_8] NVARCHAR(450),
        [Cat_Group8] NVARCHAR(MAX),
        [Brand_8] NVARCHAR(450),
        [SumQuantity8] FLOAT,
        [SumValue8] FLOAT,
        [Invoice8] NVARCHAR(MAX),
        [InvoiceDate8] DATE,
		[Dist_Channel8] NVARCHAR(MAX),
		[Material_ID9] NVARCHAR(MAX),
		[Material_Type9] NVARCHAR(MAX),
        [Category_9] NVARCHAR(450),
        [Cat_Group9] NVARCHAR(MAX),
        [Brand_9] NVARCHAR(450),
        [SumQuantity9] FLOAT,
        [SumValue9] FLOAT,
        [Invoice9] NVARCHAR(MAX),
        [InvoiceDate9] DATE,
		[Dist_Channel9] NVARCHAR(MAX),
		[Material_ID10] NVARCHAR(MAX),
		[Material_Type10] NVARCHAR(MAX),
        [Category_10] NVARCHAR(450),
        [Cat_Group10] NVARCHAR(MAX),
        [Brand_10] NVARCHAR(450),
        [SumQuantity10] FLOAT,
        [SumValue10] FLOAT,
        [Invoice10] NVARCHAR(MAX),
        [InvoiceDate10] DATE,
		[Dist_Channel10] NVARCHAR(MAX),
        [CreatedOn] DATE,
        [NewSapId] NVARCHAR(450),
        [Similarity] INT,
        [Gr] CHAR(1),
		[Sales_Flag] INT
);
INSERT INTO @sales_logic_table
EXEC sp_execute_external_script
	@language = N'Python',
	@script = @sales_python_script,
	@input_data_1 = @sales_sql_script;

INSERT INTO [dbo].[CustomerExperience]
(
	[Key], [Reference Sap IDs], [names], [Gender], [Mobile_Number],
	[Address], [region_test], [Date_Of_Birth],
	[Total_Amount], [SalesQuantity], [SalesValue],
	[Maintenance], [installation], [Complaint],
	[Category1], [Brand1], [WorkOrder1], [Warrantydate1],
	[Category2], [Brand2], [WorkOrder2], [Warrantydate2],
	[Category3], [Brand3], [WorkOrder3], [Warrantydate3],
	[Category4], [Brand4], [WorkOrder4], [Warrantydate4],
	[Category5], [Brand5], [WorkOrder5], [Warrantydate5],
	[Category6], [Brand6], [WorkOrder6], [Warrantydate6],
	[Category7], [Brand7], [WorkOrder7], [Warrantydate7],
	[Category8], [Brand8], [WorkOrder8], [Warrantydate8],
	[Category9], [Brand9], [WorkOrder9], [Warrantydate9],
	[Category10], [Brand10], [WorkOrder10], [Warrantydate10],
	[Material_ID1], [Material_Type1], [Category_1], [Cat_Group1], [Brand_1], [SumQuantity1], [SumValue1], [Invoice1], [InvoiceDate1], [Dist_Channel1],
	[Material_ID2], [Material_Type2], [Category_2], [Cat_Group2], [Brand_2], [SumQuantity2], [SumValue2], [Invoice2], [InvoiceDate2], [Dist_Channel2],
	[Material_ID3], [Material_Type3], [Category_3], [Cat_Group3], [Brand_3], [SumQuantity3], [SumValue3], [Invoice3], [InvoiceDate3], [Dist_Channel3],
	[Material_ID4], [Material_Type4], [Category_4], [Cat_Group4], [Brand_4], [SumQuantity4], [SumValue4], [Invoice4], [InvoiceDate4], [Dist_Channel4],
	[Material_ID5], [Material_Type5], [Category_5], [Cat_Group5], [Brand_5], [SumQuantity5], [SumValue5], [Invoice5], [InvoiceDate5], [Dist_Channel5],
	[Material_ID6], [Material_Type6], [Category_6], [Cat_Group6], [Brand_6], [SumQuantity6], [SumValue6], [Invoice6], [InvoiceDate6], [Dist_Channel6],
	[Material_ID7], [Material_Type7], [Category_7], [Cat_Group7], [Brand_7], [SumQuantity7], [SumValue7], [Invoice7], [InvoiceDate7], [Dist_Channel7],
	[Material_ID8], [Material_Type8], [Category_8], [Cat_Group8], [Brand_8], [SumQuantity8], [SumValue8], [Invoice8], [InvoiceDate8], [Dist_Channel8],
	[Material_ID9], [Material_Type9], [Category_9], [Cat_Group9], [Brand_9], [SumQuantity9], [SumValue9], [Invoice9], [InvoiceDate9], [Dist_Channel9],
	[Material_ID10], [Material_Type10], [Category_10], [Cat_Group10], [Brand_10], [SumQuantity10], [SumValue10], [Invoice10], [InvoiceDate10], [Dist_Channel10],
	[CreatedOn], [NewSapId], [Similarity], [Gr], [Flag_SalesCategory11]
)
SELECT
	CRM.[Key], CRM.[Reference Sap IDs], CRM.[names], CRM.[Gender], CRM.[Mobile_Number],
	CRM.[Address], CRM.[Region], CRM.[Date_Of_Birth],
	CRM.[Total_Amount], CRM.[SalesQuantity], CRM.[SalesValue],
	CRM.[Maintenance], CRM.[installation], CRM.[Complaint],
	CRM.[Category1], CRM.[Brand1], CRM.[Workorder1], CRM.[Warrantydate1],
	CRM.[Category2], CRM.[Brand2], CRM.[Workorder2], CRM.[Warrantydate2],
	CRM.[Category3], CRM.[Brand3], CRM.[Workorder3], CRM.[Warrantydate3],
	CRM.[Category4], CRM.[Brand4], CRM.[Workorder4], CRM.[Warrantydate4],
	CRM.[Category5], CRM.[Brand5], CRM.[Workorder5], CRM.[Warrantydate5],
	CRM.[Category6], CRM.[Brand6], CRM.[Workorder6], CRM.[Warrantydate6],
	CRM.[Category7], CRM.[Brand7], CRM.[Workorder7], CRM.[Warrantydate7],
	CRM.[Category8], CRM.[Brand8], CRM.[Workorder8], CRM.[Warrantydate8],
	CRM.[Category9], CRM.[Brand9], CRM.[Workorder9], CRM.[Warrantydate9],
	CRM.[Category10], CRM.[Brand10], CRM.[Workorder10], CRM.[Warrantydate10],
	Sales_table.[Material_ID1], Sales_table.[Material_Type1], Sales_table.[Category_1], Sales_table.[Cat_Group1], Sales_table.[Brand_1], Sales_table.[SumQuantity1], Sales_table.[SumValue1], Sales_table.[Invoice1], Sales_table.[InvoiceDate1], Sales_table.[Dist_Channel1],
	Sales_table.[Material_ID3], Sales_table.[Material_Type2], Sales_table.[Category_2], Sales_table.[Cat_Group2], Sales_table.[Brand_2], Sales_table.[SumQuantity2], Sales_table.[SumValue2], Sales_table.[Invoice2], Sales_table.[InvoiceDate2], Sales_table.[Dist_Channel2],
	Sales_table.[Material_ID3], Sales_table.[Material_Type3], Sales_table.[Category_3], Sales_table.[Cat_Group3], Sales_table.[Brand_3], Sales_table.[SumQuantity3], Sales_table.[SumValue3], Sales_table.[Invoice3], Sales_table.[InvoiceDate3], Sales_table.[Dist_Channel3],
	Sales_table.[Material_ID4], Sales_table.[Material_Type4], Sales_table.[Category_4], Sales_table.[Cat_Group4], Sales_table.[Brand_4], Sales_table.[SumQuantity4], Sales_table.[SumValue4], Sales_table.[Invoice4], Sales_table.[InvoiceDate4], Sales_table.[Dist_Channel4],
	Sales_table.[Material_ID5], Sales_table.[Material_Type5], Sales_table.[Category_5], Sales_table.[Cat_Group5], Sales_table.[Brand_5], Sales_table.[SumQuantity5], Sales_table.[SumValue5], Sales_table.[Invoice5], Sales_table.[InvoiceDate5], Sales_table.[Dist_Channel5],
	Sales_table.[Material_ID6], Sales_table.[Material_Type6], Sales_table.[Category_6], Sales_table.[Cat_Group6], Sales_table.[Brand_6], Sales_table.[SumQuantity6], Sales_table.[SumValue6], Sales_table.[Invoice6], Sales_table.[InvoiceDate6], Sales_table.[Dist_Channel6],
	Sales_table.[Material_ID7], Sales_table.[Material_Type7], Sales_table.[Category_7], Sales_table.[Cat_Group7], Sales_table.[Brand_7], Sales_table.[SumQuantity7], Sales_table.[SumValue7], Sales_table.[Invoice7], Sales_table.[InvoiceDate7], Sales_table.[Dist_Channel7],
	Sales_table.[Material_ID8], Sales_table.[Material_Type8], Sales_table.[Category_8], Sales_table.[Cat_Group8], Sales_table.[Brand_8], Sales_table.[SumQuantity8], Sales_table.[SumValue8], Sales_table.[Invoice8], Sales_table.[InvoiceDate8], Sales_table.[Dist_Channel8],
	Sales_table.[Material_ID9], Sales_table.[Material_Type9], Sales_table.[Category_9], Sales_table.[Cat_Group9], Sales_table.[Brand_9], Sales_table.[SumQuantity9], Sales_table.[SumValue9], Sales_table.[Invoice9], Sales_table.[InvoiceDate9], Sales_table.[Dist_Channel9],
	Sales_table.[Material_ID10], Sales_table.[Material_Type10], Sales_table.[Category_10], Sales_table.[Cat_Group10], Sales_table.[Brand_10], Sales_table.[SumQuantity10], Sales_table.[SumValue10], Sales_table.[Invoice10], Sales_table.[InvoiceDate10], Sales_table.[Dist_Channel10],
	Sales_table.[CreatedOn], Sales_table.[NewSapId], Sales_table.[Similarity], Sales_table.[Gr], Sales_table.[Sales_Flag]
FROM @logic_table CRM INNER JOIN @sales_logic_table Sales_table
ON CRM.[Mobile_Number] = Sales_table.[Mobile_Number];
		-------------------------------------------------------------------------------------------------------
        UPDATE cx
		SET cx.[CRMCaterory11Flag] = 1
		FROM [dbo].[CustomerExperience] cx
		INNER JOIN @logic_table CRM
		ON cx.[Mobile_Number] = CRM.[Mobile_Number]
		AND cx.[Gr] = 'G'
		AND COALESCE(cx.[Maintenance], 0) + COALESCE(cx.[installation], 0) + COALESCE(cx.[Complaint], 0) > 10;
		-------------------------------------------------------------------------------------------------------
        UPDATE cx
		SET cx.[CRMCaterory11Flag] = 0
		FROM [dbo].[CustomerExperience] cx
		INNER JOIN @logic_table CRM
		ON cx.[Mobile_Number] = CRM.[Mobile_Number]
		AND cx.[Gr] = 'G'
		AND COALESCE(cx.[Maintenance], 0) + COALESCE(cx.[installation], 0) + COALESCE(cx.[Complaint], 0) <= 10;
        -------------------------------------------------------------------------------------------------------
        UPDATE [dbo].[CustomerExperience]
        SET [Reference Sap IDs] = NULL
        WHERE [Gr] IN('O', 'N')
        AND [Mobile_Number] IN
        (
            SELECT [Mobile_Number]
            FROM [dbo].[CustomerExperience]
            WHERE [Modified_On] = CONVERT(date, GETDATE())
        )
        -------------------------------------------------------------------------------------------------------
        UPDATE [dbo].[CustomerExperience]
        SET [Gr] = 'O'
        WHERE [Gr] IS NULL;
        -------------------------------------------------------------------------------------------------------
        DECLARE @references_sql_query NVARCHAR(MAX);
    SET @references_sql_query = N'
    SELECT [Reference Sap IDs], [Key]
    FROM [dbo].[CustomerExperience]
    WHERE [Gr] = ''G''
    AND [Mobile_Number] IN
    (
        SELECT [Mobile_Number]
        FROM [dbo].[CustomerExperience]
        WHERE [Modified_On] = CONVERT(date, GETDATE())
    )
    ';
    DECLARE @references_python_script NVARCHAR(MAX);
    SET @references_python_script = N'
import pandas as pd
df = InputDataSet
References = []
df2 = df[[''Reference Sap IDs'', ''Key'']].copy()
for ind, row in df2.iterrows():
    lst = [row[''Reference Sap IDs''], row[''Key'']]
    for i in range(len(lst[0].split('' - ''))):
        References.append([lst[0].split('' - '')[i], lst[1]])
dataframe = pd.DataFrame(data=References, columns=[''Reference'', ''Key''])
dataframe[''Reference''] = dataframe[''Reference''].apply(lambda x: x.split(''s'', 1)[-1] if ''s'' in x else x)
OutputDataSet = dataframe
';
    DECLARE @references_table TABLE
    (
        [Reference Sap IDs] NVARCHAR(450),
        [Key] NVARCHAR(450)
    );
    INSERT INTO @references_table
    EXEC sp_execute_external_script
        @language = N'Python',
        @script = @references_python_script,
        @input_data_1 = @references_sql_query;
UPDATE cx
SET cx.[Reference Sap IDs] = tt.[Key]
FROM [dbo].[CustomerExperience] cx
INNER JOIN @references_table tt
ON cx.[Key] = tt.[Reference Sap IDs]
AND cx.[Gr] = 'D';
    -------------------------------------------------------------------------------------------------------
UPDATE cx
SET cx.[Reference Sap IDs] = tt.[Key]
FROM [dbo].[CustomerExperience] cx
INNER JOIN @references_table tt
ON cx.[NewSapId] = tt.[Reference Sap IDs]
AND cx.[Gr] = 'D';
    -------------------------------------------------------------------------------------------------------
DECLARE @references_second_sql_script NVARCHAR(MAX);
SET @references_second_sql_script = N'
SELECT [Reference Sap IDs], [NewSapId]
FROM [dbo].[CustomerExperience]
WHERE [Key] IS NULL AND [NewSapId] IS NOT NULL
AND [Gr] = ''G''
AND [Mobile_Number] IN
(
    SELECT [Mobile_Number]
    FROM [dbo].[CustomerExperience]
    WHERE [Modified_On] = CONVERT(date, GETDATE())
);';
DECLARE @references_second_python_script NVARCHAR(MAX);
SET @references_second_python_script = N'
import pandas as pd
df = InputDataSet
df2 = df[[''Reference Sap IDs'', ''NewSapId'']].copy()
References = []
for ind, row in df2.iterrows():
    lst = [row[''Reference Sap IDs''], row[''NewSapId'']]
    for i in range(len(lst[0].split('' - ''))):
        References.append([lst[0].split('' - '')[i], lst[1]])
dataframe = pd.DataFrame(data=References, columns=[''Reference'', ''NewSapId''])
dataframe[''Reference''] = dataframe[''Reference''].apply(lambda x: x.split(''s'', 1)[-1] if ''s'' in x else x)
OutputDataSet = dataframe
';
    DECLARE @references_second_table TABLE
    (
        [Reference Sap IDs] NVARCHAR(450),
        [NewSapId] NVARCHAR(450)
    );
    INSERT INTO @references_second_table
    EXEC sp_execute_external_script
        @language = N'Python',
        @script = @references_second_python_script,
        @input_data_1 = @references_second_sql_script;
    UPDATE cx
    SET cx.[Reference Sap IDs] = tt.[NewSapId]
    FROM [dbo].[CustomerExperience] cx
    INNER JOIN @references_second_table tt
    ON cx.[NewSapId] = tt.[Reference Sap IDs]
    AND cx.[Gr] = 'D';

	UPDATE cx2
	SET cx2.[centraltelephonename] = cx1.[centraltelephonename]
	FROM [dbo].[CustomerExperience] cx1
	INNER JOIN [dbo].[CustomerExperience] cx2
	ON cx1.[Mobile_Number] = cx2.[Mobile_Number]
	AND cx1.[Gr] = 'D' AND cx2.[Gr] = 'G'
	AND cx1.[names] = cx2.[names]
	AND cx1.[region_test] = cx2.[region_test];

	UPDATE [dbo].[CustomerExperience]
	SET [Maintenance] = NULL, [Complaint] = NULL, [installation] = NULL
	WHERE [Maintenance] = 0 AND [Complaint] = 0 AND [installation] = 0
	AND [Gr] = 'G';
	END;
	ELSE
	BEGIN
		DECLARE @empty_table TABLE([Key] NVARCHAR(450), [NewSapId] NVARCHAR(450), [names] NVARCHAR(450));
		SELECT * FROM @empty_table;
	END;
END;