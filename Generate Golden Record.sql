USE [CustomerProfile-DB]
GO
/****** Object:  StoredProcedure [dbo].[Generate_Golden_Record]    Script Date: 11/12/2024 11:27:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[Generate_Golden_Record]
AS
BEGIN
DECLARE @pscript NVARCHAR(MAX);
SET @pscript = N'
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from itertools import combinations
import re
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
';
DECLARE @sqlscript NVARCHAR(MAX);
SET @sqlscript = N'WITH CTE AS(SELECT [Mobile_Number], COUNT(*) AS Counters FROM [dbo].[Sandbox_31_7] GROUP BY [Mobile_Number] HAVING COUNT([Mobile_Number]) > 1) SELECT t.[Mobile_Number], cx.[Key], cx.[NewSapId], cx.[names] FROM [dbo].[Sandbox_31_7] cx INNER JOIN CTE t ON t.[Mobile_Number] = cx.[Mobile_Number] ORDER BY t.[Mobile_Number];';
DECLARE @temp_table TABLE
(
	[Mobile_Number] NVARCHAR(450),
	[Key] NVARCHAR(450),
	[NewSapId] NVARCHAR(450),
	[names] NVARCHAR(MAX),
	[Similarity] INT,
	[Gr] CHAR
);
INSERT INTO @temp_table
EXEC sp_execute_external_script
	@language = N'Python',
    @script = @pscript,
	@input_data_1 = @sqlscript;
UPDATE cx
SET cx.[Similarity] = tt.[Similarity], cx.[Gr] = tt.[Gr]
FROM [dbo].[Sandbox_31_7] cx
INNER JOIN @temp_table tt
ON cx.[Mobile_Number] = tt.[Mobile_Number];
--AND (cx.[Gr] != 'G' OR cx.[Gr] IS NULL);
DECLARE @sql_query NVARCHAR(MAX);
SET @sql_query = N'
SELECT [Mobile_Number], [Key], [names], [Gender], [Address], [Region_Test], TRY_CONVERT(date, [Date_Of_Birth], 23) AS Date_Of_Birth, CAST(COALESCE([Total_Amount], 0) AS float) AS Total_Amount, COALESCE([SalesQuantity], 0) AS SalesQuantity, CAST(COALESCE([SalesValue], 0) AS float) AS SalesValue, COALESCE([Maintenance], 0) AS Maintenance, COALESCE([installation], 0) AS installation, COALESCE([Complaint], 0) AS Complaint, [Category1], [Brand1], [Workorder1], TRY_CONVERT(date, [Warrantydate1], 23) AS Warrantydate1, [Category2], [Brand2], [Workorder2], TRY_CONVERT(date, [Warrantydate2], 23) AS Warrantydate2, [Category3], [Brand3], [Workorder3], TRY_CONVERT(date, [Warrantydate3], 23) AS Warrantydate3, [Category4], [Brand4], [Workorder4], TRY_CONVERT(date, [Warrantydate4], 23) AS Warrantydate4, [Category5], [Brand5], [Workorder5], TRY_CONVERT(date, [Warrantydate5], 23) AS Warrantydate5, [Category6], [Brand6], [Workorder6], TRY_CONVERT(date, [Warrantydate6], 23) AS Warrantydate6, [Category7], [Brand7], [Workorder7], TRY_CONVERT(date, [Warrantydate7], 23) AS Warrantydate7, [Category8], [Brand8], [Workorder8], TRY_CONVERT(date, [Warrantydate8], 23) AS Warrantydate8, [Category9], [Brand9], [Workorder9], TRY_CONVERT(date, [Warrantydate9], 23) AS Warrantydate9, [Category10], [Brand10], [Workorder10], TRY_CONVERT(date, [Warrantydate10], 23) AS Warrantydate10, TRY_CONVERT(date, COALESCE([CreatedOn], ''1990-01-01''), 23) AS CreatedOn, [NewSapId], [Similarity], [Gr] FROM [dbo].[Sandbox_31_7] WHERE [Gr] = ''D'' ORDER BY [Mobile_Number] ASC;';

DECLARE @python_script NVARCHAR(MAX);
SET @python_script = N'
import numpy as np
import pandas as pd
import re
import datetime
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
df = InputDataSet
columns = [''Category1'', ''Brand1'', ''Workorder1'', ''Warrantydate1'', ''Category2'', ''Brand2'', ''Workorder2'',
           ''Warrantydate2'', ''Category3'', ''Brand3'', ''Workorder3'', ''Warrantydate3'',
           ''Category4'', ''Brand4'', ''Workorder4'', ''Warrantydate4'', ''Category5'',
           ''Brand5'', ''Workorder5'', ''Warrantydate5'', ''Category6'', ''Brand6'',
           ''Workorder6'', ''Warrantydate6'', ''Category7'', ''Brand7'', ''Workorder7'',
           ''Warrantydate7'', ''Category8'', ''Brand8'', ''Workorder8'', ''Warrantydate8'',
           ''Category9'', ''Brand9'', ''Workorder9'', ''Warrantydate9'', ''Category10'',
           ''Brand10'', ''Workorder10'', ''Warrantydate10'']
df[''Mobile_Number''] = df[''Mobile_Number''].astype(str)
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
        mobile_number = str(chosen(lst)[0])
        addresses_list = [item[4] for item in lst if item[4] is not None]
        addresses = ''''
        for i in range(len(addresses_list)):
            if i == len(addresses_list) - 1:
                addresses += str(f''العنوان {i + 1}: {addresses_list[i]}'')
            else:
                addresses += str(f''العنوان {i + 1}: {addresses_list[i]}'') + ''\n''
        region = max(lst, key=lambda x: x[53])[5]
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
            current_client.extend(item[13:53])
        i = 0
        subs = []
        while i < len(current_client):
            subs.append(current_client[i:i + 4])
            i += 4
        i = 0
        output = []
        while i < len(subs) and len(output) < 40:
            if not all(element == subs[i][0] for element in subs[i]):
                output.extend(subs[i])
            i += 1
        for i in range(40 - len(output)):
            output.append('''')
        sample.extend(output)
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
/*

*/
DECLARE @logic_table TABLE
(
	[Key] NVARCHAR(450), [Reference Sap IDs] NVARCHAR(MAX), [names] NVARCHAR(450), [Gender] NVARCHAR(350), [Mobile_Number] NVARCHAR(450),
    [Address] NVARCHAR(MAX), [Region_Test] NVARCHAR(MAX), [Date_Of_Birth] DATE, [Total_Amount] DECIMAL(18, 2), [SalesQuantity] INT,
    [SalesValue] DECIMAL(18, 2), [Maintenance] INT, [installation] INT, [Complaint] INT, [Category1] NVARCHAR(MAX), [Brand1] NVARCHAR(MAX),
    [Workorder1] VARCHAR(12), [Warrantydate1] DATE, [Category2] NVARCHAR(MAX), [Brand2] NVARCHAR(MAX), [Workorder2] VARCHAR(12),
    [Warrantydate2] DATE, [Category3] NVARCHAR(MAX), [Brand3] NVARCHAR(MAX), [Workorder3] VARCHAR(12), [Warrantydate3] DATE,
    [Category4] NVARCHAR(MAX), [Brand4] NVARCHAR(MAX), [Workorder4] VARCHAR(12), [Warrantydate4] DATE, [Category5] NVARCHAR(MAX),
    [Brand5] NVARCHAR(MAX), [Workorder5] VARCHAR(12), [Warrantydate5] DATE, [Category6] NVARCHAR(MAX), [Brand6] NVARCHAR(MAX),
    [Workorder6] VARCHAR(12), [Warrantydate6] DATE, [Category7] NVARCHAR(MAX), [Brand7] NVARCHAR(MAX), [Workorder7] VARCHAR(12),
    [Warrantydate7] DATE, [Category8] NVARCHAR(MAX), [Brand8] NVARCHAR(MAX), [Workorder8] VARCHAR(12), [Warrantydate8] DATE,
	[Category9] NVARCHAR(MAX), [Brand9] NVARCHAR(MAX), [Workorder9] VARCHAR(12), [Warrantydate9] DATE, [Category10] NVARCHAR(MAX),
    [Brand10] NVARCHAR(MAX), [Workorder10] VARCHAR(12), [Warrantydate10] DATE,
	[CreatedOn] DATE, [NewSapId] NVARCHAR(450),
    [Similarity] INT, [Gr] CHAR(1)
);
INSERT INTO @logic_table
EXEC sp_execute_external_script
	@language = N'Python',
    @script = @python_script,
	@input_data_1 = @sql_query;
SELECT * FROM @logic_table WHERE [Mobile_Number] = '01550085615';
END;