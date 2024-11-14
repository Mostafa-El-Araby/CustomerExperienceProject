WITH temps AS
(
	SELECT
		[CustomerSAPID],
		[Name],
		SUM([Value]) OVER(PARTITION BY [CustomerSAPID]) AS SalesValue,
		SUM([Quantity]) OVER(PARTITION BY [CustomerSAPID]) AS SalesQuantity,
		LEAD([MaterialKey], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID1,
		LEAD([MaterialGroup3], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type1,
		LEAD([Material], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category1,
		LEAD([MaterialGroup2], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group1,
		LEAD([MaterialGroup1], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand1,
		LEAD([Quantity], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity1,
		LEAD([Value], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue1,
		LEAD([BillingDocument], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice1,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate1,
		LEAD([DistributionChannel], 0) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel1,
		LEAD([MaterialKey], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID2,
		LEAD([MaterialGroup3], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type2,
		LEAD([Material], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category2,
		LEAD([MaterialGroup2], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group2,
		LEAD([MaterialGroup1], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand2,
		LEAD([Quantity], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity2,
		LEAD([Value], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue2,
		LEAD([BillingDocument], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice2,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate2,
		LEAD([DistributionChannel], 1) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel2,
		LEAD([MaterialKey], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID3,
		LEAD([MaterialGroup3], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type3,
		LEAD([Material], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category3,
		LEAD([MaterialGroup2], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group3,
		LEAD([MaterialGroup1], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand3,
		LEAD([Quantity], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity3,
		LEAD([Value], 2) OVER(PARTITION BY  [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue3,
		LEAD([BillingDocument], 2) OVER(PARTITION BY  [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice3,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate3,
		LEAD([DistributionChannel], 2) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel3,
		LEAD([MaterialKey], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID4,
		LEAD([MaterialGroup3], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type4,
		LEAD([Material], 3) OVER(PARTITION BY  [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS Category4,
		LEAD([MaterialGroup2], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group4,
		LEAD([MaterialGroup1], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand4,
		LEAD([Quantity], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity4,
		LEAD([Value], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue4,
		LEAD([BillingDocument], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice4,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate4,
		LEAD([DistributionChannel], 3) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel4,
		LEAD([MaterialKey], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID5,
		LEAD([MaterialGroup3], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type5,
		LEAD([Material], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category5,
		LEAD([MaterialGroup2], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group5,
		LEAD([MaterialGroup1], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS Brand5,
		LEAD([Quantity], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity5,
		LEAD([Value], 4) OVER(PARTITION BY  [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS CountValue5,
		LEAD([BillingDocument], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice5,
		LEAD(TRY_CONVERT(DATE,[BillDate],104), 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate5,
		LEAD( [DistributionChannel], 4) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel5,
		LEAD([MaterialKey], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID6,
		LEAD([MaterialGroup3], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type6,
		LEAD([Material], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category6,
		LEAD([MaterialGroup2], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group6,
		LEAD([MaterialGroup1], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand6,
		LEAD([Quantity], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity6,
		LEAD([Value], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue6,
		LEAD([BillingDocument], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice6,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate6,
		LEAD([DistributionChannel], 5) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE,[BillDate],104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel6,
		LEAD([MaterialKey], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID7,
		LEAD([MaterialGroup3], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type7,
		LEAD([Material], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category7,
		LEAD([MaterialGroup2], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group7,
		LEAD([MaterialGroup1], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand7,
		LEAD([Quantity], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity7,
		LEAD([Value], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue7,
		LEAD([BillingDocument], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice7,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate7,
		LEAD([DistributionChannel], 6) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel7,
		LEAD([MaterialKey], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID8,
		LEAD([MaterialGroup3], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type8,
		LEAD([Material], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category8,
		LEAD([MaterialGroup2], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group8,
		LEAD([MaterialGroup1], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand8,
		LEAD([Quantity], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity8,
		LEAD([Value], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue8,
		LEAD([BillingDocument], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice8,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate8,
		LEAD([DistributionChannel], 7) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel8,
		LEAD([MaterialKey], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID9,
		LEAD([MaterialGroup3], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type9,
		LEAD([Material], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category9,
		LEAD([MaterialGroup2], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group9,
		LEAD([MaterialGroup1], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand9,
		LEAD([Quantity], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity9,
		LEAD([Value], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue9,
		LEAD([BillingDocument], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice9,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate9,
		LEAD([DistributionChannel], 8) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel9,
		LEAD([MaterialKey], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_ID10,
		LEAD([MaterialGroup3], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Material_Type10,
		LEAD([Material], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category10,
		LEAD([MaterialGroup2], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Cat_Group10,
		LEAD([MaterialGroup1], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Brand10,
		LEAD([Quantity], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountQuantity10,
		LEAD([Value], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS CountValue10,
		LEAD([BillingDocument], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Invoice10,
		LEAD(TRY_CONVERT(DATE, [BillDate], 104), 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS InvoiceDate10,
		LEAD([DistributionChannel], 9) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Dist_Channel10,
		LEAD([Material], 10) OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) AS Category11,
		ROW_NUMBER() OVER(PARTITION BY [CustomerSAPID] ORDER BY TRY_CONVERT(DATE, [BillDate], 104) DESC, [MaterialKey], [BillingDocument]) rn1
	FROM
		[dbo].[0-SalesDetailed]
	WHERE
		[CustomerSAPID] NOT LIKE 'BP%'
	AND
		[DistributionChannel] IN('03','05','07','12','13','16')
)
SELECT
	s2.[CustomerSAPID],
	MAX(s2.[Name]) AS names,
	MAX(S1.[SalesQuantity]) AS SalesQuantity,
	MAX(S1.[SalesValue]) AS SalesValue,
	MAX([Material_ID1]) AS Material_ID1,
	MAX([Material_Type1]) AS Material_Type1,
	MAX(Category1) AS Category1,
	MAX([Cat_Group1]) AS Cat_Group1,
	MAX([Brand1]) AS Brand1,
	MAX([CountQuantity1]) AS SumQuantity1,
	MAX([CountValue1]) AS SumValue1,
	MAX([Invoice1]) AS Invoice1,
	MAX([InvoiceDate1]) AS InvoiceDate1,
	MAX
	(
		CASE
			WHEN [Dist_Channel1] = '03' THEN 'B2C'
			WHEN [Dist_Channel1] = '05' THEN 'Online'
			WHEN [Dist_Channel1] = '07' THEN 'Outlet'
			WHEN [Dist_Channel1] = '12' THEN 'Replacement'
			WHEN [Dist_Channel1] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel1] = '16' THEN 'VIP'
		END
	) AS Dist_Channel1,
	MAX([Material_ID2]) AS Material_ID2,
	MAX([Material_Type2]) AS Material_Type2,
	MAX([Category2]) AS Category2,
	MAX([Cat_Group2]) AS Cat_Group2,
	MAX([Brand2]) AS Brand2,
	MAX([CountQuantity2]) AS SumQuantity2,
	MAX([CountValue2]) AS SumValue2,
	MAX([Invoice2]) AS Invoice2,
	MAX([InvoiceDate2]) AS InvoiceDate2,
	MAX
	(
		CASE
			WHEN [Dist_Channel2] = '03' THEN 'B2C'
			WHEN [Dist_Channel2] = '05' THEN 'Online'
			WHEN [Dist_Channel2] = '07' THEN 'Outlet'
			WHEN [Dist_Channel2] = '12' THEN 'Replacement'
			WHEN [Dist_Channel2] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel2] = '16' THEN 'VIP'
		END
	) AS Dist_Channel2,
	MAX([Material_ID3]) AS Material_ID3,
	MAX([Material_Type3]) AS Material_Type3,
	MAX([Category3]) AS Category3,
	MAX([Cat_Group3]) AS Cat_Group3,
	MAX([Brand3]) AS Brand3,
	MAX([CountQuantity3]) AS SumQuantity3,
	MAX([CountValue3]) AS SumValue3,
	MAX([Invoice3]) AS Invoice3,
	MAX([InvoiceDate3]) AS InvoiceDate3,
	MAX
	(
		CASE
			WHEN [Dist_Channel3] = '03' THEN 'B2C'
			WHEN [Dist_Channel3] = '05' THEN 'Online'
			WHEN [Dist_Channel3] = '07' THEN 'Outlet'
			WHEN [Dist_Channel3] = '12' THEN 'Replacement'
			WHEN [Dist_Channel3] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel3] = '16' THEN 'VIP'
		END
	) AS Dist_Channel3,
	MAX([Material_ID4]) AS Material_ID4,
	MAX([Material_Type4]) AS Material_Type4,
	MAX([Category4]) AS Category4,
	MAX([Cat_Group4]) AS Cat_Group4,
	MAX([Brand4]) AS Brand4,
	MAX([CountQuantity4]) AS SumQuantity4,
	MAX([CountValue4]) AS SumValue4,
	MAX([Invoice4]) AS Invoice4,
	MAX([InvoiceDate4]) AS InvoiceDate4,
	MAX
	(
		CASE
			WHEN [Dist_Channel4] = '03' THEN 'B2C'
			WHEN [Dist_Channel4] = '05' THEN 'Online'
			WHEN [Dist_Channel4] = '07' THEN 'Outlet'
			WHEN [Dist_Channel4] = '12' THEN 'Replacement'
			WHEN [Dist_Channel4] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel4] = '16' THEN 'VIP'
		END
	) AS Dist_Channel4,
	MAX([Material_ID5]) AS Material_ID5,
	MAX([Material_Type5]) AS Material_Type5,
	MAX([Category5]) AS Category5,
	MAX([Cat_Group5]) AS Cat_Group5,
	MAX([Brand5]) AS Brand5,
	MAX([CountQuantity5]) AS SumQuantity5,
	MAX([CountValue5]) AS SumValue5,
	MAX([Invoice5]) AS Invoice5,
	MAX([InvoiceDate5]) InvoiceDate5,
	MAX
	(
		CASE
			WHEN [Dist_Channel5] = '03' THEN 'B2C'
			WHEN [Dist_Channel5] = '05' THEN 'Online'
			WHEN [Dist_Channel5] = '07' THEN 'Outlet'
			WHEN [Dist_Channel5] = '12' THEN 'Replacement'
			WHEN [Dist_Channel5] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel5] = '16' THEN 'VIP'
		END
	) AS Dist_Channel5,
	MAX([Material_ID6]) AS Material_ID6,
	MAX([Material_Type6]) AS Material_Type6,
	MAX([Category6]) AS Category6,
	MAX([Cat_Group6]) AS Cat_Group6,
	MAX([Brand6]) AS Brand6,
	MAX([CountQuantity6]) AS SumQuantity6,
	MAX([CountValue6]) AS SumValue6,
	MAX([Invoice6]) AS Invoice6,
	MAX([InvoiceDate6]) AS InvoiceDate6,
	MAX
	(
		CASE
			WHEN [Dist_Channel6] = '03' THEN 'B2C'
			WHEN [Dist_Channel6] = '05' THEN 'Online'
			WHEN [Dist_Channel6] = '07' THEN 'Outlet'
			WHEN [Dist_Channel6] = '12' THEN 'Replacement'
			WHEN [Dist_Channel6] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel6] = '16' THEN 'VIP'
		END
	) AS Dist_Channel6,
	MAX([Material_ID7]) AS Material_ID7,
	MAX([Material_Type7]) AS Material_Type7,
	MAX([Category7]) AS Category7,
	MAX([Cat_Group7]) AS Cat_Group7,
	MAX([Brand7]) AS Brand7,
	MAX([CountQuantity7]) AS SumQuantity7,
	MAX([CountValue7]) AS SumValue7,
	MAX([Invoice7]) AS Invoice7,
	MAX([InvoiceDate7]) AS InvoiceDate7,
	MAX
	(
		CASE
			WHEN [Dist_Channel7] = '03' THEN 'B2C'
			WHEN [Dist_Channel7] = '05' THEN 'Online'
			WHEN [Dist_Channel7] = '07' THEN 'Outlet'
			WHEN [Dist_Channel7] = '12' THEN 'Replacement'
			WHEN [Dist_Channel7] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel7] = '16' THEN 'VIP'
		END
	) AS Dist_Channel7,
	MAX([Material_ID8]) AS Material_ID8,
	MAX([Material_Type8]) AS Material_Type8,
	MAX([Category8]) AS Category8,
	MAX([Cat_Group8]) AS Cat_Group8,
	MAX([Brand8]) AS Brand8,
	MAX([CountQuantity8]) AS SumQuantity8,
	MAX([CountValue8]) AS SumValue8,
	MAX([Invoice8]) AS Invoice8,
	MAX([InvoiceDate8]) AS InvoiceDate8,
	MAX
	(
		CASE
			WHEN [Dist_Channel8] = '03' THEN 'B2C'
			WHEN [Dist_Channel8] = '05' THEN 'Online'
			WHEN [Dist_Channel8] = '07' THEN 'Outlet'
			WHEN [Dist_Channel8] = '12' THEN 'Replacement'
			WHEN [Dist_Channel8] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel8] = '16' THEN 'VIP'
		END
	) AS Dist_Channel8,
	MAX([Material_ID9]) AS Material_ID9,
	MAX([Material_Type9]) AS Material_Type9,
	MAX([Category9]) AS Category9,
	MAX([Cat_Group9]) AS Cat_Group9,
	MAX([Brand9]) AS Brand9,
	MAX([CountQuantity9]) AS SumQuantity9,
	MAX([CountValue9]) AS SumValue9,
	MAX([Invoice9]) AS Invoice9,
	MAX([InvoiceDate9]) AS InvoiceDate9,
	MAX
	(
		CASE
			WHEN [Dist_Channel9] = '03' THEN 'B2C'
			WHEN [Dist_Channel9] = '05' THEN 'Online'
			WHEN [Dist_Channel9] = '07' THEN 'Outlet'
			WHEN [Dist_Channel9] = '12' THEN 'Replacement'
			WHEN [Dist_Channel9] = '13' THEN 'Gifts&samples'
			WHEN [Dist_Channel9] = '16' THEN 'VIP'
		END
	) AS Dist_Channel9,
	MAX([Material_ID10]) AS Material_ID10,
	MAX([Material_Type10]) AS Material_Type10,
	MAX([Category10]) AS Category10,
	MAX([Cat_Group10]) AS Cat_Group10,
	MAX([Brand10]) AS Brand10,
	MAX([CountQuantity10]) AS SumQuantity10,
	MAX([CountValue10]) AS SumValue10,
	MAX([Invoice10]) AS Invoice10,
	MAX([InvoiceDate10]) AS InvoiceDate10,
	MAX
	(
			CASE
				WHEN [Dist_Channel10] = '03' THEN 'B2C'
				WHEN [Dist_Channel10] = '05' THEN 'Online'
				WHEN [Dist_Channel10] = '07' THEN 'Outlet'
				WHEN [Dist_Channel10] = '12' THEN 'Replacement'
				WHEN [Dist_Channel10] = '13' THEN 'Gifts&samples'
				WHEN [Dist_Channel10] = '16' THEN 'VIP'
			END
	) AS Dist_Channel10,
	MAX
	(
		CASE
			WHEN [Category11] IS NOT NULL THEN 1
			ELSE 0
		END
	) AS Flag 
FROM
	[temps] AS s1, [dbo].[0-SalesDetailed] AS s2
WHERE
	s1.[CustomerSAPID] = s2.[CustomerSAPID]
AND
	[rn1] = 1  
GROUP BY
	s2.[CustomerSAPID];