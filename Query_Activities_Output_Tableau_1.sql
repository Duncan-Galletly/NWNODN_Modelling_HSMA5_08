DECLARE @DynamicUnpivotQuery NVARCHAR(MAX),
        @SolutionColumns NVARCHAR(MAX),
        @SelectSolutionNumbers NVARCHAR(MAX)

-- Generate the list of solution columns for unpivoting
SELECT @SolutionColumns = 
	STUFF((SELECT ', ' + QUOTENAME(COLUMN_NAME)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_NAME = 'NICU_Modelling_Activities_Output'
    AND COLUMN_NAME LIKE 'solution[_]%[^_unit]'
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')

-- Generate the SELECT statement part for solution numbers dynamically
SELECT @SelectSolutionNumbers = 
STUFF((SELECT ', '''+ RIGHT(COLUMN_NAME, LEN(COLUMN_NAME) - CHARINDEX('_', COLUMN_NAME)) + ''' AS ' + QUOTENAME(COLUMN_NAME + '_Number')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_NAME = 'NICU_Modelling_Activities_Output'
    AND COLUMN_NAME LIKE 'solution[_]%'
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')

-- Construct the dynamic SQL to perform the unpivot
SET @DynamicUnpivotQuery = '
Select Solution,
		UnitCode,
		Region,
        Site,
		SolutionNumber,
		CC_Level,
		Count(*),
		Case when CC_Activity_Date between ''2018-04-01'' and ''2019-03-31''
				then ''18/19''
			 when CC_Activity_Date between ''2019-04-01'' and ''2020-03-31''
				then ''19/20''
			 when CC_Activity_Date between ''2020-04-01'' and ''2021-03-31''
				then ''20/21''
			 when CC_Activity_Date between ''2021-04-01'' and ''2022-03-31''
				then ''21/22''
			 end as Fin_Year
		From (
			SELECT [Der_Postcode_LSOA_Code]
				  ,[CC_Activity_Date]
				  ,[SiteLSOA]
				  ,[CC_Level]
				  ,[SiteCode]
				  ,Solution
				  ,SolutionNumber
			FROM 
			(
				SELECT [Der_Postcode_LSOA_Code]
					  ,[CC_Activity_Date]
					  ,[SiteLSOA]
					  ,[CC_Level]
					  ,[SiteCode]
					  ,' + @SolutionColumns + '
					  ,' + @SelectSolutionNumbers + '
				FROM [DCBI_Live].[dbo].[NICU_Modelling_Activities_Output]
				where Latest = 1
			) AS SourceTable UNPIVOT
			(
				Solution FOR SolutionColumn IN (' + @SolutionColumns + ')
			) AS UnpivotedTable
			CROSS APPLY (VALUES (RIGHT(SolutionColumn, LEN(SolutionColumn) - CHARINDEX(''_'', SolutionColumn)))) AS SolutionNumberTable(SolutionNumber)) SubQuery
	Left Join [dbo].[NICU_Sites_LSOA_Detail]
				on [LSOA] = Solution 
	--Where Solution <> ''''
	Group by Solution,
		UnitCode,
		Region,
        Site,
		SolutionNumber,
		CC_Level,
		Case when CC_Activity_Date between ''2018-04-01'' and ''2019-03-31''
				then ''18/19''
			 when CC_Activity_Date between ''2019-04-01'' and ''2020-03-31''
				then ''19/20''
			 when CC_Activity_Date between ''2020-04-01'' and ''2021-03-31''
				then ''20/21''
			 when CC_Activity_Date between ''2021-04-01'' and ''2022-03-31''
				then ''21/22''
			 end 
	ORDER BY SolutionNumber
		,Solution
		,CC_Level
		,Case when CC_Activity_Date between ''2018-04-01'' and ''2019-03-31''
				then ''18/19''
			 when CC_Activity_Date between ''2019-04-01'' and ''2020-03-31''
				then ''19/20''
			 when CC_Activity_Date between ''2020-04-01'' and ''2021-03-31''
				then ''20/21''
			 when CC_Activity_Date between ''2021-04-01'' and ''2022-03-31''
				then ''21/22''
			 end'


PRINT @DynamicUnpivotQuery
EXEC sp_executesql @DynamicUnpivotQuery
