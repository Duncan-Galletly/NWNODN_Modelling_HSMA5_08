
DROP TABLE IF EXISTS #OriginalDataTmp

CREATE TABLE #OriginalDataTmp (CC_Level VARCHAR(50)
								,[Year] VARCHAR(50)
								,IMD_Decile INT
								,Solution_Code VARCHAR(50)
								,Site VARCHAR(255)
								,Travel_Time DECIMAL (10,2))

-- Original data
INSERT INTO #OriginalDataTmp (CC_Level, Year, IMD_Decile, Solution_Code, Site, Travel_Time)
SELECT [CC_Level]
      ,[Year]
	  ,IMD_Decile 
      ,[Solution_Code]
	  ,[Site]
      ,AVG(Travel_Time)
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated]
  LEFT JOIN [ref].[NCC_EA_Sites] ON [LSOA] = Solution_Code
  --LEFT JOIN [ref].[NCC_EA_LSOA_ICB_Lookup] ON [Der_Postcode_LSOA_Code] = [LSOA_Code]
  WHERE 1=1 
    --AND CC_Level = 'NICU'
	AND Solution_Number = 0
    --AND year = '1920'
	AND STP_Name IN (  'Healthier Lancashire and South Cumbria'
					  ,'Cheshire and Merseyside'
					  ,'Greater Manchester Health and Social Care Partnership')
					  --AND Weighted = 'True'
  GROUP BY [CC_Level]
      ,[Year]
	  ,IMD_Decile 
      ,[Solution_Code]
	  ,[Site]
	  ,[Solution_Number]
	  ORDER BY  Solution_Code, YEAR

DROP TABLE IF EXISTS #AggregatedSolutionsTmp	
	  
CREATE TABLE #AggregatedSolutionsTmp (CC_Level VARCHAR(50)
								,[Year] VARCHAR(50)
								,IMD_Decile INT
								,Solution_Code VARCHAR(50)
								,Site VARCHAR(255)
								,[Solution_Number] SMALLINT
								,Travel_Time DECIMAL (10,2)
								,Standard_Deviation DECIMAL(10,2))
     
---- average OF the other solutions
INSERT INTO #AggregatedSolutionsTmp (CC_Level, Year, IMD_Decile, Solution_Code, Site, Solution_Number, Travel_Time, Standard_Deviation)
SELECT [CC_Level]
      ,[Year]
	  ,IMD_Decile
      ,[Solution_Code]
	  ,[Site]
	  ,[Solution_Number]
      ,AVG(TT) Row_Count_Avg
	  ,STDEV(TT) Row_Count_StDev
	  FROM (
				SELECT [CC_Level]
				  ,[Year]
				  ,IMD_Decile
				  ,[Solution_Code]
				  ,[Site]
				  ,[Algorithm]
				  ,[Weighted]
				  ,[OI_Inc]
				  ,[Solution_Number]
				  ,avg(Travel_Time) TT
			  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated]
			  LEFT JOIN [ref].[NCC_EA_Sites] ON [LSOA] = Solution_Code
			  --LEFT JOIN [ref].[NCC_EA_LSOA_IMD_Data] ON [Der_Postcode_LSOA_Code] = [LSOA_Code]
			  WHERE 1=1
			   --AND CC_Level = 'NICU'
				AND Solution_Number <> 0
				--AND year = '1920'
				AND STP_Name IN (  'Healthier Lancashire and South Cumbria'
								  ,'Cheshire and Merseyside'
								  ,'Greater Manchester Health and Social Care Partnership')
					  AND Weighted = 'True'
			  GROUP BY [CC_Level]
				  ,[Year]
				  ,IMD_Decile
				  ,[Solution_Code]
				  ,[Site]
				  ,[Algorithm]
				  ,[Weighted]
				  ,[OI_Inc]
				  ,[Solution_Number]
		) Sub
	GROUP BY [CC_Level]
      ,[Year]
	  ,IMD_Decile
      ,[Solution_Code]
	  ,[Site]
	  ,[Solution_Number]
	ORDER BY YEAR, Sub.[Solution_Number], Sub.[Solution_Code]

DROP TABLE if EXISTS #ModellingOffset
	  
CREATE TABLE #ModellingOffset (CC_Level VARCHAR(50)
								,[Year] VARCHAR(50)
								,IMD_Decile int
								,Solution_Code VARCHAR(50)
								,Original_TT DECIMAL (10,2)
								,OffsetPercentage DECIMAL(20,14))


INSERT INTO #ModellingOffset (CC_Level, Year, IMD_Decile, Solution_Code, Original_TT, OffsetPercentage)
SELECT A.CC_Level
		,A.Year
		,A.IMD_Decile
		,A.Solution_Code
		,B.Travel_Time	
		,CAST(A.Travel_Time AS DECIMAL (10,2))/CAST(B.Travel_Time AS DECIMAL (10,2))
FROM #AggregatedSolutionsTmp A
LEFT JOIN #OriginalDataTmp B
ON A.CC_Level = B.CC_Level
 AND A.Year = B.Year
 AND B.Solution_Code = A.Solution_Code
 AND A.IMD_Decile = B.IMD_Decile
 WHERE A.Solution_Number = 1 

 TRUNCATE TABLE [NCC_EA_Output_Aggregated_Adjusted_Travel_Time]

 INSERT INTO [dbo].[NCC_EA_Output_Aggregated_Adjusted_Travel_Time] ([CC_Level]
													  ,[Year]
													  ,IMD_Decile
													  ,[Solution_Code]
													  ,[Site]
													  ,[Solution_Number]
													  ,[Modelled_TT]
													  ,[Original_TT]
													  ,[OffsetPercentage]
													  ,[Adjusted_TT]
													  ,Standard_Deviation)
 SELECT A.[CC_Level]
      ,A.[Year]
	  ,A.[IMD_Decile]
      ,A.[Solution_Code]
	  ,A.[Site]
	  ,A.[Solution_Number] 
	  ,A.Travel_Time Modelled_TT
	  ,B.Original_TT Original_TT
	  ,B.[OffsetPercentage]
	  ,CAST(COALESCE(A.Travel_Time / B.OffsetPercentage,A.Travel_Time) AS DECIMAL (10,2)) Adjusted_TT
	  ,A.Standard_Deviation
 FROM #AggregatedSolutionsTmp A
 LEFT JOIN #ModellingOffset B
	ON B.Solution_Code = A.Solution_Code
	AND B.CC_Level = A.CC_Level
	AND B.Year = A.Year
	AND B.IMD_Decile = A.IMD_Decile


DROP TABLE IF EXISTS #OriginalDataTmp
DROP TABLE if EXISTS #AggregatedSolutionsTmp	
DROP TABLE if EXISTS #ModellingOffset