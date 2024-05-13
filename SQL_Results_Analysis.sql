
DROP TABLE IF EXISTS #OriginalDataTmp

CREATE TABLE #OriginalDataTmp (CC_Level VARCHAR(50)
								,[Year] VARCHAR(50)
								,Solution_Code VARCHAR(50)
								,Site VARCHAR(255)
								,Record_Count INT)

-- Original data
INSERT INTO #OriginalDataTmp (CC_Level, Year, Solution_Code, Site, Record_Count)
SELECT [CC_Level]
      ,[Year]
      ,[Solution_Code]
	  ,[Site]
      ,SUM([Record_Count])
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated]
  LEFT JOIN [ref].[NCC_EA_Sites] ON [LSOA] = Solution_Code
  WHERE 1=1 
    --AND CC_Level = 'NICU'
	AND Solution_Number = 0
    --AND year = '1920'
	AND STP_Name IN (  'Healthier Lancashire and South Cumbria'
					  ,'Cheshire and Merseyside'
					  ,'Greater Manchester Health and Social Care Partnership')
  GROUP BY [CC_Level]
      ,[Year]
      ,[Solution_Code]
	  ,[Site]
	  ,[Solution_Number]
	  ORDER BY  Solution_Code, YEAR

DROP TABLE if EXISTS #AggregatedSolutionsTmp	
	  
CREATE TABLE #AggregatedSolutionsTmp (CC_Level VARCHAR(50)
								,[Year] VARCHAR(50)
								,Solution_Code VARCHAR(50)
								,Site VARCHAR(255)
								,[Solution_Number] smallint
								,Record_Count INT
								,Standard_Deviation DECIMAL(10,2))
     
---- average OF the other solutions
INSERT INTO #AggregatedSolutionsTmp (CC_Level, Year, Solution_Code, Site, Solution_Number, Record_Count, Standard_Deviation)
SELECT [CC_Level]
      ,[Year]
      ,[Solution_Code]
	  ,[Site]
	  ,[Solution_Number]
      ,AVG(RC) Row_Count_Avg
	  ,STDEV(RC) Row_Count_StDev
	  FROM (
				SELECT [CC_Level]
				  ,[Year]
				  ,[Solution_Code]
				  ,[Site]
				  ,[Algorithm]
				  ,[Weighted]
				  ,[OI_Inc]
				  ,[Solution_Number]
				  ,SUM([Record_Count]) RC
			  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated]
			  LEFT JOIN [ref].[NCC_EA_Sites] ON [LSOA] = Solution_Code
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
				  ,[Solution_Code]
				  ,[Site]
				  ,[Algorithm]
				  ,[Weighted]
				  ,[OI_Inc]
				  ,[Solution_Number]
		) Sub
	GROUP BY [CC_Level]
      ,[Year]
      ,[Solution_Code]
	  ,[Site]
	  ,[Solution_Number]
	ORDER BY YEAR, Sub.[Solution_Number], Sub.[Solution_Code]

DROP TABLE if EXISTS #ModellingOffset
	  
CREATE TABLE #ModellingOffset (CC_Level VARCHAR(50)
								,[Year] VARCHAR(50)
								,Solution_Code VARCHAR(50)
								,Original_Count int
								,OffsetPercentage DECIMAL(20,14))


INSERT INTO #ModellingOffset (CC_Level, Year, Solution_Code, Original_Count, OffsetPercentage)
SELECT A.CC_Level
		,A.Year
		,A.Solution_Code
		,B.Record_Count	
		,CAST(A.Record_Count AS DECIMAL (10,2))/CAST(B.Record_Count AS DECIMAL (10,2))
FROM #AggregatedSolutionsTmp A
LEFT JOIN #OriginalDataTmp B
ON A.CC_Level = B.CC_Level
 AND A.Year = B.Year
 AND B.Solution_Code = A.Solution_Code
 WHERE A.Solution_Number = 1 

 TRUNCATE TABLE [NCC_EA_Output_Aggregated_Adjusted]

 INSERT INTO [dbo].[NCC_EA_Output_Aggregated_Adjusted] ([CC_Level]
													  ,[Year]
													  ,[Solution_Code]
													  ,[Site]
													  ,[Solution_Number]
													  ,[Modelled_Count]
													  ,[Original_Count]
													  ,[OffsetPercentage]
													  ,[Adjusted_Count]
													  ,Standard_Deviation)
 SELECT A.[CC_Level]
      ,A.[Year]
      ,A.[Solution_Code]
	  ,A.[Site]
	  ,A.[Solution_Number] 
	  ,A.[Record_Count] Modelled_Count
	  ,B.[Original_Count] Original_Count
	  ,B.[OffsetPercentage]
	  ,CAST(COALESCE(ROUND(A.Record_Count / B.OffsetPercentage,0),0) AS INT) Adjusted_Count
	  ,A.Standard_Deviation
 FROM #AggregatedSolutionsTmp A
 LEFT JOIN #ModellingOffset B
	ON B.Solution_Code = A.Solution_Code
	AND B.CC_Level = A.CC_Level
	AND B.Year = A.Year



DROP TABLE IF EXISTS #OriginalDataTmp
DROP TABLE if EXISTS #AggregatedSolutionsTmp	
DROP TABLE if EXISTS #ModellingOffset