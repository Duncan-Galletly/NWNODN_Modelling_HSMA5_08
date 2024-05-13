CREATE OR ALTER VIEW [dbo].[NCC_EA_Output_Aggregated_Output_View] AS 
SELECT B.[Regional Objectives] 
	  ,[Solution_Code]
      ,A.[Site]
      ,A.[Solution_Number]
	  --,A.Year
      ,AVG([Modelled_Count])  [Modelled_Count]
      ,AVG([Original_Count])  [Original_Count]
      ,AVG([Adjusted_Count])  [Adjusted_Count]
      ,AVG(CAST([Adjusted_Count] AS DECIMAL(10,5))/365) AVG_NICU_Bed_Numbers
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated_Output] A
  JOIN [dbo].[NCC_EA_Scenario_Details_Lookup] B
	ON A.Solution_Number = B.Solution_Number
	JOIN (SELECT CASE [Region] WHEN 'Greater Manchester' THEN 'Greater Manchester Health and Social Care Partnership'
						WHEN 'Lancashire and Cumbria' THEN 'Lancashire and South Cumbria'
						ELSE [Region] END Region
					,Site
		FROM [DCBI_Modelling].[ref].[NCC_EA_Sites]) C  ON B.[Regional Objectives] = C.Region
								AND C.Site = A.Site
  WHERE CC_Level = 'NICU'
  AND weighted = 'True'
  GROUP BY  B.[Regional Objectives] 
	  ,[Solution_Code]
      ,A.[Site]
      ,A.[Solution_Number]
  --ORDER BY B.[Regional Objectives]
		--,Solution_Number
		--,Solution_Code

Go

CREATE OR ALTER VIEW [dbo].[NCC_EA_Output_Aggregated_Output_Travel_Times_View] AS 

SELECT B.[Regional Objectives] 
	  ,[Solution_Code]
      ,A.[Site]
      ,A.[Solution_Number]
	  ,A.IMD_Decile
	  --,A.Year
      ,AVG([Modelled_TT])  [Modelled_TT]
      ,AVG([Original_TT])  [Original_TT]
      ,AVG([Adjusted_TT])  [Adjusted_TT]
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated_Output_Travel_Time] A
  JOIN [dbo].[NCC_EA_Scenario_Details_Lookup] B
	ON A.Solution_Number = B.Solution_Number
	JOIN (SELECT CASE [Region] WHEN 'Greater Manchester' THEN 'Greater Manchester Health and Social Care Partnership'
						WHEN 'Lancashire and Cumbria' THEN 'Lancashire and South Cumbria'
						ELSE [Region] END Region
					,Site
		FROM [DCBI_Modelling].[ref].[NCC_EA_Sites]) C  ON B.[Regional Objectives] = C.Region
								AND C.Site = A.Site
  WHERE CC_Level = 'NICU'
  GROUP BY  B.[Regional Objectives] 
	  ,[Solution_Code]
      ,A.[Site]
      ,A.[Solution_Number]
	  ,A.IMD_Decile
  --ORDER BY B.[Regional Objectives]
		--,Solution_Number
		--,Solution_Code
	 --   ,IMD_Decile