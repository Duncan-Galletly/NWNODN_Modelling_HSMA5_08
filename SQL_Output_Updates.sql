
UPDATE A
  SET 
  [STP_Name] = l.[STP_Name]
  --FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Long] A
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated] A
  LEFT JOIN [ref].[NCC_EA_LSOA_ICB_Lookup] l ON A.Der_Postcode_LSOA_Code = l.[Der_Postcode_LSOA_Code]
  WHERE A.STP_Name IS NULL OR A.STP_Name = '' 

UPDATE A
  SET 
  [IMD_Decile] = imd.[IMD_Decile]
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Long] A
  LEFT JOIN [ref].[LSOA_IMD_Lookup] imd ON A.Der_Postcode_LSOA_Code = imd.[LSOA_Code]
  --WHERE A.[IMD_Decile] IS NULL OR A.[IMD_Decile] = '' 



TRUNCATE TABLE [dbo].[NCC_EA_Output_Aggregated]

INSERT INTO [DCBI_Modelling].[dbo].[NCC_EA_Output_Aggregated]
([Der_Postcode_LSOA_Code]
      ,[CC_Level]
      ,[Year]
      ,[Algorithm]
	  ,[Weighted]
      ,[OI_Inc]
      ,[Rural_Urban_Classification]
      ,[IMD_Decile]
	  ,[STP_Name]
      ,[Solution_Code]
      ,[Travel_Time]
      ,[Solution_Number]
      ,[Year_Month]
      ,[Record_Count]
      ,[File_Name_Text]) 
SELECT [Der_Postcode_LSOA_Code]
      ,[CC_Level]
      ,[Year]
      ,[Algorithm]
	  ,[Weighted]
      ,[OI_Inc]
      ,[Rural_Urban_Classification]
      ,CAST([IMD_Decile] AS DECIMAL (10,1)) IMD_Decile
	  ,[STP_Name]
      ,[Solution_Code]
      ,CAST([Travel_Time] AS DECIMAL (10,1)) Travel_Time
      ,CAST([Solution_Number] AS INT) Solution_Number
      ,[Year_Month]
      ,SUM(CAST([Record_Count] AS BIGINT)) Record_Count
      ,[File_Name_Text]
  FROM [DCBI_Modelling].[dbo].[NCC_EA_Output_Long]
  GROUP BY [Der_Postcode_LSOA_Code]
	  ,[CC_Level]
      ,[Year]
      ,[Algorithm]
	  ,[Weighted]
      ,[OI_Inc]
      ,[Rural_Urban_Classification]
	  ,[STP_Name]
      ,CAST([IMD_Decile] AS DECIMAL (10,1))
      ,[Solution_Code]
      ,CAST([Travel_Time] AS DECIMAL (10,1))
      ,CAST([Solution_Number] AS INT)
      ,[Year_Month]
      ,[File_Name_Text]