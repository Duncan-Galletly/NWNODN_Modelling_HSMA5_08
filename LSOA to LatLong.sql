
;

WITH RankedData AS (
    SELECT 
        P.[LSOA], 
        P.Latitude_1m, 
        P.Longitude_1m,
        ROW_NUMBER() OVER (PARTITION BY P.[LSOA] ORDER BY P.Latitude_1m) AS RowNum,
        COUNT(*) OVER (PARTITION BY P.[LSOA]) AS TotalCount
    FROM 
        [DCBI_Live].[dbo].[NICU_Badger_Episodes] E
    JOIN 
        (
            SELECT * 
            FROM [UK_Health_Dimensions].[ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD]
            WHERE Is_Latest = 1
        ) P ON E.GPPostCode = P.Postcode_single_space_e_Gif
	UNION 
	    SELECT 
        P.[LSOA], 
        P.Latitude_1m, 
        P.Longitude_1m,
        ROW_NUMBER() OVER (PARTITION BY P.[LSOA] ORDER BY P.Latitude_1m) AS RowNum,
        COUNT(*) OVER (PARTITION BY P.[LSOA]) AS TotalCount
    FROM 
        [DCBI_Live].[dbo].[NICU_Badger_Episodes_21-23] E
    JOIN  (SELECT * 
					FROM [UK_Health_Dimensions].[ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD]
					WHERE Is_Latest = 1) P ON E.[LSOA] = P.[LSOA]
),
MedianData AS (
    SELECT *,
        NTILE(2) OVER (PARTITION BY LSOA ORDER BY RowNum) AS HalfTile
    FROM RankedData
),
OrderedData AS (
    SELECT *,
        ABS(RowNum - CEILING(TotalCount / 2.0)) AS DistanceFromMedian
    FROM MedianData
)
SELECT * FROM OrderedData
WHERE OrderedData.RowNum = 1
ORDER BY OrderedData.LSOA

--SELECT * FROM (
--SELECT DISTINCT LSOA, Latitude_1m Lat, Longitude_1m Lng,
--				ROW_NUMBER() OVER (PARTITION BY LSOA ORDER BY Longitude_1m) AS RowNum 
--FROM (
--		SELECT * FROM (
--			SELECT DISTINCT E.[LSOA], P.Latitude_1m, P.Longitude_1m,
--				ROW_NUMBER() OVER (PARTITION BY E.[LSOA] ORDER BY P.Latitude_1m) AS RowNum 
--		  FROM [DCBI_Live].[dbo].[NICU_Badger_Episodes_21-23] E
--		  JOIN (SELECT * 
--					FROM [UK_Health_Dimensions].[ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD]
--					WHERE Is_Latest = 1) P ON E.[LSOA] = P.[LSOA]
--					) X WHERE X.RowNum = 1
--		UNION
--		SELECT * FROM (
--		SELECT DISTINCT P.[LSOA] ,P.Latitude_1m, P.Longitude_1m,
--				ROW_NUMBER() OVER (PARTITION BY P.[LSOA] ORDER BY P.Latitude_1m) AS RowNum 
--		  FROM [DCBI_Live].[dbo].[NICU_Badger_Episodes] E
--			JOIN (SELECT * 
--					FROM [UK_Health_Dimensions].[ODS].[Postcode_Grid_Refs_Eng_Wal_Sco_And_NI_SCD]
--					WHERE Is_Latest = 1) P ON E.GPPostCode = P.Postcode_single_space_e_Gif
--					) X WHERE X.RowNum = 1
--	) Data
--) Blah WHERE RowNum =1


