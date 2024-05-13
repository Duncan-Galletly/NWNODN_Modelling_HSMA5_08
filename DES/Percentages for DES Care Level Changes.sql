-- Percentages for level changes after leaving one level
;
WITH person_level_changes
AS (
	SELECT t1.[NationalIDBabyAnon]
		  ,t1.[bapm2011] AS OriginalLevel
		  ,t2.[bapm2011] AS NextLevel
	FROM [dbo].[NICU_Badger_DaySum]			 AS t1
		LEFT JOIN [dbo].[NICU_Badger_DaySum] AS t2
			ON t1.[NationalIDBabyAnon]			= t2.[NationalIDBabyAnon]
			   AND DATEADD(DAY, 1, t1.CareDate) = t2.CareDate
			   AND t2.bapm2011					<> t1.bapm2011
)
SELECT OriginalLevel
	  ,COALESCE(NextLevel, 0) SubsequentLevel
	  ,COUNT(*)															  AS Total
	  ,COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY OriginalLevel) AS Percentage
FROM person_level_changes
GROUP BY OriginalLevel
		,NextLevel
ORDER BY OriginalLevel
		,NextLevel;

--Average LOS in each Level
WITH RankedStays AS (
    SELECT
        [NationalIDBabyAnon],
        bapm2011,
        CareDate,
        ROW_NUMBER() OVER (PARTITION BY [NationalIDBabyAnon] ORDER BY CareDate) AS SeqNum,
        ROW_NUMBER() OVER (PARTITION BY [NationalIDBabyAnon], bapm2011 ORDER BY CareDate) AS SeqNumPerLevel,
        LAG(CareDate) OVER (PARTITION BY [NationalIDBabyAnon] ORDER BY CareDate) AS PrevCareDate
    FROM
        [dbo].[NICU_Badger_DaySum]
),
GroupedStays AS (
    SELECT
        [NationalIDBabyAnon],
        bapm2011,
        CareDate,
        (SeqNum - SeqNumPerLevel) + 1 AS StayGroup
    FROM
        RankedStays GS
			-- this should remove stays with gaps in which will over inflate the averages
			-- if a person is in level 1 leaves level 1 for a number of days and then comes back in at the same level
			-- the rest of the code will class that as a continuous stay... short of a better solution to dealing with them
			-- this step should remove them
		where not exists (SELECT 1 FROM RankedStays RS
					where DATEDIFF(day, coalesce(PrevCareDate,CareDate) , CareDate) > 1 and RS.[NationalIDBabyAnon] = GS.[NationalIDBabyAnon])
),
StayDurations AS (
    SELECT [NationalIDBabyAnon],
        bapm2011,
        StayGroup,
        MIN(CareDate) AS StartDate,
        MAX(CareDate) AS EndDate,
		DATEDIFF(DAY, MIN(CareDate), MAX(CareDate)) + 1 LOS
    FROM
        GroupedStays
    GROUP BY
        [NationalIDBabyAnon], bapm2011, StayGroup
)
SELECT
    bapm2011,
    AVG(CAST(LOS AS FLOAT)) AS AvgDays
FROM
    StayDurations
GROUP BY
    bapm2011;


-- Number of admittances at warrington in 2019 to calculate as % of 3000 births
SELECT (CAST(COUNT(*)AS DECIMAL (10,5)) / 3000) * 100
	  ,bapm2011
	  ,COUNT(*)
FROM (SELECT DISTINCT E.[NationalIDBabyAnon]
					 ,[CareLocationName]
					 ,[AdmitTime]
					 ,[bapm2011]
	  FROM [DCBI_Live].[dbo].[NICU_Badger_Episodes] E
		  JOIN (SELECT DISTINCT [NationalIDBabyAnon]
							   ,[bapm2011]
							   ,ROW_NUMBER() OVER (PARTITION BY [NationalIDBabyAnon] ORDER BY CareDate) RN
				FROM [dbo].[NICU_Badger_DaySum])	D
			  ON E.NationalIDBabyAnon = D.NationalIDBabyAnon AND RN = 1
	  WHERE 
	  E.CareLocationName LIKE '%warrington%'
			AND 
			CAST(E.AdmitTime AS DATE) BETWEEN CAST('2021-04-01' AS DATE) AND CAST('2022-03-31' AS DATE)) Q
GROUP BY bapm2011;