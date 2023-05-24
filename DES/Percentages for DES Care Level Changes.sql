-- Percentages for level changes after leaving one level
;
 WITH person_level_changes AS (
    SELECT t1.[NationalIDBabyAnon]
		,t1.[bapm2011] AS OriginalLevel
		,t2.[bapm2011] AS NextLevel
    FROM [dbo].[NICU_Badger_DaySum] AS t1
    LEFT JOIN [dbo].[NICU_Badger_DaySum] AS t2
        ON t1.[NationalIDBabyAnon] = t2.[NationalIDBabyAnon]
        AND DATEADD(DAY, 1, t1.CareDate) = t2.CareDate
		AND t2.bapm2011 <> t1.bapm2011
)
SELECT OriginalLevel, COALESCE(NextLevel,0), COUNT(*) as Total,
       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY OriginalLevel) as Percentage
FROM person_level_changes
GROUP BY OriginalLevel, NextLevel
ORDER BY OriginalLevel, NextLevel;

--Average LOS in each Level
;
WITH level_duration AS (
    SELECT t1.[NationalIDBabyAnon], t1.bapm2011, DATEDIFF(DAY, t1.CareDate, MIN(t2.CareDate)) AS Days
    FROM [dbo].[NICU_Badger_DaySum]  AS t1
    LEFT JOIN [dbo].[NICU_Badger_DaySum]  AS t2
        ON t1.[NationalIDBabyAnon] = t2.[NationalIDBabyAnon]
        AND t1.CareDate < t2.CareDate
        AND t1.bapm2011 <> t2.bapm2011
    GROUP BY t1.[NationalIDBabyAnon], t1.bapm2011, t1.CareDate
)
SELECT bapm2011, AVG(CAST(Days AS FLOAT)) AS AvgDays
FROM level_duration
GROUP BY bapm2011;


-- Number of admittances at warrington in 2019 to calculate as % of 3000 births
SELECT COUNT(*), bapm2011
FROM (
		SELECT DISTINCT 
			E.[NationalIDBabyAnon],
			[CareLocationName],
			[AdmitTime],
			[bapm2011]
		FROM [DCBI_Live].[dbo].[NICU_Badger_Episodes] E
		JOIN (
			SELECT DISTINCT 
				[NationalIDBabyAnon],
				[bapm2011],
				ROW_NUMBER() OVER (PARTITION BY [NationalIDBabyAnon] ORDER BY CareDate) RN
			FROM [dbo].[NICU_Badger_DaySum]
		) D ON E.NationalIDBabyAnon = D.NationalIDBabyAnon AND RN = 1
		WHERE 
			E.CareLocationName LIKE '%warrington%'
			AND CAST(E.AdmitTime AS DATE) BETWEEN CAST('2021-04-01' AS DATE) AND CAST('2022-03-31' AS DATE)
	) Q GROUP BY bapm2011