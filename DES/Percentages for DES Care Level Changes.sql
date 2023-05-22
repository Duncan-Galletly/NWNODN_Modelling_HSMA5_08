  
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
