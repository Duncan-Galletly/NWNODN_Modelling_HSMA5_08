  
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