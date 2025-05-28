WITH DataSourceRanking AS (
    SELECT 'Google' AS dataSource, 1 AS rank_order UNION ALL
    SELECT 'Microsoft', 2 UNION ALL
    SELECT 'Amazon', 3 UNION ALL
    SELECT 'IBM', 4
),
your_table AS (
    SELECT * FROM VALUES
    ('id1', 'price', 'Amazon',     'value1', '2022-01-01 01:01:00'::timestamp_ntz, NULL,               '2022-01-01 01:01:00'::timestamp_ntz),
    ('id1', 'price', 'Microsoft', 'value2', '2022-01-15 10:05:00'::timestamp_ntz, NULL,               '2022-01-15 10:05:00'::timestamp_ntz),
    ('id1', 'price', 'Google',    'value3', '2022-01-20 01:01:00'::timestamp_ntz, NULL,               '2022-01-20 01:01:00'::timestamp_ntz),
    ('id1', 'price', 'Google',    'value3', '2022-01-20 01:01:00'::timestamp_ntz, '2022-01-22 03:00:00'::timestamp_ntz, '2022-01-22 03:02:00'::timestamp_ntz),
    ('id1', 'price', 'IBM',       'value4', '2022-01-30 09:05:00'::timestamp_ntz, NULL,               '2022-01-30 09:05:00'::timestamp_ntz),
    ('id1', 'qty',   'IBM',       '100',    '2022-01-30 09:05:00'::timestamp_ntz, NULL,               '2022-01-30 09:05:00'::timestamp_ntz),
    ('id2', 'price', 'IBM',       'value21','2021-01-30 01:05:00'::timestamp_ntz, NULL,               '2021-01-30 01:05:00'::timestamp_ntz),
    ('id2', 'qty',   'IBM',       '10',     '2021-01-30 01:05:00'::timestamp_ntz, NULL,               '2021-01-30 01:05:00'::timestamp_ntz)
    AS T(rowId, rowType, dataSource, rowValue, startTimestamp, endTimestamp, changeTimestamp)
),
AuthoritativeOriginalRecords AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY rowId, rowType, dataSource, startTimestamp
               ORDER BY 
                   CASE WHEN endTimestamp IS NOT NULL THEN 0 ELSE 1 END,
                   changeTimestamp DESC
           ) AS rn
    FROM your_table
),
FilteredOriginalRecords AS (
    SELECT rowId, rowType, dataSource, rowValue, startTimestamp, endTimestamp, changeTimestamp
    FROM AuthoritativeOriginalRecords
    WHERE rn = 1
),
AllTimePoints AS (
    SELECT DISTINCT startTimestamp AS time_point FROM FilteredOriginalRecords
    UNION
    SELECT DISTINCT endTimestamp AS time_point FROM FilteredOriginalRecords WHERE endTimestamp IS NOT NULL
),
RankedOriginalRows AS (
    SELECT fr.*, dsr.rank_order AS dataSourceRank
    FROM FilteredOriginalRecords fr
    JOIN DataSourceRanking dsr ON fr.dataSource = dsr.dataSource
),
ActiveRowsAtTimePoint AS (
    SELECT
        atp.time_point,
        ror.*
    FROM AllTimePoints atp
    JOIN RankedOriginalRows ror
      ON atp.time_point >= ror.startTimestamp
     AND (ror.endTimestamp IS NULL OR atp.time_point < ror.endTimestamp)
),
WinningRowCandidates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY time_point, rowId, rowType
               ORDER BY dataSourceRank ASC, changeTimestamp DESC, startTimestamp
           ) AS rn
    FROM ActiveRowsAtTimePoint
),
CurrentWinners AS (
    SELECT
        time_point,
        rowId,
        rowType,
        dataSource,
        rowValue,
        startTimestamp AS source_start,
        endTimestamp AS source_end
    FROM WinningRowCandidates
    WHERE rn = 1
),
WinnerWithTransitions AS (
    SELECT
        *,
        LAG(rowValue) OVER (PARTITION BY rowId, rowType ORDER BY time_point) AS prev_rowValue,
        LEAD(time_point) OVER (PARTITION BY rowId, rowType ORDER BY time_point) AS next_time,
        LEAD(rowValue) OVER (PARTITION BY rowId, rowType ORDER BY time_point) AS next_rowValue
    FROM CurrentWinners
),
FinalWinnerRows AS (
    SELECT
        rowId,
        rowType,
        'Winner' AS dataSource,
        rowValue,
        time_point AS segment_startTimestamp,
        CASE
            WHEN next_time IS NOT NULL AND 
                 (
                     rowValue IS DISTINCT FROM next_rowValue OR 
                     (source_end IS NOT NULL AND next_time >= source_end)
                 )
            THEN next_time
            ELSE NULL
        END AS segment_endTimestamp
    FROM WinnerWithTransitions
    WHERE rowValue IS DISTINCT FROM prev_rowValue OR prev_rowValue IS NULL
),
DeduplicatedWinners AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY rowId, rowType, rowValue, segment_startTimestamp
               ORDER BY segment_startTimestamp
           ) AS rn
    FROM FinalWinnerRows
    WHERE rowValue IS NOT NULL
)

-- Final output
SELECT
    rowId,
    rowType,
    dataSource,
    rowValue,
    segment_startTimestamp AS startTimestamp,
    segment_endTimestamp AS endTimestamp,
    'winning_value' AS record_type
FROM DeduplicatedWinners
WHERE rn = 1

UNION ALL

SELECT
    rowId,
    rowType,
    dataSource,
    rowValue,
    startTimestamp,
    endTimestamp,
    'source' AS record_type
FROM your_table

ORDER BY rowId, rowType, startTimestamp;
