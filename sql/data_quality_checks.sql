USE CATALOG ipl_data_analysis;
USE SCHEMA silver;

select match_id, player_id, count(*) as rec_cnt from slv_player_match as dfs
group by match_id, player_id
having count(*) > 1;

SELECT player_match_sk, COUNT(*) AS cnt
FROM slv_player_match
GROUP BY player_match_sk
HAVING COUNT(*) > 1;

select * from slv_ball_by_ball limit 0;

DESCRIBE TABLE ipl_data_analysis.silver.slv_ball_by_ball;

SELECT
    COUNT(*) AS total_records,
    COUNT(strikersk) AS striker_key_count,
    COUNT(striker_match_sk) AS striker_match_key_count,
    COUNT(nonstriker_sk) AS nonstriker_key_count,
    COUNT(nonstriker_match_sk) AS nonstriker_match_key_count,
    COUNT(bowler_sk) AS bowler_key_count,
    COUNT(bowler_match_sk) AS bowler_match_key_count
FROM ipl_data_analysis.silver.slv_ball_by_ball;

SELECT
    COUNT(DISTINCT strikersk) AS distinct_strikers,
    COUNT(DISTINCT striker_match_sk) AS distinct_striker_matches,
    COUNT(DISTINCT nonstriker_sk) AS distinct_non_strikers,
    COUNT(DISTINCT bowler_sk) AS distinct_bowlers,
    COUNT(DISTINCT bowler_match_sk) AS distinct_bowler_matches
FROM ipl_data_analysis.silver.slv_ball_by_ball;


DESCRIBE TABLE ipl_data_analysis.silver.slv_player;
SELECT
    player_sk,
    player_id,
    player_name
FROM ipl_data_analysis.silver.slv_player
LIMIT 10;
SELECT DISTINCT strikersk
FROM ipl_data_analysis.silver.slv_ball_by_ball
LIMIT 10;


SELECT
    COUNT(*) AS total_records,
    COUNT(p.player_sk) AS matched_records,
    COUNT(*) - COUNT(p.player_sk) AS unmatched_records
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.strikersk AS STRING) = p.player_sk;

    SELECT
    'Non-Striker' AS player_role,
    COUNT(*) AS total_records,
    COUNT(p.player_sk) AS matched_records
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.nonstriker_sk AS STRING) = p.player_sk

UNION ALL

SELECT
    'Bowler',
    COUNT(*),
    COUNT(p.player_sk)
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.bowler_sk AS STRING) = p.player_sk;

    SELECT
    'Player Out' AS player_role,
    COUNT(*) AS total_records,
    COUNT(p.player_sk) AS matched_records
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.player_out_sk AS STRING) = p.player_sk

UNION ALL

SELECT
    'Fielder',
    COUNT(*),
    COUNT(p.player_sk)
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.fielder_sk AS STRING) = p.player_sk;

    SELECT
    COUNT(*) AS total_records,
    COUNT(player_out_sk) AS non_null_player_out,
    COUNT(fielder_sk) AS non_null_fielder
FROM ipl_data_analysis.silver.slv_ball_by_ball;

SELECT
    out_type,
    COUNT(*) AS delivery_count,
    COUNT(player_out_sk) AS player_out_count,
    COUNT(fielder_sk) AS fielder_count
FROM ipl_data_analysis.silver.slv_ball_by_ball
GROUP BY out_type
ORDER BY delivery_count DESC;


SELECT
    out_type,
    player_out_sk,
    fielder_sk,
    COUNT(*) AS record_count
FROM ipl_data_analysis.silver.slv_ball_by_ball
WHERE out_type = 'Not Applicable'
GROUP BY
    out_type,
    player_out_sk,
    fielder_sk
ORDER BY record_count DESC;

SELECT
    out_type,
    player_out_sk,
    fielder_sk,
    COUNT(*) AS record_count
FROM ipl_data_analysis.silver.slv_ball_by_ball
WHERE out_type <> 'Not Applicable'
GROUP BY
    out_type,
    player_out_sk,
    fielder_sk
ORDER BY record_count DESC
LIMIT 20;

SELECT *
FROM ipl_data_analysis.silver.slv_player
WHERE player_sk IN ('0', '-1');
SELECT
    out_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN player_out_sk = 0 THEN 1 ELSE 0 END) AS default_player_out,
    SUM(CASE WHEN fielder_sk = -1 THEN 1 ELSE 0 END) AS default_fielder
FROM ipl_data_analysis.silver.slv_ball_by_ball
GROUP BY out_type
ORDER BY total_records DESC;

SELECT *
FROM ipl_data_analysis.silver.slv_player
WHERE player_sk = '0';

SELECT
    match_id,
    innings_no,
    over_id,
    ball_id,
    striker,
    non_striker,
    bowler,
    out_type,
    player_out_sk,
    fielder_sk,
    strikersk,
    nonstriker_sk,
    bowler_sk
FROM ipl_data_analysis.silver.slv_ball_by_ball
WHERE out_type IN ('Bowled', 'LBW', 'Caught')
  AND player_out_sk = 0
LIMIT 15;


SELECT
    out_type,
    COUNT(*) AS total_dismissals,
    SUM(CASE WHEN player_out_sk = 0 THEN 1 ELSE 0 END) AS incorrect_default_count,
    SUM(CASE WHEN player_out_sk <> 0 THEN 1 ELSE 0 END) AS other_key_count
FROM ipl_data_analysis.silver.slv_ball_by_ball
WHERE out_type <> 'Not Applicable'
GROUP BY out_type
ORDER BY total_dismissals DESC;

SELECT
    match_id,
    innings_no,
    over_id,
    ball_id,
    striker,
    non_striker,
    Player_Out,
    player_out_sk,
    strikersk,
    nonstriker_sk,
    out_type
FROM ipl_data_analysis.silver.slv_ball_by_ball
WHERE out_type <> 'Not Applicable'
  AND player_out_sk = 0
LIMIT 30;


SELECT
    COUNT(*) AS total_dismissals,
    COUNT(p.player_sk) AS matched_players,
    COUNT(*) - COUNT(p.player_sk) AS unmatched_players
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.Player_Out AS STRING) = p.player_id
WHERE b.out_type <> 'Not Applicable';


SELECT
    b.Player_Out,
    typeof(b.Player_Out) AS player_out_type,
    p.player_id,
    typeof(p.player_id) AS player_id_type,
    p.player_sk,
    p.player_name
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.Player_Out AS BIGINT) = CAST(p.player_id AS BIGINT)
WHERE b.out_type <> 'Not Applicable'
LIMIT 20;

SELECT
    COUNT(*) AS total_dismissals,
    COUNT(p.player_sk) AS matched_players,
    COUNT(*) - COUNT(p.player_sk) AS unmatched_players
FROM ipl_data_analysis.silver.slv_ball_by_ball b
LEFT JOIN ipl_data_analysis.silver.slv_player p
    ON CAST(b.Player_Out AS BIGINT) = CAST(p.player_id AS BIGINT)
WHERE b.out_type <> 'Not Applicable';

SELECT
    out_type,
    COUNT(*) AS dismissal_records,
    COUNT(CASE WHEN fielder_sk = -1 THEN 1 END) AS default_fielder_records,
    COUNT(CASE WHEN fielder_sk <> -1 THEN 1 END) AS populated_fielder_records
FROM ipl_data_analysis.silver.slv_ball_by_ball
WHERE out_type <> 'Not Applicable'
GROUP BY out_type
ORDER BY dismissal_records DESC;