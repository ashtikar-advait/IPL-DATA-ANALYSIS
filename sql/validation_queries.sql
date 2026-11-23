use catalog ipl_data_analysis;
use schema silver;

-- keys identified from facts (ball_by_ball) tabls:
-- MatcH_id, Over_id, Ball_id, Striker_match_SK, StrikerSK, NonStriker_match_SK, NONStriker_SK, Fielder_match_SK, Fielder_SK, Bowler_match_SK, BOWLER_SK, PlayerOut_match_SK, BattingTeam_SK, BowlingTeam_SK, Player_out_sk, MatchDateSK

-- match_id --
--- ball_by_ball ---
select count(*) as total_records, 
    count(match_id) as non_null_match_id,
    count(*) - count(match_id) as null_match_id,
    count(distinct(match_id)) as distinct_match_ids 
from slv_ball_by_ball bbb;
--- match ---
select count(distinct(match_id)) as distinct_match_ids from slv_match; -- count = 637

select distinct(bbb.match_id) as bbb_dis_mi from slv_ball_by_ball bbb
where bbb.match_id not in (select distinct(match_id) from slv_match); -- returns blank data - means all IDs from ballby_ball.csv are present in match.csv 

select distinct(m.match_id) as m_dis_mi from slv_match m
where m.match_id not in (select distinct(match_id) from slv_ball_by_ball); -- 1082619 match_id is present in match.csv but not in ball_by_ball.csv


-- over_id, ball_id --
--- ball_by_ball ---
select count(*) as total_records,
    count(over_id) as non_null_over_id,
    count(*) - count(over_id) as null_over_id,
    count(ball_id) as non_null_ball_id,
    count(*) - count(ball_id) as null_ball_id
from slv_ball_by_ball;


-- innings_No --
--- ball_by_ball ---
select count(*) as total_records,
count(innings_no) as non_null_innings,
count(*) - count(innings_no) as null_innings
from slv_ball_by_ball;

-- striker_batting_position --
--- ball_by_ball ---
select count(*) as total_records,
count(striker_batting_position) as non_null_striker_batting_position,
count(*) - count(striker_batting_position) as null_striker_batting_position
from slv_ball_by_ball; -- null = 13861

select count(*) - count(striker_batting_position) as cnt from slv_ball_by_ball where team_batting is null and team_bowling is null; -- returns 13861 -- issue needs to be investigated later

-- extra_type, runs_scored, extra_runs, wides, legbyes, noballs, Penalty, Bowler_Extras, Out_type, Caught, Bowled, Run_out, LBW, Retired_hurt, Stumped,caught_and_bowled, hit_wicket, ObstructingFeild, Bowler_Wicket
 --
--- ball_by_ball ---
select count(*) as total_records,
    count(extra_type) as no_null_extra_type_records,
    count(*) - count(extra_type) as null_extra_type_records,
    count(runs_scored) as no_null_runs_scored_records,
    count(*) - count(runs_scored) as null_runs_scored_records,
    count(extra_runs) as no_null_extra_runs_records,
    count(*) - count(extra_runs) as null_extra_runs_records,
    count(wides) as no_null_wides_records,
    count(*) - count(wides) as null_wides_records,
    count(legbyes) as no_null_legbyes_records,
    count(*) - count(legbyes) as null_legbyes_records,
    count(noballs) as no_null_noballs_records,
    count(*) - count(noballs) as null_noballs_records,
    count(penalty) as no_null_penalty_records,
    count(*) - count(penalty) as null_penalty_records,
    count(bowler_extras) as no_null_bowler_extras_records,
    count(*) - count(bowler_extras) as null_bowler_extras_records,
    count(out_type) as no_null_out_type_records,
    count(*) - count(out_type) as null_out_type_records,
    count(caught) as no_null_caught_records,
    count(*) - count(caught) as null_caught_records,
    count(bowled) as no_null_bowled_records,
    count(*) - count(bowled) as null_bowled_records,
    count(run_out) as no_null_run_out_records,
    count(*) - count(run_out) as null_run_out_records,
    count(LBW) as no_null_LBW_records,
    count(*) - count(LBW) as null_LBW_records,
    count(retired_hurt) as no_null_retired_hurt_records,
    count(*) - count(retired_hurt) as null_retired_hurt_records,
    count(stumped) as no_null_stumped_records,
    count(*) - count(stumped) as null_stumped_records,
    count(caught_and_bowled) as no_null_caught_and_bowled_records,
    count(*) - count(caught_and_bowled) as null_caught_and_bowled_records,
    count(hit_wicket) as no_null_hit_wicket_records,
    count(*) - count(hit_wicket) as null_hit_wicket_records,
    count(obstructingfeild) as no_null_obstructingfeild_records,
    count(*) - count(obstructingfeild) as null_obstructingfeild_records,
    count(bowler_wicket) as no_null_bowler_wicket_records,
    count(*) - count(bowler_wicket) as null_bowler_wicket_records
from slv_ball_by_ball bbb; -- No Nulls detected

-- match_date, season --
--- ball_by_ball ---
select count(*) as total_records,
    count(match_date) as no_null_match_date_records,
    count(*) - count(match_date) as null_match_date_records,
    count(season) as no_null_season_records,
    count(*) - count(season) as null_season_records
from slv_ball_by_ball bbb; -- No Nulls detected


-- striker --
--- ball_by_ball ---
select count(*) as total_records,
    count(striker) as non_null_striker_records,
    count(*) - count(striker) as null_striker_records,
    count(distinct(striker))
from slv_ball_by_ball bbb;

select count(distinct(player_id)) from slv_player;
select distinct(bbb.striker) from slv_ball_by_ball bbb
where not exists(
    select 1 from slv_player p
    where p.player_id = bbb.striker
);

-- non-striker --
--- ball_by_ball ---
select count(*) as total_records,
    count(non_striker) as non_null_non_striker_records,
    count(*) - count(non_striker) as null_non_striker_records,
    count(distinct(striker))
from slv_ball_by_ball bbb;

select count(distinct(player_id)) from slv_player;
select distinct(bbb.non_striker) from slv_ball_by_ball bbb
where not exists(
    select 1 from slv_player p
    where p.player_id = bbb.non_striker
);

-- bowler --
--- ball_by_ball ---
select count(*) as total_records,
    count(bowler) as non_null_bowler_records,
    count(*) - count(bowler) as null_bowler_records,
    count(distinct(bowler))
from slv_ball_by_ball bbb;

select count(distinct(player_id)) from slv_player;
select distinct(bbb.bowler) from slv_ball_by_ball bbb
where not exists(
    select 1 from slv_player p
    where p.player_id = bbb.bowler
);

-- player_out --
select count(*) as total_records,
    count(player_out) as non_null_player_out_records,
    count(*) - count(player_out) as null_player_out_records,
    count(distinct(player_out)) as distinct
from slv_ball_by_ball bbb; -- NULL records found - 143013

select distinct(player_out) from slv_ball_by_ball bbb
where not exists(
    select 1 from slv_player p
    where p.player_id = bbb.player_out 
) and bbb.player_out is not null; -- Returns NULL since it is ball by ball data and player_out shows data only on the delivery the player was out.

-- Filders --
--- ball_by_ball ---
select count(*) as total_records,
    count(fielders) as non_null_fielders_records,
    count(*) - count(fielders) as null_fielders_records,
    count(distinct(fielders)) as distinct
from slv_ball_by_ball bbb;

select distinct(fielders) from slv_ball_by_ball bbb
where not exists(
    select 1 from slv_player p
    where p.player_id = bbb.fielders 
) and bbb.fielders is not null; -- Returns NULL since it is ball by ball data and fielders shows data only when the player was out and that too because of fielder (catch out)



-- striker_sk, non-striker_sk, bowler_sk --
--- ball_by_ball ---
select count(*) as total_records,
    count(strikersk) as non_null_strikersk_records,
    count(*) - count(strikersk) as null_strikersk_records,
    count(distinct(strikersk)) as distinct_striker_sk,
    count(nonstriker_sk) as non_null_nonstrikersk_records,
    count(*) - count(nonstriker_sk) as null_nonstrikersk_records,
    count(distinct(nonstriker_sk)) as distinct_nonstriker_sk,
    count(bowler_sk) as non_null_bowler_sk_records,
    count(*) - count(bowler_sk) as null_bowler_sk_records,
    count(distinct(bowler_sk)) as distinct_bowler_sk_sk
from slv_ball_by_ball bbb;

select distinct(bbb.strikersk) from slv_ball_by_ball bbb
where not exists (
    select 1 from slv_player p
    where p.player_sk = bbb.strikersk
) and bbb.strikersk is not null;

select distinct(bbb.nonstriker_sk) from slv_ball_by_ball bbb
where not exists (
    select 1 from slv_player p
    where p.player_sk = bbb.nonstriker_sk
) and bbb.nonstriker_sk is not null;

select distinct(bbb.bowler_sk) from slv_ball_by_ball bbb
where not exists (
    select 1 from slv_player p
    where p.player_sk = bbb.bowler_sk
) and bbb.bowler_sk is not null;

-- battingteam_sk, bowlinteam_sk --
--- ball_by_ball ---
select count(*) as total_records,
    count(battingteam_sk) as non_null_battingteam_sk_records,
    count(*) - count(battingteam_sk) as null_battingteam_sk_records,
    count(distinct(battingteam_sk)) as distinct_battingteam_sk_sk,
    count(bowlingteam_sk) as non_null_bowlingteam_sk_records,
    count(*) - count(bowlingteam_sk) as null_bowlingteam_sk_records,
    count(distinct(bowlingteam_sk)) as distinct_bowlingteam_sk
from slv_ball_by_ball bbb;

select distinct(bbb.battingteam_sk) from slv_ball_by_ball bbb
where not exists (
    select 1 from slv_team t
    where t.team_sk = bbb.battingteam_sk
) and bbb.battingteam_sk is not null;

select distinct(bbb.bowlingteam_sk) from slv_ball_by_ball bbb
where not exists (
    select 1 from slv_team t
    where t.team_sk = bbb.bowlingteam_sk
) and bbb.bowlingteam_sk is not null;

-- keeper_catch -- 
SELECT
    COUNT(*) AS total_players,
    COUNT(CASE WHEN player_sk = CAST(player_id AS INT) - 1 THEN 1 END) AS matching_records,
    COUNT(CASE WHEN player_sk <> CAST(player_id AS INT) - 1 THEN 1 END) AS non_matching_records
FROM slv_player;

---
SELECT
    COUNT(*) AS total_records,

    COUNT(striker_match_sk) AS striker_match_non_null,
    COUNT(DISTINCT striker_match_sk) AS distinct_striker_match_sk,

    COUNT(nonstriker_match_sk) AS nonstriker_match_non_null,
    COUNT(DISTINCT nonstriker_match_sk) AS distinct_nonstriker_match_sk,

    COUNT(fielder_match_sk) AS fielder_match_non_null,
    COUNT(DISTINCT fielder_match_sk) AS distinct_fielder_match_sk,

    COUNT(fielder_sk) AS fielder_non_null,
    COUNT(DISTINCT fielder_sk) AS distinct_fielder_sk,

    COUNT(bowler_match_sk) AS bowler_match_non_null,
    COUNT(DISTINCT bowler_match_sk) AS distinct_bowler_match_sk,

    COUNT(playerout_match_sk) AS playerout_match_non_null,
    COUNT(DISTINCT playerout_match_sk) AS distinct_playerout_match_sk,

    COUNT(matchdatesk) AS matchdate_non_null,
    COUNT(DISTINCT matchdatesk) AS distinct_matchdatesk

FROM slv_ball_by_ball;

-- Checking for surrogate keys match in dim_tables ---

DESCRIBE slv_ball_by_ball;
select column_name, data_type from information_schema.columns where table_name = 'slv_player';

-- striker_match_sk --
select count(*) as total_records,
count(distinct(player_match_sk)) as total_player_match_sk,
count(distinct(player_id)) as total_player_ids 
from slv_player_match; -----|total_records | total_player_match_sk | total_player_ids |
                        ----|13992 | 13992 | 13992 | => this means that for every player_id there is a player_match_sk

select count(*) as total_records, count(case when player_sk = player_id - 1 then 1 end) as matched_records,
count(case when player_sk <> player_id -1 then 1 end) as unmatched_records
from slv_player;    --- |total_records | matched_records | unmatched_records |
                    --- |497 | 497 | 0 |

select distinct(bbb.striker_match_sk) from slv_ball_by_ball bbb
where not exists (select 1 from slv_player_match p where p.player_match_sk = bbb.striker_match_sk) and bbb.striker_match_sk is not null; -- returns blank -> confirms that all striker_match_sk in ball_by_ball are present in player_match table and for each player_match_sk there exists a player_id and for each player_id there is a player_sk


------ nonstriker_match_sk ------
select distinct(bbb.nonstriker_match_sk) from slv_ball_by_ball bbb
where not exists (select 1 from slv_player_match p where p.player_match_sk = bbb.nonstriker_match_sk) and bbb.nonstriker_match_sk is not null;
-- returns null means all nonstriker_match_sk are unique

------ bowler_match_sk ------
select distinct(bbb.bowler_match_sk) from slv_ball_by_ball bbb
where not exists (select 1 from slv_player_match p where p.player_match_sk = bbb.bowler_match_sk) and bbb.bowler_match_sk is not null;
-- returns null means all bowler_match_sk are unique


--- fielder_match_sk, fielder_sk ---
select * from slv_ball_by_ball where fielder_match_sk <> -1 limit 10;

select distinct(out_type) from slv_ball_by_ball where fielder_match_sk <> -1;

select count(*) as total_players,
    count(case when player_sk = player_id -1 then 1 end) as matched_records,
    count(case when player_sk <> player_id -1 then 1 end) as unmatched_records
from slv_player;

-- fielder match sk ---
SELECT
    COUNT(*) AS total_records,
    COUNT(bbb.fielder_match_sk) AS non_null_fielder_match_sk,
    COUNT(pm.player_match_sk) AS matched_records,
    COUNT(CASE
        WHEN bbb.fielder_match_sk = -1 THEN 1
    END) AS placeholder_records
FROM slv_ball_by_ball bbb
LEFT JOIN slv_player_match pm
    ON bbb.fielder_match_sk = pm.player_match_sk
WHERE bbb.fielder_match_sk <> -1;

-- playour out match sk --
SELECT
    COUNT(*) AS total_records,
    COUNT(bbb.playerout_match_sk) AS non_null_fielder_match_sk,
    COUNT(pm.player_match_sk) AS matched_records,
    COUNT(CASE
        WHEN bbb.playerout_match_sk = -1 THEN 1
    END) AS placeholder_records
FROM slv_ball_by_ball bbb
LEFT JOIN slv_player_match pm
    ON bbb.playerout_match_sk = pm.player_match_sk
WHERE bbb.playerout_match_sk <> -1;

--- matchdatesk ---
SELECT
    COUNT(*) AS total_records,
    COUNT(matchdatesk) AS non_null_records,
    COUNT(*) - COUNT(matchdatesk) AS null_records,
    COUNT(DISTINCT matchdatesk) AS distinct_matchdatesk,
    COUNT(CASE WHEN matchdatesk = -1 THEN 1 END) AS placeholder_records
FROM slv_ball_by_ball;

select concat(year(match_date),month(match_date),day(match_date)) from slv_match;

select matchdatesk from slv_ball_by_ball
where not exists (select date_format(match_date,'yyyyMMdd') as date_match_sk from slv_match
where date_format(match_date,'yyyyMMdd') = matchdatesk) and matchdatesk is not null;


--- team_batting, team_balling, striker_batting_position ==> special attention columns ---
select * from slv_ball_by_ball bbb
where team_batting is null or team_bowling is null or striker_batting_position is null;

select match_id,strikersk,striker_match_sk,striker_batting_position from slv_ball_by_ball where Striker_Batting_Position is null;


-- player_out_sk is showing 0 whereever the player_out is NULL this is indiacting the player Sourav Ganguly as the player who gout out on every ball.  
with cte as (select bbb.player_out_sk, p.player_id, p.player_name, bbb.Player_Out,pp.player_id,pp.player_name from slv_ball_by_ball bbb
join slv_player p
on bbb.player_out_sk = p.player_sk 
join slv_player pp on pp.player_sk = bbb.Player_Out)
select * from cte;

select player_id, player_sk, player_name from slv_player where player_id = 81;


select distinct player_out_sk from slv_ball_by_ball;

select bbb.player_out,p.player_id, p.player_sk as player_out_sk from slv_ball_by_ball bbb
join slv_player p
on bbb.player_out = p.player_id;


-- fielder_sk and fielder_match_sk reporting -1

select distinct fielder_sk, fielder_match_sk from slv_ball_by_ball;

select distinct fielders, fielder_sk, fielder_match_sk from slv_ball_by_ball
where fielder_sk = -1 or fielder_match_sk = -1;