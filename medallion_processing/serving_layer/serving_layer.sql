create or replace view ipl_data_analysis.gold.vw_ball_by_ball_enriched as(
select 
    -- Match information
    m1.match_id,
    m1.match_date,
    m1.season_year,
    m1.venue_name,
    m1.city_name,
    m1.country_name,
    m1.toss_winner,
    m1.toss_name,
    m1.match_winner,
    m1.win_type,
    m1.win_margin,

    -- Delivery information
    bbb.innings_no,
    bbb.over_id,
    bbb.ball_id,
    bbb.striker_batting_position,

    -- Player information
    p1.player_name AS non_striker_name,
    p2.player_name AS fielder_name,
    p3.player_name AS striker_name,
    p4.player_name AS player_out_name,
    p5.player_name AS bowler_name,

    -- Team information
    t1.team_name AS batting_team,
    t2.team_name AS bowling_team,

    -- Runs and extras
    bbb.runs_scored,
    bbb.extra_runs,
    bbb.wides,
    bbb.legbyes,
    bbb.byes,
    bbb.noballs,
    bbb.penalty,
    bbb.bowler_extras,
    bbb.extra_type,

    -- Dismissal information
    bbb.out_type,
    bbb.caught,
    bbb.bowled,
    bbb.run_out,
    bbb.lbw,
    bbb.retired_hurt,
    bbb.stumped,
    bbb.caught_and_bowled,
    bbb.hit_wicket,
    bbb.obstructingfeild,
    bbb.bowler_wicket,
    bbb.keeper_catch
from ipl_data_analysis.gold.fact_ball_by_ball bbb
left join ipl_data_analysis.gold.dim_player p1
on bbb.nonstriker_sk = p1.player_sk
left join ipl_data_analysis.gold.dim_player p2
on bbb.fielder_sk = p2.player_sk
left join ipl_data_analysis.gold.dim_player p3
on bbb.strikersk = p3.player_sk
left join ipl_data_analysis.gold.dim_player p4
on bbb.player_out_sk = p4.player_sk
left join ipl_data_analysis.gold.dim_player p5
on bbb.bowler_sk = p5.player_sk
left join ipl_data_analysis.gold.dim_team t1
on bbb.battingteam_sk = t1.team_sk
left join ipl_data_analysis.gold.dim_team t2
on bbb.bowlingteam_sk = t2.team_sk
left join ipl_data_analysis.gold.dim_match m1
on bbb.match_sk = m1.match_sk
);


-- vw_batting_deliveries --
create or replace view ipl_data_analysis.gold.vw_batting_deliveries as (
select 
-- Match Information
m1.match_id,
m1.season_year,
m1.match_date,
m1.venue_name,

-- Team Information
t1.team_name as batting_team,
t2.team_name as bowling_team,

-- Innings Information
bbb.innings_no,
bbb.over_id,
bbb.ball_id,

-- Batter Information
p1.player_name as striker_name,
p2.player_name as non_striker_name,

-- Bowler Information
p3.player_name as bowler_name,

-- Runs Information
bbb.runs_scored,
bbb.extra_runs,
bbb.extra_type,
bbb.wides,
bbb.noballs,
bbb.legbyes,
bbb.byes,

-- Out Information
bbb.out_type,
p4.player_name as player_out_name
from ipl_data_analysis.gold.fact_ball_by_ball bbb
left join ipl_data_analysis.gold.dim_match m1
on bbb.match_sk = m1.match_sk
left join ipl_data_analysis.gold.dim_team t1
on bbb.battingteam_sk = t1.team_sk
left join ipl_data_analysis.gold.dim_team t2
on bbb.bowlingteam_sk = t2.team_sk
left join ipl_data_analysis.gold.dim_player p1
on bbb.strikersk = p1.player_sk
left join ipl_data_analysis.gold.dim_player p2
on bbb.nonstriker_sk = p2.player_sk
left join ipl_data_analysis.gold.dim_player p3
on bbb.bowler_sk = p3.player_sk
left join ipl_data_analysis.gold.dim_player p4
on bbb.player_out_sk = p4.player_sk
);




-- vw_bowling_deliveries --
create or replace view ipl_data_analysis.gold.vw_bowling_deliveries as (
select 
-- Match Information
m1.match_id,
m1.season_year,
m1.match_date,
m1.venue_name,

-- Team Information
t1.team_name as batting_team,
t2.team_name as bowling_team,

-- Innings Information
bbb.innings_no,
bbb.over_id,
bbb.ball_id,

-- Bowler Information
p3.player_name as bowler_name,

-- Batter Information
p1.player_name as striker_name,
p2.player_name as non_striker_name,

-- Runs Information
bbb.runs_scored,
bbb.extra_runs,
bbb.extra_type,
bbb.wides,
bbb.noballs,
bbb.legbyes,
bbb.byes,
bbb.penalty, 
bbb.bowler_extras,

-- Wickets Information
bbb.out_type,
p4.player_name as player_out_name,
p5.player_name as fielder_name,
bbb.caught,
bbb.bowled,
bbb.run_out,
bbb.lbw,
bbb.retired_hurt,
bbb.stumped,
bbb.caught_and_bowled,
bbb.hit_wicket,
bbb.obstructingfeild,
bbb.bowler_wicket
from ipl_data_analysis.gold.fact_ball_by_ball bbb
left join ipl_data_analysis.gold.dim_match m1
on bbb.match_sk = m1.match_sk
left join ipl_data_analysis.gold.dim_team t1
on bbb.battingteam_sk = t1.team_sk
left join ipl_data_analysis.gold.dim_team t2
on bbb.bowlingteam_sk = t2.team_sk
left join ipl_data_analysis.gold.dim_player p1
on bbb.strikersk = p1.player_sk
left join ipl_data_analysis.gold.dim_player p2
on bbb.nonstriker_sk = p2.player_sk
left join ipl_data_analysis.gold.dim_player p3
on bbb.bowler_sk = p3.player_sk
left join ipl_data_analysis.gold.dim_player p4
on bbb.player_out_sk = p4.player_sk
left join ipl_data_analysis.gold.dim_player p5
on bbb.fielder_sk = p5.player_sk
);


-- Player Match Enricher --
create or replace view ipl_data_analysis.gold.vw_fact_player_match_enriched as (
    select 
    -- Match Information
    m1.match_id,
    m1.match_date,
    m1.venue_name,
    
    -- Player Information
    p1.player_id,
    p1.player_name,
    p1.batting_hand,
    p1.bowling_skill,
    p1.country_name,
    t1.team_name as player_team_name,
    t2.team_name as opposit_team_name,

    -- Player-Match Information
    pm.role_desc,
    pm.age_as_on_match,
    pm.is_manofthematch,
    pm.isplayers_team_won,
    pm.player_captain,
    pm.opposit_captain,
    pm.player_keeper,
    pm.opposit_keeper
    from ipl_data_analysis.gold.fact_player_match pm
    left join ipl_data_analysis.gold.dim_match m1
    on pm.match_sk = m1.match_sk
    left join ipl_data_analysis.gold.dim_player p1
    on pm.player_sk = p1.player_sk
    left join ipl_data_analysis.gold.dim_team t1
    on pm.player_team_sk = t1.team_sk
    left join ipl_data_analysis.gold.dim_team t2
    on pm.opposit_team_sk = t2.team_sk

)
