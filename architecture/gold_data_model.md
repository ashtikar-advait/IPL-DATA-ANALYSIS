
erDiagram
    DIM_MATCH ||--o{ FACT_BALL_BY_BALL : "match_sk"
    DIM_MATCH ||--o{ FACT_PLAYER_MATCH : "match_sk"

    DIM_PLAYER ||--o{ FACT_BALL_BY_BALL : "player roles"
    DIM_PLAYER ||--o{ FACT_PLAYER_MATCH : "player_sk"

    DIM_TEAM ||--o{ FACT_BALL_BY_BALL : "team roles"
    DIM_TEAM ||--o{ FACT_PLAYER_MATCH : "team roles"

    DIM_MATCH {
        int match_sk PK
        int match_id
        date match_date
        string venue_name
        string city_name
        string country_name
    }

    DIM_PLAYER {
        int player_sk PK
        int player_id
        string player_name
        string batting_hand
        string bowling_skill
    }

    DIM_TEAM {
        int team_sk PK
        int team_id
        string team_name
    }

    FACT_BALL_BY_BALL {
        int match_sk FK
        int innings_no
        int over_id
        int ball_id
        int strikersk FK
        int nonstriker_sk FK
        int bowler_sk FK
        int player_out_sk FK
        int fielder_sk FK
        int battingteam_sk FK
        int bowlingteam_sk FK
        int runs_scored
        int extra_runs
    }

    FACT_PLAYER_MATCH {
        int player_match_sk PK
        int match_sk FK
        int player_sk FK
        int player_team_sk FK
        int opposit_team_sk FK
        string role_desc
        int age_as_on_match
        boolean is_manofthematch
    }
