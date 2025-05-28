--CASE 3 DAU Analysis
SELECT
event_date,
count(distinct user_id) as DAU
FROM `game-analysis-01.project_game_v2.users_daily`
GROUP BY 1
ORDER BY 1
;

--CASE 4 Conversion Rate Analysis
SELECT
event_date,
COUNT(user_id) AS DAU,
COUNT(CASE WHEN purchases > 0 THEN user_id END) AS paying_users,
SAFE_DIVIDE(COUNT(CASE WHEN purchases > 0 THEN user_id END),COUNT(user_id)) AS CVR_Rate
FROM `game-analysis-01.project_game_v2.users_daily`
GROUP BY 1;

--CASE 5 Player Behavior Analysis: Average Number of Enemies Defeated per Level

SELECT
SAFE_CAST(stage_index AS INT64) as stage_index,
SUM(SAFE_CAST(SPLIT(stage_kills, '|')[SAFE_OFFSET(0)] AS INT64)) AS total_kills,
COUNT(DISTINCT user_id) AS total_users,
ROUND(SAFE_DIVIDE(
SUM(SAFE_CAST(SPLIT(stage_kills, '|')[SAFE_OFFSET(0)] AS INT64)),
COUNT(DISTINCT user_id)
)) AS avg_kills_per_user
FROM `game-analysis-01.project_game_v2.stage_events`
GROUP BY 1
ORDER BY 1;


--Eğer tek tek oyuncuların tam olarak ne kadar öldürdüğünü öğrenmek istersem şu şekilde yazıyorum:
SELECT
user_id,
       SAFE_CAST(stage_index AS INT64) as stage_index,
SUM(SAFE_CAST(SPLIT(stage_kills, '|')[OFFSET(0)] AS INT64)) AS total_kills
FROM game-analysis-01.project_game_v2.stage_events
GROUP BY 1,2
ORDER BY 2;

--CASE 6 Difficulty Analysis

SELECT
stage_index,
SUM(SAFE_CAST(stage_attempt as INT64))  as total_attempt_per_index,
COUNTIF(result = 'fail') as fail_result,
SUM(SAFE_CAST(survival_time as INT64)) as total_survivor_time
 FROM `game-analysis-01.project_game_v2.stage_events`
 GROUP BY 1
 order by total_attempt_per_index desc;

--CASE 7 Most Efficient Gem Source for Players

SELECT
    reason,
    SUM(SAFE_CAST(currency_change_amount AS INT64)) AS total_amount
FROM `game-analysis-01.project_game_v2.currency_changes`
WHERE change_type = 'Gain'
  AND currency_type = 'Gem'
GROUP BY reason
ORDER BY total_amount DESC
LIMIT 10;

--CASE 8 Player Progression on Day 5

WITH stage_ranked AS (
SELECT
user_id,
event_date,
stage_index,
RANK() OVER (PARTITION BY event_date,user_id ORDER BY stage_index desc) AS rnk
FROM `game-analysis-01.project_game_v2.stage_events`
),
cohort AS (
SELECT
DATE_DIFF(ud.event_date, u.install_date, DAY) AS cohort_age,
COALESCE(s.stage_index, 'Bilinmiyor') as stage_index,
COUNT(DISTINCT ud.user_id) AS cohort_size
FROM `game-analysis-01.project_game_v2.users_daily` AS ud
LEFT JOIN `game-analysis-01.project_game_v2.users` AS u ON  u.user_id  = SAFE_CAST(ud.user_id AS INT64)
LEFT JOIN stage_ranked AS s
ON SAFE_CAST(ud.user_id AS INT64) = s.user_id
AND ud.event_date = s.event_date
WHERE DATE_DIFF(ud.event_date, u.install_date, DAY) = 5
AND s.rnk = 1
GROUP BY 1,2
)
SELECT
stage_index,
sum(cohort_size) AS players
FROM cohort
GROUP BY 1
ORDER BY 2 DESC;

--CASE 9 Player Progression by 15 Attempts

WITH stage_cum_attempt AS (
SELECT
stage_index,
SUM(SAFE_CAST(stage_attempt AS INT64)) OVER (PARTITION BY user_id ORDER BY stage_index, stage_attempt) AS cum_attempt
FROM `game-analysis-01.project_game_v2.stage_events`
)
SELECT
stage_index,
count(*) as players
FROM stage_cum_attempt
WHERE cum_attempt = 15
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

--CASE 10 D1-D3-D7 LTV for United States Users


WITH users_info as (
SELECT
install_date,
user_id,
country_first
 FROM`game-analysis-01.project_game_v2.users`
 )
 ,
 cohort as (
  SELECT
  UI.install_date,
  DATE_DIFF(PD.event_date,UI.install_date, DAY) as cohort_age,
  COUNT(distinct PD.user_id) as cohort_size,
  SUM(purchases + ad_revenue) as total_revenue
  FROM `game-analysis-01.project_game_v2.users_daily` as PD
  LEFT JOIN users_info as UI ON SAFE_CAST(PD.user_id AS INT64) = UI.user_id
  WHERE UI.country_first = 'UNITED STATES'
  GROUP BY 1,2
 )
 SELECT
 install_date,
 ROUND(SUM(CASE WHEN cohort_age <=1 THEN total_revenue else null end) / max(cohort_size),2) as D1LTV,
 ROUND(SUM(CASE WHEN cohort_age <=3 THEN total_revenue else null end) / max(cohort_size),2) as D3LTV,
 ROUND(SUM(CASE WHEN cohort_age <=7 THEN total_revenue else null end) / max(cohort_size),2) as D7LTV
 FROM cohort
 GROUP BY 1
 ORDER BY 1