-- Gold layer: star schema + 30-day readmission logic (Spark SQL, Microsoft Fabric)
-- Source notebook: notebooks/02_silver_to_gold.ipynb


-- 02 · Silver → Gold
-- Builds the star schema (6 dimensions + fact_encounter) and the 30-day readmission logic in Spark SQL, then investigates the readmission rate before trusting it.

-- Step 1: Create the gold schema
CREATE SCHEMA IF NOT EXISTS gold;


-- Step 2: Build dimension tables
-- dim_date is generated (one row per day) so time intelligence works. dim_condition includes an 'UNSPECIFIED' member so no fact row has a blank key.

-- 2a · dim_date
CREATE OR REPLACE TABLE gold.dim_date AS
SELECT
  d AS date,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month_num,
  date_format(d, 'MMM') AS month_name,
  date_format(d, 'yyyy-MM') AS year_month,
  trunc(d, 'MM') AS month_start,
  dayofweek(d) AS day_of_week_num,
  date_format(d, 'EEE') AS day_name
FROM (SELECT explode(sequence(DATE'2012-01-01', DATE'2022-12-31', INTERVAL 1 DAY)) AS d);


-- 2b · dim_patient
CREATE OR REPLACE TABLE gold.dim_patient AS
SELECT patient_id, gender, race, ethnicity, marital_status, city, county, state, birth_date, death_date
FROM silver.patients;


-- 2c · dim_organization
CREATE OR REPLACE TABLE gold.dim_organization AS
SELECT organization_id, organization_name, city, state, zip
FROM silver.organizations;


-- 2d · dim_provider
CREATE OR REPLACE TABLE gold.dim_provider AS
SELECT provider_id, organization_id, gender AS provider_gender, specialty, state
FROM silver.providers;


-- 2e · dim_payer
CREATE OR REPLACE TABLE gold.dim_payer AS
SELECT payer_id, payer_name
FROM silver.payers;


-- 2f · dim_condition
CREATE OR REPLACE TABLE gold.dim_condition AS
SELECT CAST(CAST(reason_code AS DECIMAL(20,0)) AS STRING) AS condition_code,
       MAX(reason_description) AS condition_description
FROM silver.encounters
WHERE reason_code IS NOT NULL
GROUP BY 1
UNION ALL
SELECT 'UNSPECIFIED', 'No reason recorded';


-- Step 3: Build fact_encounter (with 30-day readmission logic)
-- Grain: one row per encounter, 2012–2021. LEAD() finds each patient's next inpatient admission across full history. Only stays with 30 days of follow-up and patient alive at discharge are eligible.
CREATE OR REPLACE TABLE gold.fact_encounter AS
WITH enc AS (
  SELECT e.*, p.birth_date, p.death_date
  FROM silver.encounters e
  JOIN silver.patients p ON e.patient_id = p.patient_id
),
inpatient_seq AS (
  -- For every inpatient stay, find the same patient's NEXT inpatient admission and its reason
  SELECT encounter_id,
         LEAD(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_admit_ts,
         LEAD(reason_description) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_admit_reason
  FROM silver.encounters
  WHERE encounter_class = 'inpatient'
),
base AS (
  SELECT
    e.encounter_id,
    e.patient_id,
    e.organization_id,
    e.provider_id,
    e.payer_id,
    COALESCE(CAST(CAST(e.reason_code AS DECIMAL(20,0)) AS STRING), 'UNSPECIFIED') AS condition_code,
    to_date(e.start_ts) AS admit_date,
    to_date(e.stop_ts) AS discharge_date,
    e.encounter_class,
    e.los_days,
    e.duration_hours,
    e.total_claim_cost,
    e.payer_coverage,
    e.total_claim_cost - e.payer_coverage AS out_of_pocket_cost,
    FLOOR(months_between(to_date(e.start_ts), e.birth_date) / 12) AS age_at_encounter,
    -- Eligible index stay: inpatient, has 30 days of follow-up in the data, patient alive at discharge
    CASE WHEN e.encounter_class = 'inpatient'
          AND to_date(e.stop_ts) <= DATE'2022-01-06'
          AND (e.death_date IS NULL OR e.death_date > to_date(e.stop_ts))
         THEN 1 ELSE 0 END AS is_readmit_eligible,
    datediff(i.next_admit_ts, e.stop_ts) AS days_to_next_admit,
    i.next_admit_reason
  FROM enc e
  LEFT JOIN inpatient_seq i ON e.encounter_id = i.encounter_id
  WHERE e.start_ts >= '2012-01-01' AND e.start_ts < '2022-01-01'
)
SELECT
  *,
  CASE
    WHEN age_at_encounter < 18 THEN '0-17'
    WHEN age_at_encounter < 35 THEN '18-34'
    WHEN age_at_encounter < 50 THEN '35-49'
    WHEN age_at_encounter < 65 THEN '50-64'
    WHEN age_at_encounter < 80 THEN '65-79'
    ELSE '80+'
  END AS age_group,
  -- All-cause: any inpatient readmission 0-30 days after discharge (original definition)
  CASE WHEN is_readmit_eligible = 1 AND days_to_next_admit BETWEEN 0 AND 30
       THEN 1 ELSE 0 END AS readmit_30d_all_cause,
  -- Planned: readmission for cancer treatment (chemotherapy cycles), see Investigation 2
  CASE WHEN is_readmit_eligible = 1 AND days_to_next_admit BETWEEN 0 AND 30
        AND lower(COALESCE(next_admit_reason, '')) LIKE '%malignant%'
       THEN 1 ELSE 0 END AS is_planned_readmit,
  -- Final unplanned readmission: day 2-30 (day 0-1 = transfer / continued stay) and not planned cancer care
  CASE WHEN is_readmit_eligible = 1 AND days_to_next_admit BETWEEN 2 AND 30
        AND lower(COALESCE(next_admit_reason, '')) NOT LIKE '%malignant%'
       THEN 1 ELSE 0 END AS readmit_30d
FROM base;


-- Step 4: Sanity check - headline KPIs
SELECT
  COUNT(*) AS total_encounters,
  SUM(CASE WHEN encounter_class = 'inpatient' THEN 1 ELSE 0 END) AS inpatient_stays,
  SUM(is_readmit_eligible) AS eligible_index_stays,
  SUM(readmit_30d) AS readmissions_30d,
  ROUND(100.0 * SUM(readmit_30d) / SUM(is_readmit_eligible), 2) AS readmit_rate_pct,
  ROUND(AVG(CASE WHEN encounter_class = 'inpatient' THEN los_days END), 2) AS avg_inpatient_los
FROM gold.fact_encounter;


-- Investigation 1: How many days after discharge do readmissions happen?
-- The first readmission rate was 30.6%, roughly double typical real-world rates, so I checked the timing before trusting it.
SELECT days_to_next_admit, COUNT(*) AS readmissions
FROM gold.fact_encounter
WHERE readmit_30d = 1
GROUP BY days_to_next_admit
ORDER BY days_to_next_admit;


-- Investigation 2: Why were patients readmitted?
-- Day 18–30 readmissions are dominated by lung and breast cancer, matching 21/28-day chemotherapy cycles (planned care). Day 0–1 readmissions look like transfers or continued stays.
WITH inp AS (
  SELECT patient_id, start_ts, stop_ts,
         LEAD(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_admit_ts,
         LEAD(reason_description) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_reason
  FROM silver.encounters
  WHERE encounter_class = 'inpatient'
)
SELECT
  COALESCE(next_reason, 'No reason recorded') AS readmission_reason,
  SUM(CASE WHEN datediff(next_admit_ts, stop_ts) BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS day_0_1,
  SUM(CASE WHEN datediff(next_admit_ts, stop_ts) BETWEEN 2 AND 17 THEN 1 ELSE 0 END) AS day_2_17,
  SUM(CASE WHEN datediff(next_admit_ts, stop_ts) BETWEEN 18 AND 30 THEN 1 ELSE 0 END) AS day_18_30,
  COUNT(*) AS total
FROM inp
WHERE start_ts >= '2012-01-01' AND start_ts < '2022-01-01'
  AND datediff(next_admit_ts, stop_ts) BETWEEN 0 AND 30
GROUP BY 1
ORDER BY total DESC
LIMIT 20;


-- Investigation 3: What are the "no reason recorded" readmissions?
-- Almost all are generic 'Encounter for problem' admissions, so the encounter type alone can't tell planned from unplanned.
WITH inp AS (
  SELECT patient_id, start_ts, stop_ts,
         LEAD(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_admit_ts,
         LEAD(reason_description) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_reason,
         LEAD(encounter_description) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_encounter_type
  FROM silver.encounters
  WHERE encounter_class = 'inpatient'
)
SELECT
  next_encounter_type,
  SUM(CASE WHEN datediff(next_admit_ts, stop_ts) BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS day_0_1,
  SUM(CASE WHEN datediff(next_admit_ts, stop_ts) BETWEEN 2 AND 17 THEN 1 ELSE 0 END) AS day_2_17,
  SUM(CASE WHEN datediff(next_admit_ts, stop_ts) BETWEEN 18 AND 30 THEN 1 ELSE 0 END) AS day_18_30,
  COUNT(*) AS total
FROM inp
WHERE start_ts >= '2012-01-01' AND start_ts < '2022-01-01'
  AND datediff(next_admit_ts, stop_ts) BETWEEN 0 AND 30
  AND next_reason IS NULL
GROUP BY 1
ORDER BY total DESC
LIMIT 15;


-- Investigation 4: What was the index stay for 'no reason' readmissions at day 18–30?
-- If these patients' original stays were for cancer, the day 18–30 readmissions are likely planned treatment cycles too.
WITH inp AS (
  SELECT patient_id, start_ts, stop_ts, reason_description,
         LEAD(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_admit_ts,
         LEAD(reason_description) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_reason
  FROM silver.encounters
  WHERE encounter_class = 'inpatient'
)
SELECT
  COALESCE(reason_description, 'No reason recorded') AS index_stay_reason,
  COUNT(*) AS no_reason_readmits_day_18_30
FROM inp
WHERE start_ts >= '2012-01-01' AND start_ts < '2022-01-01'
  AND next_reason IS NULL
  AND datediff(next_admit_ts, stop_ts) BETWEEN 18 AND 30
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15;


-- Step 5: Final readmission definition
-- Based on Investigations 1–4:
-- - **Day 0–1 readmissions are excluded** as transfers or continued stays (counted as one hospital stay).
-- - **Readmissions for cancer treatment are excluded as planned care** — they cluster at 21 and 28 days, matching chemotherapy cycles.
-- - **'No reason recorded' readmissions are kept as unplanned.** Investigation 4 showed their original stays also have no recorded reason, so there is no evidence they are planned. Excluding them without evidence would understate the rate. This is noted as a data limitation.
-- The table below shows the step-by-step reconciliation from all-cause to final unplanned readmissions.
SELECT
  SUM(is_readmit_eligible) AS eligible_index_stays,
  SUM(readmit_30d_all_cause) AS all_cause_readmits,
  SUM(CASE WHEN readmit_30d_all_cause = 1 AND days_to_next_admit BETWEEN 0 AND 1 THEN 1 ELSE 0 END) AS excluded_transfers_day_0_1,
  SUM(CASE WHEN readmit_30d_all_cause = 1 AND days_to_next_admit >= 2 AND is_planned_readmit = 1 THEN 1 ELSE 0 END) AS excluded_planned_cancer_care,
  SUM(readmit_30d) AS final_unplanned_readmits,
  ROUND(100.0 * SUM(readmit_30d_all_cause) / SUM(is_readmit_eligible), 2) AS all_cause_rate_pct,
  ROUND(100.0 * SUM(readmit_30d) / SUM(is_readmit_eligible), 2) AS final_unplanned_rate_pct
FROM gold.fact_encounter;
