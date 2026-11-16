-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SBC Tax Pipeline: Bronze -> Silver -> Gold in SQL
-- MAGIC Run `sbc_pipeline.py` sections 0-2 first so the `bronze_*` tables exist in `workspace.sbc`.
-- MAGIC This notebook rebuilds silver and gold with SQL only, in schema `workspace.sbc_sql`, so you can compare to the Python version.

-- COMMAND ----------
-- MAGIC %md ## 0. Setup (latest bronze batch, parameters)

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS workspace.sbc_sql

-- COMMAND ----------
USE workspace.sbc_sql

-- COMMAND ----------
DECLARE OR REPLACE VARIABLE v_batch STRING

-- COMMAND ----------
SET VAR v_batch = (SELECT MAX(_batch_id) FROM workspace.sbc.bronze_equity_events)

-- COMMAND ----------
DECLARE OR REPLACE VARIABLE v_as_of DATE DEFAULT DATE'2025-09-30'

-- COMMAND ----------
DECLARE OR REPLACE VARIABLE v_tol DOUBLE DEFAULT 1.0

-- COMMAND ----------
DECLARE OR REPLACE VARIABLE v_cap DOUBLE DEFAULT 1000000.0

-- COMMAND ----------
SELECT v_batch, v_as_of, v_tol, v_cap

-- COMMAND ----------
-- MAGIC %md ## 1. SILVER: remove duplicate events (keep the first ingested row)

-- COMMAND ----------
CREATE OR REPLACE TABLE silver_events_dedup AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingest_ts) AS rn
  FROM workspace.sbc.bronze_equity_events
  WHERE _batch_id = v_batch
)
WHERE rn = 1

-- COMMAND ----------
-- MAGIC %md ## 2. SILVER: taxable events, joined to the effective-dated HRIS row, with tax income and book expense

-- COMMAND ----------
CREATE OR REPLACE TABLE silver_taxable_events AS
SELECT e.event_id, e.grant_id, e.employee_id, e.award_type, e.event_type, e.event_date,
       e.shares, e.strike_price, e.grant_date_fv, e.fmv_at_event,
       g.grant_date,
       h.legal_entity, h.country, h.effective_start AS hris_effective_start,
       ROUND(e.shares * (e.fmv_at_event - e.strike_price), 2) AS tax_income,
       ROUND(e.shares * e.grant_date_fv, 2)                   AS book_expense_recognized
FROM silver_events_dedup e
JOIN (SELECT grant_id, event_date AS grant_date
      FROM silver_events_dedup WHERE event_type = 'GRANT') g
  ON e.grant_id = g.grant_id
JOIN (SELECT * FROM workspace.sbc.bronze_hris WHERE _batch_id = v_batch) h
  ON e.employee_id = h.employee_id
 AND e.event_date BETWEEN h.effective_start AND h.effective_end
WHERE (e.award_type = 'RSU' AND e.event_type = 'VEST')
   OR (e.award_type = 'NSO' AND e.event_type = 'EXERCISE')

-- COMMAND ----------
-- MAGIC %md ## 3. GOLD: grant-level detail (deduction, book-tax difference, excess tax effect)

-- COMMAND ----------
CREATE OR REPLACE TABLE gold_sbc_detail AS
SELECT s.*,
       r.tax_rate, r.rate_version,
       YEAR(s.event_date)                                                   AS tax_year,
       s.tax_income                                                         AS tax_deduction,
       ROUND(s.tax_income - s.book_expense_recognized, 2)                   AS book_tax_difference,
       ROUND((s.tax_income - s.book_expense_recognized) * r.tax_rate, 2)    AS excess_tax_effect
FROM silver_taxable_events s
LEFT JOIN workspace.sbc.config_tax_rates r ON s.country = r.country

-- COMMAND ----------
-- MAGIC %md ## 4. GOLD: summary by entity and year

-- COMMAND ----------
CREATE OR REPLACE TABLE gold_sbc_by_entity_year AS
SELECT legal_entity, country, tax_year,
       COUNT(*)                          AS events,
       SUM(shares)                       AS shares,
       ROUND(SUM(tax_deduction), 2)      AS tax_deduction,
       ROUND(SUM(book_expense_recognized), 2) AS book_expense,
       ROUND(SUM(book_tax_difference), 2)     AS book_tax_difference,
       ROUND(SUM(excess_tax_effect), 2)       AS excess_tax_effect
FROM gold_sbc_detail
GROUP BY legal_entity, country, tax_year

-- COMMAND ----------
-- MAGIC %md ## 5. GOLD: payroll tie-out (computed income vs wages reported)

-- COMMAND ----------
CREATE OR REPLACE TABLE gold_recon_payroll AS
SELECT s.event_id, s.employee_id, s.legal_entity, s.tax_income, p.wages_reported,
       ROUND(s.tax_income - COALESCE(p.wages_reported, 0), 2) AS diff,
       CASE WHEN p.wages_reported IS NULL THEN 'MISSING_PAYROLL'
            WHEN ABS(s.tax_income - p.wages_reported) > v_tol THEN 'BREAK'
            ELSE 'OK' END AS status
FROM silver_taxable_events s
LEFT JOIN (SELECT event_id, wages_reported
           FROM workspace.sbc.bronze_payroll WHERE _batch_id = v_batch) p
  ON s.event_id = p.event_id

-- COMMAND ----------
-- MAGIC %md ## 6. GOLD: GL tie-out (recompute straight-line book expense from events, compare to GL)

-- COMMAND ----------
CREATE OR REPLACE TABLE gold_recon_gl AS
WITH grants AS (
  SELECT grant_id, employee_id, shares, grant_date_fv, event_date AS grant_date
  FROM silver_events_dedup WHERE event_type = 'GRANT'
),
forf AS (
  SELECT grant_id, event_date AS forfeit_date
  FROM silver_events_dedup WHERE event_type = 'FORFEIT'
),
quarters AS (
  SELECT g.employee_id, g.shares, g.grant_date_fv, g.grant_date,
         EXPLODE(SEQUENCE(1, COALESCE(CAST(ROUND(MONTHS_BETWEEN(f.forfeit_date, g.grant_date) / 3) AS INT) - 1, 16))) AS k
  FROM grants g LEFT JOIN forf f ON g.grant_id = f.grant_id
),
dated AS (
  SELECT *, ADD_MONTHS(grant_date, 3 * k) AS q_date,
         shares * grant_date_fv / 16        AS q_expense
  FROM quarters
),
recomputed AS (
  SELECT h.legal_entity, YEAR(d.q_date) AS tax_year,
         ROUND(SUM(d.q_expense), 2) AS book_expense_recomputed
  FROM dated d
  JOIN (SELECT * FROM workspace.sbc.bronze_hris WHERE _batch_id = v_batch) h
    ON d.employee_id = h.employee_id
   AND d.q_date BETWEEN h.effective_start AND h.effective_end
  WHERE d.q_date <= v_as_of
  GROUP BY h.legal_entity, YEAR(d.q_date)
),
gl AS (
  SELECT legal_entity, tax_year, book_expense_gl
  FROM workspace.sbc.bronze_gl_sbc WHERE _batch_id = v_batch
)
SELECT legal_entity, tax_year, book_expense_gl, book_expense_recomputed,
       ROUND(COALESCE(book_expense_gl, 0) - COALESCE(book_expense_recomputed, 0), 2) AS diff,
       CASE WHEN ABS(COALESCE(book_expense_gl, 0) - COALESCE(book_expense_recomputed, 0)) > v_tol
            THEN 'BREAK' ELSE 'OK' END AS status
FROM recomputed FULL JOIN gl USING (legal_entity, tax_year)

-- COMMAND ----------
-- MAGIC %md ## 7. GOLD: income sourcing (allocate by days worked from grant date to event date)

-- COMMAND ----------
CREATE OR REPLACE TABLE gold_sbc_sourced AS
WITH hh AS (
  SELECT employee_id AS h_emp, effective_start AS h_start, effective_end AS h_end,
         legal_entity AS src_entity, country AS src_country
  FROM workspace.sbc.bronze_hris WHERE _batch_id = v_batch
),
alloc AS (
  SELECT s.*, hh.src_entity, hh.src_country,
         DATEDIFF(LEAST(hh.h_end, s.event_date), GREATEST(hh.h_start, s.grant_date)) + 1 AS days
  FROM silver_taxable_events s
  JOIN hh ON s.employee_id = hh.h_emp
         AND hh.h_start <= s.event_date AND hh.h_end >= s.grant_date
),
pct AS (
  SELECT *, days / SUM(days) OVER (PARTITION BY event_id) AS source_pct FROM alloc
)
SELECT p.event_id, p.grant_id, p.employee_id, YEAR(p.event_date) AS tax_year,
       p.src_entity, p.src_country, p.days, p.source_pct,
       ROUND(p.tax_income * p.source_pct, 2)               AS sourced_deduction,
       ROUND(p.book_expense_recognized * p.source_pct, 2)  AS sourced_book,
       r.tax_rate                                          AS src_rate,
       ROUND((p.tax_income - p.book_expense_recognized) * p.source_pct * r.tax_rate, 2) AS sourced_excess_tax_effect
FROM pct p
LEFT JOIN workspace.sbc.config_tax_rates r ON p.src_country = r.country

-- COMMAND ----------
-- MAGIC %md ## 8. GOLD: §162(m) limitation for covered employees

-- COMMAND ----------
CREATE OR REPLACE TABLE gold_162m AS
SELECT d.employee_id, d.tax_year,
       ROUND(SUM(d.tax_deduction), 2) AS sbc_deduction,
       v_cap                          AS cap,
       ROUND(GREATEST(0, SUM(d.tax_deduction) - v_cap), 2) AS disallowed,
       ROUND(GREATEST(0, SUM(d.tax_deduction) - v_cap) * MAX(d.tax_rate), 2) AS tax_cost
FROM gold_sbc_detail d
JOIN (SELECT employee_id, covered_since
      FROM workspace.sbc.bronze_covered_employees WHERE _batch_id = v_batch) c
  ON d.employee_id = c.employee_id AND d.tax_year >= c.covered_since
WHERE d.country = 'US'
GROUP BY d.employee_id, d.tax_year

-- COMMAND ----------
-- MAGIC %md ## 9. AUDIT: exceptions from every control, in one queue

-- COMMAND ----------
CREATE OR REPLACE TABLE audit_dq_exceptions AS
SELECT 'DUPLICATE_EVENT' AS `check`, event_id AS `key`,
       'duplicate event_id in bronze; first row kept' AS detail
FROM (SELECT event_id, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingest_ts) AS rn
      FROM workspace.sbc.bronze_equity_events WHERE _batch_id = v_batch)
WHERE rn > 1
UNION ALL
SELECT 'NO_HRIS_MATCH', e.event_id, 'no effective-dated HRIS row on event date'
FROM silver_events_dedup e
LEFT ANTI JOIN silver_taxable_events s ON e.event_id = s.event_id
WHERE (e.award_type = 'RSU' AND e.event_type = 'VEST')
   OR (e.award_type = 'NSO' AND e.event_type = 'EXERCISE')
UNION ALL
SELECT CONCAT('PAYROLL_', status), event_id,
       CONCAT('computed=', tax_income, ' payroll=', wages_reported)
FROM gold_recon_payroll WHERE status <> 'OK'
UNION ALL
SELECT 'GL_BREAK', CONCAT_WS('-', legal_entity, CAST(tax_year AS STRING)),
       CONCAT('gl=', book_expense_gl, ' recomputed=', book_expense_recomputed)
FROM gold_recon_gl WHERE status <> 'OK'

-- COMMAND ----------
-- MAGIC %md ## 10. Review

-- COMMAND ----------
SELECT * FROM gold_sbc_by_entity_year ORDER BY legal_entity, tax_year

-- COMMAND ----------
SELECT * FROM audit_dq_exceptions ORDER BY `check`

-- COMMAND ----------
-- Drill-down: summary number -> grant-level rows
SELECT * FROM gold_sbc_detail
WHERE legal_entity = 'ACME_US' AND tax_year = 2024
ORDER BY ABS(book_tax_difference) DESC
LIMIT 50
