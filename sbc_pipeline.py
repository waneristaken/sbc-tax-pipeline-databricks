# Databricks notebook source
# MAGIC %md
# MAGIC # SBC Tax Pipeline (Databricks Free Edition, serverless)
# MAGIC Bronze (raw, immutable, batch-stamped) -> Silver (dedup, enrich, DQ) -> Gold (calc, recon, detail with lineage) -> Audit (run log, DQ exceptions).
# MAGIC Data is **synthetic**. Swap Section 2 for real extracts (equity platform, HRIS, payroll) later.

# COMMAND ----------
# MAGIC %md ## 0. Setup

# COMMAND ----------
import math, random, uuid, json, datetime as dt
from pyspark.sql import functions as F, Window

CATALOG, SCHEMA = "workspace", "sbc"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

BATCH_ID      = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
LOGIC_VERSION = "v0.1"          # bump whenever calc rules change
TOL           = 1.00            # payroll tie-out tolerance (USD)
AS_OF         = dt.date(2025, 9, 30)
CAP_162M      = 1_000_000.0     # §162(m) per-covered-employee deduction cap (SBC only in this demo)
print("batch:", BATCH_ID)

# COMMAND ----------
# MAGIC %md ## 1. Helpers

# COMMAND ----------
def write_bronze(rows, ddl_schema, table, source):
    """Append-only landing with batch id, source and ingest timestamp. Returns (rows, checksum)."""
    df = (spark.createDataFrame(rows, ddl_schema)
          .withColumn("_batch_id", F.lit(BATCH_ID))
          .withColumn("_source", F.lit(source))
          .withColumn("_ingest_ts", F.current_timestamp()))
    biz = [c for c in df.columns if not c.startswith("_")]
    stats = df.agg(F.count("*").alias("n"), F.sum(F.hash(*biz)).alias("chk")).first()
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table)
    return stats["n"], stats["chk"]

def log_dq(df):
    """df columns: check, key, detail (all strings)."""
    (df.withColumn("_batch_id", F.lit(BATCH_ID)).withColumn("_ts", F.current_timestamp())
       .write.mode("append").option("mergeSchema", "true").saveAsTable("audit_dq_exceptions"))

def table_version(t):
    return spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()["version"]

# COMMAND ----------
# MAGIC %md ## 2. Generate synthetic source extracts -> Bronze

# COMMAND ----------
random.seed(42)

def fmv(d):  # deterministic fake share price
    m = (d.year - 2021) * 12 + d.month
    return round(50 * (1 + 0.03 * m) * (1 + 0.15 * math.sin(m / 3)), 2)

def add_months(d, n):
    y = d.year + (d.month - 1 + n) // 12
    mo = (d.month - 1 + n) % 12 + 1
    return dt.date(y, mo, min(d.day, 28))

ENTITY = {"US": "ACME_US", "UK": "ACME_UK", "IN": "ACME_IN"}
EMPS = [f"E{i:04d}" for i in range(1, 41)]
COVERED = EMPS[:3]                                  # synthetic §162(m) covered employees (US, large grants)

# --- HRIS (effective-dated) ---
hris = []
for e in EMPS:
    c = "US" if e in COVERED else random.choices(list(ENTITY), [0.6, 0.2, 0.2])[0]
    if e not in COVERED and random.random() < 0.25:  # mid-period transfer
        c2 = random.choice([x for x in ENTITY if x != c])
        mv = dt.date(2023, random.randint(1, 12), 1)
        hris.append((e, dt.date(2021, 1, 1), mv - dt.timedelta(days=1), ENTITY[c], c))
        hris.append((e, mv, dt.date(9999, 12, 31), ENTITY[c2], c2))
    else:
        hris.append((e, dt.date(2021, 1, 1), dt.date(9999, 12, 31), ENTITY[c], c))

# --- Equity platform events ---
events, n, gmeta = [], 0, []
for e in EMPS:
    for _ in range(random.randint(2, 4)):
        n += 1
        gid = f"G{n:05d}"
        gd = dt.date(random.randint(2021, 2024), random.randint(1, 12), 15)
        typ = random.choices(["RSU", "NSO"], [0.7, 0.3])[0]
        sh = random.choice([320, 640, 1600, 3200]) * (50 if e in COVERED else 1)
        strike = fmv(gd) if typ == "NSO" else 0.0
        gfv = fmv(gd) if typ == "RSU" else round(0.4 * fmv(gd), 2)
        events.append((f"{gid}-GRANT", gid, e, typ, "GRANT", gd, sh, strike, gfv, fmv(gd)))
        per = sh // 16
        forfeit_after = 6 if random.random() < 0.05 else 16
        gmeta.append((gid, e, gd, sh, gfv, forfeit_after))
        for k in range(1, 17):
            vd = add_months(gd, 3 * k)
            if vd > AS_OF:
                break
            if k > forfeit_after:
                events.append((f"{gid}-FORFEIT", gid, e, typ, "FORFEIT", vd,
                               sh - per * forfeit_after, strike, gfv, fmv(vd)))
                break
            if typ == "RSU":
                events.append((f"{gid}-VEST-{k}", gid, e, typ, "VEST", vd, per, strike, gfv, fmv(vd)))
            else:
                events.append((f"{gid}-VEST-{k}", gid, e, typ, "VEST", vd, per, strike, gfv, fmv(vd)))
                xd = vd + dt.timedelta(days=180)
                if xd <= AS_OF:
                    events.append((f"{gid}-EXERCISE-{k}", gid, e, typ, "EXERCISE", xd, per, strike, gfv, fmv(xd)))
 
events += random.sample(events, 2)                  # inject 2 duplicates (DQ test)

# --- GL: straight-line quarterly expense per grant, booked to the entity employing the person that quarter ---
def ent_at(emp, d):
    for (e_, s_, en_, ent_, c_) in hris:
        if e_ == emp and s_ <= d <= en_:
            return ent_

gl = {}
for gid, emp, gd, sh, gfv, fa in gmeta:
    for k in range(1, fa + 1):
        qd = add_months(gd, 3 * k)
        if qd > AS_OF:
            break
        key = (ent_at(emp, qd), qd.year)
        gl[key] = gl.get(key, 0.0) + sh * gfv / 16
gl_rows = [(ent, yr, round(v, 2)) for (ent, yr), v in sorted(gl.items())]
gl_rows[0] = (gl_rows[0][0], gl_rows[0][1], gl_rows[0][2] + 12000.0)   # inject 1 GL break

covered_rows = [(e, 2021) for e in COVERED]

# --- Payroll (wages reported on taxable events) ---
payroll = []
for ev in events:
    eid, gid, emp, typ, et, d, sh, strike, gfv, px = ev
    if (typ == "RSU" and et == "VEST") or (typ == "NSO" and et == "EXERCISE"):
        payroll.append([eid, emp, d, round(sh * (px - strike), 2)])
payroll = list({p[0]: p for p in payroll}.values())  # one payroll row per event
for i in random.sample(range(len(payroll)), 3):      # inject 3 breaks
    payroll[i][3] += 250.0
for i in sorted(random.sample(range(len(payroll)), 2), reverse=True):  # 2 missing
    payroll.pop(i)
payroll = [tuple(p) for p in payroll]

rates = [("US", 0.25, "2025.1"), ("UK", 0.25, "2025.1"), ("IN", 0.2517, "2025.1")]

# COMMAND ----------
log = {}
log["hris"]    = write_bronze(hris, "employee_id string, effective_start date, effective_end date, legal_entity string, country string", "bronze_hris", "synthetic_hris")
log["equity"]  = write_bronze(events, "event_id string, grant_id string, employee_id string, award_type string, event_type string, event_date date, shares int, strike_price double, grant_date_fv double, fmv_at_event double", "bronze_equity_events", "synthetic_equity_platform")
log["payroll"] = write_bronze(payroll, "event_id string, employee_id string, pay_date date, wages_reported double", "bronze_payroll", "synthetic_payroll")
log["gl"]      = write_bronze(gl_rows, "legal_entity string, tax_year int, book_expense_gl double", "bronze_gl_sbc", "synthetic_gl")
log["covered"] = write_bronze(covered_rows, "employee_id string, covered_since int", "bronze_covered_employees", "synthetic_legal_list")
spark.createDataFrame(rates, "country string, tax_rate double, rate_version string") \
     .write.mode("overwrite").saveAsTable("config_tax_rates")
print(log)

# COMMAND ----------
# MAGIC %md ## 3. Silver: dedup, taxable events, as-of join to HRIS, DQ

# COMMAND ----------
b = lambda t: spark.table(t).filter(F.col("_batch_id") == BATCH_ID)

ev = b("bronze_equity_events").withColumn(
    "rn", F.row_number().over(Window.partitionBy("event_id").orderBy("_ingest_ts")))
log_dq(ev.filter("rn > 1").select(F.lit("DUPLICATE_EVENT").alias("check"),
                                  F.col("event_id").alias("key"),
                                  F.lit("duplicate event_id in bronze; first row kept").alias("detail")))
ev = ev.filter("rn = 1").drop("rn")

taxable = ev.filter(((F.col("award_type") == "RSU") & (F.col("event_type") == "VEST")) |
                    ((F.col("award_type") == "NSO") & (F.col("event_type") == "EXERCISE")))

grant_dates = ev.filter("event_type = 'GRANT'").select("grant_id", F.col("event_date").alias("grant_date"))
taxable = taxable.join(grant_dates, "grant_id", "left")

h = b("bronze_hris").select("employee_id", "effective_start", "effective_end", "legal_entity", "country")
j = (taxable.alias("e")
     .join(h.alias("h"), (F.col("e.employee_id") == F.col("h.employee_id")) &
                         F.col("e.event_date").between(F.col("h.effective_start"), F.col("h.effective_end")), "left")
     .select("e.*", "h.legal_entity", "h.country", "h.effective_start"))

log_dq(j.filter(F.col("legal_entity").isNull())
        .select(F.lit("NO_HRIS_MATCH").alias("check"), F.col("event_id").alias("key"),
                F.lit("no effective-dated HRIS row on event date").alias("detail")))

silver = (j.filter(F.col("legal_entity").isNotNull())
          .withColumn("tax_income", F.round(F.col("shares") * (F.col("fmv_at_event") - F.col("strike_price")), 2))
          .withColumn("book_expense_recognized", F.round(F.col("shares") * F.col("grant_date_fv"), 2))
          .withColumnRenamed("effective_start", "hris_effective_start"))
silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_taxable_events")

# COMMAND ----------
# MAGIC %md ## 4. Gold: SBC detail (row-level lineage), entity/year summary, payroll recon

# COMMAND ----------
s = spark.table("silver_taxable_events")
r = spark.table("config_tax_rates")

detail = (s.join(r, "country", "left")
          .withColumn("tax_year", F.year("event_date"))
          .withColumn("tax_deduction", F.col("tax_income"))
          .withColumn("book_tax_difference", F.round(F.col("tax_deduction") - F.col("book_expense_recognized"), 2))
          .withColumn("excess_tax_effect", F.round(F.col("book_tax_difference") * F.col("tax_rate"), 2))  # +windfall / -shortfall
          .withColumn("logic_version", F.lit(LOGIC_VERSION)))
detail.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_sbc_detail")

(spark.table("gold_sbc_detail")
 .groupBy("legal_entity", "country", "tax_year")
 .agg(F.count("*").alias("events"), F.sum("shares").alias("shares"),
      F.round(F.sum("tax_deduction"), 2).alias("tax_deduction"),
      F.round(F.sum("book_expense_recognized"), 2).alias("book_expense"),
      F.round(F.sum("book_tax_difference"), 2).alias("book_tax_difference"),
      F.round(F.sum("excess_tax_effect"), 2).alias("excess_tax_effect"))
 .withColumn("_batch_id", F.lit(BATCH_ID)).withColumn("logic_version", F.lit(LOGIC_VERSION))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_sbc_by_entity_year"))

p = b("bronze_payroll").select("event_id", "wages_reported")
recon = (s.select("event_id", "employee_id", "legal_entity", "tax_income")
         .join(p, "event_id", "left")
         .withColumn("diff", F.round(F.col("tax_income") - F.coalesce(F.col("wages_reported"), F.lit(0.0)), 2))
         .withColumn("status", F.when(F.col("wages_reported").isNull(), "MISSING_PAYROLL")
                                .when(F.abs("diff") > TOL, "BREAK").otherwise("OK")))
recon.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_recon_payroll")
log_dq(recon.filter("status != 'OK'").select(
    F.concat(F.lit("PAYROLL_"), F.col("status")).alias("check"), F.col("event_id").alias("key"),
    F.concat(F.lit("computed="), F.col("tax_income").cast("string"),
             F.lit(" payroll="), F.col("wages_reported").cast("string")).alias("detail")))

# COMMAND ----------
# MAGIC %md ## 4b. GL tie-out, income sourcing across the vesting period, §162(m)

# COMMAND ----------
# --- GL tie-out: independently recompute straight-line book expense from equity events, compare to GL ---
grants = ev.filter("event_type = 'GRANT'").select(
    "grant_id", "employee_id", "shares", "grant_date_fv", F.col("event_date").alias("grant_date"))
forf = ev.filter("event_type = 'FORFEIT'").select("grant_id", F.col("event_date").alias("forfeit_date"))
q = (grants.join(forf, "grant_id", "left")
     .withColumn("n_q", F.when(F.col("forfeit_date").isNotNull(),
                               F.round(F.months_between("forfeit_date", "grant_date") / 3).cast("int") - 1)
                         .otherwise(F.lit(16)))
     .withColumn("k", F.explode(F.sequence(F.lit(1), F.col("n_q"))))
     .withColumn("q_date", F.expr("add_months(grant_date, 3 * k)"))
     .filter(F.col("q_date") <= F.lit(AS_OF))
     .withColumn("q_expense", F.col("shares") * F.col("grant_date_fv") / 16))
recomputed = (q.alias("q")
              .join(h.alias("h"), (F.col("q.employee_id") == F.col("h.employee_id")) &
                                  F.col("q.q_date").between(F.col("h.effective_start"), F.col("h.effective_end")))
              .groupBy(F.col("h.legal_entity").alias("legal_entity"), F.year("q.q_date").alias("tax_year"))
              .agg(F.round(F.sum("q.q_expense"), 2).alias("book_expense_recomputed")))
gl_book = b("bronze_gl_sbc").select("legal_entity", "tax_year", "book_expense_gl")
glr = (recomputed.join(gl_book, ["legal_entity", "tax_year"], "full")
       .withColumn("diff", F.round(F.coalesce(F.col("book_expense_gl"), F.lit(0.0)) -
                                   F.coalesce(F.col("book_expense_recomputed"), F.lit(0.0)), 2))
       .withColumn("status", F.when(F.abs("diff") > TOL, "BREAK").otherwise("OK")))
glr.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_recon_gl")
log_dq(glr.filter("status != 'OK'").select(
    F.lit("GL_BREAK").alias("check"),
    F.concat_ws("-", F.col("legal_entity"), F.col("tax_year").cast("string")).alias("key"),
    F.concat(F.lit("gl="), F.col("book_expense_gl").cast("string"),
             F.lit(" recomputed="), F.col("book_expense_recomputed").cast("string")).alias("detail")))

# COMMAND ----------
# --- Sourcing: allocate each event's income across jurisdictions by days worked from grant date to event date ---
hh = h.select(F.col("employee_id").alias("h_emp"), F.col("effective_start").alias("h_start"),
              F.col("effective_end").alias("h_end"), F.col("legal_entity").alias("src_entity"),
              F.col("country").alias("src_country"))
rates_src = r.select(F.col("country").alias("src_country"), F.col("tax_rate").alias("src_rate"))
src = (s.join(hh, (F.col("employee_id") == F.col("h_emp")) &
                  (F.col("h_start") <= F.col("event_date")) & (F.col("h_end") >= F.col("grant_date")))
       .withColumn("days", F.datediff(F.least(F.col("h_end"), F.col("event_date")),
                                      F.greatest(F.col("h_start"), F.col("grant_date"))) + 1)
       .withColumn("source_pct", F.col("days") / F.sum("days").over(Window.partitionBy("event_id")))
       .join(rates_src, "src_country", "left")
       .withColumn("tax_year", F.year("event_date"))
       .withColumn("sourced_deduction", F.round(F.col("tax_income") * F.col("source_pct"), 2))
       .withColumn("sourced_book", F.round(F.col("book_expense_recognized") * F.col("source_pct"), 2))
       .withColumn("sourced_excess_tax_effect",
                   F.round((F.col("sourced_deduction") - F.col("sourced_book")) * F.col("src_rate"), 2))
       .select("event_id", "grant_id", "employee_id", "tax_year", "src_entity", "src_country", "days",
               "source_pct", "sourced_deduction", "sourced_book", "src_rate", "sourced_excess_tax_effect"))
src.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_sbc_sourced")

# COMMAND ----------
# --- §162(m): cap SBC deduction for covered employees (US) per employee-year ---
US_RATE = r.filter("country = 'US'").first()["tax_rate"]
m162 = (spark.table("gold_sbc_detail").filter("country = 'US'")
        .join(b("bronze_covered_employees").select("employee_id", "covered_since"), "employee_id")
        .filter(F.col("tax_year") >= F.col("covered_since"))
        .groupBy("employee_id", "tax_year")
        .agg(F.round(F.sum("tax_deduction"), 2).alias("sbc_deduction"))
        .withColumn("cap", F.lit(CAP_162M))
        .withColumn("disallowed", F.round(F.greatest(F.lit(0.0), F.col("sbc_deduction") - F.col("cap")), 2))
        .withColumn("tax_cost", F.round(F.col("disallowed") * F.lit(US_RATE), 2)))
m162.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_162m")

# COMMAND ----------
# MAGIC %md ## 5. Audit: run log with row counts, input checksums, and Delta versions

# COMMAND ----------
tables = ["bronze_equity_events", "bronze_hris", "bronze_payroll", "silver_taxable_events",
          "gold_sbc_detail", "gold_sbc_by_entity_year", "gold_recon_payroll",
          "bronze_gl_sbc", "bronze_covered_employees", "gold_recon_gl", "gold_sbc_sourced", "gold_162m"]
run = [(BATCH_ID, dt.datetime.utcnow(), LOGIC_VERSION, TOL, AS_OF,
        json.dumps({k: {"rows": v[0], "checksum": v[1]} for k, v in log.items()}),
        json.dumps({t: table_version(t) for t in tables}),
        spark.table("audit_dq_exceptions").filter(F.col("_batch_id") == BATCH_ID).count())]
spark.createDataFrame(run, "batch_id string, run_ts timestamp, logic_version string, tolerance double, as_of date, "
                           "input_stats string, table_versions string, dq_exceptions long") \
     .write.mode("append").saveAsTable("audit_run_log")

# COMMAND ----------
# MAGIC %md ## 6. Review

# COMMAND ----------
display(spark.table("gold_sbc_by_entity_year").orderBy("legal_entity", "tax_year"))

# COMMAND ----------
display(spark.table("audit_dq_exceptions").filter(F.col("_batch_id") == BATCH_ID))

# COMMAND ----------
# Drill-down: any summary number -> grant-level rows
display(spark.sql("""
  SELECT * FROM gold_sbc_detail
  WHERE legal_entity = 'ACME_US' AND tax_year = 2024
  ORDER BY ABS(book_tax_difference) DESC LIMIT 50"""))

# COMMAND ----------
# Reproduce a prior result: look up table_versions in audit_run_log, then e.g.
#   SELECT * FROM gold_sbc_by_entity_year VERSION AS OF <n>
display(spark.sql("SELECT batch_id, run_ts, logic_version, dq_exceptions, table_versions FROM audit_run_log ORDER BY run_ts DESC"))

# COMMAND ----------
# MAGIC %md ## 7. Dashboard datasets (views for an AI/BI dashboard; always show the latest batch)

# COMMAND ----------
LATEST = "(SELECT batch_id FROM audit_run_log ORDER BY run_ts DESC LIMIT 1)"
views = {
 "v_dash_kpis": f"""
   SELECT (SELECT SUM(tax_deduction) FROM gold_sbc_by_entity_year)      AS tax_deduction,
          (SELECT SUM(book_expense) FROM gold_sbc_by_entity_year)       AS book_expense,
          (SELECT SUM(excess_tax_effect) FROM gold_sbc_by_entity_year)  AS excess_tax_effect,
          (SELECT COUNT(*) FROM audit_dq_exceptions WHERE _batch_id = {LATEST}) AS open_exceptions,
          (SELECT COALESCE(SUM(tax_cost), 0) FROM gold_162m)            AS sec162m_tax_cost""",
 "v_dash_excess_by_entity": "SELECT legal_entity, country, tax_year, tax_deduction, book_expense, book_tax_difference, excess_tax_effect FROM gold_sbc_by_entity_year",
 "v_dash_controls": """
   SELECT 'Payroll tie-out' AS control, status, COUNT(*) AS items FROM gold_recon_payroll GROUP BY status
   UNION ALL
   SELECT 'GL tie-out', status, COUNT(*) FROM gold_recon_gl GROUP BY status""",
 "v_dash_exceptions": f"""
   SELECT `check`, `key`, detail, _ts FROM audit_dq_exceptions WHERE _batch_id = {LATEST}""",
 "v_dash_sourcing": """
   SELECT src_country, tax_year, ROUND(SUM(sourced_deduction), 2) AS sourced_deduction,
          ROUND(SUM(sourced_excess_tax_effect), 2) AS sourced_excess_tax_effect
   FROM gold_sbc_sourced GROUP BY src_country, tax_year""",
 "v_dash_162m": "SELECT employee_id, tax_year, sbc_deduction, cap, disallowed, tax_cost FROM gold_162m",
 "v_dash_runs": "SELECT batch_id, run_ts, logic_version, dq_exceptions FROM audit_run_log",
}
for name, sql in views.items():
    spark.sql(f"CREATE OR REPLACE VIEW {name} AS {sql}")
print("views created:", list(views))
