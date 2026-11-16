# SBC Tax Data Pipeline on Databricks

A layered pipeline that turns fragmented stock-based-compensation data into an auditable, grant-level tax calculation.
Built on Databricks Free Edition (serverless, Unity Catalog, Delta). **All data is synthetic.**

## The problem
SBC tax data lives in four systems (equity platform, HRIS, payroll, GL) that arrive late and don't tie out.
Book expense (grant-date fair value, over the vesting period) and the tax deduction (value at vest/exercise) diverge,
so the provision needs grant-level detail, jurisdiction sourcing and traceability.

## Architecture
```
 Equity platform ─┐                                        ┌─ gold_sbc_detail (grant-level, lineage)
 HRIS (eff-dated) ─┼─> BRONZE (append-only, batch-stamped) ─> SILVER ─┼─ gold_sbc_by_entity_year
 Payroll ──────────┤       raw + _batch_id/_source/_ingest_ts   dedup, ├─ gold_sbc_sourced (days-worked allocation)
 GL ───────────────┤                                            as-of   ├─ gold_162m
 §162(m) list ─────┘                                            join   ├─ gold_recon_payroll / gold_recon_gl
                                                                       └─> audit_run_log, audit_dq_exceptions
                                                                            └─> v_dash_* views -> AI/BI dashboard
```

## Tax logic
| Item | Rule in this demo |
|---|---|
| Tax deduction | RSU: shares × FMV at vest. NSO: shares × (FMV − strike) at exercise |
| Book expense on vested shares | shares × grant-date fair value |
| Excess tax benefit / shortfall | (deduction − book) × jurisdiction rate (ASU 2016-09 style) |
| Sourcing | Each event's income allocated to jurisdictions by days worked from grant date to event date, using effective-dated HRIS |
| §162(m) | US covered employees: SBC deduction above $1M per employee-year is disallowed |
| GL book expense | Straight-line quarterly over 16 quarters, stops at forfeiture; booked to the entity employing the person that quarter |

## Controls (each is an evidence-producing, tolerance-based check)
| Control | Check | Evidence |
|---|---|---|
| Completeness of equity data | Duplicate `event_id` detection | `audit_dq_exceptions` |
| Entity mapping | Every taxable event matches an effective-dated HRIS row | `audit_dq_exceptions` |
| Income to payroll | Computed income vs wages reported, tolerance $1 | `gold_recon_payroll` |
| Book to GL | Independently recomputed expense vs GL by entity-year | `gold_recon_gl` |
| Reproducibility | Run log stores input row counts and checksums, logic version, and Delta version of every table | `audit_run_log` + `VERSION AS OF` |
| Traceability | Every summary drills to `event_id` / `grant_id` | `gold_sbc_detail` |

Seeded issues the controls should catch: 2 duplicate events, 3 payroll breaks, 2 missing payroll rows, 1 GL break.

## Run it
1. Databricks Free Edition: Workspace -> Import -> upload `sbc_pipeline.py` (arrives as a notebook).
2. Attach serverless compute, Run all. Tables and views are created in `workspace.sbc`.

## Build the dashboard (AI/BI dashboards, Free Edition)
Dashboards -> Create dashboard -> Data tab -> add each view below as a dataset (`SELECT * FROM workspace.sbc.<view>`), then add widgets:

| Widget | Dataset | Setup |
|---|---|---|
| KPI counters (5) | `v_dash_kpis` | one counter per column: deduction, book expense, excess tax effect, open exceptions, §162(m) tax cost |
| Bar: excess tax effect by entity and year | `v_dash_excess_by_entity` | x = tax_year, y = SUM(excess_tax_effect), color = legal_entity |
| Table: deduction vs book | `v_dash_excess_by_entity` | all columns |
| Stacked bar: sourced deduction by country | `v_dash_sourcing` | x = tax_year, y = sourced_deduction, color = src_country |
| Bar: control results | `v_dash_controls` | x = control, y = items, color = status (OK vs BREAK/MISSING) |
| Table: exceptions queue | `v_dash_exceptions` | check, key, detail |
| Table: §162(m) | `v_dash_162m` | filter disallowed > 0 |
| Filters | any | tax_year, legal_entity |

Layout: KPIs on top, entity/sourcing charts in the middle, controls and exceptions at the bottom.

## Known limitations (what production would add)
- Synthetic data and made-up rates; single currency (no FX).
- GL expense uses straight-line with no forfeiture reversal or graded attribution; no ESPP or PSU market conditions.
- §162(m) modelled on SBC only, with no other covered compensation.
- Sourcing is a simple days-worked split; no tax-treaty, state apportionment or withholding logic.
- Free Edition has no job scheduling or enterprise access control. Production: Workflows, row-level security, CI on logic changes, ticketing for exceptions.
