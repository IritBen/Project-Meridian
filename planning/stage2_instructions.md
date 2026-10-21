# Stage 2 — Survive the Night

Stage 1 answers correctly — when somebody types three commands. Nobody at Meridian will. The vendor publishes
each month a few days into the next one, unannounced, and operations wants the history since 2021 loaded and
every new month in the warehouse without anyone watching.

This stage adds the **operational layer**, on **Airflow 3**: it schedules, waits, runs, retries, keeps the books
on what was loaded, and tells you how far the warehouse has got.

```{important}
**Stage 1's jobs stay exactly as they are — not one line changes.** `just run`, `just inspect` and `just report`
keep working, by hand, with Airflow switched off. Everything in this stage lives *around* the jobs and calls them.
```

---

## What you add

**1. A DAG per market.** One run processes one month of one market.

**2. An earliest window per market** — where its history starts. Committed configuration; the format is yours,
the values are ours: **`jc 2021-01`**, **`nyc 2026-01`**.

**3. A control table**, in your warehouse, written by the **operational layer, never by a job**: after a job
succeeds, the task records which job loaded which window, and when. Record **only after** the job has
succeeded — never before — and make recording the same load twice harmless. Hand runs are not recorded.

**4. A progress function** — computed from the control table whenever it is asked, never stored:

- a month is **complete** when its silver load is recorded, and a gold load is recorded for **every day** of the
  month **after** that silver load;
- the **watermark** is the last month of the unbroken run of complete months starting at the earliest window
  (`null` if the earliest window is not complete);
- a **gap** is an incomplete month between the earliest window and the newest complete month;
- the **next month due** is the earliest incomplete month from the earliest window on.

**5. The recipes.**

```
just up                                       # also starts Airflow, with every schedule OFF
just schedule     <market> <on|off>
just run pipeline <market>                    # the next month due
just run pipeline <market> <month>            # that month, even if already complete
just run pipeline <market> <month> <month>    # every month in the range, as an Airflow backfill
just inspect coverage <job>
just inspect runs     <market>
```

`run pipeline` blocks until its runs finish, and exits non-zero if any failed.

```
just inspect coverage trips:jc
 -> {"job":"trips:jc","earliest":"2021-01","watermark":"2021-02","complete":3,"gaps":["2021-03"],"next":"2021-03"}

just inspect runs jc          # newest run first; types and states are Airflow's own words
 -> {"market":"jc","runs":[{"month":"2026-06","type":"manual","state":"success","tasks":{
      "ingest-to-bronze trips:jc 2026-06":    {"state":"success","tries":1},
      "transform-to-silver trips:jc 2026-06": {"state":"success","tries":2},
      "transform-to-gold station-daily":      {"state":"success","tries":1,"days":30,"failed_days":[]}}}]}
```

---

## How the DAG should look

```{mermaid}
flowchart LR
  win["which month?<br/>is it due?"] --> wait["wait until<br/>published"] --> ing["ingest-to-bronze"] --> sil["transform-to-silver"] --> gold["gold batch:<br/>station-daily × every day"]
```

| Task | Runs how | Why |
|---|---|---|
| ingest-to-bronze | **in-process** — import your package, call the job | I/O |
| transform-to-silver | **subprocess:** `just run transform-to-silver <job> <month>` | CPU — keep it off Airflow's workers |
| gold batch | **in-process, one task per month** — call stage 1's day job for each day, recording each day as it succeeds | I/O, and a day is too small to be a task |

Hints:

- **Task bodies call code.** The day loop, the recording and the progress function live in your package; the DAG
  file only wires calls together.
- **The subprocess needs glue:** `just` and Docker Compose inside Airflow, access to the Docker daemon, and the
  same compose project as `just up`, so it sees the same bronze volume and warehouse.
- **Each scheduled run must be about exactly one calendar month.** A bare cron string on Airflow 3 gives an
  *empty* data interval — use an interval timetable, or derive the month yourself.
- **Wait without holding a worker** (a reschedule-mode sensor), with a timeout that fails the run.
- **Months are independent** — let several run at once. Airflow's default concurrency is far below what a laptop
  can do.

---

## Definition of done

**Stage 1 is untouched**

1. With Airflow stopped, stage 1's commands still give stage 1's answers — and `run pipeline jc 2026-06` gives
   byte-identical answers (216 departures, 209 arrivals at JC115 on 2026-06-02).
2. Stage 1's checks still pass.

**Running**

3. A run has three job tasks; the gold batch reports `days: 30` for 2026-06 and `28` for 2021-02.
4. `run pipeline jc 2021-01 2021-02` succeeds across the schema change; both runs are `type: backfill`.
5. On a fresh stack, `schedule jc on` fills **68 complete months, no gaps, watermark 2026-08** within **30
   minutes**, as `type: scheduled` runs.
6. A scheduled run for a month already complete does nothing: `success`, work `skipped`.

**Progress**

7. Loading 2021-01 entirely by hand leaves coverage at nothing complete; `run pipeline jc` then takes 2021-01.
8. After 2021-01, 2021-02 and 2021-04 are complete: watermark `2021-02`, gaps `["2021-03"]`. Windowless runs take
   **2021-03**, then **2021-05**.
9. Re-running a complete month by name changes no answer and no coverage.
10. With nothing due (the next month is unpublished), `run pipeline jc` exits 0: `month: null`, work `skipped`.
11. A named re-run killed after silver and before gold finishes makes that month incomplete again, and it becomes
    `next`.

**Failure**

12. A month before the earliest window (`run pipeline jc 2019-06`) fails at once: `failed`, `tries: 1`,
    downstream `upstream_failed`, nothing written. A month after the newest published one waits.
13. A silver container killed mid-work is retried: the run succeeds with `tries ≥ 2`.
14. When a gold day fails, the batch finishes the other days, records them, then fails naming the failed days; the
    retry redoes only those. While gold rebuilds, `report` always returns a complete answer for every day.
15. The same month fired twice at once gives exactly what one run gives.
