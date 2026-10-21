# Stage 1 — The Shape of the Job

You are joining the data platform team at **Meridian**, a bike-share operator running seven city
markets. Nobody here can currently answer *"how many trips started at Grove St PATH on the second of
June?"* without someone opening a spreadsheet. Your job, over this term, is to make that — and much
harder questions — answerable from a warehouse you build. This stage is the first of them.

**You write all of it.** No starter repo, no scaffold, no skeleton, no copy of our tests. Run `git init`
in an empty directory and build the thing. We do not name your tables, your schema, your modules, your
files, your packages or your libraries, and we will never read your source to grade you. The stack is
yours too — Python and Postgres, in containers you define, brought up by `just up`.

What follows is the specification: the source you will read, the question you must answer, the layers
that get you there, and the interface we will call.

---

## 1. The source

**No API. No auth.** `GET https://s3.amazonaws.com/tripdata/` returns an S3 XML listing of every object
in the bucket — 173 of them when this was written, and one more every month. One unauthenticated
request gives you the whole catalogue. The files are zipped CSV.

**Start by looking at it, not by writing a pipeline.** There is a browsable view of the same objects at
<https://s3.amazonaws.com/tripdata/index.html>. Scroll it, open the 2019 file and the 2026 file side by
side, and list what is inside the 2018 archive *before* you decide what to load. Every surprise below is
visible in fifteen minutes of looking and invisible forever if you skip it. That page is a **reading
aid, not an interface** — your pipeline reads the XML listing, and must never scrape it.

Two markets share this one bucket: **JC** (Jersey City) and **New York**, same operator — though you
will only ever see one of them named. `tripdata` *is* the New York bucket, so New York's files carry no
market token at all (`2018-citibike-tripdata.zip`) while Jersey City, added later, got the prefix
because it was the exception: **131 of the 173 keys start with `JC-`, and not one key anywhere contains
"nyc" or "new york."** Selecting New York means matching the *absence* of the other market's prefix.
Count them yourself and the totals will have moved — a new month lands every month — but the zero will
not, and the zero is the part your code depends on.

None of what follows is invented; all of it is currently true.

### Three things the archive does to you

- **The extension drifts.** `JC-202604-citibike-tripdata.zip` sits directly beside
  `JC-202605-citibike-tripdata.csv.zip`. You cannot construct these URLs. List, and parse the listing.
- **Each market changed schema at its own moment.** New York switched to the 13-column standard at
  **2020-01**; JC — same bucket, same operator — switched at **2021-02**, thirteen months later. The old
  schema spells its columns `start station id`, with spaces; the new one `start_station_id`. A single
  global cutover date is wrong by construction.
- **The request grain is not the file grain, and one archive lies.** New York publishes yearly zips
  through 2023 and monthly only from 2024-01, so one month of 2018 means opening a 1.34 GB file and
  taking April out. April is in there **three times over**: an export of the whole month made in 2018,
  and a re-export made in 2024 split across two files — which is itself stored twice, once beside the
  original and once under `4_April/`. Load every `.csv` and you count **3,922,629** rows where there
  are **1,307,543** trips.

  Your instinct will be to load all of it and sort the duplicates out later. **You cannot**, and the
  reason is worth more than the fix. Every copy carries the same 1,307,543 trips, so the *set* is not
  in question — but they are not the same bytes. The 2024 re-export rendered some latitudes one
  floating-point step away from the 2018 original: `40.742753828659026` against `40.74275382865903`,
  the same place to within a nanometre. Match rows against each other and you leave **1,359,703** —
  wrong by 52,160, and far more plausible-looking than being wrong threefold. There is no comparison
  between rows that recognises those two lines as one trip.

  **So do not merge the exports. Choose one, and take it whole.** Every member of a zip carries its
  own timestamp, and the archive therefore tells you which export is which: 2018-09-06 for the
  original, 2024-02-21 for the re-export. Each folder in the archive is one export run; the newest run
  is the one the publisher currently stands behind, and `4_April/` is it. Load exactly that and April
  lands **1,307,543 rows** — one export, no de-duplication, nothing thrown away that the publisher
  still claims.

  Last-writer-wins, ordered by something the source actually gives you, is the rule you want here and
  the one you will want again the first time a publisher restates a month you have already loaded.
  Watch the timestamps rather than the dates, though: the 2024 re-export is stored twice and **both
  copies are dated 2024-02-21**, so "everything from the newest day" lands that run twice and gives
  you 2,615,086.

---

## 2. The question, and what counts as a trip

Meridian's question is **departures and arrivals, per station, per day**. Everything below follows from
it.

A departure is counted on the day the trip **started**; an arrival on the day it **ended**. Grove St
PATH — station `JC115` — is the commuter gate to Manhattan and the busiest dock in the market. On
2026-06-02 it lets out seven more bikes than it takes in. A day of that is noise. Fifteen years of it,
across a market, is the rebalancing problem this whole project exists to solve.

### What counts as a trip

A row is a trip you can count when the row itself says all four of:

- **where it started** — a start station identifier
- **where it ended** — an end station identifier
- **when it started**, and **when it ended**

If any of those is missing, the trip cannot be placed at a station on a day. It is **quarantined**: kept
somewhere you can query it, with a reason, and never deleted. Deleting it puts you back to having thrown
away information you cannot get back.

That rule is ours, not yours to guess. Three things that look like validity questions and are not:

- **A missing station *name*.** The name is a label; the identifier is the claim about where the bike
  was. 350 rows in June 2026 have no `end_station_name` and are perfectly countable.
- **An identifier that resolves to no station you have heard of.** That is a **referential** question,
  and referential questions need a dimension table. There is no station feed in this stage. The only
  list you could infer is the one inside the trips themselves — and 145 of June 2026's end stations
  never appear as anyone's start station, simply because nobody happened to begin a trip there.
  Quarantine on "not in my inferred list" and you throw away **419 good trips, more than the 387 you
  were trying to catch**.
- **A suspicious duration.** The publisher already removed every trip under 60 seconds and every staff
  trip before you saw the file. This stage asks no duration question.

**The rule is ours; the label is yours.** How you name a reason is your call, and naming it is part of
the work — a quarantine you cannot explain is a quarantine nobody will trust. Prefer a business
condition ("never docked") to a code path ("null_check_failed").

---

## 3. The three layers

| Layer | Holds | May it be rebuilt? |
|---|---|---|
| **bronze** | the raw published bytes plus provenance — what the source currently says, and when you heard it | **No.** It is the record. |
| **silver** | one conformed, typed, de-duplicated row per real trip | Yes, from bronze. |
| **gold** | the answer to a question somebody actually asks | Yes, from silver. |

The rule that generates them: **never throw away information you cannot get back.** The vendor rewrites
its own archive — on 2025-07-03 it re-published seventeen objects, including every month from 2024-01
to 2025-05 — and bronze is what protects you from that. Bronze holds the published bytes exactly as
published: untyped, unconformed, un-cleaned. It does not decide what a row *means*; that is silver's
job, and keeping the bytes is what lets you change your mind about the meaning later.

Note where the boundary falls. Choosing *which files are this window* — the April question above — is
ingestion's decision, made before a row exists. Deciding what a row means, and whether two rows are the
same trip, is silver's. Bronze lands one export of a window, whole.

---

## 4. The interface we will call

The only thing we mandate — because it is the whole of an orchestrator's task interface, arrived at
before you meet an orchestrator.

```
just up                                                     # stand the stack up. No inputs, ever.
just down                                                   # tear it down.

just run     <layer> <job> <window>                         # do the work
just inspect <layer> <job> <window>                         # what does that layer hold there?
just report  daily-station-trips <market> <station> <day>   # the business question
```

That is the whole of it. Five recipes, three of them taking arguments, and every argument below is
**positional and required** — there are no flags, no defaults and no optional slots anywhere in this
interface.

```
layer     run     -> ingest-to-bronze | transform-to-silver | transform-to-gold
          inspect -> bronze | silver     (there is no `inspect gold`)
job       bronze/silver -> a SOURCE, market-qualified:  trips:jc  trips:nyc
          gold          -> a FACT, named for its grain: station-daily
window    exactly one ISO-8601 literal, always required, at the job's own grain:
          trips publish monthly       -> trips:jc      takes a MONTH
          the fact is per station-day -> station-daily takes a DAY

market    the market token, lower case:  jc | nyc
station   the source's own station identifier, exactly as published:  JC115
day       one ISO-8601 day:  2026-06-02
```

`run` and `inspect` take the same triple, so you always inspect a layer at the coordinate you gave the
job that filled it. There is no `inspect gold` because gold exists only to answer a question — the only
way to look at it is to ask one, and at this stage there is exactly one question to ask.
**`daily-station-trips` is a literal**: it is the question's name, not a placeholder for you to fill in,
and it is the only value `report` accepts here. Later stages add questions beside it; none of them
change a job.

Standing up, answering, tearing down — the entire stage, end to end:

```
just up
just run ingest-to-bronze    trips:jc      2026-06
just run transform-to-silver trips:jc      2026-06
just run transform-to-gold   station-daily 2026-06-02
just report daily-station-trips jc JC115 2026-06-02
just down
```

### The three rules

1. **One job per responsibility.** Per source: one ingestion job, one cleaning job, and at least one
   business job — each **independently runnable**. Not three functions called in a row from one
   `main()`; three entry points. A scheduler can only orchestrate parts that are separable, and we will
   test this by running one of them alone.

2. **Only `ingest-to-bronze` may touch the network.** Everything downstream reads what is already
   landed. We test this by cutting your containers' **route to the internet** and running your silver
   job. Your own stack stays reachable — your database is still there — so the only thing that breaks
   is a transform that tried to go back to the source.

3. **The window's ISO-8601 truncation is its grain, and a job takes exactly its own grain.**
   `transform-to-silver trips:jc 2026-06-14` must **fail loudly and do nothing** — non-zero exit, the
   reason on stderr, and not one row written. `station-daily 2026-06` must refuse for the same reason,
   in the other direction. A wrong grain is a silent 30× error, not a typo; it exits zero and looks fine.

### Every answer is one JSON object

`inspect` and `report` each write **one JSON object to stdout and nothing else**. We diff those objects;
that is the whole of how you are graded, and it is why the shape is fixed even though nothing behind it
is.

```
just inspect bronze trips:jc 2026-06
   -> {"layer":"bronze","job":"trips:jc","window":"2026-06","objects":1,"rows":109897}

just inspect silver trips:jc 2026-06
   -> {"layer":"silver","job":"trips:jc","window":"2026-06","rows":109510,
       "rejects":387,"reasons":{"never docked":387}}

just report daily-station-trips jc JC115 2026-06-02
   -> {"market":"jc","station":"JC115","day":"2026-06-02","departures":216,"arrivals":209}
```

`reasons` maps *your* labels to counts, and they must account for the total. The keys shown above must
be present and correct. **Extra keys are fine** — an answer that also breaks departures down by member
type is a better answer, not a failing one.

---

## 5. Done, and what we check

Eleven checks, in seven groups, all asked through `just`. We never read your code. Every number is
**measured**, and you can verify each one yourself as you go; the private grader adds adversarial
coordinates you should have designed for anyway.

### The windows we publish

| coordinate | what it is | published | conformed |
|---|---|---:|---:|
| `trips:jc 2019-06` | the old schema, perfectly clean | 39,430 | 39,430 |
| `trips:jc 2026-06` | the new schema, with real dirt | 109,897 | 109,510 |
| `trips:nyc 2018-04` | inside a yearly archive, published three times | 1,307,543 | 1,307,543 |

Note the last row: **both of its numbers are checked, and they agree.** That is not a typo — it is the
result. The duplication was settled when ingestion chose which files were the window, so nothing is left
for the conform step to collapse. A project that skips the choice lands 3,922,629 in bronze and can
still reach the right conformed count, which is exactly why bronze is counted too.

### Correctness

1. **Conformance, and the clean control.** June 2026 conforms to 109,510 with 387 quarantined and
   reasons that account for them. June 2019 quarantines **zero** and conforms to all 39,430. A pipeline
   that quarantines nothing passes 2019 and fails 2026; one that over-quarantines fails the reverse. You
   cannot satisfy both by guessing.
2. **The break is per market.** JC's windows either side of its own 2021-02 migration both ingest, and
   the same answers come back over both; New York's April 2018 lands on the old schema in check 5.
   Detect the shape from the header and every window works. Hardcode a date and you are betting on
   which windows we picked.
3. **Windows are independent.** Land 2019-06, then 2026-06: the first is untouched, the second is added.
4. **Idempotency.** Any job run twice → every answer byte-identical.
5. **The vendor publishes 2018 three times.** Both layers land **1,307,543** — one export run, taken
   whole. Not 3,922,629 (every `.csv` loaded), not 2,615,086 (the newest *date*, which matches that run
   twice), and not 1,359,703 (the exports merged by comparing raw lines).
6. **The gold answer.** `report daily-station-trips jc JC115 2026-06-02` → 216 departures, 209 arrivals.

   ```{important}
   **216, not 217.** One of those 387 never-docked trips left JC115 that day. If you do not quarantine
   it, your gold answer is wrong by one and nothing else in your pipeline will tell you. The rule in §2
   is not paperwork — it reaches all the way to the business answer.
   ```

### Separability

7. **A job runs alone, transforms never fetch, and bronze is enough.** From a fresh stack, run **only**
   the ingest: `inspect bronze` answers, `inspect silver` reports zero rows, nothing crashes. Then **cut
   the route out** — your containers can still reach each other, so this is not a test of whether your
   stack survives — and run only the silver job. The trips must still appear, because everything they
   need is already landed. Still offline, rebuild silver and gold from scratch; both must reproduce
   completely. If they cannot, your bronze layer is not doing its job.

   Order must not matter: `trips:jc` and `trips:nyc` in either order give identical results, and a gold
   job run before its silver job produces no wrong answer — empty, or a clear failure, never silently
   partial. And the wrong grain must fail loudly and write nothing, verified by an `inspect silver` that
   is identical before and after.

---

> **What this stage is really teaching.** By the end you will have three jobs that each take a name and
> a window, run alone, and are safe to run twice. That is not a coincidence and it is not bureaucracy —
> it is exactly the shape a scheduler can drive.
>
> Notice what you were *not* asked for: no watermark, no "run the next window", no date ranges, no
> backfill. Those are properties of *operating* a pipeline, and this stage is about *implementing* the
> task. They arrive in the next stage, and if you have built this one right they will cost you nothing —
> your job already takes its window as an argument and is already idempotent over it, which is the
> entire trick. The discipline is the deliverable.
