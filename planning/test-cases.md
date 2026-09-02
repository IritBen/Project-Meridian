Interfaces:

- just up                                                     # stand the stack up. No inputs, ever.
- just down                                                   # tear it down.
- just run     <layer> <job> <window>                         # do the work
- just inspect <layer> <job> <window>                         # what does that layer hold there?
- just report  daily-station-trips <market> <station> <day>   # the business question

Test Cases:

*Just Up*
- postgress is up and available in an agreed upon DSN
- check that there is a volumn named bronze-data

*Just Down*

- the postgress container that was created was up and down
- the volume is deleted

*Just Run*

- bronze:
    - successful - exit code 0 (no errors)
    - idempotency - run the just run ingest-to-bronze trips:nyc 2024-01 --> just inspect on the same job and window --> just run ingest-to-bronze trips:nyc 2024-01 --> just inspect again on the same job and window, and check that the first inspect and the second inspect return the same amounts
    - right amount of rows: 2 use cases:
        - trips:nyc 2024-01 - 1888085 (standard)
        - trips:nyc 2018-04 - 1307543 (multiple files for the same window)
        - trips:jc 2021-02 - 4881 (standard after schema changed)
        - trips:jc 2021-01 - 11624 (standard before schema changed)
        - run trips:nyc 2018-04, inspect trips:nyc 2018-04, run trips:jc 2021-02, insect trips:nyc 2018-04 (stays the same)

- silver:
    - successful - exit code 0 (no errors)
    - number of rejects and rows - trips:nyc 2024-01 -> 1888085 total, 1881977 conform, 6108 rejected
    - rows amount is preserved from bronze to silver - to-bronze trips:jc 2021-02 -> inspect bronze trips:jc 2021-02 -> silver trips:jc 2021-02 -> inspect silver trips:jc 2021-02 -> silver(rejected + rows) = bronze(rows)
    - to-bronze trips:jc 2021-02 -> to-silver trips:jc 2021-02 -> inspect silver trips:jc 2021-02 -> to-silver trips:jc 2021-02 -> inspect silver trips:jc 2021-02 - the two inspects should have the same amount

- gold:
    - successful - exit code 0 (no errors)
    - number of rows - (first, bronze and silver) transform-to-gold station-daily 2026-06-02 -> report daily-station-trips jc JC115 2026-06-02 ->  "departures":216,"arrivals":209
    - idempotency - (first, bronze and silver) transform-to-gold station-daily 2026-06-02 -> report daily-station-trips jc JC115 2026-06-02 ->  "departures":216,"arrivals":209 -> transform-to-gold station-daily 2026-06-02 -> report daily-station-trips jc JC115 2026-06-02 ->  "departures":216,"arrivals":209