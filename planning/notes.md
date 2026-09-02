General
- ride id is not specified in JC files but it exists in NYC
- The same data might occur in different files with different last_modified, we need to take the last occurrence
- File names might not be consistent -> mostly .zip vs .csv.zip
- In NYC the files until 2024 are yearly but after 202401 monthly, the naming is consistent
- In some moment trough the history the schema of the input data changes with new columns or a bit different types for the same column for example floating-point depth


Modeling
- Final fact table will be exploded from trips, each trip will become 2 different events: departure and arrival (gold)
- We need a surrogate key for rides because not all the source markets have built in business key (silver)
- We want 2 dim tables: DimDate and DimStation (gold)


Conform (silver)
Valid row is:
- Has Start Time
- Has End Time
- Has Start Station ID
- Has End Station ID

Loading Techniques
- Always to overwrite on window grain (month, market)

Tech Stack & Infrastructure
- Python, Postgres, Dockerized
- On just up we need to initialize:
    - bronze data volume
    - postgres db up
- Container per job (ephemeral, dies when finishes)
- Postgres should have a consistent name
- Shared network between compute containers and db container
- One Image for compute containers
- Bronze in files (partitioned by market, year) in a persistent docker volume
- Silver in postgres (schema)
- Gold in postgres (schema)