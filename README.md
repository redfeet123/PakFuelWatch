# PakFuelWatch

PakFuelWatch is a modern lakehouse pipeline tracking Pakistan fuel prices and macroeconomic cost-of-living drivers.

The pipeline ingests multi-source data across fuel pricing, currency exchange rates (FX), global crude benchmarks (Brent), and consumer/sensitive price indices (CPI/SPI), orchestrating bronze, silver, and gold tiers using Dagster, MinIO, Delta Lake, and DuckDB/dbt.
