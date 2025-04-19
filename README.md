# Monthly Data Loader

This directory contains the `monthly_data_loader.py` script which simulates monthly extraction of data from a public streaming source, saves the data as CSV files, and loads it into a Snowflake data warehouse.

## Features

- Fetches data monthly (simulated with placeholder data).
- Saves extracted data as timestamped CSV files.
- Loads data into Snowflake securely using environment variables for credentials.
- Tracks last run time to ensure monthly execution.

## Setup

1. Ensure Python 3.7+ is installed.
2. Install required packages:
   ```
   pip install -r ../requirements.txt
   ```
3. Set Snowflake credentials as environment variables:
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_ACCOUNT`
   - Optional: `SNOWFLAKE_WAREHOUSE`
   - Optional: `SNOWFLAKE_DATABASE`
   - Optional: `SNOWFLAKE_SCHEMA`
   - Optional: `SNOWFLAKE_ROLE`
4. Optionally set `DATA_SAVE_PATH` environment variable to specify where CSV files are saved (default is `./monthly_data`).

## Usage

Run the monthly data loader script:
```
python monthly_data_loader.py
```

## Notes

- Replace the placeholder data extraction function with actual streaming data extraction logic as needed.
- The script manages monthly execution by tracking the last run timestamp.

## License

MIT License
