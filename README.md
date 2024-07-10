# Ethereum Log Parser

## Acknowledgment

This project is based on [ethereum-scraping](https://github.com/bhemen/ethereum-scraping/) repo. Credit goes to [bhemen](https://github.com/bhemen).

## Abstract

This project is designed to efficiently parse Ethereum logs into events using the ABI (Application Binary Interface) of the contract being scrapped. The primary source of these logs is [Google BigQuery](https://cloud.google.com/bigquery), thanks to its partnership with [Nansen](https://www.nansen.ai/), allowing for rapid retrieval of logs associated with specific smart contracts.

## Why Google BigQuery?

While public nodes like Infura and Alchemy offer access to Ethereum logs, they might require subscriptions for extensive data queries and can be rate-limited. Running a personal Ethereum node, although free of subscription costs, is bound by hardware limitations and might not offer the same speed as commercial services.

Google BigQuery, in collaboration with [Nansen](https://docs.nansen.ai/database-access/getting-started-database), provides a swift and efficient way to access all Ethereum logs. Using just the smart contract address, you can download all associated logs within a matter of minutes or seconds, ready to be parsed locally. Most importantly, BigQuery credit each new user $300, which is more than enough for casual usage.

## Repository Structure

This repository contains scripts for processing Ethereum contract logs. The main scripts for general purposes are `preprocess_jsonlogs.py` and `parse_allevents.py`, with additional variants for specific scenarios:

1. **[preprocess_jsonlogs.py](scripts/preprocess_jsonlogs.py):**
   - Consolidates multiple JSON logs into a single DataFrame and adds event names.

2. **[parse_allevents.py](scripts/parse_allevents.py):**
   - Processes and decodes logs into separate CSV files for each event type.

3. **[preprocess_jsonlogs_RamEz.py](scripts/preprocess_jsonlogs_RamEz.py):** (Variant for Large Files/Limited RAM)
   - Optimized for handling very large logs with controlled RAM usage, an alternative to `preprocess_jsonlogs.py`.

4. **[parse_allevents_RamEz.py](scripts/parse_allevents_RamEz.py):** (Variant for Large Files/Limited RAM)
   - Designed for efficiently processing and parsing events from large log datasets, employing DataFrame chunking and explicit garbage collection for optimal RAM management. Alternative to `parse_allevents.py`.

These scripts form a comprehensive toolkit for diverse Ethereum log processing needs.




## How to Use

1. Google BigQuery Setup:

    - Register for a free account with Google BigQuery.
    Run the following SQL query to get logs for a specific contract:
    ```sql
   SELECT l.*, t.from_address as msg_sender
   FROM `bigquery-public-data.crypto_ethereum.logs` l
   JOIN `bigquery-public-data.crypto_ethereum.transactions` t ON l.transaction_hash = t.hash
   WHERE UPPER(l.address) = UPPER('your contract');
    ```
    - Once the query is complete, a result table will appear. Click "Export", choose "Export to GCS".
    - Choose a Google Cloud Storage bucket and specify the filename with a wildcard (e.g., filename_*). Make sure to select the JSON format.
    - The entire table will be exported to your Google Cloud Storage bucket in multiple files (size dependent).

2. Downloading Data from Google Cloud Storage:

    - It's recommended to use the gcloud command-line tool to download files from the bucket:

    ```bash
    gcloud storage cp gs://BUCKET_NAME/OBJECT_NAME SAVE_TO_LOCATION
    ```

    - Detailed instructions for downloading objects from Google Cloud Storage can be found [here](https://cloud.google.com/storage/docs/downloading-objects).

3. Using the Scripts:
    - Set up a Python environment using [requirements.txt](requirements.txt).
    - Specify the folder path and contract name in the scripts.
    - For general purposes, run [preprocess_jsonlogs.py](scripts/preprocess_jsonlogs.py) to consolidate logs and add event names, followed by [parse_allevents.py](scripts/parse_allevents.py) for decoding logs into separate event CSVs.
    - For large files or limited RAM scenarios, use [preprocess_jsonlogs_RamEz.py](scripts/preprocess_jsonlogs_RamEz.py) and then [parse_allevents_RamEz.py](scripts/parse_allevents_RamEz.py). These scripts utilize DataFrame chunking, garbage collection, and Pandarallel. The combination of chunking and garbage collection controls RAM usage, enabling optimal configuration of `nb_workers` for Pandarallel to achieve the fastest processing speed.
    - Note: `get_cached_abi` does not work with proxy addresses. In such cases, use `get_proxy_address`. The first run of `get_cached_abi` will create `abis/cached_abis.json`. If necessary, manually overwrite this file with the correct ABIs.

