# End-to-End Extract Load Transform (ELT) Yellow Taxi Data Warehouse
This project goals is to create a data warehouse and star schema data model using nyc tlc yellow taxi trip data from 2021 to 2024, you can access the full data here: [NYC-TLC-Taxi-Trip-Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page#). However, I don't use the full data which have 40 millions+ rows. I only use 0.01% data sample from 2021 to 2024 and then combined it. You can access sampled data here: [NYC-TLC-Taxi-Trip-Data-Sampled](https://drive.google.com/drive/folders/12HGoYKjYh8XQci7tGTqy6cE1MHSt_WBY?usp=sharing).

## High-Level Architecture

![High Level Architecture](https://drive.google.com/uc?export=view&id=1xMEXTWs0V0tez_9Kbo0PUFB5tDsOm7cm)

This project ingest data from parquet files in google drive and tables in postgresql using Airbyte and then load the data to bronze database in snowflake. When the data arrived, dbt (data build tool) start the job to transform raws data to staging, intermediate, dimensional, fact, and mart models, then generate dbt documentation and stored it in minio. All workflow are orchestrated, monitored, and automated using Apache Airflow.  

## Prerequisite
- [Docker](https://www.docker.com/get-started/)
- [Astro-cli](https://www.astronomer.io/docs/astro/cli/install-cli/)
- [Postgresql](https://www.docker.com/blog/how-to-use-the-postgres-docker-official-image/)
- [Airbyte](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart)
- [Snowflake account](https://www.snowflake.com/en/)
- [NYC-TLC-Taxi-Trip-Data-Sampled](https://drive.google.com/drive/folders/12HGoYKjYh8XQci7tGTqy6cE1MHSt_WBY?usp=sharing)

## DAG (Direct Acyclic Graph)
### DAG Graph
![DAG Graph](https://drive.google.com/uc?export=view&id=1ohdI2jsiW4YFV_sdvsq4gFf94g-T1ME1)
### DAG Run
![DAG Run](https://drive.google.com/uc?export=view&id=1kwfvqJJTod_CW-NTJmCPyes3u8nXotzz)

## Snowflake
| Bronze    | Silver   | Gold |
| :-------: | :------: | :-------: |
| ![Bronze](https://drive.google.com/uc?export=view&id=1OHime41bSTvuLjdvloHuxlwo0eURg8vs) | ![Silver](https://drive.google.com/uc?export=view&id=1HVwpm9VFohqMFz4AaLaK4f3hlm-61vpT) | ![Gold](https://drive.google.com/uc?export=view&id=1HG0dSp17A2Ol1I_gC6vaW3KR4SedgbHt) |
| Raw       | Intermediate   |   Dimensional |
| Staging   |                |   Fact        |
|           |                |   Mart        |

## Minio dbt-docs Bucket
![Minio](https://drive.google.com/uc?export=view&id=1d71afQzK5Mdv04BjibUh0FCGMFaENdrP)

## dbt-docs index.html
![dbt-docs-index.html](https://drive.google.com/uc?export=view&id=1y0BjhM9B4UQb4YAPxG60aMx7zkLE8QtZ)
