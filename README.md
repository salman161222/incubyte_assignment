# Incubyte ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline designed to load customer data from a source file into a PostgreSQL database and handle incremental updates based on specific conditions. The pipeline is designed to partition data by country and load customer records incrementally based on the consultation date.

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Pipeline Components](#pipeline-components)
  - [Extract](#extract)
  - [Load to Staging](#load-to-staging)
  - [Incremental Load](#incremental-load)
- [Logging](#logging)
- [How to Run](#how-to-run)
- [License](#license)

## Project Structure

data_pipeline/
├── incubyte/
│   ├── Include/
│   ├── Lib/
│   ├── Scripts/
│   └── pyvenv.cfg
├── logs/
│   └── etl_pipeline.log
├── source/
│   ├── dummy_customer_data.xlsx
│   └── dummy_data.txt
├── Tables/
│   ├── DDL/
│   │   ├── Curate_customer_info.sql
│   │   └── STG_Customer_Info.sql
│   └── Models/
├── trans/
│   └── data.csv
├── etl_pipeline.py
├── requirements.txt
├── test_etl_pipeline.py
└── util.py
