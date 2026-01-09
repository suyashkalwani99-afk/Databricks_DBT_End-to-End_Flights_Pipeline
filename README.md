# Databricks End-to-End Flights Pipeline

A production-ready data pipeline implementing the medallion architecture (Bronze-Silver-Gold) on Databricks for processing flight booking data with incremental loads and Change Data Capture (CDC).

## Overview

This project demonstrates enterprise data engineering best practices using Databricks, Delta Lake, and Delta Live Tables to build a scalable data warehouse for flight booking analytics. The pipeline ingests raw CSV data, applies transformations and data quality rules, and produces analytics-ready fact and dimension tables.

## Architecture

### Medallion Architecture Layers

```
Raw CSV Files (Input_Dataset/)
    ↓
Bronze Layer (Auto Loader Streaming)
    ↓
Silver Layer (DLT with CDC & SCD Type 1)
    ↓
Gold Layer (Star Schema with Fact & Dimensions)
    ↓
Ready for Analytics/BI
```

### Bronze Layer - Raw Data Ingestion
- **Purpose**: Ingest raw CSV data into Delta Lake format
- **Technology**: Databricks Auto Loader with streaming
- **Features**:
  - Schema inference and evolution
  - Append-only streaming writes
  - Checkpoint-based incremental processing
  - Rescue column for schema mismatches

**Components**:
- [SrcParameters.ipynb](Bronze_Layer/SrcParameters.ipynb) - Source entity configuration
- [Incremental_Data_Ingestion_Bronze Layer.ipynb](Bronze_Layer/Incremental_Data_Ingestion_Bronze%20Layer.ipynb) - Ingestion logic

### Silver Layer - Transformation & CDC
- **Purpose**: Clean, deduplicate, and apply Change Data Capture
- **Technology**: Delta Live Tables (DLT)
- **Features**:
  - SCD Type 1 CDC implementation
  - Data quality rules and validation
  - Change tracking with `modifyDate` column
  - Declarative pipeline framework

**Components**:
- [dltPipeline.py](Silver_Layer/dltPipeline.py) - DLT pipeline with CDC logic

**Data Quality Rules**:
- Bookings: `booking_id` and `passenger_id` must not be null
- Type casting: `amount` to Double, `booking_date` to Date
- Invalid records are dropped automatically

### Gold Layer - Business Analytics
- **Purpose**: Create analytics-ready star schema
- **Technology**: PySpark with MERGE operations
- **Features**:
  - Surrogate key generation
  - Audit columns (create_date, update_date)
  - Incremental upserts with idempotent MERGE
  - Star schema with fact and dimension tables

**Components**:
- [GOLD_DIMS.ipynb](Gold_Layer/GOLD_DIMS.ipynb) - Dimension tables
- [GOLD_FACT.ipynb](Gold_Layer/GOLD_FACT.ipynb) - Fact table

## Data Model

### Fact Table
**FactBookings**:
- `booking_id` - Unique booking identifier
- `passenger_id` - Reference to passenger
- `flight_id` - Reference to flight
- `airport_id` - Reference to airport
- `amount` - Booking amount
- `booking_date` - Date of booking
- `DimPassengersKey` - Surrogate key for passenger dimension
- `DimFlightsKey` - Surrogate key for flight dimension
- `DimAirportsKey` - Surrogate key for airport dimension

### Dimension Tables
**DimPassengers**:
- `DimPassengersKey` - Surrogate key
- `passenger_id` - Business key
- `name` - Passenger name
- `gender` - Gender
- `nationality` - Nationality
- `create_date`, `update_date` - Audit columns

**DimFlights**:
- `DimFlightsKey` - Surrogate key
- `flight_id` - Business key
- `airline` - Airline name
- `origin` - Origin airport
- `destination` - Destination airport
- `flight_date` - Flight date
- `create_date`, `update_date` - Audit columns

**DimAirports**:
- `DimAirportsKey` - Surrogate key
- `airport_id` - Business key
- `airport_name` - Airport name
- `city` - City
- `country` - Country
- `create_date`, `update_date` - Audit columns

## Project Structure

```
Databricks_DBT_End-to-End_Flights_Pipeline/
├── Bronze_Layer/
│   ├── SrcParameters.ipynb                              # Source configuration
│   └── Incremental_Data_Ingestion_Bronze Layer.ipynb   # Bronze ingestion logic
├── Silver_Layer/
│   └── dltPipeline.py                                   # DLT pipeline with CDC
├── Gold_Layer/
│   ├── GOLD_DIMS.ipynb                                  # Dimension transformations
│   └── GOLD_FACT.ipynb                                  # Fact transformations
├── Input_Dataset/
│   ├── dim_passengers.csv                               # 200 initial records
│   ├── dim_passengers_increment.csv                     # Incremental updates
│   ├── dim_passengers_scd.csv                           # SCD test data
│   ├── dim_flights.csv                                  # 100 initial records
│   ├── dim_flights_increment.csv                        # Incremental updates
│   ├── dim_flights_scd.csv                              # SCD test data
│   ├── dim_airports.csv                                 # 50 initial records
│   ├── dim_airports_increment.csv                       # Incremental updates
│   ├── dim_airports_scd.csv                             # SCD test data
│   ├── fact_bookings.csv                                # 1000 initial records
│   └── fact_bookings_increment.csv                      # 300 incremental records
└── Running_Jobs_and_Pipelines_Screenshots/              # Execution evidence
```

## Technologies Used

- **Databricks** - Cloud data platform
- **Delta Lake** - Unified data format for streaming and batch
- **Delta Live Tables (DLT)** - Declarative pipeline framework
- **Apache Spark** - Distributed computing engine
- **PySpark** - Python API for Spark
- **Python** - Primary programming language
- **Databricks Auto Loader** - Incremental file ingestion

## Setup and Configuration

### Prerequisites
- Databricks workspace
- Unity Catalog enabled
- Volumes configured for data storage

### Configuration Parameters

**Bronze Layer**:
- Source entities: `bookings`, `flights`, `airports`, `customers`
- Source path: `/Volumes/workspace/raw/rawvolume/rawdata/`
- Target path: `/Volumes/workspace/bronze/bronzevolume/`

**Silver Layer** ([dltPipeline.py](Silver_Layer/dltPipeline.py)):
```python
catalog = "workspace"
source_path = "/Volumes/workspace/bronze/bronzevolume/[entity]/data/"
sequence_column = "modifyDate"
```

**Gold Layer**:
```python
catalog = "workspace"
source_schema = "silver"
target_schema = "gold"
cdc_col = "modifyDate"
```

## Pipeline Execution

### 1. Bronze Layer - Initial Load
Run [SrcParameters.ipynb](Bronze_Layer/SrcParameters.ipynb) to configure sources, then execute [Incremental_Data_Ingestion_Bronze Layer.ipynb](Bronze_Layer/Incremental_Data_Ingestion_Bronze%20Layer.ipynb) to ingest initial CSV files.

### 2. Silver Layer - DLT Pipeline
Execute [dltPipeline.py](Silver_Layer/dltPipeline.py) as a Delta Live Tables pipeline to apply transformations and CDC.

### 3. Gold Layer - Star Schema
Run [GOLD_DIMS.ipynb](Gold_Layer/GOLD_DIMS.ipynb) to create dimensions, then [GOLD_FACT.ipynb](Gold_Layer/GOLD_FACT.ipynb) to create the fact table.

### 4. Incremental Loads
Add incremental CSV files to the source location and re-run the pipeline. The system will automatically:
- Detect new files (Bronze)
- Apply CDC logic (Silver)
- Upsert changes (Gold)

## Key Features

### Incremental Processing
- Supports both initial and incremental data loads
- Checkpoint-based streaming for efficient processing
- MERGE operations ensure idempotent upserts

### Change Data Capture
- SCD Type 1 implementation at Silver layer
- Change tracking via `modifyDate` column
- Automatic detection and application of updates

### Data Quality
- Validation rules at Silver layer
- Automatic dropping of invalid records
- Rescue column for schema mismatch handling

### Schema Evolution
- Auto Loader handles schema changes gracefully
- New columns automatically detected and added
- Backwards compatibility maintained

### Audit Trail
- `create_date` and `update_date` columns in Gold layer
- Full lineage tracking through layers
- `modifyDate` column tracks source changes

## Data Volumes

### Initial Load
- Passengers: 200 records
- Flights: 100 records
- Airports: 50 records
- Bookings: 1,000 records

### Incremental Load
- Bookings: 300 additional records
- Dimension updates via increment and SCD files

## Best Practices Implemented

1. **Medallion Architecture** - Clear separation of concerns across Bronze, Silver, and Gold layers
2. **Idempotent Pipelines** - Safe to re-run without duplicates
3. **Incremental Processing** - Efficient handling of large datasets
4. **Data Quality Gates** - Validation rules prevent bad data propagation
5. **Surrogate Keys** - Dimension tables use surrogate keys for stability
6. **Audit Columns** - Full tracking of data lineage and changes
7. **Declarative Pipelines** - DLT enables maintainable, self-documenting code

## Execution Evidence

The [Running_Jobs_and_Pipelines_Screenshots](Running_Jobs_and_Pipelines_Screenshots/) directory contains proof of successful pipeline execution:
- Bronze layer initial and incremental job runs
- Silver DLT pipeline initial and incremental loads

## Future Enhancements

- Implement SCD Type 2 for historical tracking
- Add data observability and monitoring
- Implement data retention policies
- Add automated testing framework
- Create BI dashboards for analytics

## License

This project is for educational and demonstration purposes.

## Contact

For questions or support, please refer to the project repository.
