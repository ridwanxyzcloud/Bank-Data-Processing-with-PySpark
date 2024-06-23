# Bank Data Processing with Apache Spark Pyspark

## Overview

This project aims to streamline a simple data processing (exploration, cleaning)  and loading processes for Villy Bank's financial data using Apache Spark and PySpark.
- Pyspark is the python api of Apche Spark which allow us access the spark ecosysytem using python.

## Objectives

- Automate data exploration and cleaning.
- Normalize data into 2NF or 3NF.
- Create a data model that will be used to create a database
- Load the cleaned data into Azure Synapse SQL.

## Directory Structure

- `data/`: Raw and processed data files.
- `notebooks/`: Jupyter notebooks for data exploration and cleaning.
- `scripts/`: Standalone Python scripts.
- `src/`: Core modules for data processing.
- `tests/`: Unit tests for the modules.

## Setup

### Step 1
1. Clone the repository:
   ```bash
   git clone https://github.com/ridwanxyzcloud/bank-data-processing-with-apache-spark.git

2. Install required dependencies

    pip install -r requirements.txt

3. Run the unit tests 

    pytest

4. 

# Projects Steps 

### Step 1: Setting up environement and installing required dependencies
- to avaoid dependencies error, create a virtual environment , then activate it

    python -m venv spark_env

    source spark_env/bin/activate

    pyspark

    quit() 
## Step 2: Data Extraction

- initializing a spark session
- Extract data from the file. 

### Step 3

- Exoplore the data and clean data

### Step 3
- transform the data and generate each table 

### Step 4
- Model database

### Step 5
- Load data into respective table on the database 