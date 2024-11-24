# *BigQuery ETL exercise*

# ETL Pipeline for Annual Movie Summary

This ETL pipeline project extracts movie data from a MySQL database, processes it to summarize annual movie statistics, and loads the transformed data into Google BigQuery.

## Features
- Extracts data from a **MySQL** database using **SQLAlchemy**.
- Transforms the data by calculating average movie durations, ratings, and categorizing movie years.
- Loads the transformed data into a **BigQuery** table.
- Implements robust error handling and logging for better debugging.
- Modular structure for maintainability and scalability.

## Prerequisites
1. **Python 3.7+** 
2. **Google Cloud SDK** 
3. **Required Python Packages**:
   - `pandas`
   - `numpy`
   - `sqlalchemy`
   - `pymysql`
   - `google-cloud-bigquery`
4. A MySQL database containing the source data.


## Setup Instructions

### 1. Clone the Repository
```sh
git clone [<repository_url>](https://github.com/carlotagouveia/bigquery-etl.git)
cd etl_pipeline
```

### 2. Install dependencies
```sh
pip install pandas numpy sqlalchemy pymysql google-cloud-bigquery
```

### 3. Configurations
Create a db_credentials.json file with the following structure:
```sh
{
    "username": "<your_mysql_username>",
    "password": "<your_mysql_password>",
    "host": "<your_mysql_host>",
    "database": "<your_mysql_database>"
}
```

### 4. Update BigQuery Details
Update the following variables in etl_pipeline.py:

- **Project ID:** proj (e.g., valued-vault-439309-a6)
- **Dataset:** dataset (e.g., sample_dataset)
- **Target Table:** target_table (e.g., annual_movie_summary)

### 5.Run the ETL Script
```sh
python etl_pipeline.py
```

