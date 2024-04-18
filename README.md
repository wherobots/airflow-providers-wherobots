# Airflow Wherobots Provider

Airflow Provider that integrates [wherobots cloud](https://wherobots.com/)'s computation features into your workflows.

## Installation

If you use [Poetry](https://python-poetry.org) in your project, add the
dependency with `poetry add`:

```
$ poetry add git+https://github.com/wherobots/airflow-providers-wherobots
```

Otherwise, just `pip install` it:

```
$ pip install git+https://github.com/wherobots/airflow-providers-wherobots
```

## Usage

### Create Connection
Create a connection first, the default wherobots connection name is `wherobots_default`.
For any other name, please pass in the `wherobots_conn_id` parameter when initialize Wherobots Operators.
Fill in the following fields in connection detail page:
1. Fill the wherobots API Service endpoint into the `Host` field
2. Fill the personal API Token into the `Password` field

### Execute SQL query
You can use the `WherobotsSqlOperator` to run sql queries on the Wherobots cloud.
You can build your ETL jobs on Wherobots Catalogs.
Check this [guidance](https://docs.wherobots.services/1.2.2/tutorials/sedonadb/vector-data/vector-load/) to learn how to **read data, transformation and write results by pure SQL in Wherobots Cloud**.  
Below is an example dag that executes SQL query on Wherobots Cloud 
```python
import datetime

from airflow import DAG
from airflow_providers_wherobots.operators.sql import WherobotsSqlOperator


with DAG(
    dag_id="example_wherobots_sql_dag",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False
):
    # operator = PythonOperator(task_id="print_the_context", python_callable=print_context)
    operator = WherobotsSqlOperator(
        task_id="execute_query",
        sql=f"""
        INSERT INTO
            wherobots.test_db.example_geom
        SELECT
            id, geometry, confidence, geohash
        FROM
            wherobots_open_data.overture.places_place
        """,
        return_last=False,
    )
```
