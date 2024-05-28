# Airflow Providers for Wherobots

Airflow providers to bring [Wherobots Cloud](https://www.wherobots.com)'s
spatial compute to your data workflows and ETLs.

## Installation

If you use [Poetry](https://python-poetry.org) in your project, add the
dependency with `poetry add`:

```
$ poetry add airflow-providers-wherobots
```

Otherwise, just `pip install` it:

```
$ pip install airflow-providers-wherobots
```

## Usage

### Create a connection

You first need to create a Connection in Airflow. This can be done from
the UI, or from the command-line. The default Wherobots connection name
is `wherobots_default`; if you use another name you must specify that
name with the `wherobots_conn_id` parameter when initializing Wherobots
operators.

The only required fields for the connection are:
- the Wherobots API endpoint in the `host` field;
- your Wherobots API key in the `password` field.

```
$ airflow connections add "wherobots_default" \
    --conn-type "generic" \
    --conn-host "api.cloud.wherobots.com" \
    --conn-password "$(< api.key)"
```

### Execute a SQL query

The `WherobotsSqlOperator` allows you to run SQL queries on the
Wherobots cloud, from which you can build your ETLs and data
transformation workflows by querying, manipulating, and producing
datasets with WherobotsDB.

Refer to the [Wherobots Documentation](https://docs.wherobots.com) and
this [guidance](https://docs.wherobots.com/latest/tutorials/sedonadb/vector-data/vector-load/)
to learn how to read data, transform data, and write results in Spatial
SQL with WherobotsDB.

Refer to the [Wherobots Apache Airflow Provider Documentation](https://docs.wherobots.com/latest/develop/airflow-provider)
to get more detailed guidance about how to use the Wherobots Apache Airflow Provider.

## Example

Below is an example Airflow DAG that executes a SQL query on Wherobots
Cloud:

```python
import datetime

from airflow import DAG
from airflow_providers_wherobots.operators.sql import WherobotsSqlOperator


with DAG(
    dag_id="example_wherobots_sql_dag",
    start_date=datetime.datetime.now(),
    schedule="@hourly",
    catchup=False
):
    # Create a `wherobots.test.airflow_example` table with 100 records
    # from the OMF `places_place` dataset.
    operator = WherobotsSqlOperator(
        task_id="execute_query",
        sql=f"""
        INSERT INTO wherobots.test.airflow_example
        SELECT id, geometry, confidence, geohash
        FROM wherobots_open_data.overture.places_place
        LIMIT 100
        """,
        return_last=False,
    )
```
