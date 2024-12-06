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

### Create an http connection

Create a Connection in Airflow. This can be done from Apache Airflow's
[Web UI](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-connection-ui), 
or from the command-line. The default Wherobots connection name is
`wherobots_default`; if you use another name you must specify that name with the
`wherobots_conn_id` parameter when initializing Wherobots operators.

The only required fields for the connection are:
- the Wherobots API endpoint in the `host` field;
- your Wherobots API key in the `password` field.

```
$ airflow connections add "wherobots_default" \
    --conn-type "generic" \
    --conn-host "api.cloud.wherobots.com" \
    --conn-password "$(< api.key)"
```

## Usage

### Execute a `Run` on Wherobots Cloud

Wherobots allows users to upload their code (.py, .jar), execute it on the
cloud, and monitor the status of the run. Each execution is called a `Run`.

The `WherobotsRunOperator` allows you to execute a `Run` on Wherobots Cloud.
`WherobotsRunOperator` triggers the run according to the parameters you provide,
and waits for the run to finish before completing the task.

Refer to the [Wherobots Managed Storage Documentation](https://docs.wherobots.com/latest/develop/storage-management/storage/)
to learn more about how to upload and manage your code on Wherobots Cloud.

Below is an example of `WherobotsRunOperator`

```python
operator = WherobotsRunOperator(
        task_id="your_task_id",
        name="airflow_operator_test_run_{{ ts_nodash }}",
        runtime=Runtime.TINY_A10_GPU,
        run_python={
            "uri": "s3://wbts-wbc-m97rcg45xi/42ly7mi0p1/data/shared/classification.py"
        },
        dag=dag,
        poll_logs=True,
    )
```

#### Arguments

The arguments for the `WherobotsRunOperator` constructor:

* `name: str`: The name of the run. If not specified, a default name will be
  generated.
* `runtime: Runtime`: The runtime dictates the size and amount of resources
  powering the run. The default value is `Runtime.TINY`; see available values
  [here](https://github.com/wherobots/wherobots-python-dbapi/blob/main/wherobots/db/runtime.py).
* `poll_logs: bool`: If `True`, the operator will poll the logs of the run until it finishes.
  If `False`, the operator will not poll the logs, just track the status of the run.
* `polling_interval`: The interval in seconds to poll the status of the run.
  The default value is `30`.
* `timeout_seconds: int`: This parameter sets a maximum run time (in seconds) to prevent runaway processes. 
If the specified value exceeds the Max Workload Alive Hours, the timeout will be capped at the maximum permissible limit. 
Defaults to `3600` seconds (1 hour).
* `run_python: dict`: A dictionary with the following keys:
  * `uri: str`: The URI of the Python file to run.
  * `args: list[str]`: A list of arguments to pass to the Python file.
* `run_jar: dict`: A dictionary with the following keys:
  * `uri: str`: The URI of the JAR file to run.
  * `args: list[str]`: A list of arguments to pass to the JAR file.
  * `mainClass: str`: The main class to run in the JAR file.
* `environment: dict`: A dictionary with the following keys:
  * `sparkDriverDiskGB: int`: The disk size for the Spark driver.
  * `sparkExecutorDiskGB: int`: The disk size for the Spark executor.
  * `sparkConfigs: dict`: A dictionary of Spark configurations.
  * `dependencies: list[dict]`: A list of dependant libraries to install.

> [!IMPORTANT]
> Wherobots Community Edition users only have access to the "Tiny" runtime type.

> [!WARNING]
> The `run_*` arguments are mutually exclusive, you can only specify one of them.

The `dependencies` argument is a list of dictionaries. There are two
types of dependencies supported.

1. `PYPI` dependencies:
```json
{
    "sourceType": "PYPI",
    "libraryName": "package_name",
    "libraryVersion": "package_version"
}
```

2. `FILE` dependencies:
```json
{
    "sourceType": "FILE",
    "filePath": "s3://bucket/path/to/dependency.whl"
}
```

The file types supported are `.whl`, `.zip`, and `.jar`.


### Execute a SQL query

The `WherobotsSqlOperator` allows you to run SQL queries on the Wherobots cloud,
from which you can build your ETLs and data transformation workflows by
querying, manipulating, and producing datasets with WherobotsDB.

Refer to the [Wherobots Documentation](https://docs.wherobots.com) and this
[guidance](https://docs.wherobots.com/latest/tutorials/sedonadb/vector-data/vector-load/)
to learn how to read data, transform data, and write results in Spatial SQL with
WherobotsDB.

Refer to the [Wherobots Apache Airflow Provider Documentation](https://docs.wherobots.com/latest/develop/airflow-provider)
to get more detailed guidance about how to use the Wherobots Apache Airflow Provider.

#### Example

Below is an example Airflow DAG that executes a SQL query on Wherobots Cloud:

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
        return_last=False,
        runtime=Runtime.TINY,
        sql=f"""
        INSERT INTO wherobots.test.airflow_example
        SELECT id, geometry, confidence, geohash
        FROM wherobots_open_data.overture.places_place
        LIMIT 100
        """,
    )
```

### Arguments

* `runtime: Runtime`: The runtime dictates the size and amount of resources
  powering the run. The default value is `Runtime.TINY`; see available values
  [here](https://github.com/wherobots/wherobots-python-dbapi/blob/main/wherobots/db/runtime.py).
* `sql: str`: The Spatial SQL query to execute.
