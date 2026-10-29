"""
Airflow version compatibility.

Airflow 3 moved the authoring interface (operators, hooks, connections) into
the Task SDK under ``airflow.sdk``; the Airflow 2 locations survive there only
as deprecated aliases. Import from the SDK when it provides a name and fall
back to the Airflow 2 location otherwise. ``BaseHook`` joined the SDK in
Airflow 3.1, so Airflow 3.0 takes the fallback for it.
"""

try:
    from airflow.sdk import BaseOperator, Connection
except ImportError:  # Airflow 2.x
    from airflow.models import BaseOperator, Connection

try:
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2.x and 3.0
    from airflow.hooks.base import BaseHook

__all__ = ["BaseHook", "BaseOperator", "Connection"]
