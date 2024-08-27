"""
The data models for the Wherobots API
"""

import string
from datetime import datetime
from enum import auto, Enum
from typing import Optional, Sequence

from pydantic import BaseModel, Field, ConfigDict, computed_field
from strenum import StrEnum
from wherobots.db import Runtime

RUN_NAME_ALPHABET = string.ascii_letters + string.digits + "-_."


class RunStatus(str, Enum):
    PENDING = auto()
    RUNNING = auto()
    FAILED = auto()
    COMPLETED = auto()
    CANCELLED = auto()

    def is_active(self) -> bool:
        return self in [self.PENDING, self.RUNNING]


class WherobotsModel(BaseModel):
    ext_id: str = Field(alias="id")
    create_time: datetime = Field(alias="createTime")
    update_time: datetime = Field(alias="updateTime")
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class Run(WherobotsModel):
    name: str
    status: RunStatus
    start_time: Optional[datetime] = Field(default=None, alias="startTime")
    end_time: Optional[datetime] = Field(default=None, alias="completeTime")


class PythonRunPayload(BaseModel):
    """
    Model for the payload of Run with type == "python"
    """

    # For airflow to render the template fields
    template_fields: Sequence[str] = Field(
        ("uri", "args", "entrypoint"), exclude=True, init=False
    )

    uri: str = Field(..., alias="uri")
    args: list[str] = []
    entrypoint: Optional[str] = Field(None, alias="entrypoint")

    @classmethod
    def create(cls, uri: str, args: list[str], entrypoint: Optional[str] = None):
        return cls(uri=uri, args=args, entrypoint=entrypoint)


class JavaRunPayload(BaseModel):
    """
    Model for the payload of Run with type == "python"
    """

    # For airflow to render the template fields
    template_fields: Sequence[str] = Field(("uri", "args", "mainClass"), exclude=True)

    uri: str = Field(..., alias="uri")
    args: list[str] = []
    mainClass: Optional[str] = Field(None, alias="mainClass")

    @classmethod
    def create(cls, uri: str, args: list[str], mainClass: Optional[str] = None):
        return cls(uri=uri, args=args, mainClass=mainClass)


class RunType(StrEnum):
    python = auto()
    java = auto()


class CreateRunPayload(BaseModel):
    # For airflow to render the template fields
    template_fields: Sequence[str] = Field(("name", "python", "java"), exclude=True)

    runtime: Runtime
    name: Optional[str] = None
    python: Optional[PythonRunPayload] = None
    java: Optional[JavaRunPayload] = None

    @computed_field
    def type(self) -> RunType:
        run_type = RunType.python if self.python else RunType.java
        assert isinstance(run_type, RunType)
        return run_type

    @classmethod
    def create(
        cls,
        runtime: Runtime,
        name: str,
        python: Optional[PythonRunPayload] = None,
        java: Optional[JavaRunPayload] = None,
    ):
        return cls(runtime=runtime, name=name, python=python, java=java)