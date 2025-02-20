"""
The data models for the Wherobots API
"""

import string
from datetime import datetime
from enum import auto
from typing import Optional, List

from pydantic import BaseModel, Field, ConfigDict
from strenum import StrEnum

RUN_NAME_ALPHABET = string.ascii_letters + string.digits + "-_."


class RunStatus(StrEnum):
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


class KubeAppEvent(BaseModel):
    code: str
    message: Optional[str] = None


class KubeApp(BaseModel):
    events: List[KubeAppEvent]


class Run(WherobotsModel):
    name: str
    status: RunStatus
    start_time: Optional[datetime] = Field(default=None, alias="startTime")
    end_time: Optional[datetime] = Field(default=None, alias="completeTime")
    kube_app: Optional[KubeApp] = Field(default=None, alias="kubeApp")

    @property
    def is_timeout(self) -> bool:
        if not self.kube_app or not self.kube_app.events:
            return False
        return any(
            (
                event.code == "RUN_FAIL_EXEC"
                and event.message
                and "timeout" in event.message.lower()
            )
            for event in self.kube_app.events
        )


class LogItem(BaseModel):
    timestamp: int
    raw: str


class LogsResponse(BaseModel):
    items: List[LogItem]
    current_page: int
    next_page: Optional[int] = None
