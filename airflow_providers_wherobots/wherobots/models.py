"""
The data models for the Wherobots API
"""

import string
from dataclasses import dataclass
from datetime import datetime
from enum import auto
from typing import Optional, List

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


@dataclass
class KubeAppEvent:
    code: str
    message: Optional[str] = None


@dataclass
class KubeApp:
    events: List[KubeAppEvent]


@dataclass
class Run:
    id: str
    createTime: datetime
    updateTime: datetime
    name: str
    status: RunStatus
    startTime: Optional[datetime] = None
    completeTime: Optional[datetime] = None
    kubeApp: Optional[KubeApp] = None

    @property
    def is_timeout(self) -> bool:
        if not self.kubeApp or not self.kubeApp.events:
            return False
        return any(
            (
                event.code == "RUN_FAIL_EXEC"
                and event.message
                and "timeout" in event.message.lower()
            )
            for event in self.kubeApp.events
        )


@dataclass
class LogItem:
    timestamp: int
    raw: str


@dataclass
class LogsResponse:
    items: List[LogItem]
    current_page: int
    next_page: Optional[int] = None
