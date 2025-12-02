"""
The data models for the Wherobots API
"""
import logging
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

class LogProcessor:
    """
    Stateless log parser.
    Determines log level based strictly on the content of the current line and naively tracking of stack traces.
    """

    # memory optimization
    __slots__ = ('_trace_remaining', '_in_trace')

    def __init__(self):
        self._in_trace = False
        self._trace_remaining = 0

    def process_line(self, line: str) -> int:
        """
        Analyzes a log line to determine its priority level.

        Returns: logging.LEVEL (int)

        TODO add a smarter way to correctly log stack traces
        """

        # Scanning only the first 75 chars
        header = line[:75]

        # Handle multi-line traces
        if self._in_trace and self._trace_remaining > 0:
            self._trace_remaining -= 1
            return logging.ERROR

        # Priority: Specific Spark/Java Exceptions
        # If detected, log next 3 lines as ERROR
        if "AnalysisException" in header or \
                "ParseException" in header or \
                "Py4JJavaError" in header or \
           "Traceback (most recent call last):" in header:
            # return next 3 lines as error when this is detected
            self._in_trace = True
            self._trace_remaining = 3
            return logging.ERROR

        # Standard Log Levels
        if "ERROR" in header: return logging.ERROR
        if "WARN" in header: return logging.WARNING
        if "WARNING" in header: return logging.WARNING
        if "DEBUG" in header: return logging.DEBUG
        if "CRITICAL" in header: return logging.CRITICAL
        if "INFO" in header: return logging.INFO

        # Default fallback
        return logging.INFO
