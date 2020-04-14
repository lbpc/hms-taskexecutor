import attr
from enum import IntEnum
from typing import Any, Type


class TaskState(IntEnum):
    NEW = 0
    PROCESSING = 1
    DONE = 2
    FAILED = 3


@attr.s(auto_attribs=True, slots=True)
class Task:
    tag: Any
    origin: Type
    opid: str
    actid: str
    res_type: str
    action: str
    params: dict
    state: TaskState = attr.ib(default=TaskState.NEW, converter=TaskState)
