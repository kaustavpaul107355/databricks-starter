"""
Python SDK for the Ingest API.

This is the synchronous version of the SDK. For the asynchronous version,
please use `from zerobus_sdk.aio import ...`.
"""

from . import sync
from . import aio  # auto-grandfathered # noqa: F401
from .shared import (
    TableProperties,
    StreamState,
    ZerobusException,
    StreamConfigurationOptions,
    NonRetriableException,
    get_zerobus_token,
)

ZerobusSdk = sync.ZerobusSdk
ZerobusStream = sync.ZerobusStream
RecordAcknowledgment = sync.RecordAcknowledgment

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "ZerobusException",
    "NonRetriableException",
    "StreamState",
    "get_zerobus_token",
]
