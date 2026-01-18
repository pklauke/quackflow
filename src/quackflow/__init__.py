"""Quackflow - Stream processing with SQL."""

# Core API
from quackflow.app import Quackflow
from quackflow.runtime import Runtime

# Schema types
from quackflow.schema import (
    Bool,
    Float,
    Int,
    List,
    Long,
    Schema,
    String,
    Struct,
    Timestamp,
)

# Time notions
from quackflow.time_notion import EventTimeNotion, ProcessingTimeNotion

# Protocols (for custom sources/sinks)
from quackflow.sink import Sink
from quackflow.source import ReplayableSource, Source

__all__ = [
    # Core
    "Quackflow",
    "Runtime",
    # Schema
    "Schema",
    "String",
    "Int",
    "Long",
    "Float",
    "Bool",
    "Timestamp",
    "List",
    "Struct",
    # Time
    "EventTimeNotion",
    "ProcessingTimeNotion",
    # Protocols
    "Source",
    "ReplayableSource",
    "Sink",
]
