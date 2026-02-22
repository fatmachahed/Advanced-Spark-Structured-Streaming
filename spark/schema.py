"""
This file defines the expected schema of the streaming JSON events.

You will use this schema in streaming_app.py to:
- Parse raw JSON messages
- Enable event-time processing
- Detect malformed or incomplete records
"""

from pyspark.sql.types import StructType, StringType, DoubleType

# Expected schema for valid events
event_schema = StructType() \
    .add("device_id", StringType()) \
    .add("event_time", StringType()) \
    .add("temperature", DoubleType()) \
    .add("country", StringType())