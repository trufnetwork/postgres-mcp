"""
Primitive Stream Tool
"""

import logging
from typing import Any, Dict, List, Optional

from postgres_mcp.truf.query import PRIMITIVE_STREAM_LAST_RECORD_QUERY, PRIMITIVE_STREAM_RECORD_QUERY, STREAM_REF_QUERY
from ..sql import SafeSqlDriver

logger = logging.getLogger(__name__)


class PrimitiveStreamTool:
    """Tool for querying primitive streams."""
    
    def __init__(self, sql_driver):
        """Initialize with SQL driver for database operations."""
        self.sql_driver = sql_driver

    async def get_index(
        self,
        data_provider: str,
        stream_id: str,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
        frozen_at: Optional[int] = None,
        base_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get index data for primitive streams.
        
        Calculates index values by normalizing against a base value.
        """
        try:
            # Constants
            max_int8 = 9223372036854775000
            effective_frozen_at = frozen_at if frozen_at is not None else max_int8
            
            # Get stream reference
            stream_ref = await self._get_stream_ref(data_provider, stream_id)
            
            # Determine effective base time
            effective_base_time = base_time
            
            if effective_base_time is None:
                # Try to get from metadata
                metadata_query = """
                  SELECT value_i
                  FROM main.metadata
                  WHERE stream_ref = {}
                    AND metadata_key = 'default_base_time'
                    AND disabled_at IS NULL
                  ORDER BY created_at DESC
                  LIMIT 1;
                """
                
                metadata_rows = await SafeSqlDriver.execute_param_query(
                    self.sql_driver,
                    metadata_query,
                    [stream_ref]
                )
                
                if metadata_rows:
                    effective_base_time = metadata_rows[0].cells.get("value_i")
            
            # Get base value
            base_value = await self._get_base_value(
                data_provider, stream_id, effective_base_time, frozen_at
            )
            
            # Check for division by zero
            if base_value == 0:
                raise ValueError("Base value is 0, cannot calculate index")
            
            # Handle latest record mode (both from_time and to_time are None)
            if from_time is None and to_time is None:
                latest_record = await self._get_last_record_primitive(
                    data_provider, stream_id, None, effective_frozen_at
                )
                
                if latest_record:
                    indexed_value = (float(latest_record["value"]) * 100) / base_value
                    return [{
                        "event_time": latest_record["event_time"],
                        "value": str(indexed_value)
                    }]
                else:
                    return []
            
            # Get records in range and calculate index values
            records = await self._get_record_primitive(
                data_provider, stream_id, from_time, to_time, frozen_at
            )
            
            # Calculate index for each record
            index_records = []
            for record in records:
                indexed_value = (float(record["value"]) * 100) / base_value
                index_records.append({
                    "event_time": record["event_time"],
                    "value": str(indexed_value)
                })
            
            logger.info(f"Retrieved {len(index_records)} primitive index records for {data_provider}/{stream_id}")
            return index_records
            
        except Exception as e:
            logger.error(f"Error in get_index_primitive for {data_provider}/{stream_id}: {e}")
            raise

    async def _get_stream_ref(self, data_provider: str, stream_id: str) -> int:
        """Get stream reference ID."""

        rows = await SafeSqlDriver.execute_param_query(
            self.sql_driver,
            STREAM_REF_QUERY,
            [data_provider, stream_id]
        )
        
        if not rows:
            raise ValueError(f"Stream not found: {data_provider}/{stream_id}")
        
        stream_ref = rows[0].cells.get("stream_ref")
        if stream_ref is None:
            raise ValueError(f"Stream ref is null for {data_provider}/{stream_id}")
        
        return int(stream_ref)

    async def _get_record_primitive(
        self,
        data_provider: str,
        stream_id: str,
        from_time: Optional[int],
        to_time: Optional[int],
        frozen_at: Optional[int]
    ) -> List[Dict[str, Any]]:
        max_int8 = 9223372036854775000
        effective_from = from_time if from_time is not None else 0
        effective_to = to_time if to_time is not None else max_int8
        effective_frozen_at = frozen_at if frozen_at is not None else max_int8
        
        # Handle latest record mode
        if from_time is None and to_time is None:
            latest_record = await self._get_last_record_primitive(
                data_provider, stream_id, None, effective_frozen_at
            )
            return [latest_record] if latest_record else []
        
        stream_ref = await self._get_stream_ref(data_provider, stream_id)
        
        params = [
            stream_ref, effective_frozen_at, effective_from, effective_to,  # interval_records
            stream_ref, effective_from, effective_frozen_at  # anchor_record
        ]
        
        rows = await SafeSqlDriver.execute_param_query(
            self.sql_driver,
            PRIMITIVE_STREAM_RECORD_QUERY,
            params
        )

        if not rows:
            raise ValueError(f"error when getting record for: {data_provider}/{stream_id}")
        
        records = []
        for row in rows:
            records.append({
                "event_time": row.cells.get("event_time"),
                "value": str(row.cells.get("value", "0"))
            })
        
        return records

    async def _get_last_record_primitive(
        self,
        data_provider: str,
        stream_id: str,
        before: Optional[int],
        frozen_at: int
    ) -> Optional[Dict[str, Any]]:
        max_int8 = 9223372036854775000
        effective_before = before if before is not None else max_int8
        
        stream_ref = await self._get_stream_ref(data_provider, stream_id)
        
        rows = await SafeSqlDriver.execute_param_query(
            self.sql_driver,
            PRIMITIVE_STREAM_LAST_RECORD_QUERY,
            [stream_ref, effective_before, frozen_at]
        )
        
        if rows:
            return {
                "event_time": rows[0].cells.get("event_time"),
                "value": str(rows[0].cells.get("value", "0"))
            }
        
        return None

    async def _get_base_value(
        self,
        data_provider: str,
        stream_id: str,
        base_time: Optional[int],
        frozen_at: Optional[int]
    ) -> float:
        effective_base_time = base_time
        stream_ref = await self._get_stream_ref(data_provider, stream_id)
        
        # If base_time is null, try to get it from metadata
        if effective_base_time is None:
            metadata_query = """
            SELECT value_i
            FROM main.metadata
            WHERE stream_ref = {}
              AND metadata_key = 'default_base_time'
              AND disabled_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1;
            """
            
            metadata_rows = await SafeSqlDriver.execute_param_query(
                self.sql_driver,
                metadata_query,
                [stream_ref]
            )
            
            if metadata_rows:
                effective_base_time = metadata_rows[0].cells.get("value_i")
            
            # If still null, get the first ever record
            if effective_base_time is None:
                first_record = await self._get_first_record_primitive(
                    data_provider, stream_id, frozen_at
                )
                if first_record:
                    return float(first_record["value"])
                else:
                    raise ValueError("No base value found: no records in stream")
        
        # Try to find exact match at base_time
        exact_records = await self._get_record_primitive(
            data_provider, stream_id, effective_base_time, effective_base_time, frozen_at
        )
        
        if exact_records:
            return float(exact_records[0]["value"])
        
        # If no exact match, find closest value before base_time
        before_record = await self._get_last_record_primitive(
            data_provider, stream_id, effective_base_time, 
            frozen_at if frozen_at is not None else 9223372036854775000
        )
        
        if before_record:
            return float(before_record["value"])
        
        # If no value before, find closest value after base_time
        after_record = await self._get_first_record_primitive(
            data_provider, stream_id, frozen_at, effective_base_time
        )
        
        if after_record:
            return float(after_record["value"])
        
        raise ValueError("No base value found")

    async def _get_first_record_primitive(
        self,
        data_provider: str,
        stream_id: str,
        frozen_at: Optional[int],
        after_time: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        max_int8 = 9223372036854775000
        effective_frozen_at = frozen_at if frozen_at is not None else max_int8
        
        stream_ref = await self._get_stream_ref(data_provider, stream_id)
        
        if after_time is not None:
            # Find first record after specified time
            query = """
            SELECT pe.event_time, pe.value
            FROM main.primitive_events pe
            WHERE pe.stream_ref = {}
              AND pe.event_time > {}
              AND pe.created_at <= {}
            ORDER BY pe.event_time ASC, pe.created_at DESC
            LIMIT 1;
            """
            params = [stream_ref, after_time, effective_frozen_at]
        else:
            # Find very first record
            query = """
            SELECT pe.event_time, pe.value
            FROM main.primitive_events pe
            WHERE pe.stream_ref = {}
              AND pe.created_at <= {}
            ORDER BY pe.event_time ASC, pe.created_at DESC
            LIMIT 1;
            """
            params = [stream_ref, effective_frozen_at]
        
        rows = await SafeSqlDriver.execute_param_query(
            self.sql_driver,
            query,
            params
        )
        
        if rows:
            return {
                "event_time": rows[0].cells.get("event_time"),
                "value": str(rows[0].cells.get("value", "0"))
            }
        
        return None