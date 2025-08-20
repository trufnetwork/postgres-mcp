"""
Composed Stream Tool for handling complex time series calculations.
Converts Kwil-DB stored procedure logic to PostgreSQL for composed streams.
"""

import logging
from typing import Any, Dict, List, Optional
from .query import COMPOSED_STREAM_RECORD_QUERY

logger = logging.getLogger(__name__)


class ComposedStreamTool:
    """Tool for querying composed streams with complex time series calculations."""
    
    def __init__(self, sql_driver):
        """Initialize with SQL driver for database operations."""
        self.sql_driver = sql_driver
    
    async def get_record_composed(
        self,
        data_provider: str,
        stream_id: str,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
        frozen_at: Optional[int] = None,
        use_cache: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get records from a composed stream using complex time series calculations.
        """
        try:
            from ..sql import SafeSqlDriver
            
            # Parameters
            params = [
                data_provider.lower(),  # data_provider
                stream_id,              # stream_id  
                from_time,              # from_param
                to_time,                # to_param
                frozen_at,              # frozen_at_param
                use_cache,              # use_cache_param
                from_time,              # effective_from
                to_time,                # effective_to  
                frozen_at,              # effective_frozen_at
            ]
            
            logger.debug(f"Executing composed stream query for {data_provider}/{stream_id}")
            
            # Execute the query
            rows = await SafeSqlDriver.execute_param_query(
                self.sql_driver,
                COMPOSED_STREAM_RECORD_QUERY,
                params
            )
            
            if not rows:
                logger.info(f"No records found for composed stream {data_provider}/{stream_id}")
                return []
            
            # Convert results
            records = []
            for row in rows:
                record = {
                    "event_time": row.cells.get("event_time"),
                    "value": str(row.cells.get("value", "0"))
                }
                records.append(record)
            
            logger.info(f"Retrieved {len(records)} records for composed stream {data_provider}/{stream_id}")
            return records
            
        except Exception as e:
            logger.error(f"Error in get_record_composed for {data_provider}/{stream_id}: {e}")
            raise
