"""
Composed Stream Tool
"""

import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from ..sql import SafeSqlDriver
from .query import COMPOSED_STREAM_INDEX_QUERY
from .query import COMPOSED_STREAM_RECORD_QUERY
from .query import TAXONOMIES_QUERY

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

    async def describe_taxonomies(
      self,
      data_provider: str,
      stream_id: str,
      latest_group_sequence: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Describe the taxonomy composition of a composed stream.
        
        Shows the child streams, their weights, and taxonomy details.
        
        Args:
            data_provider: Parent stream deployer address
            stream_id: Parent stream ID
            latest_group_sequence: If True, only returns the latest (active) taxonomy version
            
        Returns:
            List of taxonomy records showing composition
        """
        try:
            params = [data_provider, stream_id, latest_group_sequence]

            logger.debug(f"Describing taxonomies for {data_provider}/{stream_id}, latest_only={latest_group_sequence}")

            # Execute the query
            rows = await SafeSqlDriver.execute_param_query(
                self.sql_driver,
                TAXONOMIES_QUERY,
                params
            )

            if not rows:
                logger.info(f"No taxonomy found for stream {data_provider}/{stream_id}")
                return []

            # Convert results
            records = []
            for row in rows:
                record = {
                    "data_provider": row.cells.get("data_provider"),
                    "stream_id": row.cells.get("stream_id"),
                    "child_data_provider": row.cells.get("child_data_provider"),
                    "child_stream_id": row.cells.get("child_stream_id"),
                    "weight": str(row.cells.get("weight", "0")),  # Preserve precision
                    "created_at": row.cells.get("created_at"),
                    "group_sequence": row.cells.get("group_sequence"),
                    "start_date": row.cells.get("start_date")
                }
                records.append(record)

            logger.info(f"Retrieved {len(records)} taxonomy records for {data_provider}/{stream_id}")
            return records

        except Exception as e:
            logger.error(f"Error describing taxonomies for {data_provider}/{stream_id}: {e}")
            raise

    async def get_index(
        self,
        data_provider: str,
        stream_id: str,
        from_time: Optional[int] = None,
        to_time: Optional[int] = None,
        frozen_at: Optional[int] = None,
        base_time: Optional[int] = None,
        use_cache: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get index data for composed streams.
        
        Args:
            data_provider: Stream deployer address
            stream_id: Stream identifier
            from_time: Start timestamp (inclusive)
            to_time: End timestamp (inclusive)
            frozen_at: Created-at cutoff for time-travel queries
            base_time: Base timestamp for index calculations
            use_cache: Whether to use cache for performance
            
        Returns:
            List of index records with event_time and value
        """
        try:
          params = [
              data_provider,      # {} - data_provider
              stream_id,          # {} - stream_id
              from_time,          # {} - from_param
              to_time,            # {} - to_param
              frozen_at,          # {} - frozen_at_param
              base_time,          # {} - base_time_param
              use_cache,          # {} - use_cache_param
              from_time,          # {} - effective_from
              to_time,            # {} - effective_to
              frozen_at,          # {} - effective_frozen_at
              base_time,          # {} - effective_base_time
          ]

          logger.debug(f"Executing composed index query for {data_provider}/{stream_id}")

          rows = await SafeSqlDriver.execute_param_query(
              self.sql_driver,
              COMPOSED_STREAM_INDEX_QUERY,
              params
          )

          if not rows:
            logger.info(f"Index cannot be calculated for stream {data_provider}/{stream_id}")
            return []

          # Convert results
          records = []
          for row in rows:
              record = {
                  "event_time": row.cells.get("event_time"),
                  "value": str(row.cells.get("value", "0"))
              }
              records.append(record)

          logger.info(f"Retrieved {len(records)} composed index records for {data_provider}/{stream_id}")
          return records

        except Exception as e:
            logger.error(f"Error in get_index for {data_provider}/{stream_id}: {e}")
            raise
