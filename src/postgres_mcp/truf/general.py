"""
General Stream Tool
"""

import logging
from decimal import Decimal, getcontext
from typing import Any, Dict, List

# Set precision to match NUMERIC(36,18)
getcontext().prec = 36

logger = logging.getLogger(__name__)


class GeneralStreamTool:
    """Tool for general stream operations and queries."""
    
    def __init__(self, sql_driver):
        """Initialize with SQL driver for database operations."""
        self.sql_driver = sql_driver
    
    async def get_index_change(
        self,
        current_data: List[Dict[str, Any]],
        prev_data: List[Dict[str, Any]], 
        time_interval: int
    ) -> List[Dict[str, Any]]:
        """
        Calculate index change data comparing current values to previous values.
        
        Args:
            current_data: Current index data from get_index calls
            prev_data: Previous index data from get_index calls
            time_interval: Time interval used for comparison
            
        Returns:
            List of change records with event_time and percentage change value
        """
        try:
            logger.debug("Calculating index changes")
            
            if not current_data:
                logger.info("No current data provided")
                return []
            
            if not prev_data:
                logger.info("No previous data provided")
                return []
            
            # Calculate changes using two-pointer approach
            changes = self._calculate_index_changes(current_data, prev_data, time_interval)
            
            logger.info(f"Calculated {len(changes)} index changes")
            return changes
            
        except Exception as e:
            logger.error(f"Error in get_index_change: {e}")
            raise
    
    def _calculate_index_changes(
        self, 
        current_data: List[Dict[str, Any]], 
        prev_data: List[Dict[str, Any]], 
        time_interval: int
    ) -> List[Dict[str, Any]]:
        """Calculate percentage changes using two-pointer approach."""
        
        # Sort data by event_time to ensure proper ordering
        current_sorted = sorted(current_data, key=lambda x: int(x["event_time"]))
        prev_sorted = sorted(prev_data, key=lambda x: int(x["event_time"]))
        
        changes = []
        j = 0  # pointer for prev_data
        
        for current_record in current_sorted:
            current_time = int(current_record["event_time"])
            current_value = Decimal(str(current_record["value"]))
            target_time = current_time - time_interval
            
            # Move j forward while the next item is still <= target_time
            while (j + 1 < len(prev_sorted) and 
                   int(prev_sorted[j + 1]["event_time"]) <= target_time):
                j += 1
            
            # Check if we found a valid previous value
            if (j < len(prev_sorted) and 
                int(prev_sorted[j]["event_time"]) <= target_time):
                
                prev_value = Decimal(str(prev_sorted[j]["value"]))
                
                # Skip division by zero
                if prev_value != 0:
                    # Match kwildb calculation exactly: ((current - previous) * 100) / previous
                    change_percent = ((current_value - prev_value) * Decimal('100')) / prev_value
                    changes.append({
                        "event_time": current_time,
                        "value": str(change_percent)
                    })
        
        return changes