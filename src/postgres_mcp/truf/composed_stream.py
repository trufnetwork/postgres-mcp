"""
Composed Stream Tool for handling complex time series calculations.
Converts Kwil-DB stored procedure logic to PostgreSQL for composed streams.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

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
        
        Converts the Kwil-DB stored procedure logic to PostgreSQL and executes it.
        
        Args:
            data_provider: Stream deployer address (0x... format)
            stream_id: Target composed stream ID (starts with 'st')
            from_time: Start timestamp (inclusive), None for no lower bound
            to_time: End timestamp (inclusive), None for no upper bound  
            frozen_at: Created-at cutoff for time-travel queries
            use_cache: Whether to use cache (placeholder for future implementation)
            
        Returns:
            List of records with event_time and value fields
        """
        try:
            # Build the converted PostgreSQL query
            pg_query = self._build_composed_record_query()
            
            # Prepare parameters - convert None to SQL NULL
            query_params = [
                data_provider.lower(),  # $1: data_provider (lowercased)
                stream_id,              # $2: stream_id
                from_time,              # $3: from_time (can be None)
                to_time,                # $4: to_time (can be None)
                frozen_at,              # $5: frozen_at (can be None)
                use_cache               # $6: use_cache
            ]
            
            logger.debug(f"Executing composed stream query for {data_provider}/{stream_id} with params: {query_params}")
            
            # Execute the converted PostgreSQL query
            rows = await self.sql_driver.execute_query(pg_query, query_params)
            
            if not rows:
                logger.info(f"No records found for composed stream {data_provider}/{stream_id}")
                return []
            
            # Convert results to proper format
            records = []
            for row in rows:
                record = {
                    "event_time": row.cells.get("event_time"),
                    "value": str(row.cells.get("value", "0"))  # Convert to string to preserve precision
                }
                records.append(record)
            
            logger.info(f"Retrieved {len(records)} records for composed stream {data_provider}/{stream_id}")
            return records
            
        except Exception as e:
            logger.error(f"Error in get_record_composed for {data_provider}/{stream_id}: {e}")
            raise
    
    def _build_composed_record_query(self) -> str:
        """
        Convert the Kwil-DB stored procedure to PostgreSQL syntax.
        
        This converts the complex recursive CTE logic from Kwil-DB to standard PostgreSQL.
        
        Returns:
            PostgreSQL query string with parameter placeholders ($1, $2, etc.)
        """
        return """
        WITH RECURSIVE
        -- Query parameters and constants
        query_params AS (
            SELECT 
                LOWER($1) as data_provider,
                $2 as stream_id,
                COALESCE($3, 0) as effective_from,
                COALESCE($4, 9223372036854775000) as effective_to,
                COALESCE($5, 9223372036854775000) as effective_frozen_at,
                COALESCE($6, false) as use_cache,
                9223372036854775000 as max_int8
        ),
        
        -- Get the stream reference and validate it's a composed stream
        stream_validation AS (
            SELECT 
                s.data_provider || '/' || s.stream_id as stream_ref,
                s.data_provider,
                s.stream_id,
                s.stream_type
            FROM streams s
            CROSS JOIN query_params qp
            WHERE s.data_provider = qp.data_provider 
              AND s.stream_id = qp.stream_id
              AND s.stream_type = 'composed'
        ),
        
        -- Parent distinct start times - all unique start times where parents have taxonomy definitions
        parent_distinct_start_times AS (
            SELECT DISTINCT
                t.stream_ref,
                t.start_time
            FROM taxonomies t
            WHERE t.disabled_at IS NULL
        ),
        
        -- Parent next starts - determine the next start time for each parent's definition
        parent_next_starts AS (
            SELECT
                stream_ref,
                start_time,
                LEAD(start_time) OVER (PARTITION BY stream_ref ORDER BY start_time) as next_start_time
            FROM parent_distinct_start_times
        ),
        
        -- Taxonomy true segments - determine active time segments for each parent-child relationship
        taxonomy_true_segments AS (
            SELECT
                t.stream_ref,
                t.child_stream_ref,
                t.weight_for_segment,
                t.segment_start,
                COALESCE(pns.next_start_time, (SELECT max_int8 FROM query_params)) - 1 AS segment_end
            FROM (
                -- Select latest taxonomy version (highest group_sequence) for each parent/start_time
                SELECT
                    tx.stream_ref,
                    tx.child_stream_ref,
                    tx.weight AS weight_for_segment,
                    tx.start_time AS segment_start
                FROM taxonomies tx
                JOIN (
                    -- Find max group_sequence for each parent's start_time
                    SELECT
                        stream_ref, start_time,
                        MAX(group_sequence) as max_gs
                    FROM taxonomies
                    WHERE disabled_at IS NULL
                    GROUP BY stream_ref, start_time
                ) max_gs_filter
                ON tx.stream_ref = max_gs_filter.stream_ref
                   AND tx.start_time = max_gs_filter.start_time
                   AND tx.group_sequence = max_gs_filter.max_gs
                WHERE tx.disabled_at IS NULL
            ) t
            JOIN parent_next_starts pns
              ON t.stream_ref = pns.stream_ref
             AND t.segment_start = pns.start_time
        ),
        
        -- Hierarchy - recursively resolve the dependency tree
        hierarchy AS (
            -- Base case: Direct children of the root composed stream
            SELECT
                sv.stream_ref AS root_stream_ref,
                tts.child_stream_ref AS descendant_stream_ref,
                tts.weight_for_segment AS raw_weight,
                tts.segment_start AS path_start,
                tts.segment_end AS path_end,
                1 AS level
            FROM taxonomy_true_segments tts
            CROSS JOIN stream_validation sv
            CROSS JOIN query_params qp
            WHERE tts.stream_ref = sv.stream_ref
              AND tts.segment_end >= COALESCE(
                  (SELECT t_anchor.start_time FROM taxonomies t_anchor
                   WHERE t_anchor.stream_ref = sv.stream_ref
                     AND t_anchor.disabled_at IS NULL 
                     AND t_anchor.start_time <= qp.effective_from
                   ORDER BY t_anchor.start_time DESC, t_anchor.group_sequence DESC 
                   LIMIT 1),
                  0
              )
              AND tts.segment_start <= qp.effective_to

            UNION ALL

            -- Recursive step: Children of children
            SELECT
                h.root_stream_ref,
                tts.child_stream_ref AS descendant_stream_ref,
                (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
                GREATEST(h.path_start, tts.segment_start) AS path_start,
                LEAST(h.path_end, tts.segment_end) AS path_end,
                h.level + 1
            FROM hierarchy h
            JOIN taxonomy_true_segments tts
                ON h.descendant_stream_ref = tts.stream_ref
            CROSS JOIN query_params qp
            WHERE GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
              AND LEAST(h.path_end, tts.segment_end) >= COALESCE(
                   (SELECT t_anchor.start_time FROM taxonomies t_anchor
                    CROSS JOIN stream_validation sv
                    WHERE t_anchor.stream_ref = sv.stream_ref
                      AND t_anchor.disabled_at IS NULL 
                      AND t_anchor.start_time <= qp.effective_from
                    ORDER BY t_anchor.start_time DESC, t_anchor.group_sequence DESC 
                    LIMIT 1),
                   0
               )
              AND GREATEST(h.path_start, tts.segment_start) <= qp.effective_to
              AND h.level < 1000 -- Prevent infinite recursion
        ),
        
        -- Hierarchy primitive paths - filter to paths ending in primitive streams
        hierarchy_primitive_paths AS (
            SELECT
                h.descendant_stream_ref AS primitive_stream_ref,
                h.raw_weight,
                h.path_start,
                h.path_end
            FROM hierarchy h
            WHERE EXISTS (
                SELECT 1 FROM streams s
                WHERE s.data_provider || '/' || s.stream_id = h.descendant_stream_ref
                  AND s.stream_type = 'primitive'
            )
        ),
        
        -- Primitive weights - pass through each segment of primitive activity
        primitive_weights AS (
            SELECT
                hpp.primitive_stream_ref,
                hpp.raw_weight,
                hpp.path_start AS group_sequence_start,
                hpp.path_end AS group_sequence_end
            FROM hierarchy_primitive_paths hpp
        ),
        
        -- Initial primitive states - find latest value before/at effective_from
        initial_primitive_states AS (
            SELECT
                pe.stream_ref,
                pe.event_time,
                pe.value
            FROM (
                SELECT
                    pe_inner.stream_ref,
                    pe_inner.event_time,
                    pe_inner.value,
                    ROW_NUMBER() OVER (
                        PARTITION BY pe_inner.stream_ref
                        ORDER BY pe_inner.event_time DESC, pe_inner.created_at DESC
                    ) as rn
                FROM primitive_events pe_inner
                CROSS JOIN query_params qp
                WHERE pe_inner.event_time <= qp.effective_from
                  AND EXISTS (
                      SELECT 1 FROM primitive_weights pw
                      WHERE pw.primitive_stream_ref = pe_inner.stream_ref
                  )
                  AND pe_inner.created_at <= qp.effective_frozen_at
            ) pe
            WHERE pe.rn = 1
        ),
        
        -- Primitive events in interval - events strictly within (from, to]
        primitive_events_in_interval AS (
            SELECT
                pe.stream_ref,
                pe.event_time,
                pe.value
            FROM (
                SELECT
                    pe_inner.stream_ref,
                    pe_inner.event_time,
                    pe_inner.created_at,
                    pe_inner.value,
                    ROW_NUMBER() OVER (
                        PARTITION BY pe_inner.stream_ref, pe_inner.event_time
                        ORDER BY pe_inner.created_at DESC
                    ) as rn
                FROM primitive_events pe_inner
                JOIN primitive_weights pw
                    ON pe_inner.stream_ref = pw.primitive_stream_ref
                   AND pe_inner.event_time >= pw.group_sequence_start
                   AND pe_inner.event_time <= pw.group_sequence_end
                CROSS JOIN query_params qp
                WHERE pe_inner.event_time > qp.effective_from
                  AND pe_inner.event_time <= qp.effective_to
                  AND pe_inner.created_at <= qp.effective_frozen_at
            ) pe
            WHERE pe.rn = 1
        ),
        
        -- All primitive points - combine initial states and interval events
        all_primitive_points AS (
            SELECT stream_ref, event_time, value FROM initial_primitive_states
            UNION ALL
            SELECT stream_ref, event_time, value FROM primitive_events_in_interval
        ),
        
        -- Primitive event changes - calculate value deltas
        primitive_event_changes AS (
            SELECT stream_ref, event_time, value, delta_value
            FROM (
                SELECT 
                    stream_ref, 
                    event_time, 
                    value,
                    COALESCE(
                        value - LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time), 
                        value
                    )::NUMERIC(36,18) AS delta_value
                FROM all_primitive_points
            ) calc 
            WHERE delta_value != 0::NUMERIC(36,18)
        ),
        
        -- First value times - when each primitive first provides a value
        first_value_times AS (
            SELECT
                stream_ref,
                MIN(event_time) as first_value_time
            FROM all_primitive_points
            GROUP BY stream_ref
        ),
        
        -- Effective weight changes - when weights effectively start/stop
        effective_weight_changes AS (
            -- Positive delta: weight starts (at later of definition start or first value)
            SELECT
                pw.primitive_stream_ref as stream_ref,
                GREATEST(pw.group_sequence_start, fvt.first_value_time) AS event_time,
                pw.raw_weight AS weight_delta
            FROM primitive_weights pw
            INNER JOIN first_value_times fvt
                ON pw.primitive_stream_ref = fvt.stream_ref
            WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
              AND pw.raw_weight != 0::NUMERIC(36,18)

            UNION ALL

            -- Negative delta: weight ends
            SELECT
                pw.primitive_stream_ref as stream_ref,
                pw.group_sequence_end + 1 AS event_time,
                -pw.raw_weight AS weight_delta
            FROM primitive_weights pw
            INNER JOIN first_value_times fvt
                ON pw.primitive_stream_ref = fvt.stream_ref
            CROSS JOIN query_params qp
            WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
              AND pw.raw_weight != 0::NUMERIC(36,18)
              AND pw.group_sequence_end < (qp.max_int8 - 1)
        ),
        
        -- Unified events - combine value and weight changes
        unified_events AS (
            SELECT
                pec.stream_ref,
                pec.event_time,
                pec.delta_value,
                0::NUMERIC(36,18) AS weight_delta,
                1 AS event_type_priority
            FROM primitive_event_changes pec

            UNION ALL

            SELECT
                ewc.stream_ref,
                ewc.event_time,
                0::NUMERIC(36,18) AS delta_value,
                ewc.weight_delta,
                2 AS event_type_priority
            FROM effective_weight_changes ewc
        ),
        
        -- Primitive state timeline - calculate states before each event
        primitive_state_timeline AS (
            SELECT
                stream_ref,
                event_time,
                delta_value,
                weight_delta,
                COALESCE(
                    LAG(value_after_event, 1, 0::NUMERIC(36,18)) 
                    OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 
                    0::NUMERIC(36,18)
                ) as value_before_event,
                COALESCE(
                    LAG(weight_after_event, 1, 0::NUMERIC(36,18)) 
                    OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 
                    0::NUMERIC(36,18)
                ) as weight_before_event
            FROM (
                SELECT
                    stream_ref,
                    event_time,
                    delta_value,
                    weight_delta,
                    event_type_priority,
                    (SUM(delta_value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::NUMERIC(36,18) as value_after_event,
                    (SUM(weight_delta) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::NUMERIC(36,18) as weight_after_event
                FROM unified_events
            ) state_calc
        ),
        
        -- Final deltas - aggregate changes per time point
        final_deltas AS (
            SELECT
                event_time,
                SUM(
                    (delta_value * weight_before_event) +
                    (value_before_event * weight_delta) +
                    (delta_value * weight_delta)
                )::NUMERIC(72, 18) AS delta_ws,
                SUM(weight_delta)::NUMERIC(36, 18) AS delta_sw
            FROM primitive_state_timeline
            GROUP BY event_time
            HAVING SUM(
                    (delta_value * weight_before_event) +
                    (value_before_event * weight_delta) +
                    (delta_value * weight_delta)
                )::NUMERIC(72, 18) != 0::NUMERIC(72, 18)
                OR SUM(weight_delta)::NUMERIC(36, 18) != 0::NUMERIC(36, 18)
        ),
        
        -- Cumulative values - calculate running totals
        cumulative_values AS (
            SELECT
                event_time,
                (COALESCE(
                    SUM(delta_ws) OVER (ORDER BY event_time ASC), 
                    0::NUMERIC(72,18)
                )) as cum_ws,
                (COALESCE(
                    SUM(delta_sw) OVER (ORDER BY event_time ASC), 
                    0::NUMERIC(36,18)
                )) as cum_sw
            FROM final_deltas
        ),
        
        -- Aggregated - compute final weighted averages
        aggregated AS (
            SELECT 
                cv.event_time,
                CASE 
                    WHEN cv.cum_sw = 0::NUMERIC(36,18) THEN 0::NUMERIC(72,18)
                    ELSE cv.cum_ws / cv.cum_sw::NUMERIC(72,18)
                END AS value
            FROM cumulative_values cv
        ),
        
        -- Handle special case: if both from_time and to_time are NULL, return latest record
        latest_record_case AS (
            SELECT event_time, value
            FROM aggregated
            CROSS JOIN query_params qp
            WHERE $3 IS NULL AND $4 IS NULL  -- Both from_time and to_time are NULL
            ORDER BY event_time DESC
            LIMIT 1
        ),
        
        -- Handle range case: return records in specified range
        range_case AS (
            SELECT event_time, value
            FROM aggregated agg
            CROSS JOIN query_params qp
            WHERE NOT ($3 IS NULL AND $4 IS NULL)  -- Not the latest-record case
              AND agg.event_time >= qp.effective_from
              AND agg.event_time <= qp.effective_to
        )
        
        -- Final result
        SELECT event_time, value::NUMERIC(36,18) as value
        FROM latest_record_case
        WHERE EXISTS (SELECT 1 FROM latest_record_case)
        
        UNION ALL
        
        SELECT event_time, value::NUMERIC(36,18) as value  
        FROM range_case
        WHERE NOT EXISTS (SELECT 1 FROM latest_record_case)
        
        ORDER BY event_time ASC;
        """