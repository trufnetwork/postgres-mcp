COMPOSED_STREAM_RECORD_QUERY = """
  WITH RECURSIVE
  -- Query parameters
  query_vars AS (
      SELECT 
          LOWER({}) as data_provider,
          {} as stream_id,
          {} as from_param,
          {} as to_param,
          {} as frozen_at_param,
          {} as use_cache_param,
          9223372036854775000 as max_int8,
          COALESCE({}, 0) as effective_from,
          COALESCE({}, 9223372036854775000) as effective_to,
          COALESCE({}, 9223372036854775000) as effective_frozen_at
  ),
  
  -- Stream reference calculation
  stream_ref_calc AS (
      SELECT 
          -- Get the actual stream_ref from the streams table instead of concatenating
          (SELECT s.id FROM main.streams s 
            WHERE LOWER(s.data_provider) = qv.data_provider 
              AND s.stream_id = qv.stream_id 
            LIMIT 1) as stream_ref,
          qv.*
      FROM query_vars qv
  ),
  
  -- Latest mode check
  latest_mode_check AS (
      SELECT 
          (src.from_param IS NULL AND src.to_param IS NULL) as is_latest_mode,
          src.*
      FROM stream_ref_calc src
  ),
  
  -- Parent distinct start times
  parent_distinct_start_times AS (
      SELECT DISTINCT
          t.stream_ref,
          t.start_time
      FROM main.taxonomies t
      WHERE t.disabled_at IS NULL
  ),

  -- Parent next starts
  parent_next_starts AS (
      SELECT
          stream_ref,
          start_time,
          LEAD(start_time) OVER (PARTITION BY stream_ref ORDER BY start_time) as next_start_time
      FROM parent_distinct_start_times
  ),

  -- Taxonomy true segments
  taxonomy_true_segments AS (
      SELECT
          t.stream_ref,
          t.child_stream_ref,
          t.weight_for_segment,
          t.segment_start,
          COALESCE(pns.next_start_time, src.max_int8) - 1 AS segment_end
      FROM (
          SELECT
              tx.stream_ref,
              tx.child_stream_ref,
              tx.weight AS weight_for_segment,
              tx.start_time AS segment_start
          FROM main.taxonomies tx
          JOIN (
              SELECT
                  stream_ref, start_time,
                  MAX(group_sequence) as max_gs
              FROM main.taxonomies
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
      CROSS JOIN stream_ref_calc src
  ),

  -- Hierarchy
  hierarchy AS (
      -- Base case
      SELECT
          tts.stream_ref AS root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          tts.weight_for_segment AS raw_weight,
          tts.segment_start AS path_start,
          tts.segment_end AS path_end,
          1 AS level
      FROM taxonomy_true_segments tts
      CROSS JOIN stream_ref_calc src
      WHERE tts.stream_ref = src.stream_ref
        AND tts.segment_end >= COALESCE(
            (SELECT t_anchor_base.start_time FROM main.taxonomies t_anchor_base
              WHERE t_anchor_base.stream_ref = src.stream_ref
                AND t_anchor_base.disabled_at IS NULL 
                AND t_anchor_base.start_time <= src.effective_from
              ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC 
              LIMIT 1),
            0
        )
        AND tts.segment_start <= src.effective_to

      UNION ALL

      -- Recursive step
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
      CROSS JOIN stream_ref_calc src
      WHERE GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
        AND LEAST(h.path_end, tts.segment_end) >= COALESCE(
              (SELECT t_anchor_base.start_time FROM main.taxonomies t_anchor_base
              WHERE t_anchor_base.stream_ref = src.stream_ref
                AND t_anchor_base.disabled_at IS NULL 
                AND t_anchor_base.start_time <= src.effective_from
              ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC 
              LIMIT 1),
              0
          )
        AND GREATEST(h.path_start, tts.segment_start) <= src.effective_to
        AND h.level < 1000
  ),

  -- Hierarchy primitive paths
  hierarchy_primitive_paths AS (
      SELECT
          h.descendant_stream_ref AS primitive_stream_ref,
          h.raw_weight,
          h.path_start,
          h.path_end
      FROM hierarchy h
      WHERE EXISTS (
          SELECT 1 FROM main.streams s
          WHERE s.id = h.descendant_stream_ref
            AND s.stream_type = 'primitive'
      )
  ),

  -- Primitive weights
  primitive_weights AS (
      SELECT
          hpp.primitive_stream_ref,
          hpp.raw_weight,
          hpp.path_start AS group_sequence_start,
          hpp.path_end AS group_sequence_end
      FROM hierarchy_primitive_paths hpp
  ),

  -- Initial primitive states
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
          FROM main.primitive_events pe_inner
          CROSS JOIN stream_ref_calc src
          WHERE pe_inner.event_time <= src.effective_from
            AND EXISTS (
                SELECT 1 FROM primitive_weights pw_exists
                WHERE pw_exists.primitive_stream_ref = pe_inner.stream_ref
            )
            AND pe_inner.created_at <= src.effective_frozen_at
      ) pe
      WHERE pe.rn = 1
  ),

  -- Primitive events in interval
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
          FROM main.primitive_events pe_inner
          JOIN primitive_weights pw_check
              ON pe_inner.stream_ref = pw_check.primitive_stream_ref
              AND pe_inner.event_time >= pw_check.group_sequence_start
              AND pe_inner.event_time <= pw_check.group_sequence_end
          CROSS JOIN stream_ref_calc src
          WHERE pe_inner.event_time > src.effective_from
              AND pe_inner.event_time <= src.effective_to
              AND pe_inner.created_at <= src.effective_frozen_at
      ) pe
      WHERE pe.rn = 1
  ),

  -- All primitive points
  all_primitive_points AS (
      SELECT stream_ref, event_time, value FROM initial_primitive_states
      UNION ALL
      SELECT stream_ref, event_time, value FROM primitive_events_in_interval
  ),

  -- Primitive event changes
  primitive_event_changes AS (
      SELECT * FROM (
          SELECT stream_ref, event_time, value,
                  COALESCE(value - LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time), value)::numeric(36,18) AS delta_value
          FROM all_primitive_points
      ) calc WHERE delta_value != 0::numeric(36,18)
  ),

  -- First value times
  first_value_times AS (
      SELECT
          stream_ref,
          MIN(event_time) as first_value_time
      FROM all_primitive_points
      GROUP BY stream_ref
  ),

  -- Effective weight changes
  effective_weight_changes AS (
      -- Positive delta
      SELECT
          pw.primitive_stream_ref as stream_ref,
          GREATEST(pw.group_sequence_start, fvt.first_value_time) AS event_time,
          pw.raw_weight AS weight_delta
      FROM primitive_weights pw
      INNER JOIN first_value_times fvt
          ON pw.primitive_stream_ref = fvt.stream_ref
      WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
        AND pw.raw_weight != 0::numeric(36,18)

      UNION ALL

      -- Negative delta
      SELECT
          pw.primitive_stream_ref as stream_ref,
          pw.group_sequence_end + 1 AS event_time,
          -pw.raw_weight AS weight_delta
      FROM primitive_weights pw
      INNER JOIN first_value_times fvt
          ON pw.primitive_stream_ref = fvt.stream_ref
      CROSS JOIN stream_ref_calc src
      WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
        AND pw.raw_weight != 0::numeric(36,18)
        AND pw.group_sequence_end < (src.max_int8 - 1)
  ),

  -- Unified events
  unified_events AS (
      SELECT
          pec.stream_ref,
          pec.event_time,
          pec.delta_value,
          0::numeric(36,18) AS weight_delta,
          1 AS event_type_priority
      FROM primitive_event_changes pec

      UNION ALL

      SELECT
          ewc.stream_ref,
          ewc.event_time,
          0::numeric(36,18) AS delta_value,
          ewc.weight_delta,
          2 AS event_type_priority
      FROM effective_weight_changes ewc
  ),

  -- Primitive state timeline
  primitive_state_timeline AS (
      SELECT
          stream_ref,
          event_time,
          delta_value,
          weight_delta,
          COALESCE(LAG(value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as value_before_event,
          COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
      FROM (
          SELECT
              stream_ref,
              event_time,
              delta_value,
              weight_delta,
              event_type_priority,
              (SUM(delta_value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as value_after_event,
              (SUM(weight_delta) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
          FROM unified_events
      ) state_calc
  ),

  -- Final deltas
  final_deltas AS (
      SELECT
          event_time,
          SUM(
              (delta_value * weight_before_event) +
              (value_before_event * weight_delta) +
              (delta_value * weight_delta)
          )::numeric(72, 18) AS delta_ws,
          SUM(weight_delta)::numeric(36, 18) AS delta_sw
      FROM primitive_state_timeline
      GROUP BY event_time
      HAVING SUM(
              (delta_value * weight_before_event) +
              (value_before_event * weight_delta) +
              (delta_value * weight_delta)
          )::numeric(72, 18) != 0::numeric(72, 18)
          OR SUM(weight_delta)::numeric(36, 18) != 0::numeric(36, 18)
  ),

  -- Cumulative values
  cumulative_values AS (
      SELECT
          event_time,
          COALESCE(
              SUM(delta_ws) OVER (ORDER BY event_time ASC), 
              0::numeric(72,18)
          ) as cum_ws,
          COALESCE(
              SUM(delta_sw) OVER (ORDER BY event_time ASC), 
              0::numeric(36,18)
          ) as cum_sw
      FROM final_deltas
  ),

  -- Aggregated
  aggregated AS (
      SELECT 
          cv.event_time,
          CASE 
              WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
              ELSE cv.cum_ws / cv.cum_sw::numeric(72,18)
          END AS value
      FROM cumulative_values cv
  ),

  -- Result
  latest_result AS (
      SELECT event_time, value FROM aggregated
      CROSS JOIN latest_mode_check lmc
      WHERE lmc.is_latest_mode = true
      ORDER BY event_time DESC
      LIMIT 1
  ),
  
  range_result AS (
      SELECT event_time, value FROM aggregated agg
      CROSS JOIN stream_ref_calc src
      CROSS JOIN latest_mode_check lmc
      WHERE lmc.is_latest_mode = false
        AND agg.event_time >= src.effective_from
        AND agg.event_time <= src.effective_to
  ),
  
  result AS (
      SELECT event_time, value FROM latest_result
      UNION ALL
      SELECT event_time, value FROM range_result
  )
  
  SELECT 
      event_time, 
      value::NUMERIC(36,18) as value
  FROM result
  WHERE event_time IS NOT NULL
  ORDER BY event_time DESC;
"""