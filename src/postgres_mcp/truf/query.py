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

COMPOSED_STREAM_INDEX_QUERY = """
  WITH RECURSIVE
  -- Query parameters
  query_vars AS (
      SELECT 
          LOWER({}) as data_provider,
          {} as stream_id,
          {} as from_param,
          {} as to_param,
          {} as frozen_at_param,
          {} as base_time_param,
          {} as use_cache_param,
          9223372036854775000 as max_int8,
          COALESCE({}, 0) as effective_from,
          COALESCE({}, 9223372036854775000) as effective_to,
          COALESCE({}, 9223372036854775000) as effective_frozen_at
  ),
  
  -- Base time calculation with metadata lookup (like kwildb's get_latest_metadata_int)
  base_time_calc AS (
      SELECT 
          qv.*,
          CASE 
              WHEN qv.base_time_param IS NOT NULL THEN qv.base_time_param::BIGINT
              ELSE COALESCE(
                  (SELECT m.value_i FROM main.metadata m
                   JOIN main.streams s ON m.stream_ref = s.id
                   WHERE LOWER(s.data_provider) = qv.data_provider
                     AND s.stream_id = qv.stream_id
                     AND m.metadata_key = 'default_base_time'
                     AND m.disabled_at IS NULL
                   ORDER BY m.created_at DESC
                   LIMIT 1),
                  0::BIGINT
              )
          END as effective_base_time
      FROM query_vars qv
  ),
  
  -- Stream reference calculation
  stream_ref_calc AS (
      SELECT 
          (SELECT s.id FROM main.streams s 
            WHERE LOWER(s.data_provider) = btc.data_provider 
              AND s.stream_id = btc.stream_id 
            LIMIT 1) as stream_ref,
          btc.*
      FROM base_time_calc btc
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

  -- Hierarchy with effective weight calculation
  hierarchy AS (
      -- Base case
      SELECT
          tts.stream_ref AS root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          tts.weight_for_segment AS raw_weight,
          tts.weight_for_segment AS effective_weight,
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

      -- Recursive step with effective weight calculation
      SELECT
          h.root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
          (h.effective_weight * (
              tts.weight_for_segment / 
              NULLIF((SELECT SUM(sibling_tts.weight_for_segment) 
                     FROM taxonomy_true_segments sibling_tts 
                     WHERE sibling_tts.stream_ref = h.descendant_stream_ref
                       AND sibling_tts.segment_start = tts.segment_start
                       AND sibling_tts.segment_end = tts.segment_end), 0)
          ))::NUMERIC(36,18) AS effective_weight,
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
        AND h.level < 10
  ),

  -- Hierarchy primitive paths using effective_weight
  hierarchy_primitive_paths AS (
      SELECT
          h.descendant_stream_ref AS primitive_stream_ref,
          h.effective_weight AS raw_weight,
          h.path_start AS group_sequence_start,
          h.path_end AS group_sequence_end
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
          hpp.group_sequence_start,
          hpp.group_sequence_end
      FROM hierarchy_primitive_paths hpp
  ),

  -- Cleaned event times
  cleaned_event_times AS (
      SELECT DISTINCT event_time
      FROM (
          SELECT pe.event_time
          FROM main.primitive_events pe
          JOIN primitive_weights pw
            ON pe.stream_ref = pw.primitive_stream_ref
           AND pe.event_time >= pw.group_sequence_start
           AND pe.event_time <= pw.group_sequence_end
          CROSS JOIN stream_ref_calc src
          WHERE pe.event_time > src.effective_from
            AND pe.event_time <= src.effective_to
            AND pe.created_at <= src.effective_frozen_at

          UNION

          SELECT pw.group_sequence_start AS event_time
          FROM primitive_weights pw
          CROSS JOIN stream_ref_calc src
          WHERE pw.group_sequence_start > src.effective_from
            AND pw.group_sequence_start <= src.effective_to
      ) all_times_in_range

      UNION

      SELECT event_time FROM (
          SELECT event_time
          FROM (
              SELECT pe.event_time
              FROM main.primitive_events pe
              JOIN primitive_weights pw
                ON pe.stream_ref = pw.primitive_stream_ref
               AND pe.event_time >= pw.group_sequence_start
               AND pe.event_time <= pw.group_sequence_end
              CROSS JOIN stream_ref_calc src
              WHERE pe.event_time <= src.effective_from
                AND pe.created_at <= src.effective_frozen_at

              UNION

              SELECT pw.group_sequence_start AS event_time
              FROM primitive_weights pw
              CROSS JOIN stream_ref_calc src
              WHERE pw.group_sequence_start <= src.effective_from
          ) all_times_before
          ORDER BY event_time DESC
          LIMIT 1
      ) as anchor_event
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

  -- Distinct primitives for base calculation
  distinct_primitives_for_base AS (
      SELECT DISTINCT stream_ref
      FROM all_primitive_points
  ),

  -- Base value calculation (key for index calculation)
  primitive_base_values AS (
      SELECT
          dp.stream_ref,
          COALESCE(
              -- Priority 1: Exact match at base_time
              (SELECT pe.value FROM main.primitive_events pe
               CROSS JOIN stream_ref_calc src
               WHERE pe.stream_ref = dp.stream_ref
                 AND pe.event_time = src.effective_base_time
                 AND pe.created_at <= src.effective_frozen_at
               ORDER BY pe.created_at DESC LIMIT 1),

              -- Priority 2: Latest before base_time
              (SELECT pe.value FROM main.primitive_events pe
               CROSS JOIN stream_ref_calc src
               WHERE pe.stream_ref = dp.stream_ref
                 AND pe.event_time < src.effective_base_time
                 AND pe.created_at <= src.effective_frozen_at
               ORDER BY pe.event_time DESC, pe.created_at DESC LIMIT 1),

              -- Priority 3: Earliest after base_time
              (SELECT pe.value FROM main.primitive_events pe
               CROSS JOIN stream_ref_calc src
               WHERE pe.stream_ref = dp.stream_ref
                 AND pe.event_time > src.effective_base_time
                 AND pe.created_at <= src.effective_frozen_at
               ORDER BY pe.event_time ASC, pe.created_at DESC LIMIT 1),

              1::numeric(36,18) -- Default value
          )::numeric(36,18) AS base_value
      FROM distinct_primitives_for_base dp
  ),

  -- Calculate index value changes (key difference from get_record_composed)
  primitive_event_changes AS (
      SELECT
          calc.stream_ref,
          calc.event_time,
          calc.value,
          calc.delta_value,
          -- Calculate the change in indexed value (percentage change from base)
          CASE
              WHEN COALESCE(pbv.base_value, 0::numeric(36,18)) = 0::numeric(36,18) 
              THEN 0::numeric(36,18)
              ELSE (calc.delta_value * 100::numeric(36,18) / pbv.base_value)::numeric(36,18)
          END AS delta_indexed_value
      FROM (
          SELECT stream_ref, event_time, value,
                 COALESCE(value - LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time), value)::numeric(36,18) AS delta_value
          FROM all_primitive_points
      ) calc
      JOIN primitive_base_values pbv
          ON calc.stream_ref = pbv.stream_ref
      WHERE calc.delta_value != 0::numeric(36,18)
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

  -- Unified events using indexed values
  unified_events AS (
      SELECT
          pec.stream_ref,
          pec.event_time,
          pec.delta_indexed_value,
          0::numeric(36,18) AS weight_delta,
          1 AS event_type_priority
      FROM primitive_event_changes pec

      UNION ALL

      SELECT
          ewc.stream_ref,
          ewc.event_time,
          0::numeric(36,18) AS delta_indexed_value,
          ewc.weight_delta,
          2 AS event_type_priority
      FROM effective_weight_changes ewc
  ),

  -- Primitive state timeline using indexed values
  primitive_state_timeline AS (
      SELECT
          stream_ref,
          event_time,
          delta_indexed_value,
          weight_delta,
          COALESCE(LAG(indexed_value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as indexed_value_before_event,
          COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
      FROM (
          SELECT
              stream_ref,
              event_time,
              delta_indexed_value,
              weight_delta,
              event_type_priority,
              (SUM(delta_indexed_value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as indexed_value_after_event,
              (SUM(weight_delta) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
          FROM unified_events
      ) state_calc
  ),

  -- Final deltas using indexed values
  final_deltas AS (
      SELECT
          event_time,
          SUM(
              (delta_indexed_value * weight_before_event) +
              (indexed_value_before_event * weight_delta) +
              (delta_indexed_value * weight_delta)
          )::numeric(72, 18) AS delta_ws_indexed,
          SUM(weight_delta)::numeric(36, 18) AS delta_sw
      FROM primitive_state_timeline
      GROUP BY event_time
      HAVING SUM(
              (delta_indexed_value * weight_before_event) +
              (indexed_value_before_event * weight_delta) +
              (delta_indexed_value * weight_delta)
          )::numeric(72, 18) != 0::numeric(72, 18)
          OR SUM(weight_delta)::numeric(36, 18) != 0::numeric(36, 18)
  ),

  -- All combined times
  all_combined_times AS (
      SELECT time_point FROM (
          SELECT event_time as time_point FROM final_deltas
          UNION
          SELECT event_time as time_point FROM cleaned_event_times
      ) distinct_times
  ),

  -- Cumulative values using indexed weighted sum
  cumulative_values AS (
      SELECT
          act.time_point as event_time,
          COALESCE(
              SUM(fd.delta_ws_indexed) OVER (ORDER BY act.time_point ASC), 
              0::numeric(72,18)
          ) as cum_ws_indexed,
          COALESCE(
              SUM(fd.delta_sw) OVER (ORDER BY act.time_point ASC), 
              0::numeric(36,18)
          ) as cum_sw
      FROM all_combined_times act
      LEFT JOIN final_deltas fd ON fd.event_time = act.time_point
  ),

  -- Aggregated index values
  aggregated AS (
      SELECT 
          cv.event_time,
          CASE 
              WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
              ELSE cv.cum_ws_indexed / cv.cum_sw::numeric(72,18)
          END AS value
      FROM cumulative_values cv
  ),

  -- LOCF logic (same as get_record_composed)
  real_change_times AS (
      SELECT DISTINCT event_time AS time_point
      FROM final_deltas
      UNION
      SELECT DISTINCT event_time
      FROM primitive_events_in_interval
  ),

  anchor_time_calc AS (
      SELECT MAX(time_point) as anchor_time
      FROM real_change_times
      CROSS JOIN stream_ref_calc src
      WHERE time_point < src.effective_from
  ),

  final_mapping AS (
      SELECT agg.event_time, agg.value,
             (SELECT MAX(rct.time_point) FROM real_change_times rct WHERE rct.time_point <= agg.event_time) AS effective_time,
             EXISTS (SELECT 1 FROM real_change_times rct WHERE rct.time_point = agg.event_time) AS query_time_had_real_change
      FROM aggregated agg
  ),

  filtered_mapping AS (
      SELECT fm.*
      FROM final_mapping fm
      JOIN anchor_time_calc atc ON 1=1
      CROSS JOIN stream_ref_calc src
      WHERE (fm.event_time >= src.effective_from AND fm.event_time <= src.effective_to)
         OR (atc.anchor_time IS NOT NULL AND fm.event_time = atc.anchor_time)
  ),

  range_check AS (
      SELECT EXISTS (
          SELECT 1 FROM final_mapping fm_check
          CROSS JOIN stream_ref_calc src
          WHERE fm_check.event_time >= src.effective_from
            AND fm_check.event_time <= src.effective_to
      ) AS range_has_direct_hits
  ),

  locf_applied AS (
      SELECT
          fm.*,
          rc.range_has_direct_hits,
          atc.anchor_time,
          CASE 
              WHEN fm.query_time_had_real_change
                  THEN fm.event_time 
                  ELSE fm.effective_time 
          END as final_event_time
      FROM filtered_mapping fm
      JOIN range_check rc ON 1=1
      JOIN anchor_time_calc atc ON 1=1
  ),

  direct_hits AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      CROSS JOIN stream_ref_calc src
      WHERE la.event_time >= src.effective_from
        AND la.event_time <= src.effective_to
        AND la.final_event_time IS NOT NULL
  ),

  anchor_hit AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      CROSS JOIN stream_ref_calc src
      WHERE la.anchor_time IS NOT NULL
        AND la.event_time = la.anchor_time
        AND src.effective_from > la.anchor_time
        AND la.final_event_time IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM locf_applied dh
            CROSS JOIN stream_ref_calc src2
            WHERE dh.event_time = src2.effective_from
        )
  ),

  result AS (
      SELECT event_time, value FROM direct_hits
      UNION ALL
      SELECT event_time, value FROM anchor_hit
  )
  
  SELECT DISTINCT event_time, value 
  FROM result
  ORDER BY event_time ASC;
"""

TAXONOMIES_QUERY = """
  WITH stream_ref_lookup AS (
      SELECT s.id as stream_ref
      FROM main.streams s
      WHERE LOWER(s.data_provider) = LOWER({})
        AND s.stream_id = {}
      LIMIT 1
  ),
  latest_group_sequence AS (
      SELECT MAX(t.group_sequence) as max_group_sequence
      FROM main.taxonomies t
      CROSS JOIN stream_ref_lookup srl
      WHERE t.stream_ref = srl.stream_ref
        AND t.disabled_at IS NULL
  )
  SELECT
      parent_s.data_provider,
      parent_s.stream_id,
      child_s.data_provider as child_data_provider,
      child_s.stream_id as child_stream_id,
      t.weight,
      t.created_at,
      t.group_sequence,
      t.start_time AS start_date
  FROM main.taxonomies t
  JOIN main.streams parent_s ON t.stream_ref = parent_s.id
  JOIN main.streams child_s ON t.child_stream_ref = child_s.id
  CROSS JOIN stream_ref_lookup srl
  CROSS JOIN latest_group_sequence lgs
  WHERE t.disabled_at IS NULL
    AND t.stream_ref = srl.stream_ref
    AND (
        CASE 
            WHEN {} = true THEN t.group_sequence = lgs.max_group_sequence
            ELSE true
        END
    )
  ORDER BY t.group_sequence DESC, t.created_at DESC;
"""


STREAM_REF_QUERY = """
  SELECT s.id as stream_ref
  FROM main.streams s
  WHERE LOWER(s.data_provider) = LOWER({})
    AND s.stream_id = {}
  LIMIT 1;
"""

PRIMITIVE_STREAM_RECORD_QUERY = """
   WITH
    -- Get base records within time range
    interval_records AS (
        SELECT
            pe.event_time,
            pe.value,
            ROW_NUMBER() OVER (
                PARTITION BY pe.event_time
                ORDER BY pe.created_at DESC
            ) as rn
        FROM main.primitive_events pe
        WHERE pe.stream_ref = {}
            AND pe.created_at <= {}
            AND pe.event_time > {}
            AND pe.event_time <= {}
    ),

    -- Get anchor at or before from date
    anchor_record AS (
        SELECT pe.event_time, pe.value
        FROM main.primitive_events pe
        WHERE pe.stream_ref = {}
            AND pe.event_time <= {}
            AND pe.created_at <= {}
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    ),

    -- Combine results with gap filling logic
    combined_results AS (
        -- Add gap filler if needed
        SELECT event_time, value FROM anchor_record
        UNION ALL
        -- Add filtered base records
        SELECT event_time, value FROM interval_records
        WHERE rn = 1
    )
    -- Final selection
    SELECT event_time, value FROM combined_results
    ORDER BY event_time ASC;
"""

PRIMITIVE_STREAM_LAST_RECORD_QUERY = """
  SELECT pe.event_time, pe.value
  FROM main.primitive_events pe
  WHERE pe.stream_ref = {}
    AND pe.event_time < {}
    AND pe.created_at <= {}
  ORDER BY pe.event_time DESC, pe.created_at DESC
  LIMIT 1;
"""
