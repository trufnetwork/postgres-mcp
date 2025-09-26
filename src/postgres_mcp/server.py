# ruff: noqa: B008
import argparse
import asyncio
import logging
import os
import signal
import sys
from enum import Enum
from typing import Any
from typing import List
from typing import Literal
from typing import Optional
from typing import Union

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from pydantic import validate_call

from postgres_mcp.index.dta_calc import DatabaseTuningAdvisor
from postgres_mcp.truf.primitive_stream import PrimitiveStreamTool

from .artifacts import ErrorResult
from .artifacts import ExplainPlanArtifact
from .database_health import DatabaseHealthTool
from .database_health import HealthType
from .explain import ExplainPlanTool
from .index.index_opt_base import MAX_NUM_INDEX_TUNING_QUERIES
from .index.llm_opt import LLMOptimizerTool
from .index.presentation import TextPresentation
from .sql import DbConnPool
from .sql import SafeSqlDriver
from .sql import SqlDriver
from .sql import check_hypopg_installation_status
from .sql import obfuscate_password
from .top_queries import TopQueriesCalc
from .truf.composed_stream import ComposedStreamTool
from .truf.general import GeneralStreamTool

# Initialize FastMCP with default settings
mcp = FastMCP("postgres-mcp")

# Constants
PG_STAT_STATEMENTS = "pg_stat_statements"
HYPOPG_EXTENSION = "hypopg"

ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


class AccessMode(str, Enum):
    """SQL access modes for the server."""

    UNRESTRICTED = "unrestricted"  # Unrestricted access
    RESTRICTED = "restricted"  # Read-only with safety features


# Global variables
db_connection = DbConnPool()
current_access_mode = AccessMode.UNRESTRICTED
shutdown_in_progress = False


async def get_sql_driver() -> Union[SqlDriver, SafeSqlDriver]:
    """Get the appropriate SQL driver based on the current access mode."""
    base_driver = SqlDriver(conn=db_connection)

    if current_access_mode == AccessMode.RESTRICTED:
        logger.debug("Using SafeSqlDriver with restrictions (RESTRICTED mode)")
        return SafeSqlDriver(sql_driver=base_driver, timeout=30)  # 30 second timeout
    else:
        logger.debug("Using unrestricted SqlDriver (UNRESTRICTED mode)")
        return base_driver


def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")


@mcp.tool(description="List all schemas in the database")
async def list_schemas() -> ResponseType:
    """List all schemas in the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
                    WHEN schema_name = 'information_schema' THEN 'System Information Schema'
                    ELSE 'User Schema'
                END as schema_type
            FROM information_schema.schemata
            ORDER BY schema_type, schema_name
            """
        )
        schemas = [row.cells for row in rows] if rows else []
        return format_text_response(schemas)
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return format_error_response(str(e))


@mcp.tool(description="List objects in a schema")
async def list_objects(
    schema_name: str = Field(description="Schema name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """List objects of a given type in a schema."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            table_type = "BASE TABLE" if object_type == "table" else "VIEW"
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = {} AND table_type = {}
                ORDER BY table_name
                """,
                [schema_name, table_type],
            )
            objects = (
                [{"schema": row.cells["table_schema"], "name": row.cells["table_name"], "type": row.cells["table_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type
                FROM information_schema.sequences
                WHERE sequence_schema = {}
                ORDER BY sequence_name
                """,
                [schema_name],
            )
            objects = (
                [{"schema": row.cells["sequence_schema"], "name": row.cells["sequence_name"], "data_type": row.cells["data_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "extension":
            # Extensions are not schema-specific
            rows = await sql_driver.execute_query(
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                ORDER BY extname
                """
            )
            objects = (
                [{"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]} for row in rows]
                if rows
                else []
            )

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(objects)
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Show detailed information about a database object")
async def get_object_details(
    schema_name: str = Field(description="Schema name"),
    object_name: str = Field(description="Object name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """Get detailed information about a database object."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            # Get columns
            col_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = {} AND table_name = {}
                ORDER BY ordinal_position
                """,
                [schema_name, object_name],
            )
            columns = (
                [
                    {
                        "column": r.cells["column_name"],
                        "data_type": r.cells["data_type"],
                        "is_nullable": r.cells["is_nullable"],
                        "default": r.cells["column_default"],
                    }
                    for r in col_rows
                ]
                if col_rows
                else []
            )

            # Get constraints
            con_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                FROM information_schema.table_constraints AS tc
                LEFT JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = {} AND tc.table_name = {}
                """,
                [schema_name, object_name],
            )

            constraints = {}
            if con_rows:
                for row in con_rows:
                    cname = row.cells["constraint_name"]
                    ctype = row.cells["constraint_type"]
                    col = row.cells["column_name"]

                    if cname not in constraints:
                        constraints[cname] = {"type": ctype, "columns": []}
                    if col:
                        constraints[cname]["columns"].append(col)

            constraints_list = [{"name": name, **data} for name, data in constraints.items()]

            # Get indexes
            idx_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = {} AND tablename = {}
                """,
                [schema_name, object_name],
            )

            indexes = [{"name": r.cells["indexname"], "definition": r.cells["indexdef"]} for r in idx_rows] if idx_rows else []

            result = {
                "basic": {"schema": schema_name, "name": object_name, "type": object_type},
                "columns": columns,
                "constraints": constraints_list,
                "indexes": indexes,
            }

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type, start_value, increment
                FROM information_schema.sequences
                WHERE sequence_schema = {} AND sequence_name = {}
                """,
                [schema_name, object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {
                    "schema": row.cells["sequence_schema"],
                    "name": row.cells["sequence_name"],
                    "data_type": row.cells["data_type"],
                    "start_value": row.cells["start_value"],
                    "increment": row.cells["increment"],
                }
            else:
                result = {}

        elif object_type == "extension":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                WHERE extname = {}
                """,
                [object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]}
            else:
                result = {}

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting object details: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Explains the execution plan for a SQL query, showing how the database will execute it and provides detailed cost estimates.")
async def explain_query(
    sql: str = Field(description="SQL query to explain"),
    analyze: bool = Field(
        description="When True, actually runs the query to show real execution statistics instead of estimates. "
        "Takes longer but provides more accurate information.",
        default=False,
    ),
    hypothetical_indexes: list[dict[str, Any]] = Field(
        description="""A list of hypothetical indexes to simulate. Each index must be a dictionary with these keys:
    - 'table': The table name to add the index to (e.g., 'users')
    - 'columns': List of column names to include in the index (e.g., ['email'] or ['last_name', 'first_name'])
    - 'using': Optional index method (default: 'btree', other options include 'hash', 'gist', etc.)

Examples: [
    {"table": "users", "columns": ["email"], "using": "btree"},
    {"table": "orders", "columns": ["user_id", "created_at"]}
]
If there is no hypothetical index, you can pass an empty list.""",
        default=[],
    ),
) -> ResponseType:
    """
    Explains the execution plan for a SQL query.

    Args:
        sql: The SQL query to explain
        analyze: When True, actually runs the query for real statistics
        hypothetical_indexes: Optional list of indexes to simulate
    """
    try:
        sql_driver = await get_sql_driver()
        explain_tool = ExplainPlanTool(sql_driver=sql_driver)
        result: ExplainPlanArtifact | ErrorResult | None = None

        # If hypothetical indexes are specified, check for HypoPG extension
        if hypothetical_indexes and len(hypothetical_indexes) > 0:
            if analyze:
                return format_error_response("Cannot use analyze and hypothetical indexes together")
            try:
                # Use the common utility function to check if hypopg is installed
                (
                    is_hypopg_installed,
                    hypopg_message,
                ) = await check_hypopg_installation_status(sql_driver)

                # If hypopg is not installed, return the message
                if not is_hypopg_installed:
                    return format_text_response(hypopg_message)

                # HypoPG is installed, proceed with explaining with hypothetical indexes
                result = await explain_tool.explain_with_hypothetical_indexes(sql, hypothetical_indexes)
            except Exception:
                raise  # Re-raise the original exception
        elif analyze:
            try:
                # Use EXPLAIN ANALYZE
                result = await explain_tool.explain_analyze(sql)
            except Exception:
                raise  # Re-raise the original exception
        else:
            try:
                # Use basic EXPLAIN
                result = await explain_tool.explain(sql)
            except Exception:
                raise  # Re-raise the original exception

        if result and isinstance(result, ExplainPlanArtifact):
            return format_text_response(result.to_text())
        else:
            error_message = "Error processing explain plan"
            if isinstance(result, ErrorResult):
                error_message = result.to_text()
            return format_error_response(error_message)
    except Exception as e:
        logger.error(f"Error explaining query: {e}")
        return format_error_response(str(e))


# Query function declaration without the decorator - we'll add it dynamically based on access mode
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([r.cells for r in rows]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Analyze frequently executed queries in the database and recommend optimal indexes")
@validate_call
async def analyze_workload_indexes(
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze frequently executed queries in the database and recommend optimal indexes."""
    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_workload(max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing workload: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Analyze a list of (up to 10) SQL queries and recommend optimal indexes")
@validate_call
async def analyze_query_indexes(
    queries: list[str] = Field(description="List of Query strings to analyze"),
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze a list of SQL queries and recommend optimal indexes."""
    if len(queries) == 0:
        return format_error_response("Please provide a non-empty list of queries to analyze.")
    if len(queries) > MAX_NUM_INDEX_TUNING_QUERIES:
        return format_error_response(f"Please provide a list of up to {MAX_NUM_INDEX_TUNING_QUERIES} queries to analyze.")

    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_queries(queries=queries, max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing queries: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyzes database health. Here are the available health checks:\n"
    "- index - checks for invalid, duplicate, and bloated indexes\n"
    "- connection - checks the number of connection and their utilization\n"
    "- vacuum - checks vacuum health for transaction id wraparound\n"
    "- sequence - checks sequences at risk of exceeding their maximum value\n"
    "- replication - checks replication health including lag and slots\n"
    "- buffer - checks for buffer cache hit rates for indexes and tables\n"
    "- constraint - checks for invalid constraints\n"
    "- all - runs all checks\n"
    "You can optionally specify a single health check or a comma-separated list of health checks. The default is 'all' checks."
)
async def analyze_db_health(
    health_type: str = Field(
        description=f"Optional. Valid values are: {', '.join(sorted([t.value for t in HealthType]))}.",
        default="all",
    ),
) -> ResponseType:
    """Analyze database health for specified components.

    Args:
        health_type: Comma-separated list of health check types to perform.
                    Valid values: index, connection, vacuum, sequence, replication, buffer, constraint, all
    """
    health_tool = DatabaseHealthTool(await get_sql_driver())
    result = await health_tool.health(health_type=health_type)
    return format_text_response(result)


@mcp.tool(
    name="get_top_queries",
    description=f"Reports the slowest or most resource-intensive queries using data from the '{PG_STAT_STATEMENTS}' extension.",
)
async def get_top_queries(
    sort_by: str = Field(
        description="Ranking criteria: 'total_time' for total execution time or 'mean_time' for mean execution time per call, or 'resources' "
        "for resource-intensive queries",
        default="resources",
    ),
    limit: int = Field(description="Number of queries to return when ranking based on mean_time or total_time", default=10),
) -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        top_queries_tool = TopQueriesCalc(sql_driver=sql_driver)

        if sort_by == "resources":
            result = await top_queries_tool.get_top_resource_queries()
            return format_text_response(result)
        elif sort_by == "mean_time" or sort_by == "total_time":
            # Map the sort_by values to what get_top_queries_by_time expects
            result = await top_queries_tool.get_top_queries_by_time(limit=limit, sort_by="mean" if sort_by == "mean_time" else "total")
        else:
            return format_error_response("Invalid sort criteria. Please use 'resources' or 'mean_time' or 'total_time'.")
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting slow queries: {e}")
        return format_error_response(str(e))

@mcp.tool(description="Get records from COMPOSED streams only (stream_type='composed'). Use check_stream_type first if unsure.")
async def get_composed_stream_records(
    data_provider: str = Field(description="Stream deployer address (0x... format, 42 characters)"),
    stream_id: str = Field(description="Composed stream ID (starts with 'st', 32 characters total)"),
    from_time: Optional[int] = Field(description="Start timestamp (inclusive). If both from_time and to_time are omitted, returns latest record only.", default=None),
    to_time: Optional[int] = Field(description="End timestamp (inclusive). If both from_time and to_time are omitted, returns latest record only.", default=None),
    frozen_at: Optional[int] = Field(description="Created-at cutoff timestamp for time-travel queries (optional)", default=None),
    use_cache: bool = Field(description="Whether to use cache for performance optimization", default=False),
) -> ResponseType:
    """
    Get records from composed streams with complex time series calculations.
    
    This tool is specifically for streams where stream_type='composed' in the streams table.
    It handles recursive taxonomy resolution, time-varying weights, aggregation, 
    and Last Observation Carried Forward (LOCF) logic.
    
    Use this when:
    - Querying streams with stream_type = 'composed' 
    - Need calculated/aggregated values from multiple primitive streams
    - Require time series data with complex dependencies
    
    For primitive streams (stream_type='primitive'), use direct queries on primitive_events table.
    
    Args:
        data_provider: Stream deployer address (must be 0x followed by 40 hex characters)
        stream_id: Composed stream identifier (must start with 'st' and be 32 chars total)
        from_time: Start timestamp (inclusive) - omit both from/to for latest record
        to_time: End timestamp (inclusive) - omit both from/to for latest record  
        frozen_at: Optional timestamp for time-travel queries
        use_cache: Whether to use cache for better performance
        
    Returns:
        List of records with event_time and calculated value fields
    """
    try:
        # Validate input parameters
        if not data_provider.startswith('0x') or len(data_provider) != 42:
            return format_error_response("data_provider must be 0x followed by 40 hex characters")

        if not stream_id.startswith('st') or len(stream_id) != 32:
            return format_error_response("stream_id must start with 'st' and be 32 characters total")

        # Validate time range
        if from_time is not None and to_time is not None and from_time > to_time:
            return format_error_response(f"Invalid time range: from_time ({from_time}) > to_time ({to_time})")

        sql_driver = await get_sql_driver()
        composed_tool = ComposedStreamTool(sql_driver)

        # Execute the composed stream calculation
        records = await composed_tool.get_record_composed(
            data_provider=data_provider,
            stream_id=stream_id,
            from_time=from_time,
            to_time=to_time,
            frozen_at=frozen_at,
            use_cache=use_cache
        )

        # Format successful response
        result = {
            "success": True,
            "stream_type": "composed",
            "data_provider": data_provider,
            "stream_id": stream_id,
            "record_count": len(records),
            "records": records,
            "query_parameters": {
                "from_time": from_time,
                "to_time": to_time,
                "frozen_at": frozen_at,
                "use_cache": use_cache
            }
        }

        if len(records) == 0:
            result["message"] = "No records found for the specified composed stream and time range"

        return format_text_response(result)

    except Exception as e:
        logger.error(f"Error in get_composed_stream_records: {e}")
        return format_error_response(f"Failed to get composed stream records: {e!s}")


@mcp.tool(description="Get the latest record from a composed stream (convenience function)")
async def get_latest_composed_stream_record(
    data_provider: str = Field(description="Stream deployer address (0x... format)"),
    stream_id: str = Field(description="Composed stream ID (starts with 'st')"),
    frozen_at: Optional[int] = Field(description="Optional created-at cutoff timestamp for time-travel queries", default=None),
    use_cache: bool = Field(description="Whether to use cache for performance", default=False),
) -> ResponseType:
    """
    Get the most recent record from a composed stream.
    
    This is a convenience function that calls get_composed_stream_records with 
    both from_time and to_time set to None, which triggers the "latest record only" mode.
    
    Use this when you only need the current/latest calculated value from a composed stream.
    
    Args:
        data_provider: Stream deployer address  
        stream_id: Composed stream identifier
        frozen_at: Optional timestamp for time-travel queries
        use_cache: Whether to use cache for performance
        
    Returns:
        Single latest record with event_time and calculated value
    """
    return await get_composed_stream_records(
        data_provider=data_provider,
        stream_id=stream_id,
        from_time=None,  # Both None triggers latest record mode
        to_time=None,
        frozen_at=frozen_at,
        use_cache=use_cache
    )


@mcp.tool(description="Check stream type first - use this to determine if stream is 'primitive' or 'composed' before using other tools")
async def check_stream_type(
    data_provider: str = Field(description="Stream deployer address (0x... format)"),
    stream_id: str = Field(description="Stream ID to check"),
) -> ResponseType:
    """
    Check whether a stream is primitive or composed type.
    
    This helper tool allows Claude to determine which tool to use:
    - For primitive streams: Query primitive_events table directly
    - For composed streams: Use get_composed_stream_records tool
    
    Args:
        data_provider: Stream deployer address
        stream_id: Stream identifier to check
        
    Returns:
        Stream type information and guidance on which tool to use
    """
    try:
        sql_driver = await get_sql_driver()

        rows = await SafeSqlDriver.execute_param_query(
            sql_driver,
            """
            SELECT 
                data_provider,
                stream_id, 
                stream_type,
                created_at
            FROM main.streams
            WHERE LOWER(data_provider) = {} AND stream_id = {}
            """,
            [data_provider.lower(), stream_id],
        )
        if not rows:
            return format_text_response({
                "found": False,
                "message": f"Stream not found: {data_provider}/{stream_id}",
                "data_provider": data_provider,
                "stream_id": stream_id
            })

        row = rows[0]
        stream_type = row.cells["stream_type"]

        result = {
            "found": True,
            "data_provider": row.cells["data_provider"],
            "stream_id": row.cells["stream_id"],
            "stream_type": stream_type,
            "created_at": row.cells["created_at"],
            "recommended_action": {
                "primitive": "Query the primitive_events table directly for this stream",
                "composed": "Use get_composed_stream_records tool for calculated time series data"
            }.get(stream_type, "Unknown stream type")
        }

        return format_text_response(result)

    except Exception as e:
        logger.error(f"Error checking stream type: {e}")
        return format_error_response(f"Failed to check stream type: {e!s}")

@mcp.tool(description="Describe the taxonomy or composition of a composed stream - shows child streams, weights, and relationships")
async def describe_stream_taxonomies(
    data_provider: str = Field(description="Parent stream deployer address (0x... format)"),
    stream_id: str = Field(description="Parent stream ID (starts with 'st')"),
    latest_group_sequence: bool = Field(description="If True, only returns the latest/active taxonomy version", default=True),
) -> ResponseType:
    """
    Describe the taxonomy composition of a composed stream.
    
    Shows what child streams make up a composed stream, their weights, 
    and when the taxonomy definitions were created.
    
    Use this when users ask:
    - "What is the composition of this stream?"
    - "What streams make up this composed stream?"
    - "Show me the taxonomy of this stream"
    - "What are the weights in this stream?"
    
    Args:
        data_provider: Parent stream deployer address
        stream_id: Parent stream identifier
        latest_group_sequence: True = only current active taxonomy, False = all historical versions
        
    Returns:
        List of child streams with their weights and taxonomy details
    """
    try:
        sql_driver = await get_sql_driver()
        composed_tool = ComposedStreamTool(sql_driver)

        records = await composed_tool.describe_taxonomies(
            data_provider=data_provider,
            stream_id=stream_id,
            latest_group_sequence=latest_group_sequence
        )

        if not records:
            result = {
                "success": True,
                "message": f"No taxonomy found for stream {data_provider}/{stream_id}. This might be a primitive stream or the stream might not exist.",
                "data_provider": data_provider,
                "stream_id": stream_id,
                "taxonomy_count": 0,
                "taxonomy": []
            }
        else:
            result = {
                "success": True,
                "data_provider": data_provider,
                "stream_id": stream_id,
                "latest_only": latest_group_sequence,
                "taxonomy_count": len(records),
                "taxonomy": records,
                "summary": {
                    "total_child_streams": len(set(r["child_stream_id"] for r in records)),
                    "total_weight": sum(float(r["weight"]) for r in records),
                    "group_sequences": sorted(set(r["group_sequence"] for r in records))
                }
            }

        return format_text_response(result)

    except Exception as e:
        logger.error(f"Error describing taxonomies: {e}")
        return format_error_response(f"Failed to describe stream taxonomies: {e!s}")

@mcp.tool(description="Get index data for COMPOSED STREAM ONLY")
async def get_composed_stream_index(
    data_provider: str = Field(description="Stream deployer address (0x... format)"),
    stream_id: str = Field(description="Stream ID (starts with 'st')"),
    from_time: Optional[int] = Field(description="Start timestamp (inclusive)", default=None),
    to_time: Optional[int] = Field(description="End timestamp (inclusive)", default=None),
    frozen_at: Optional[int] = Field(description="Created-at cutoff for time-travel queries", default=None),
    base_time: Optional[int] = Field(description="Base timestamp for index calculations", default=None),
    use_cache: bool = Field(description="Whether to use cache (composed streams only)", default=False),
) -> ResponseType:
    """
    Get index data for composed stream only.
    """
    try:
        sql_driver = await get_sql_driver()

        composed_tool = ComposedStreamTool(sql_driver)
        records = await composed_tool.get_index(
            data_provider=data_provider,
            stream_id=stream_id,
            from_time=from_time,
            to_time=to_time,
            frozen_at=frozen_at,
            base_time=base_time,
            use_cache=use_cache
        )

        result = {
            "success": True,
            "stream_type": "composed",
            "data_provider": data_provider,
            "stream_id": stream_id,
            "index_count": len(records),
            "index_data": records
        }

        return format_text_response(result)

    except Exception as e:
        logger.error(f"Error in get_stream_index: {e}")
        return format_error_response(f"Failed to get stream index: {e!s}")

@mcp.tool(description="Get index data for PRIMITIVE STREAM ONLY")
async def get_primitive_stream_index(
    data_provider: str = Field(description="Stream deployer address (0x... format)"),
    stream_id: str = Field(description="Stream ID (starts with 'st')"),
    from_time: Optional[int] = Field(description="Start timestamp (inclusive)", default=None),
    to_time: Optional[int] = Field(description="End timestamp (inclusive)", default=None),
    frozen_at: Optional[int] = Field(description="Created-at cutoff for time-travel queries", default=None),
    base_time: Optional[int] = Field(description="Base timestamp for index calculations", default=None),
) -> ResponseType:
    """
    Get index data for primitive stream only.
    """
    try:
        sql_driver = await get_sql_driver()

        primitive_tool = PrimitiveStreamTool(sql_driver)
        records = await primitive_tool.get_index(
            data_provider=data_provider,
            stream_id=stream_id,
            from_time=from_time,
            to_time=to_time,
            frozen_at=frozen_at,
            base_time=base_time,
        )

        result = {
            "success": True,
            "stream_type": "primitive",
            "data_provider": data_provider,
            "stream_id": stream_id,
            "index_count": len(records),
            "index_data": records
        }

        return format_text_response(result)

    except Exception as e:
        logger.error(f"Error in get_stream_index: {e}")
        return format_error_response(f"Failed to get stream index: {e!s}")


@mcp.tool(description="Calculate index change percentage over time interval - returns percentage change values (e.g., 2.147 = 2.147% change)")
async def get_index_change(
    data_provider: str = Field(description="Stream deployer address (0x... format)"),
    stream_id: str = Field(description="Stream ID (starts with 'st')"),
    from_time: int = Field(description="Start timestamp (inclusive) for current data"),
    to_time: int = Field(description="End timestamp (inclusive) for current data"),
    time_interval: int = Field(description="Time interval to look back for comparison (e.g., 86400 for 1 day)"),
    frozen_at: Optional[int] = Field(description="Created-at cutoff for time-travel queries", default=None),
    base_time: Optional[int] = Field(description="Base timestamp for index calculations", default=None),
    use_cache: bool = Field(description="Whether to use cache for composed streams", default=False),
) -> ResponseType:
    """
    Calculate percentage change in index values over a specified time interval.
    
    This tool compares current index values with previous values from time_interval ago.
    For each current data point at time T, it finds the corresponding previous value 
    at or before time (T - time_interval) and calculates: ((current - previous) * 100) / previous
    
    Returns percentage change values where 2.147 means a 2.147% increase.
    
    The tool automatically detects whether the stream is composed or primitive and uses
    the appropriate index calculation method.
    
    Use this when:
    - Analyzing index performance over time periods
    - Calculating returns or percentage changes
    - Comparing current values to historical baselines
    
    Args:
        data_provider: Stream deployer address
        stream_id: Stream identifier
        from_time: Start timestamp for current data range
        to_time: End timestamp for current data range  
        time_interval: Time period to look back (in same units as timestamps)
        frozen_at: Optional timestamp for time-travel queries
        base_time: Optional base timestamp for index calculations
        use_cache: Whether to use cache for performance (composed streams only)
        
    Returns:
        List of time points with their percentage change values (e.g., 2.147 = 2.147% change)
    """
    try:
        if from_time > to_time:
            return format_error_response(
                f"Invalid time range: from_time ({from_time}) > to_time ({to_time})"
            )
        if time_interval <= 0:
            return format_error_response(
                f"time_interval must be > 0 (got {time_interval})"
            )

        sql_driver = await get_sql_driver()

        rows = await SafeSqlDriver.execute_param_query(
            sql_driver,
            """
            SELECT stream_type
            FROM main.streams
            WHERE LOWER(data_provider) = {} AND stream_id = {}
            """,
            [data_provider.lower(), stream_id],
        )

        if not rows:
            return format_error_response(f"Stream not found: {data_provider}/{stream_id}")

        stream_type = rows[0].cells["stream_type"]

        # Calculate previous data time range
        earliest_prev = from_time - time_interval
        latest_prev = to_time - time_interval

        # Get current index data based on stream type
        if stream_type == "composed":
            composed_tool = ComposedStreamTool(sql_driver)
            current_data = await composed_tool.get_index(
                data_provider=data_provider,
                stream_id=stream_id,
                from_time=from_time,
                to_time=to_time,
                frozen_at=frozen_at,
                base_time=base_time,
                use_cache=use_cache
            )
            # Get previous data
            prev_data = await composed_tool.get_index(
                data_provider=data_provider,
                stream_id=stream_id,
                from_time=earliest_prev,
                to_time=latest_prev,
                frozen_at=frozen_at,
                base_time=base_time,
                use_cache=use_cache
            )
        else:  # primitive
            primitive_tool = PrimitiveStreamTool(sql_driver)
            current_data = await primitive_tool.get_index(
                data_provider=data_provider,
                stream_id=stream_id,
                from_time=from_time,
                to_time=to_time,
                frozen_at=frozen_at,
                base_time=base_time
            )
            # Get previous data
            prev_data = await primitive_tool.get_index(
                data_provider=data_provider,
                stream_id=stream_id,
                from_time=earliest_prev,
                to_time=latest_prev,
                frozen_at=frozen_at,
                base_time=base_time
            )

        # Calculate changes using GeneralStreamTool
        general_tool = GeneralStreamTool(sql_driver)
        changes = await general_tool.get_index_change(
            current_data=current_data,
            prev_data=prev_data,
            time_interval=time_interval
        )

        result = {
            "success": True,
            "stream_type": stream_type,
            "data_provider": data_provider,
            "stream_id": stream_id,
            "time_interval": time_interval,
            "change_count": len(changes),
            "changes": changes,
            "query_parameters": {
                "from_time": from_time,
                "to_time": to_time,
                "time_interval": time_interval,
                "frozen_at": frozen_at,
                "base_time": base_time,
                "use_cache": use_cache if stream_type == "composed" else None
            }
        }

        if not changes:
            result["message"] = "No index changes could be calculated for the specified parameters"

        return format_text_response(result)

    except Exception as e:
        logger.error(f"Error in get_index_change: {e}")
        return format_error_response(f"Failed to calculate index change: {e!s}")


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PostgreSQL MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    parser.add_argument(
        "--access-mode",
        type=str,
        choices=[mode.value for mode in AccessMode],
        default=AccessMode.UNRESTRICTED.value,
        help="Set SQL access mode: unrestricted (unrestricted) or restricted (read-only with protections)",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse"],
        default="stdio",
        help="Select MCP transport: stdio (default) or sse",
    )
    parser.add_argument(
        "--sse-host",
        type=str,
        default="localhost",
        help="Host to bind SSE server to (default: localhost)",
    )
    parser.add_argument(
        "--sse-port",
        type=int,
        default=8000,
        help="Port for SSE server (default: 8000)",
    )

    args = parser.parse_args()

    # Store the access mode in the global variable
    global current_access_mode
    current_access_mode = AccessMode(args.access_mode)

    # Add the query tool with a description appropriate to the access mode
    if current_access_mode == AccessMode.UNRESTRICTED:
        mcp.add_tool(execute_sql, description="Execute any SQL query")
    else:
        mcp.add_tool(execute_sql, description="Execute a read-only SQL query")

    logger.info(f"Starting PostgreSQL MCP Server in {current_access_mode.upper()} mode")

    # Get database URL from environment variable or command line
    database_url = os.environ.get("DATABASE_URI", args.database_url)

    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via 'DATABASE_URI' environment variable or command-line argument.",
        )

    # Initialize database connection pool
    try:
        await db_connection.pool_connect(database_url)
        logger.info("Successfully connected to database and initialized connection pool")
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid connection is established.",
        )

    # Set up proper shutdown handling
    try:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        # Windows doesn't support signals properly
        logger.warning("Signal handling not supported on Windows")
        pass

    # Run the server with the selected transport (always async)
    if args.transport == "stdio":
        await mcp.run_stdio_async()
    else:
        # Update FastMCP settings based on command line arguments
        mcp.settings.host = args.sse_host
        mcp.settings.port = args.sse_port
        await mcp.run_sse_async()


async def shutdown(sig=None):
    """Clean shutdown of the server."""
    global shutdown_in_progress

    if shutdown_in_progress:
        logger.warning("Forcing immediate exit")
        # Use sys.exit instead of os._exit to allow for proper cleanup
        sys.exit(1)

    shutdown_in_progress = True

    if sig:
        logger.info(f"Received exit signal {sig.name}")

    # Close database connections
    try:
        await db_connection.close()
        logger.info("Closed database connections")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

    # Exit with appropriate status code
    sys.exit(128 + sig if sig is not None else 0)
