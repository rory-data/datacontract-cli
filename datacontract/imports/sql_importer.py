"""SQL importer for data contracts.

This module provides functionality to import SQL DDL statements and convert them
into data contract specifications (DataContractSpecification or OpenDataContractStandard).
"""

import logging
import pathlib
import re
from dataclasses import dataclass

import sqlglot
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)
from open_data_contract_standard.model import (
    Server as OdcsServer,
)
from sqlglot.dialects.dialect import Dialects
from sqlglot.expressions import ColumnDef

from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model, Server
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Shared column metadata extracted from SQL column definitions.

    This dataclass provides a common structure for column information
    used by both DCS and ODCS output formats.
    """

    name: str
    logical_type: str | None
    physical_type: str | None
    description: str | None
    max_length: int | None
    precision: int | None
    scale: int | None
    is_primary_key: bool | None
    is_required: bool


def _preprocess_teradata_sql(sql: str) -> str:
    """Pre-process Teradata SQL to handle INTERVAL types that sqlglot cannot parse.

    Teradata INTERVAL types (INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND) are not
    fully supported by sqlglot. This function removes INTERVAL column definitions
    from the SQL to allow parsing of other columns.

    Args:
        sql: The raw SQL string.

    Returns:
        SQL string with INTERVAL column definitions removed.

    Note:
        This is a workaround for sqlglot limitations. INTERVAL columns will not
        appear in the resulting data contract. A warning is logged when columns
        are removed.
    """
    # Pattern matches full INTERVAL column lines including:
    # - Optional leading whitespace (spaces/tabs only, not newlines)
    # - Column name (word characters)
    # - INTERVAL YEAR(...) TO MONTH or INTERVAL DAY(...) TO SECOND(...)
    # - Optional comma
    # - Optional spaces/tabs (not newlines) and comment to end of line
    # - Optional single newline at end
    interval_pattern = (
        r"^[ \t]*\w+[ \t]+INTERVAL[ \t]+(?:YEAR|DAY)[ \t]*\([^)]*\)[ \t]+TO[ \t]+"
        r"(?:MONTH|SECOND)(?:[ \t]*\([^)]*\))?[ \t]*,?[ \t]*(?:--[^\n]*)?\n?"
    )

    # Check if any INTERVAL columns exist before modifying
    if re.search(interval_pattern, sql, flags=re.MULTILINE | re.IGNORECASE):
        logger.warning(
            "Teradata INTERVAL column definitions detected and will be skipped. "
            "These columns are not supported by the SQL parser."
        )

    # Remove INTERVAL column definitions (entire lines)
    processed_sql = re.sub(
        interval_pattern,
        "",
        sql,
        flags=re.MULTILINE | re.IGNORECASE,
    )

    # Clean up any double commas that might result from removal
    processed_sql = re.sub(r",\s*,", ",", processed_sql)
    # Clean up trailing comma before closing parenthesis
    processed_sql = re.sub(r",\s*\)", ")", processed_sql)
    # Clean up comma followed by newline then closing paren
    processed_sql = re.sub(r",\s*\n\s*\)", ")", processed_sql)

    return processed_sql


def _extract_column_metadata(
    column: sqlglot.expressions.ColumnDef,
    dialect: Dialects | None,
) -> ColumnMetadata:
    """Extract common metadata from a SQL column definition.

    This function centralises the extraction of column information used by both
    DCS and ODCS output formats, reducing code duplication.

    Args:
        column: A sqlglot ColumnDef expression.
        dialect: The SQL dialect.

    Returns:
        ColumnMetadata containing all extracted column information.
    """
    col_type = to_col_type(column, dialect)
    precision, scale = get_precision_scale(column)

    return ColumnMetadata(
        name=column.this.name,
        logical_type=map_type_from_sql(col_type, dialect),
        physical_type=col_type,
        description=get_description(column),
        max_length=get_max_length(column),
        precision=precision,
        scale=scale,
        is_primary_key=get_primary_key(column),
        is_required=column.find(sqlglot.exp.NotNullColumnConstraint) is not None,
    )


class SqlImporter(Importer):
    """Importer for SQL files."""

    def import_source(
        self,
        data_contract_specification: DataContractSpecification | OpenDataContractStandard,
        source: str,
        import_args: dict,
    ) -> DataContractSpecification | OpenDataContractStandard:
        """Import SQL source into the data contract specification.

        Args:
            data_contract_specification (DataContractSpecification | OpenDataContractStandard): The data contract specification to populate.
            source (str): The source file path.
            import_args (dict): Additional import arguments.

        Returns:
            DataContractSpecification | OpenDataContractStandard: The populated data contract specification.
        """
        return import_sql(data_contract_specification, source, import_args)


def import_sql(
    data_contract_specification: DataContractSpecification | OpenDataContractStandard,
    source: str,
    import_args: dict | None = None,
) -> DataContractSpecification | OpenDataContractStandard:
    """Import SQL source into the data contract specification.

    Args:
        data_contract_specification (DataContractSpecification | OpenDataContractStandard): The data contract specification to populate.
        source (str): The source file path.
        import_args (dict | None): Additional import arguments.

    Returns:
        DataContractSpecification | OpenDataContractStandard: The populated data contract specification.
    """
    sql = read_file(source)
    dialect: Dialects | None = to_dialect(import_args)

    try:
        # Pre-process SQL to handle dialect-specific syntax that sqlglot doesn't support
        if dialect == sqlglot.dialects.Dialects.TERADATA:
            sql = _preprocess_teradata_sql(sql)

        parsed = sqlglot.parse_one(sql=sql, read=dialect)
    except Exception as e:
        raise DataContractException(
            type="import",
            name=f"Reading source from {source}",
            reason=f"Error parsing SQL: {e!s}",
            engine="datacontract",
            result=ResultEnum.error,
        ) from e

    tables = parsed.find_all(sqlglot.expressions.Table)

    if isinstance(data_contract_specification, DataContractSpecification):
        _populate_dcs_from_sql(data_contract_specification, tables, dialect, source, parsed)
    elif isinstance(data_contract_specification, OpenDataContractStandard):
        _populate_odcs_from_sql(data_contract_specification, tables, dialect, source, parsed)

    return data_contract_specification


def _populate_dcs_from_sql(
    data_contract_specification: DataContractSpecification,
    tables,
    dialect: Dialects | None,
    source: str,
    parsed,
) -> None:
    """Populate a DataContractSpecification with SQL table and column information.

    Args:
        data_contract_specification: The DCS to populate.
        tables: Iterable of sqlglot Table expressions.
        dialect: The SQL dialect.
        source: Source file path (for server type detection).
        parsed: The parsed sqlglot statement.
    """
    server_type: str | None = to_server_type(source, dialect)
    if server_type is not None:
        if data_contract_specification.servers is None:  # type: ignore[attr-defined]
            data_contract_specification.servers = {}  # type: ignore[attr-defined]
        data_contract_specification.servers[server_type] = Server(type=server_type)  # type: ignore[index]

    if data_contract_specification.models is None:  # type: ignore[attr-defined]
        data_contract_specification.models = {}  # type: ignore[attr-defined]

    for table in tables:
        table_name = table.this.name.lower()
        fields = _extract_fields_from_columns(table_name, parsed, dialect)
        data_contract_specification.models[table_name] = Model(  # type: ignore[index]
            type="table",
            fields=fields,
        )


def _populate_odcs_from_sql(
    data_contract_specification: OpenDataContractStandard,
    tables,
    dialect: Dialects | None,
    source: str,
    parsed,
) -> None:
    """Populate an OpenDataContractStandard with SQL table and column information.

    Args:
        data_contract_specification: The ODCS to populate.
        tables: Iterable of sqlglot Table expressions.
        dialect: The SQL dialect.
        source: Source file path (for server type detection).
        parsed: The parsed sqlglot statement.
    """
    # For ODCS, servers is a list, not a dict
    server_type: str | None = to_server_type(source, dialect)
    if server_type is not None:
        if data_contract_specification.servers is None:
            data_contract_specification.servers = []
        server = OdcsServer(server=server_type, type=server_type)
        data_contract_specification.servers.append(server)

    if data_contract_specification.schema_ is None:
        data_contract_specification.schema_ = []

    for table in tables:
        table_name = table.this.name.lower()
        properties = _extract_properties_from_columns(table_name, parsed, dialect)
        schema_obj = SchemaObject(name=table_name, properties=properties)
        data_contract_specification.schema_.append(schema_obj)


def _extract_properties_from_columns(table_name: str, parsed, dialect: Dialects | None) -> list[SchemaProperty]:
    """Extract SchemaProperty definitions from SQL table columns for ODCS.

    Args:
        table_name: The name of the table to extract properties for (lowercase).
        parsed: The parsed sqlglot statement.
        dialect: The SQL dialect.

    Returns:
        List of SchemaProperty objects.
    """
    properties = []

    for column in parsed.find_all(sqlglot.exp.ColumnDef):
        if column.parent is None or column.parent.this is None:
            continue
        if column.parent.this.name.lower() != table_name:
            continue

        metadata = _extract_column_metadata(column, dialect)

        prop = SchemaProperty(name=metadata.name)
        prop.logicalType = metadata.logical_type
        prop.physicalType = metadata.physical_type
        if metadata.description:
            prop.description = metadata.description
        if metadata.is_primary_key:
            prop.primaryKey = True
        if metadata.is_required:
            prop.required = True

        # Build logicalTypeOptions for additional constraints
        _populate_logical_type_options_from_metadata(prop, metadata)

        properties.append(prop)

    return properties


def _populate_logical_type_options_from_metadata(prop: SchemaProperty, metadata: ColumnMetadata) -> None:
    """Populate logicalTypeOptions for a schema property from extracted metadata.

    Args:
        prop: The SchemaProperty to populate.
        metadata: The extracted column metadata.
    """
    logical_type_options = {}

    if metadata.max_length is not None:
        logical_type_options["maxLength"] = metadata.max_length
    if metadata.precision is not None:
        logical_type_options["precision"] = metadata.precision
    if metadata.scale is not None:
        logical_type_options["scale"] = metadata.scale

    if logical_type_options:
        prop.logicalTypeOptions = logical_type_options


def _extract_fields_from_columns(table_name: str, parsed, dialect: Dialects | None) -> dict[str, Field]:
    """Extract field definitions from SQL table columns.

    Args:
        table_name: The name of the table to extract fields for (lowercase).
        parsed: The parsed sqlglot statement.
        dialect: The SQL dialect.

    Returns:
        Dictionary of field name to Field object.
    """
    fields = {}

    for column in parsed.find_all(sqlglot.exp.ColumnDef):
        if column.parent is None or column.parent.this is None:
            continue
        if column.parent.this.name.lower() != table_name:
            continue

        metadata = _extract_column_metadata(column, dialect)

        field = Field()
        field.type = metadata.logical_type
        field.description = metadata.description
        field.maxLength = metadata.max_length
        field.precision = metadata.precision
        field.scale = metadata.scale
        field.primaryKey = metadata.is_primary_key
        if metadata.is_required:
            field.required = True

        physical_type_key = to_physical_type_key(dialect)
        field.config = {
            physical_type_key: metadata.physical_type,
        }

        fields[metadata.name] = field

    return fields


def get_primary_key(column) -> bool | None:
    """Determine if the column is a primary key.

    Args:
        column (sqlglot.expressions.ColumnDef): The column definition.

    Returns:
        bool | None: True if primary key, False if not, None if unknown.
    """
    if column.find(sqlglot.exp.PrimaryKeyColumnConstraint) is not None:
        return True
    if column.find(sqlglot.exp.PrimaryKey) is not None:
        return True
    return None


def to_dialect(import_args: dict[str, str] | None) -> Dialects | None:
    """Convert import args to sqlglot Dialects enum value.

    Args:
        import_args (dict[str, str] | None): Import arguments containing dialect information.

    Returns:
        Dialects | None: Corresponding Dialects enum value or None if not found.
    """
    if not import_args or (dialect := import_args.get("dialect")) is None:
        return None

    # Handle special case mapping (sqlserver -> TSQL)
    dialect_map = {"sqlserver": Dialects.TSQL}
    if dialect in dialect_map:
        return dialect_map[dialect]

    # Try standard dialect lookup
    dialect_upper = dialect.upper()
    if dialect_upper in Dialects.__members__:
        return Dialects[dialect_upper]

    return None


def to_physical_type_key(dialect: Dialects | str | None) -> str:
    """Get the physical type key based on the SQL dialect.

    Args:
        dialect (Dialects | str | None): The SQL dialect.

    Returns:
        str: The corresponding physical type key.
    """
    dialect_map = {
        Dialects.TSQL: "sqlserverType",
        Dialects.POSTGRES: "postgresType",
        Dialects.BIGQUERY: "bigqueryType",
        Dialects.SNOWFLAKE: "snowflakeType",
        Dialects.REDSHIFT: "redshiftType",
        Dialects.ORACLE: "oracleType",
        Dialects.MYSQL: "mysqlType",
        Dialects.DATABRICKS: "databricksType",
        Dialects.TERADATA: "teradataType",
    }
    if isinstance(dialect, str):
        dialect = Dialects[dialect.upper()] if dialect.upper() in Dialects.__members__ else None
    if dialect is None:
        return "physicalType"
    return dialect_map.get(dialect, "physicalType")


def to_server_type(source, dialect: Dialects | None) -> str | None:
    """Get the server type based on the SQL dialect.

    Args:
        source (str): The source file path.
        dialect (Dialects | None): The SQL dialect.

    Returns:
        str | None: The corresponding server type or None if not found.
    """
    if dialect is None:
        return None
    dialect_map = {
        Dialects.TSQL: "sqlserver",
        Dialects.POSTGRES: "postgres",
        Dialects.BIGQUERY: "bigquery",
        Dialects.SNOWFLAKE: "snowflake",
        Dialects.REDSHIFT: "redshift",
        Dialects.ORACLE: "oracle",
        Dialects.MYSQL: "mysql",
        Dialects.DATABRICKS: "databricks",
        Dialects.TERADATA: "teradata",
    }
    return dialect_map.get(dialect)


def to_col_type(column: ColumnDef, dialect: Dialects | None) -> str | None:
    """Extract and format the SQL type from a column definition.

    Args:
        column: A sqlglot ColumnDef expression.
        dialect: Dialects enum or None.

    Returns:
        SQL type string formatted for the dialect, or None if unavailable.
    """
    col_type_kind = column.args.get("kind")
    if col_type_kind is None:
        logger.warning("Column %s has no type information", column.name)
        return None

    return col_type_kind.sql(dialect)


def to_col_type_normalized(column):
    """Get normalised (lowercase) base type name from a column definition.

    Args:
        column: A sqlglot ColumnDef expression.

    Returns:
        Normalised type name (e.g., 'varchar', 'int') or None if unavailable.
    """
    col_type = column.args.get("kind")
    if col_type is None or col_type.this is None:
        logger.debug("Column %s has no type kind information", getattr(column, "name", "unknown"))
        return None
    col_type_name = col_type.this.name
    return col_type_name.lower() if col_type_name else None


def get_description(column: sqlglot.expressions.ColumnDef) -> str | None:
    """Get the description from column comments.

    Args:
        column (sqlglot.expressions.ColumnDef): The column definition.

    Returns:
        str | None: The description if available, otherwise None.
    """
    if column.comments is None:
        return None
    return " ".join(comment.strip() for comment in column.comments)


def get_max_length(column: sqlglot.expressions.ColumnDef) -> int | None:
    """Get the maximum length for string types.

    Args:
        column (sqlglot.expressions.ColumnDef): The column definition.

    Returns:
        int | None: The maximum length if applicable, otherwise None.
    """
    col_type = to_col_type_normalized(column)
    if col_type is None:
        return None
    if col_type not in ["varchar", "char", "nvarchar", "nchar"]:
        return None
    col_params = list(column.args["kind"].find_all(sqlglot.expressions.DataTypeParam))
    max_length_str = None
    if len(col_params) == 0:
        return None
    if len(col_params) == 1:
        max_length_str = col_params[0].name
    if len(col_params) == 2:
        max_length_str = col_params[1].name
    if max_length_str is not None:
        return int(max_length_str) if max_length_str.isdigit() else None


def get_precision_scale(column):
    """Get the precision and scale for decimal/numeric types.

    Args:
        column (sqlglot.expressions.ColumnDef): The column definition.

    Returns:
        tuple[int | None, int | None]: The precision and scale if applicable, otherwise (None, None).
    """
    col_type = to_col_type_normalized(column)
    if col_type is None:
        return None, None
    if col_type not in ["decimal", "numeric", "float", "number"]:
        return None, None
    col_params = list(column.args["kind"].find_all(sqlglot.expressions.DataTypeParam))
    if len(col_params) == 0:
        return None, None
    if len(col_params) == 1:
        if not col_params[0].name.isdigit():
            return None, None
        precision = int(col_params[0].name)
        scale = 0
        return precision, scale
    if len(col_params) == 2:
        if not col_params[0].name.isdigit() or not col_params[1].name.isdigit():
            return None, None
        precision = int(col_params[0].name)
        scale = int(col_params[1].name)
        return precision, scale
    return None, None


def map_type_from_sql(sql_type: str | None, dialect: Dialects | None = None) -> str | None:
    """Map SQL data types to standard physical types.

    Supports types from multiple SQL dialects including Teradata, PostgreSQL,
    BigQuery, Snowflake, Oracle, MySQL, SQL Server, and others.

    Args:
        sql_type: The SQL data type as a string, or None.
        dialect: The SQL dialect (optional, for dialect-specific mappings).

    Returns:
        The corresponding standard physical type or 'variant' if not mappable.
    """
    if sql_type is None:
        return "variant"

    sql_type_normed = sql_type.lower().strip()

    # Handle INTERVAL types first (before checking for "int" prefix)
    if sql_type_normed.startswith("interval"):
        return "variant"

    # Exact matches for specific types
    exact_type_map = {
        "date": "date",
        "time": "string",
        "datetimeoffset": "timestamp_tz",
        "uniqueidentifier": "string",  # SQL Server
        "json": "string",
        "xml": "string",
        "clob": "text",
        "nclob": "text",
        "blob": "bytes",
        "bfile": "bytes",
        "byte": "bytes",  # Teradata
        "real": "float",  # Teradata 32-bit float
        "number": "number",
    }

    # Teradata-specific: DECIMAL without parameters is NUMBER type
    if sql_type_normed == "decimal" and dialect == sqlglot.dialects.Dialects.TERADATA:
        return "number"

    if sql_type_normed in exact_type_map:
        return exact_type_map[sql_type_normed]

    # Prefix-based matches
    # IMPORTANT: Order matters! Longer prefixes must come before shorter ones to avoid
    # incorrect matches. For example, "bigint" must be checked before "int", otherwise
    # "bigint" would incorrectly match as "int". The list is ordered from longest to
    # shortest prefix within each type family.
    prefix_type_map = [
        ("bigint", "long"),
        ("tinyint", "int"),
        ("smallint", "int"),
        ("integer", "int"),
        ("int", "int"),
        ("nvarchar", "string"),
        ("varchar", "string"),
        ("nchar", "string"),
        ("ntext", "string"),
        ("char", "string"),
        ("text", "string"),
        ("string", "string"),
        ("varbinary", "bytes"),
        ("binary", "bytes"),
        ("raw", "bytes"),
        ("double", "double"),
        ("float", "float"),
        ("numeric", "decimal"),
        ("decimal", "decimal"),  # DECIMAL with parameters -> decimal type
        ("bool", "boolean"),
        ("bit", "boolean"),
        ("timestamp", "timestamp"),  # Handle with special logic below
    ]

    for prefix, target_type in prefix_type_map:
        if sql_type_normed.startswith(prefix):
            # Special handling for timestamp types
            if prefix == "timestamp":
                return map_timestamp(sql_type_normed)
            return target_type

    # Handle datetime types
    if sql_type_normed in ["datetime", "datetime2", "smalldatetime"]:
        return "timestamp_ntz"

    # Default fallback for unknown types
    return "variant"


def map_timestamp(timestamp_type: str) -> str:
    """Map various timestamp SQL types to standard physical timestamp types.

    Args:
        timestamp_type (str): The SQL timestamp type as a string (normalised to lowercase).

    Returns:
        str: The corresponding standard physical timestamp type.
    """
    match timestamp_type:
        case "timestamp" | "timestampntz" | "timestamp_ntz":
            return "timestamp_ntz"
        case "timestamptz" | "timestamp_tz" | "timestamp with time zone":
            return "timestamp_tz"
        case localTimezone if localTimezone.startswith("timestampltz"):
            return "timestamp_tz"
        case timezoneWrittenOut if timezoneWrittenOut.endswith("time zone"):
            return "timestamp_tz"
        case _:
            return "timestamp_ntz"


def read_file(path) -> str:
    """Read the content of a file.

    Args:
        path (str): The file path.

    Returns:
        str: The content of the file.
    """
    if not pathlib.Path(path).exists():
        raise DataContractException(
            type="import",
            name=f"Reading source from {path}",
            reason=f"The file '{path}' does not exist.",
            engine="datacontract",
            result=ResultEnum.error,
        )
    with pathlib.Path(path).open("r") as file:
        file_content = file.read()
    return file_content
