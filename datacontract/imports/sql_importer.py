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


# Constants
TERADATA_INTERVAL_PATTERN = (
    r"^[ \t]*\w+[ \t]+INTERVAL[ \t]+(?:YEAR|DAY)(?:[ \t]*\([^)]*\))?[ \t]+TO[ \t]+"
    r"(?:MONTH|SECOND)(?:[ \t]*\([^)]*\))?[ \t]*,?[ \t]*(?:--[^\n]*)?\n?"
)

EXACT_TYPE_MAP = {
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

# IMPORTANT: Order matters! Longer prefixes must come before shorter ones.
PREFIX_TYPE_MAP = [
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
    ("timestamp", "timestamp"),  # Handle with special logic
]

DIALECT_CONFIG = {
    Dialects.TSQL: {"server": "sqlserver", "physical_type": "sqlserverType"},
    Dialects.POSTGRES: {"server": "postgres", "physical_type": "postgresType"},
    Dialects.BIGQUERY: {"server": "bigquery", "physical_type": "bigqueryType"},
    Dialects.SNOWFLAKE: {"server": "snowflake", "physical_type": "snowflakeType"},
    Dialects.REDSHIFT: {"server": "redshift", "physical_type": "redshiftType"},
    Dialects.ORACLE: {"server": "oracle", "physical_type": "oracleType"},
    Dialects.MYSQL: {"server": "mysql", "physical_type": "mysqlType"},
    Dialects.DATABRICKS: {"server": "databricks", "physical_type": "databricksType"},
    Dialects.TERADATA: {"server": "teradata", "physical_type": "teradataType"},
}


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

    tables = list(parsed.find_all(sqlglot.expressions.Table))

    if isinstance(data_contract_specification, DataContractSpecification):
        _populate_dcs_from_sql(data_contract_specification, tables, dialect, source, parsed)
    elif isinstance(data_contract_specification, OpenDataContractStandard):
        _populate_odcs_from_sql(data_contract_specification, tables, dialect, source, parsed)

    return data_contract_specification


def _extract_table_metadata(
    create_expression: sqlglot.expressions.Create,
    dialect: Dialects | None,
) -> tuple[str, list[ColumnMetadata]]:
    """Extract table name and column metadata from a CREATE TABLE expression.

    Args:
        create_expression: The CREATE TABLE expression.
        dialect: The SQL dialect.

    Returns:
        Tuple of (table_name, list[ColumnMetadata]).
    """
    schema = create_expression.this
    if not isinstance(schema, sqlglot.exp.Schema):
        # Should not happen if filtered by kind="TABLE" and valid SQL, but safe fallback
        return "", []

    table_name = schema.this.name.lower()
    columns = []

    for expression in schema.expressions:
        if isinstance(expression, sqlglot.exp.ColumnDef):
            columns.append(_extract_column_metadata(expression, dialect))

    return table_name, columns


def _create_field(metadata: ColumnMetadata, dialect: Dialects | None) -> Field:
    """Create a Field object from column metadata.

    Args:
        metadata: The column metadata.
        dialect: The SQL dialect.

    Returns:
        Field object.
    """
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
    return field


def _create_schema_property(metadata: ColumnMetadata) -> SchemaProperty:
    """Create a SchemaProperty object from column metadata.

    Args:
        metadata: The column metadata.

    Returns:
        SchemaProperty object.
    """
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
    return prop


def _populate_dcs_from_sql(
    data_contract_specification: DataContractSpecification,
    tables: list[sqlglot.expressions.Table],
    dialect: Dialects | None,
    source: str,
    parsed: sqlglot.expressions.Expression,
) -> None:
    """Populate a DataContractSpecification with SQL table and column information.

    Args:
        data_contract_specification: The DCS to populate.
        tables: Unused (kept for signature compatibility).
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

    # Extract the first CREATE TABLE statement
    create = next((c for c in parsed.find_all(sqlglot.exp.Create) if c.kind == "TABLE"), None)
    if not create:
        return

    table_name, columns = _extract_table_metadata(create, dialect)
    if not table_name:
        return

    fields = {metadata.name: _create_field(metadata, dialect) for metadata in columns}

    data_contract_specification.models[table_name] = Model(  # type: ignore[index]
        type="table",
        fields=fields,
    )


def _populate_odcs_from_sql(
    data_contract_specification: OpenDataContractStandard,
    tables: list[sqlglot.expressions.Table],
    dialect: Dialects | None,
    source: str,
    parsed: sqlglot.expressions.Expression,
) -> None:
    """Populate an OpenDataContractStandard with SQL table and column information.

    Args:
        data_contract_specification: The ODCS to populate.
        tables: Unused (kept for signature compatibility).
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

    # Extract the first CREATE TABLE statement
    create = next((c for c in parsed.find_all(sqlglot.exp.Create) if c.kind == "TABLE"), None)
    if not create:
        return

    table_name, columns = _extract_table_metadata(create, dialect)
    if not table_name:
        return

    properties = [_create_schema_property(metadata) for metadata in columns]
    schema_obj = SchemaObject(name=table_name, properties=properties)
    data_contract_specification.schema_.append(schema_obj)


def _populate_logical_type_options_from_metadata(prop: SchemaProperty, metadata: ColumnMetadata) -> None:
    """Populate logicalTypeOptions for a schema property from extracted metadata.

    Args:
        prop: The SchemaProperty to populate.
        metadata: The extracted column metadata.
    """
    options = {
        "maxLength": metadata.max_length,
        "precision": metadata.precision,
        "scale": metadata.scale,
    }
    logical_type_options = {k: v for k, v in options.items() if v is not None}

    if logical_type_options:
        prop.logicalTypeOptions = logical_type_options


def get_primary_key(column: sqlglot.expressions.ColumnDef) -> bool | None:
    """Determine if the column is a primary key.

    Args:
        column: The column definition.

    Returns:
        True if primary key, None otherwise.
    """
    if column.find(sqlglot.exp.PrimaryKeyColumnConstraint) is not None:
        return True
    if column.find(sqlglot.exp.PrimaryKey) is not None:
        return True

    # Check table-level constraints
    if column.parent:
        for pk in column.parent.find_all(sqlglot.exp.PrimaryKey):
            for expr in pk.expressions:
                if expr.name == column.this.name:
                    return True

    return None


def to_dialect(import_args: dict[str, str] | None) -> Dialects | None:
    """Convert import args to sqlglot Dialects enum value.

    Args:
        import_args: Import arguments containing dialect information.

    Returns:
        Corresponding Dialects enum value or None if not found.
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
    if isinstance(dialect, str):
        dialect = Dialects[dialect.upper()] if dialect.upper() in Dialects.__members__ else None

    if dialect is None:
        return "physicalType"

    config = DIALECT_CONFIG.get(dialect)
    return config["physical_type"] if config else "physicalType"


def to_server_type(source: str, dialect: Dialects | None) -> str | None:
    """Get the server type based on the SQL dialect.

    Args:
        source: The source file path.
        dialect: The SQL dialect.

    Returns:
        The corresponding server type or None if not found.
    """
    if dialect is None:
        return None

    config = DIALECT_CONFIG.get(dialect)
    return config["server"] if config else None


def _get_col_kind(column: ColumnDef) -> sqlglot.expressions.Expression | None:
    """Get the column type kind expression.

    Args:
        column: A sqlglot ColumnDef expression.

    Returns:
        The type kind expression or None if unavailable.
    """
    col_type_kind = column.args.get("kind")
    if col_type_kind is None:
        logger.warning("Column %s has no type information", getattr(column, "name", "unknown"))
        return None
    return col_type_kind


def to_col_type(column: ColumnDef, dialect: Dialects | None) -> str | None:
    """Extract and format the SQL type from a column definition.

    Args:
        column: A sqlglot ColumnDef expression.
        dialect: Dialects enum or None.

    Returns:
        SQL type string formatted for the dialect, or None if unavailable.
    """
    col_type_kind = _get_col_kind(column)
    if col_type_kind is None:
        return None

    return col_type_kind.sql(dialect)


def to_col_type_normalized(column: ColumnDef) -> str | None:
    """Get normalised (lowercase) base type name from a column definition.

    Args:
        column: A sqlglot ColumnDef expression.

    Returns:
        Normalised type name (e.g., 'varchar', 'int') or None if unavailable.
    """
    col_type = _get_col_kind(column)
    if col_type is None or col_type.this is None:
        return None
    col_type_name = col_type.this.name
    return col_type_name.lower() if col_type_name else None


def get_description(column: sqlglot.expressions.ColumnDef) -> str | None:
    """Get the description from column comments.

    Args:
        column: The column definition.

    Returns:
        The description if available, otherwise None.
    """
    for constraint in column.args.get("constraints", []):
        if isinstance(constraint.kind, sqlglot.exp.CommentColumnConstraint):
            return constraint.kind.this.name

    if column.comments is None:
        return None
    return " ".join(comment.strip() for comment in column.comments)


def get_max_length(column: sqlglot.expressions.ColumnDef) -> int | None:
    """Get the maximum length for string types.

    Args:
        column: The column definition.

    Returns:
        The maximum length if applicable, otherwise None.
    """
    col_type = to_col_type_normalized(column)
    if col_type is None:
        return None
    if col_type not in ["varchar", "char", "nvarchar", "nchar"]:
        return None

    kind = _get_col_kind(column)
    if kind is None:
        return None

    col_params = list(kind.find_all(sqlglot.expressions.DataTypeParam))
    max_length_str = None
    if len(col_params) == 0:
        return None

    max_length_str = col_params[0].name

    if max_length_str is not None:
        return int(max_length_str) if max_length_str.isdigit() else None


def get_precision_scale(column: sqlglot.expressions.ColumnDef) -> tuple[int | None, int | None]:
    """Get the precision and scale for decimal/numeric types.

    Args:
        column: The column definition.

    Returns:
        Tuple of (precision, scale) if applicable, otherwise (None, None).
    """
    col_type = to_col_type_normalized(column)
    if col_type is None:
        return None, None
    if col_type not in ["decimal", "numeric", "float", "number"]:
        return None, None

    kind = _get_col_kind(column)
    if kind is None:
        return None, None

    col_params = list(kind.find_all(sqlglot.expressions.DataTypeParam))
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

    # Teradata-specific: DECIMAL without parameters is NUMBER type
    if sql_type_normed == "decimal" and dialect == sqlglot.dialects.Dialects.TERADATA:
        return "number"

    if sql_type_normed in EXACT_TYPE_MAP:
        return EXACT_TYPE_MAP[sql_type_normed]

    for prefix, target_type in PREFIX_TYPE_MAP:
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


def read_file(path: str) -> str:
    """Read the content of a file.

    Args:
        path: The file path.

    Returns:
        The content of the file.

    Raises:
        DataContractException: If the file does not exist.
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


def _cleanup_teradata_sql(sql: str) -> str:
    """Clean up SQL after removing Teradata INTERVAL columns.

    Args:
        sql: The SQL string to clean up.

    Returns:
        Cleaned SQL string.
    """
    # Clean up any double commas that might result from removal
    sql = re.sub(r",\s*,", ",", sql)
    # Clean up trailing comma before closing parenthesis
    sql = re.sub(r",\s*\)", ")", sql)
    # Clean up comma followed by newline then closing paren
    sql = re.sub(r",\s*\n\s*\)", ")", sql)
    return sql


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
    # Check if any INTERVAL columns exist before modifying
    if re.search(TERADATA_INTERVAL_PATTERN, sql, flags=re.MULTILINE | re.IGNORECASE):
        logger.warning(
            "Teradata INTERVAL column definitions detected and will be skipped. "
            "These columns are not supported by the SQL parser."
        )

    # Remove INTERVAL column definitions (entire lines)
    processed_sql = re.sub(
        TERADATA_INTERVAL_PATTERN,
        "",
        sql,
        flags=re.MULTILINE | re.IGNORECASE,
    )

    return _cleanup_teradata_sql(processed_sql)
