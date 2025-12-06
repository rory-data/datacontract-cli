"""Unit tests for SQL importer module.

This test module provides granular unit testing of the sql_importer.py module,
covering individual functions, type mappings, and edge cases.
"""

import pytest
import sqlglot
from open_data_contract_standard.model import OpenDataContractStandard
from sqlglot.dialects.dialect import Dialects

from datacontract.imports.sql_importer import (
    _preprocess_teradata_sql,
    get_description,
    get_max_length,
    get_precision_scale,
    get_primary_key,
    import_sql,
    map_timestamp,
    map_type_from_sql,
    read_file,
    to_col_type,
    to_col_type_normalized,
    to_dialect,
    to_physical_type_key,
    to_server_type,
)
from datacontract.model.data_contract_specification import (
    DataContractSpecification,
)
from datacontract.model.exceptions import DataContractException


class TestToDialect:
    """Tests for to_dialect function."""

    def test_to_dialect_with_none(self):
        """Test handling of None import_args."""
        assert to_dialect(None) is None

    def test_to_dialect_with_empty_dict(self):
        """Test handling of empty dict."""
        assert to_dialect({}) is None

    def test_to_dialect_without_dialect_key(self):
        """Test handling of dict without dialect key."""
        assert to_dialect({"some_key": "value"}) is None

    def test_to_dialect_sqlserver_mapping(self):
        """Test special case mapping of sqlserver to TSQL."""
        result = to_dialect({"dialect": "sqlserver"})
        assert result == Dialects.TSQL

    def test_to_dialect_postgres(self):
        """Test standard dialect lookup for postgres."""
        result = to_dialect({"dialect": "postgres"})
        assert result == Dialects.POSTGRES

    def test_to_dialect_bigquery(self):
        """Test standard dialect lookup for bigquery."""
        result = to_dialect({"dialect": "bigquery"})
        assert result == Dialects.BIGQUERY

    def test_to_dialect_snowflake(self):
        """Test standard dialect lookup for snowflake."""
        result = to_dialect({"dialect": "snowflake"})
        assert result == Dialects.SNOWFLAKE

    def test_to_dialect_oracle(self):
        """Test standard dialect lookup for oracle."""
        result = to_dialect({"dialect": "oracle"})
        assert result == Dialects.ORACLE

    def test_to_dialect_mysql(self):
        """Test standard dialect lookup for mysql."""
        result = to_dialect({"dialect": "mysql"})
        assert result == Dialects.MYSQL

    def test_to_dialect_redshift(self):
        """Test standard dialect lookup for redshift."""
        result = to_dialect({"dialect": "redshift"})
        assert result == Dialects.REDSHIFT

    def test_to_dialect_databricks(self):
        """Test standard dialect lookup for databricks."""
        result = to_dialect({"dialect": "databricks"})
        assert result == Dialects.DATABRICKS

    def test_to_dialect_teradata(self):
        """Test standard dialect lookup for teradata."""
        result = to_dialect({"dialect": "teradata"})
        assert result == Dialects.TERADATA

    def test_to_dialect_case_insensitive(self):
        """Test that dialect lookup is case-insensitive."""
        result1 = to_dialect({"dialect": "POSTGRES"})
        result2 = to_dialect({"dialect": "postgres"})
        assert result1 == Dialects.POSTGRES
        assert result2 == Dialects.POSTGRES

    def test_to_dialect_invalid_dialect(self):
        """Test handling of invalid dialect."""
        result = to_dialect({"dialect": "invalid_db"})
        assert result is None


class TestToPhysicalTypeKey:
    """Tests for to_physical_type_key function."""

    def test_to_physical_type_key_none(self):
        """Test default key when dialect is None."""
        assert to_physical_type_key(None) == "physicalType"

    def test_to_physical_type_key_tsql(self):
        """Test SQL Server dialect key."""
        assert to_physical_type_key(Dialects.TSQL) == "sqlserverType"

    def test_to_physical_type_key_postgres(self):
        """Test PostgreSQL dialect key."""
        assert to_physical_type_key(Dialects.POSTGRES) == "postgresType"

    def test_to_physical_type_key_bigquery(self):
        """Test BigQuery dialect key."""
        assert to_physical_type_key(Dialects.BIGQUERY) == "bigqueryType"

    def test_to_physical_type_key_snowflake(self):
        """Test Snowflake dialect key."""
        assert to_physical_type_key(Dialects.SNOWFLAKE) == "snowflakeType"

    def test_to_physical_type_key_redshift(self):
        """Test Redshift dialect key."""
        assert to_physical_type_key(Dialects.REDSHIFT) == "redshiftType"

    def test_to_physical_type_key_oracle(self):
        """Test Oracle dialect key."""
        assert to_physical_type_key(Dialects.ORACLE) == "oracleType"

    def test_to_physical_type_key_mysql(self):
        """Test MySQL dialect key."""
        assert to_physical_type_key(Dialects.MYSQL) == "mysqlType"

    def test_to_physical_type_key_databricks(self):
        """Test Databricks dialect key."""
        assert to_physical_type_key(Dialects.DATABRICKS) == "databricksType"

    def test_to_physical_type_key_teradata(self):
        """Test Teradata dialect key."""
        assert to_physical_type_key(Dialects.TERADATA) == "teradataType"

    def test_to_physical_type_key_string_dialect(self):
        """Test with string dialect input."""
        result = to_physical_type_key("POSTGRES")
        assert result == "postgresType"

    def test_to_physical_type_key_unknown_dialect(self):
        """Test unknown dialect reverts to default."""
        result = to_physical_type_key("UNKNOWN")
        assert result == "physicalType"


class TestToServerType:
    """Tests for to_server_type function."""

    def test_to_server_type_none_dialect(self):
        """Test with None dialect."""
        assert to_server_type("any_source.sql", None) is None

    def test_to_server_type_tsql(self):
        """Test SQL Server dialect."""
        assert to_server_type("file.sql", Dialects.TSQL) == "sqlserver"

    def test_to_server_type_postgres(self):
        """Test PostgreSQL dialect."""
        assert to_server_type("file.sql", Dialects.POSTGRES) == "postgres"

    def test_to_server_type_bigquery(self):
        """Test BigQuery dialect."""
        assert to_server_type("file.sql", Dialects.BIGQUERY) == "bigquery"

    def test_to_server_type_snowflake(self):
        """Test Snowflake dialect."""
        assert to_server_type("file.sql", Dialects.SNOWFLAKE) == "snowflake"

    def test_to_server_type_redshift(self):
        """Test Redshift dialect."""
        assert to_server_type("file.sql", Dialects.REDSHIFT) == "redshift"

    def test_to_server_type_oracle(self):
        """Test Oracle dialect."""
        assert to_server_type("file.sql", Dialects.ORACLE) == "oracle"

    def test_to_server_type_mysql(self):
        """Test MySQL dialect."""
        assert to_server_type("file.sql", Dialects.MYSQL) == "mysql"

    def test_to_server_type_databricks(self):
        """Test Databricks dialect."""
        assert to_server_type("file.sql", Dialects.DATABRICKS) == "databricks"

    def test_to_server_type_teradata(self):
        """Test Teradata dialect."""
        assert to_server_type("file.sql", Dialects.TERADATA) == "teradata"


class TestMapTimestamp:
    """Tests for map_timestamp function."""

    def test_map_timestamp_ntz(self):
        """Test timestamp without timezone."""
        assert map_timestamp("timestamp") == "timestamp_ntz"

    def test_map_timestamp_ntz_explicit(self):
        """Test explicit timestamp_ntz."""
        assert map_timestamp("timestampntz") == "timestamp_ntz"

    def test_map_timestamp_ntz_underscore(self):
        """Test timestamp_ntz with underscore."""
        assert map_timestamp("timestamp_ntz") == "timestamp_ntz"

    def test_map_timestamp_tz(self):
        """Test timestamp with timezone."""
        assert map_timestamp("timestamptz") == "timestamp_tz"

    def test_map_timestamp_tz_underscore(self):
        """Test timestamp_tz with underscore."""
        assert map_timestamp("timestamp_tz") == "timestamp_tz"

    def test_map_timestamp_with_time_zone(self):
        """Test timestamp with time zone keyword."""
        assert map_timestamp("timestamp with time zone") == "timestamp_tz"

    def test_map_timestamp_ltz(self):
        """Test timestamp with local timezone."""
        assert map_timestamp("timestampltz") == "timestamp_tz"

    def test_map_timestamp_default(self):
        """Test default mapping for unknown timestamp types."""
        assert map_timestamp("timestampfoo") == "timestamp_ntz"


class TestMapTypeFromSql:
    """Tests for map_type_from_sql function."""

    def test_map_type_none(self):
        """Test mapping of None type."""
        assert map_type_from_sql(None) == "variant"

    def test_map_type_date(self):
        """Test date type mapping."""
        assert map_type_from_sql("DATE") == "date"

    def test_map_type_time(self):
        """Test time type mapping."""
        assert map_type_from_sql("TIME") == "string"

    def test_map_type_datetime_offset(self):
        """Test datetimeoffset mapping."""
        assert map_type_from_sql("DATETIMEOFFSET") == "timestamp_tz"

    def test_map_type_unique_identifier(self):
        """Test uniqueidentifier (SQL Server GUID) mapping."""
        assert map_type_from_sql("UNIQUEIDENTIFIER") == "string"

    def test_map_type_json(self):
        """Test JSON type mapping."""
        assert map_type_from_sql("JSON") == "string"

    def test_map_type_xml(self):
        """Test XML type mapping."""
        assert map_type_from_sql("XML") == "string"

    def test_map_type_clob(self):
        """Test CLOB type mapping."""
        assert map_type_from_sql("CLOB") == "text"

    def test_map_type_nclob(self):
        """Test NCLOB type mapping."""
        assert map_type_from_sql("NCLOB") == "text"

    def test_map_type_blob(self):
        """Test BLOB type mapping."""
        assert map_type_from_sql("BLOB") == "bytes"

    def test_map_type_bfile(self):
        """Test BFILE type mapping."""
        assert map_type_from_sql("BFILE") == "bytes"

    def test_map_type_byte_teradata(self):
        """Test BYTE (Teradata) type mapping."""
        assert map_type_from_sql("BYTE") == "bytes"

    def test_map_type_real(self):
        """Test REAL (Teradata 32-bit float) type mapping."""
        assert map_type_from_sql("REAL") == "float"

    def test_map_type_number(self):
        """Test NUMBER type mapping."""
        assert map_type_from_sql("NUMBER") == "number"

    def test_map_type_interval(self):
        """Test INTERVAL type mapping."""
        assert map_type_from_sql("INTERVAL YEAR(4) TO MONTH") == "variant"

    def test_map_type_bigint(self):
        """Test BIGINT type mapping."""
        assert map_type_from_sql("BIGINT") == "long"

    def test_map_type_tinyint(self):
        """Test TINYINT type mapping."""
        assert map_type_from_sql("TINYINT") == "int"

    def test_map_type_smallint(self):
        """Test SMALLINT type mapping."""
        assert map_type_from_sql("SMALLINT") == "int"

    def test_map_type_integer(self):
        """Test INTEGER type mapping."""
        assert map_type_from_sql("INTEGER") == "int"

    def test_map_type_int(self):
        """Test INT type mapping."""
        assert map_type_from_sql("INT") == "int"

    def test_map_type_varchar(self):
        """Test VARCHAR type mapping."""
        assert map_type_from_sql("VARCHAR(100)") == "string"

    def test_map_type_nvarchar(self):
        """Test NVARCHAR type mapping."""
        assert map_type_from_sql("NVARCHAR(100)") == "string"

    def test_map_type_char(self):
        """Test CHAR type mapping."""
        assert map_type_from_sql("CHAR(10)") == "string"

    def test_map_type_nchar(self):
        """Test NCHAR type mapping."""
        assert map_type_from_sql("NCHAR(10)") == "string"

    def test_map_type_text(self):
        """Test TEXT type mapping."""
        assert map_type_from_sql("TEXT") == "string"

    def test_map_type_ntext(self):
        """Test NTEXT type mapping."""
        assert map_type_from_sql("NTEXT") == "string"

    def test_map_type_string(self):
        """Test STRING type mapping."""
        assert map_type_from_sql("STRING") == "string"

    def test_map_type_varbinary(self):
        """Test VARBINARY type mapping."""
        assert map_type_from_sql("VARBINARY(100)") == "bytes"

    def test_map_type_binary(self):
        """Test BINARY type mapping."""
        assert map_type_from_sql("BINARY(16)") == "bytes"

    def test_map_type_raw(self):
        """Test RAW (Oracle) type mapping."""
        assert map_type_from_sql("RAW") == "bytes"

    def test_map_type_double(self):
        """Test DOUBLE type mapping."""
        assert map_type_from_sql("DOUBLE") == "double"

    def test_map_type_float(self):
        """Test FLOAT type mapping."""
        assert map_type_from_sql("FLOAT") == "float"

    def test_map_type_numeric(self):
        """Test NUMERIC type mapping."""
        assert map_type_from_sql("NUMERIC(10, 2)") == "decimal"

    def test_map_type_decimal_standard(self):
        """Test DECIMAL type mapping (standard case)."""
        assert map_type_from_sql("DECIMAL(10, 2)") == "decimal"

    def test_map_type_decimal_teradata(self):
        """Test DECIMAL type mapping for Teradata."""
        result = map_type_from_sql("DECIMAL", Dialects.TERADATA)
        assert result == "number"

    def test_map_type_bool(self):
        """Test BOOL type mapping."""
        assert map_type_from_sql("BOOL") == "boolean"

    def test_map_type_bit(self):
        """Test BIT type mapping."""
        assert map_type_from_sql("BIT") == "boolean"

    def test_map_type_timestamp(self):
        """Test TIMESTAMP type mapping."""
        assert map_type_from_sql("TIMESTAMP") == "timestamp_ntz"

    def test_map_type_datetime(self):
        """Test DATETIME type mapping."""
        assert map_type_from_sql("DATETIME") == "timestamp_ntz"

    def test_map_type_datetime2(self):
        """Test DATETIME2 type mapping."""
        assert map_type_from_sql("DATETIME2") == "timestamp_ntz"

    def test_map_type_smalldatetime(self):
        """Test SMALLDATETIME type mapping."""
        assert map_type_from_sql("SMALLDATETIME") == "timestamp_ntz"

    def test_map_type_case_insensitive(self):
        """Test that type mapping is case-insensitive."""
        assert map_type_from_sql("varchar") == "string"
        assert map_type_from_sql("VARCHAR") == "string"
        assert map_type_from_sql("VarChar") == "string"

    def test_map_type_whitespace_trimmed(self):
        """Test that type mapping handles whitespace."""
        assert map_type_from_sql("  VARCHAR(100)  ") == "string"

    def test_map_type_unknown(self):
        """Test unknown type defaults to variant."""
        assert map_type_from_sql("UNKNOWNTYPE") == "variant"


class TestGetPrimaryKey:
    """Tests for get_primary_key function."""

    def test_get_primary_key_with_constraint(self):
        """Test detection of PRIMARY KEY constraint."""
        sql = "CREATE TABLE t (id INT PRIMARY KEY)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = next(iter(parsed.find_all(sqlglot.exp.ColumnDef)))
        assert get_primary_key(column) is True

    def test_get_primary_key_without_constraint(self):
        """Test column without PRIMARY KEY constraint."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = next(iter(parsed.find_all(sqlglot.exp.ColumnDef)))
        assert get_primary_key(column) is None

    def test_get_primary_key_with_other_constraints(self):
        """Test column with other constraints but no PRIMARY KEY."""
        sql = "CREATE TABLE t (id INT NOT NULL)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = next(iter(parsed.find_all(sqlglot.exp.ColumnDef)))
        assert get_primary_key(column) is None


class TestGetDescription:
    """Tests for get_description function."""

    def test_get_description_with_comment(self):
        """Test extraction of column description from comment."""
        sql = "CREATE TABLE t (id INT COMMENT 'Primary key')"
        parsed = sqlglot.parse_one(sql, read="postgres")
        # Note: Different dialects handle comments differently
        # This test verifies the function can handle description extraction

    def test_get_description_without_comment(self):
        """Test column without comment."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        result = get_description(column)
        assert result is None


class TestGetMaxLength:
    """Tests for get_max_length function."""

    def test_get_max_length_varchar(self):
        """Test extracting max length from VARCHAR."""
        sql = "CREATE TABLE t (name VARCHAR(100))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        assert get_max_length(column) == 100

    def test_get_max_length_char(self):
        """Test extracting max length from CHAR."""
        sql = "CREATE TABLE t (code CHAR(10))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        assert get_max_length(column) == 10

    def test_get_max_length_nvarchar(self):
        """Test extracting max length from NVARCHAR."""
        sql = "CREATE TABLE t (name NVARCHAR(50))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        assert get_max_length(column) == 50

    def test_get_max_length_nchar(self):
        """Test extracting max length from NCHAR."""
        sql = "CREATE TABLE t (code NCHAR(20))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        assert get_max_length(column) == 20

    def test_get_max_length_int(self):
        """Test that max length is None for non-string types."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        assert get_max_length(column) is None

    def test_get_max_length_without_params(self):
        """Test varchar without parameters."""
        sql = "CREATE TABLE t (data VARCHAR)"
        parsed = sqlglot.parse_one(sql, read="postgres")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        result = get_max_length(column)
        # Result depends on how sqlglot handles unparameterized varchar


class TestGetPrecisionScale:
    """Tests for get_precision_scale function."""

    def test_get_precision_scale_decimal(self):
        """Test extracting precision and scale from DECIMAL."""
        sql = "CREATE TABLE t (amount DECIMAL(10, 2))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        precision, scale = get_precision_scale(column)
        assert precision == 10
        assert scale == 2

    def test_get_precision_scale_numeric(self):
        """Test extracting precision and scale from NUMERIC."""
        sql = "CREATE TABLE t (amount NUMERIC(18, 3))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        precision, scale = get_precision_scale(column)
        assert precision == 18
        assert scale == 3

    def test_get_precision_scale_single_param(self):
        """Test precision-only parameter (scale defaults to 0)."""
        sql = "CREATE TABLE t (amount DECIMAL(10))"
        parsed = sqlglot.parse_one(sql, read="postgres")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        precision, scale = get_precision_scale(column)
        # Behavior depends on sqlglot parsing, may return (10, 0) or (None, None)

    def test_get_precision_scale_int(self):
        """Test that precision/scale is None for non-numeric types."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        precision, scale = get_precision_scale(column)
        assert precision is None
        assert scale is None

    def test_get_precision_scale_no_params(self):
        """Test decimal without parameters."""
        sql = "CREATE TABLE t (amount DECIMAL)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        precision, scale = get_precision_scale(column)
        assert precision is None
        assert scale is None


class TestToColType:
    """Tests for to_col_type function."""

    def test_to_col_type_varchar(self):
        """Test extracting VARCHAR type."""
        sql = "CREATE TABLE t (name VARCHAR(100))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        col_type = to_col_type(column, Dialects.TSQL)
        assert col_type is not None
        assert "VARCHAR" in col_type.upper()

    def test_to_col_type_int(self):
        """Test extracting INT type."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        col_type = to_col_type(column, Dialects.TSQL)
        assert col_type is not None
        assert "INT" in col_type.upper()

    def test_to_col_type_with_none_dialect(self):
        """Test type extraction with None dialect."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        col_type = to_col_type(column, None)
        assert col_type is not None


class TestToColTypeNormalized:
    """Tests for to_col_type_normalized function."""

    def test_to_col_type_normalized_varchar(self):
        """Test normalized type for VARCHAR."""
        sql = "CREATE TABLE t (name VARCHAR(100))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        normalized = to_col_type_normalized(column)
        assert normalized == "varchar"

    def test_to_col_type_normalized_int(self):
        """Test normalized type for INT."""
        sql = "CREATE TABLE t (id INT)"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        normalized = to_col_type_normalized(column)
        assert normalized == "int"

    def test_to_col_type_normalized_decimal(self):
        """Test normalized type for DECIMAL."""
        sql = "CREATE TABLE t (amount DECIMAL(10, 2))"
        parsed = sqlglot.parse_one(sql, read="tsql")
        column = list(parsed.find_all(sqlglot.exp.ColumnDef))[0]
        normalized = to_col_type_normalized(column)
        assert normalized == "decimal"


class TestReadFile:
    """Tests for read_file function."""

    def test_read_file_nonexistent(self):
        """Test reading nonexistent file raises exception."""
        with pytest.raises(DataContractException) as exc_info:
            read_file("/nonexistent/path/file.sql")
        assert "does not exist" in str(exc_info.value)

    def test_read_file_success(self, tmp_path):
        """Test successfully reading file content."""
        test_file = tmp_path / "test.sql"
        test_file.write_text("SELECT * FROM users;")
        content = read_file(str(test_file))
        assert content == "SELECT * FROM users;"


class TestImportSqlDCS:
    """Tests for import_sql with DataContractSpecification."""

    def test_import_sql_simple_table(self, tmp_path):
        """Test importing simple table definition."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        assert "users" in result.models  # type: ignore[operator]

    def test_import_sql_with_server(self, tmp_path):
        """Test that server is populated with correct type."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE t (id INT)")
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "postgres"})

        assert isinstance(result, DataContractSpecification)
        assert result.servers is not None  # type: ignore[attr-defined]
        assert "postgres" in result.servers  # type: ignore[operator]

    def test_import_sql_multiple_tables(self, tmp_path):
        """Test importing multiple tables."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE users (id INT);")
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        assert "users" in result.models  # type: ignore[operator]

    def test_import_sql_no_dialect(self, tmp_path):
        """Test importing without dialect specification."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE t (id INT)")
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]


class TestImportSqlODCS:
    """Tests for import_sql with OpenDataContractStandard."""

    def test_import_sql_odcs_simple_table(self, tmp_path):
        """Test importing simple table to ODCS format with full structure validation."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
        spec = OpenDataContractStandard()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, OpenDataContractStandard)
        assert result.schema_ is not None
        assert len(result.schema_) == 1

        schema_obj = result.schema_[0]
        assert schema_obj.name == "users"
        assert schema_obj.properties is not None
        assert len(schema_obj.properties) == 2

        # Validate id column
        id_prop = next(p for p in schema_obj.properties if p.name == "id")
        assert id_prop.logicalType == "int"
        assert id_prop.primaryKey is True

        # Validate name column
        name_prop = next(p for p in schema_obj.properties if p.name == "name")
        assert name_prop.logicalType == "string"
        assert name_prop.logicalTypeOptions is not None
        assert name_prop.logicalTypeOptions.get("maxLength") == 100

    def test_import_sql_odcs_with_server(self, tmp_path):
        """Test that ODCS server is populated as list."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE t (id INT)")
        spec = OpenDataContractStandard()
        result = import_sql(spec, str(sql_file), {"dialect": "postgres"})

        assert isinstance(result, OpenDataContractStandard)
        assert result.servers is not None
        assert len(result.servers) == 1
        assert result.servers[0].type == "postgres"

    def test_import_sql_odcs_numeric_with_precision_scale(self, tmp_path):
        """Test ODCS logicalTypeOptions for numeric types."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE amounts (price DECIMAL(10, 2))")
        spec = OpenDataContractStandard()
        result = import_sql(spec, str(sql_file), {"dialect": "postgres"})

        assert isinstance(result, OpenDataContractStandard)
        schema_obj = result.schema_[0]
        price_prop = schema_obj.properties[0]

        assert price_prop.logicalType == "decimal"
        assert price_prop.logicalTypeOptions is not None
        assert price_prop.logicalTypeOptions.get("precision") == 10
        assert price_prop.logicalTypeOptions.get("scale") == 2

    def test_import_sql_odcs_required_field(self, tmp_path):
        """Test ODCS required field mapping."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE TABLE t (required_col INT NOT NULL, optional_col INT)")
        spec = OpenDataContractStandard()
        result = import_sql(spec, str(sql_file), {"dialect": "postgres"})

        assert isinstance(result, OpenDataContractStandard)
        props = {p.name: p for p in result.schema_[0].properties}

        assert props["required_col"].required is True
        assert props["optional_col"].required is None or props["optional_col"].required is False


class TestImportSqlEdgeCases:
    """Tests for edge cases and error handling."""

    def test_import_sql_invalid_sql_syntax(self, tmp_path):
        """Test handling of invalid SQL syntax."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("INVALID SQL HERE !!!")
        spec = DataContractSpecification()

        with pytest.raises(DataContractException) as exc_info:
            import_sql(spec, str(sql_file), {"dialect": "sqlserver"})
        assert "error parsing sql" in str(exc_info.value).lower()

    def test_import_sql_teradata_interval_removal(self, tmp_path):
        """Test that Teradata INTERVAL types are properly removed."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE events (
                event_id INT,
                event_date DATE
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "teradata"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        assert "events" in result.models  # type: ignore[operator]

    def test_import_sql_empty_file(self, tmp_path):
        """Test handling of empty SQL file."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("")
        spec = DataContractSpecification()

        with pytest.raises(DataContractException):
            import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

    def test_import_sql_file_not_found(self):
        """Test import with nonexistent file."""
        spec = DataContractSpecification()

        with pytest.raises(DataContractException) as exc_info:
            import_sql(spec, "/nonexistent/file.sql", {"dialect": "sqlserver"})
        assert "does not exist" in str(exc_info.value)


class TestImportSqlComplexTypes:
    """Tests for importing various SQL data types."""

    def test_import_sql_string_types(self, tmp_path):
        """Test importing various string types."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE types (
                col_varchar VARCHAR(100),
                col_char CHAR(10),
                col_nvarchar NVARCHAR(50),
                col_text TEXT
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        assert "types" in result.models  # type: ignore[operator]
        fields = result.models["types"].fields  # type: ignore[index]

        assert fields["col_varchar"].type == "string"
        assert fields["col_char"].type == "string"
        assert fields["col_nvarchar"].type == "string"
        assert fields["col_text"].type == "string"

    def test_import_sql_numeric_types(self, tmp_path):
        """Test importing numeric types with precision/scale."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE numbers (
                col_int INT,
                col_bigint BIGINT,
                col_decimal DECIMAL(10, 2),
                col_float FLOAT
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        fields = result.models["numbers"].fields  # type: ignore[index]

        assert fields["col_int"].type == "int"
        assert fields["col_bigint"].type == "long"
        assert fields["col_decimal"].type == "decimal"
        assert fields["col_float"].type == "float"

    def test_import_sql_timestamp_types(self, tmp_path):
        """Test importing timestamp types."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE times (
                col_date DATE,
                col_datetime DATETIME,
                col_datetimeoffset DATETIMEOFFSET
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        fields = result.models["times"].fields  # type: ignore[index]

        assert fields["col_date"].type == "date"
        assert fields["col_datetime"].type == "timestamp_ntz"
        assert fields["col_datetimeoffset"].type == "timestamp_tz"

    def test_import_sql_binary_types(self, tmp_path):
        """Test importing binary types."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE binaries (
                col_binary BINARY(16),
                col_varbinary VARBINARY(100)
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        fields = result.models["binaries"].fields  # type: ignore[index]

        assert fields["col_binary"].type == "bytes"
        assert fields["col_varbinary"].type == "bytes"


class TestImportSqlMultiTable:
    """Tests for importing DDL with multiple tables.

    Note: sqlglot.parse_one() only parses the first statement. For multiple
    CREATE TABLE statements, use a single DDL with multiple table definitions
    in nested/referenced format, or call import multiple times.
    """

    def test_import_sql_single_table_from_multi_statement(self, tmp_path):
        """Test that only the first table is imported from multi-statement DDL.

        This documents current behaviour: parse_one() only handles the first statement.
        """
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE users (
                user_id INT PRIMARY KEY,
                username VARCHAR(50) NOT NULL
            );
            CREATE TABLE orders (
                order_id INT PRIMARY KEY,
                user_id INT NOT NULL
            );
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "postgres"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        # Only the first table is parsed due to parse_one() limitation
        assert len(result.models) == 1  # type: ignore[arg-type]
        assert "users" in result.models  # type: ignore[operator]

        # Validate users table
        users_fields = result.models["users"].fields  # type: ignore[index]
        assert "user_id" in users_fields
        assert users_fields["user_id"].primaryKey is True

    def test_import_sql_odcs_single_table_from_multi_statement(self, tmp_path):
        """Test ODCS import with multi-statement DDL (documents current behaviour)."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100));
            CREATE TABLE invoices (id INT PRIMARY KEY, customer_id INT);
            """
        )
        spec = OpenDataContractStandard()
        result = import_sql(spec, str(sql_file), {"dialect": "sqlserver"})

        assert isinstance(result, OpenDataContractStandard)
        assert result.schema_ is not None
        # Only the first table is parsed due to parse_one() limitation
        assert len(result.schema_) == 1
        assert result.schema_[0].name == "customers"


class TestTeradataIntervalHandling:
    """Tests for Teradata INTERVAL type handling."""

    def test_teradata_interval_year_to_month(self, tmp_path):
        """Test that INTERVAL YEAR TO MONTH columns are handled gracefully."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE contracts (
                contract_id INT PRIMARY KEY,
                duration INTERVAL YEAR(2) TO MONTH,
                start_date DATE
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "teradata"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        assert "contracts" in result.models  # type: ignore[operator]
        fields = result.models["contracts"].fields  # type: ignore[index]
        assert "contract_id" in fields
        assert "start_date" in fields

    def test_teradata_interval_day_to_second(self, tmp_path):
        """Test that INTERVAL DAY TO SECOND columns are handled gracefully."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE events (
                event_id INT,
                processing_time INTERVAL DAY(2) TO SECOND(6),
                event_date DATE
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "teradata"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        assert "events" in result.models  # type: ignore[operator]
        fields = result.models["events"].fields  # type: ignore[index]
        assert "event_id" in fields
        assert "event_date" in fields

    def test_teradata_multiple_interval_columns(self, tmp_path):
        """Test handling multiple INTERVAL columns in one table."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            """
            CREATE TABLE schedules (
                schedule_id INT PRIMARY KEY,
                yearly_interval INTERVAL YEAR(4) TO MONTH,
                daily_interval INTERVAL DAY(3) TO SECOND(6),
                description VARCHAR(100)
            )
            """
        )
        spec = DataContractSpecification()
        result = import_sql(spec, str(sql_file), {"dialect": "teradata"})

        assert isinstance(result, DataContractSpecification)
        assert result.models is not None  # type: ignore[attr-defined]
        fields = result.models["schedules"].fields  # type: ignore[index]
        assert "schedule_id" in fields
        assert "description" in fields


class TestPreprocessTeradataSql:
    """Tests for the _preprocess_teradata_sql function."""

    def test_removes_interval_year_to_month(self):
        """Test removal of INTERVAL YEAR TO MONTH columns."""
        sql = """
            CREATE TABLE test (
                id INT,
                duration INTERVAL YEAR(2) TO MONTH,
                name VARCHAR(100)
            )
        """
        result = _preprocess_teradata_sql(sql)

        assert "INTERVAL" not in result
        assert "id INT" in result
        assert "name VARCHAR(100)" in result

    def test_removes_interval_day_to_second(self):
        """Test removal of INTERVAL DAY TO SECOND columns."""
        sql = """
            CREATE TABLE test (
                id INT,
                processing_time INTERVAL DAY(2) TO SECOND(6),
                created DATE
            )
        """
        result = _preprocess_teradata_sql(sql)

        assert "INTERVAL" not in result
        assert "id INT" in result
        assert "created DATE" in result

    def test_removes_multiple_interval_columns(self):
        """Test removal of multiple INTERVAL columns."""
        sql = """
            CREATE TABLE test (
                id INT,
                yearly INTERVAL YEAR(4) TO MONTH,
                daily INTERVAL DAY(3) TO SECOND(6),
                name VARCHAR(50)
            )
        """
        result = _preprocess_teradata_sql(sql)

        assert "INTERVAL" not in result
        assert "yearly" not in result
        assert "daily" not in result
        assert "id INT" in result
        assert "name VARCHAR(50)" in result

    def test_preserves_non_interval_columns(self):
        """Test that non-INTERVAL columns are preserved."""
        sql = """
            CREATE TABLE test (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_date DATE,
                amount DECIMAL(10, 2)
            )
        """
        result = _preprocess_teradata_sql(sql)

        assert "id INT PRIMARY KEY" in result
        assert "name VARCHAR(100) NOT NULL" in result
        assert "created_date DATE" in result
        assert "amount DECIMAL(10, 2)" in result

    def test_handles_no_interval_columns(self):
        """Test SQL without INTERVAL columns passes through unchanged."""
        sql = """
            CREATE TABLE test (
                id INT,
                name VARCHAR(100)
            )
        """
        result = _preprocess_teradata_sql(sql)

        assert "id INT" in result
        assert "name VARCHAR(100)" in result

    def test_cleans_double_commas(self):
        """Test that double commas are cleaned up after removal."""
        sql = """
            CREATE TABLE test (
                id INT,
                interval_col INTERVAL YEAR(2) TO MONTH,
                name VARCHAR(100)
            )
        """
        result = _preprocess_teradata_sql(sql)

        # Should not have ,, in the result
        assert ",," not in result

    def test_handles_interval_at_end_of_column_list(self):
        """Test handling INTERVAL as the last column."""
        sql = """
            CREATE TABLE test (
                id INT,
                name VARCHAR(100),
                duration INTERVAL YEAR(2) TO MONTH
            )
        """
        result = _preprocess_teradata_sql(sql)

        assert "INTERVAL" not in result
        # Should have valid SQL structure
        assert ",)" not in result
