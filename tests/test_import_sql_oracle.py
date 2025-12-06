import yaml
from typer.testing import CliRunner

from datacontract.cli import app
from datacontract.data_contract import DataContract

# logging.basicConfig(level=logging.DEBUG, force=True)

data_definition_file = "fixtures/oracle/import/ddl.sql"


def test_cli():
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import",
            "--format",
            "sql",
            "--source",
            data_definition_file,
        ],
    )
    assert result.exit_code == 0


def test_cli_odcs():
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import",
            "--format",
            "sql",
            "--source",
            data_definition_file,
            "--spec",
            "odcs",
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "apiVersion" in output
    assert "kind: DataContract" in output


def test_import_sql_oracle():
    result = DataContract.import_from_source("sql", data_definition_file, dialect="oracle")

    expected = """
dataContractSpecification: 1.2.1
id: my-data-contract-id
info:
  title: My Data Contract
  version: 0.0.1
servers:
  oracle:
    type: oracle
models:
  field_showcase:
    type: table
    fields:
      field_primary_key:
        type: int
        primaryKey: true
        description: Primary key
        config:
          oracleType: INT
      field_not_null:
        type: int
        required: true
        description: Not null
        config:
          oracleType: INT
      field_varchar:
        type: string
        description: Variable-length string
        config:
          oracleType: VARCHAR2
      field_nvarchar:
        type: string
        description: Variable-length Unicode string
        config:
          oracleType: NVARCHAR2
      field_number:
        type: number
        description: Number
        config:
          oracleType: NUMBER
      field_float:
        type: float
        description: Float
        config:
          oracleType: FLOAT
      field_date:
        type: date
        description: Date and Time down to second precision
        config:
          oracleType: DATE
      field_binary_float:
        type: float
        description: 32-bit floating point number
        config:
          oracleType: FLOAT
      field_binary_double:
        type: double
        description: 64-bit floating point number
        config:
          oracleType: DOUBLE PRECISION
      field_timestamp:
        type: timestamp_ntz
        description: Timestamp with fractional second precision of 6, no timezones
        config:
          oracleType: TIMESTAMP
      field_timestamp_tz:
        type: timestamp_tz
        description: Timestamp with fractional second precision of 6, with timezones
          (TZ)
        config:
          oracleType: TIMESTAMP WITH TIME ZONE
      field_timestamp_ltz:
        type: timestamp_tz
        description: Timestamp with fractional second precision of 6, with local timezone
          (LTZ)
        config:
          oracleType: TIMESTAMPLTZ
      field_interval_year:
        type: variant
        description: Interval of time in years and months with default (2) precision
        config:
          oracleType: INTERVAL YEAR TO MONTH
      field_interval_day:
        type: variant
        description: Interval of time in days, hours, minutes and seconds with default
          (2 / 6) precision
        config:
          oracleType: INTERVAL DAY TO SECOND
      field_raw:
        type: bytes
        description: Large raw binary data
        config:
          oracleType: RAW
      field_rowid:
        type: variant
        description: Base 64 string representing a unique row address
        config:
          oracleType: ROWID
      field_urowid:
        type: variant
        description: Base 64 string representing the logical address
        config:
          oracleType: UROWID
      field_char:
        type: string
        description: Fixed-length string
        maxLength: 10
        config:
          oracleType: CHAR(10)
      field_nchar:
        type: string
        description: Fixed-length Unicode string
        maxLength: 10
        config:
          oracleType: NCHAR(10)
      field_clob:
        type: text
        description: Character large object
        config:
          oracleType: CLOB
      field_nclob:
        type: text
        description: National character large object
        config:
          oracleType: NCLOB
      field_blob:
        type: bytes
        description: Binary large object
        config:
          oracleType: BLOB
      field_bfile:
        type: bytes
        config:
          oracleType: BFILE
    """
    print("Result", result.to_yaml())
    assert yaml.safe_load(result.to_yaml()) == yaml.safe_load(expected)
    # Disable linters so we don't get "missing description" warnings
    assert DataContract(data_contract_str=expected).lint().has_passed()
