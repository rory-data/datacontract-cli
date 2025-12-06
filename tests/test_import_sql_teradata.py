import yaml
from typer.testing import CliRunner

from datacontract.cli import app
from datacontract.data_contract import DataContract

# logging.basicConfig(level=logging.DEBUG, force=True)

datacontract = "fixtures/teradata/datacontract-teradata-dcs.yaml"
sql_file_path = "fixtures/teradata/data/testcase.sql"


def test_cli():
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import",
            "--format",
            "sql",
            "--source",
            sql_file_path,
            "--dialect",
            "teradata",
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "dataContractSpecification" in output
    assert "teradata" in output


def test_cli_odcs():
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "import",
            "--format",
            "sql",
            "--source",
            sql_file_path,
            "--dialect",
            "teradata",
            "--spec",
            "odcs",
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "apiVersion" in output
    assert "kind: DataContract" in output
    assert "teradata" in output


def test_import_sql_teradata():
    result = DataContract.import_from_source("sql", sql_file_path, dialect="teradata")

    expected = """
dataContractSpecification: 1.2.1
id: my-data-contract-id
info:
  title: My Data Contract
  version: 0.0.1
servers:
  teradata:
    type: teradata
models:
  checks_testcase:
    type: table
    fields:
      CTC_ID:
        type: number
        primaryKey: true
        description: Primary key
        config:
          teradataType: DECIMAL
      DESCRIPTION:
        type: string
        required: true
        maxLength: 30
        description: Description
        config:
          teradataType: VARCHAR(30)
      AMOUNT:
        type: decimal
        precision: 10
        scale: 0
        description: Amount purchased
        config:
          teradataType: DECIMAL(10)
      QUALITY:
        type: number
        description: Percentage of checks passed
        config:
          teradataType: DECIMAL
      CUSTOM_ATTRIBUTES:
        type: string
        description: Custom attributes
        config:
          teradataType: TEXT
      FIELD_VARCHAR:
        type: string
        maxLength: 100
        description: Variable-length string
        config:
          teradataType: VARCHAR(100)
      FIELD_NVARCHAR:
        type: string
        maxLength: 100
        description: Variable-length Unicode string (Teradata uses VARCHAR)
        config:
          teradataType: VARCHAR(100)
      FIELD_NUMBER:
        type: number
        description: Number
        config:
          teradataType: DECIMAL
      FIELD_FLOAT:
        type: float
        description: Float
        config:
          teradataType: FLOAT
      FIELD_DATE:
        type: date
        description: Date and Time down to day precision
        config:
          teradataType: DATE
      FIELD_BINARY_FLOAT:
        type: float
        description: 32-bit floating point number
        config:
          teradataType: FLOAT
      FIELD_BINARY_DOUBLE:
        type: double
        description: 64-bit floating point number
        config:
          teradataType: DOUBLE PRECISION
      FIELD_TIMESTAMP:
        type: timestamp_ntz
        description: Timestamp with fractional second precision of 6, no timezones
        config:
          teradataType: TIMESTAMP(6)
      FIELD_TIMESTAMP_TZ:
        type: timestamp_tz
        description: Timestamp with fractional second precision of 6, with timezones (TZ)
        config:
          teradataType: TIMESTAMP(6) WITH TIME ZONE
      FIELD_TIMESTAMP_LTZ:
        type: timestamp_tz
        description: Timestamp with fractional second precision of 6, with timezone support
        config:
          teradataType: TIMESTAMP(6) WITH TIME ZONE
      FIELD_RAW:
        type: int
        description: Large raw binary data
        config:
          teradataType: TINYINT
      FIELD_ROWID:
        type: string
        maxLength: 18
        description: Base 64 string representing a unique row address
        config:
          teradataType: VARCHAR(18)
      FIELD_UROWID:
        type: string
        maxLength: 18
        description: Base 64 string representing the logical address
        config:
          teradataType: VARCHAR(18)
      FIELD_CHAR:
        type: string
        maxLength: 10
        description: Fixed-length string
        config:
          teradataType: CHAR(10)
      FIELD_NCHAR:
        type: string
        maxLength: 10
        description: Fixed-length Unicode string (Teradata uses CHAR)
        config:
          teradataType: CHAR(10)
      FIELD_CLOB:
        type: string
        description: Character large object
        config:
          teradataType: TEXT
      FIELD_NCLOB:
        type: string
        description: National character large object (Teradata uses CLOB)
        config:
          teradataType: TEXT
      FIELD_BLOB:
        type: bytes
        description: Binary large object
        config:
          teradataType: VARBINARY
      FIELD_BFILE:
        type: bytes
        description: Binary file (Teradata uses BLOB)
        config:
          teradataType: VARBINARY
    """
    print("Result", result.to_yaml())
    assert yaml.safe_load(result.to_yaml()) == yaml.safe_load(expected)
    # Disable linters so we don't get "missing description" warnings
    assert DataContract(data_contract_str=expected).lint().has_passed()


def test_import_sql_constraints():
    result = DataContract.import_from_source("sql", "fixtures/teradata/data/data_constraints.sql", dialect="teradata")

    expected = """
dataContractSpecification: 1.2.1
id: my-data-contract-id
info:
  title: My Data Contract
  version: 0.0.1
servers:
  teradata:
    type: teradata
models:
  customer_location:
    type: table
    fields:
      id:
        type: number
        required: true
        config:
          teradataType: DECIMAL
      created_by:
        type: string
        required: true
        maxLength: 30
        config:
          teradataType: VARCHAR(30)
      create_date:
        type: timestamp_ntz
        required: true
        config:
          teradataType: TIMESTAMP
      changed_by:
        type: string
        maxLength: 30
        config:
          teradataType: VARCHAR(30)
      change_date:
        type: timestamp_ntz
        config:
          teradataType: TIMESTAMP
    """
    print("Result", result.to_yaml())
    assert yaml.safe_load(result.to_yaml()) == yaml.safe_load(expected)
    # Disable linters so we don't get "missing description" warnings
    assert DataContract(data_contract_str=expected).lint().has_passed()
