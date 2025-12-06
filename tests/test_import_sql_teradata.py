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
        config:
          teradataType: DECIMAL
      DESCRIPTION:
        type: string
        required: true
        maxLength: 30
        config:
          teradataType: VARCHAR(30)
      AMOUNT:
        type: number
        config:
          teradataType: DECIMAL(10)
      QUALITY:
        type: number
        config:
          teradataType: DECIMAL
      CUSTOM_ATTRIBUTES:
        type: text
        config:
          teradataType: CLOB
      FIELD_VARCHAR:
        type: string
        maxLength: 100
        config:
          teradataType: VARCHAR(100)
      FIELD_NVARCHAR:
        type: string
        maxLength: 100
        config:
          teradataType: VARCHAR(100)
      FIELD_NUMBER:
        type: decimal
        config:
          teradataType: DECIMAL
      FIELD_FLOAT:
        type: float
        config:
          teradataType: FLOAT
      FIELD_DATE:
        type: date
        config:
          teradataType: DATE
      FIELD_BINARY_FLOAT:
        type: float
        config:
          teradataType: REAL
      FIELD_BINARY_DOUBLE:
        type: double
        config:
          teradataType: DOUBLE PRECISION
      FIELD_TIMESTAMP:
        type: timestamp_ntz
        config:
          teradataType: TIMESTAMP(6)
      FIELD_TIMESTAMP_TZ:
        type: timestamp_tz
        config:
          teradataType: TIMESTAMP(6) WITH TIME ZONE
      FIELD_TIMESTAMP_LTZ:
        type: timestamp_tz
        config:
          teradataType: TIMESTAMP(6) WITH TIME ZONE
      FIELD_INTERVAL_YEAR:
        type: variant
        config:
          teradataType: INTERVAL YEAR(2) TO MONTH
      FIELD_INTERVAL_DAY:
        type: variant
        config:
          teradataType: INTERVAL DAY(2) TO SECOND(6)
      FIELD_RAW:
        type: bytes
        config:
          teradataType: BYTE
      FIELD_ROWID:
        type: string
        maxLength: 18
        config:
          teradataType: VARCHAR(18)
      FIELD_UROWID:
        type: string
        maxLength: 18
        config:
          teradataType: VARCHAR(18)
      FIELD_CHAR:
        type: string
        maxLength: 10
        config:
          teradataType: CHAR(10)
      FIELD_NCHAR:
        type: string
        maxLength: 10
        config:
          teradataType: CHAR(10)
      FIELD_CLOB:
        type: text
        config:
          teradataType: CLOB
      FIELD_NCLOB:
        type: text
        config:
          teradataType: CLOB
      FIELD_BLOB:
        type: bytes
        config:
          teradataType: BLOB
      FIELD_BFILE:
        type: bytes
        config:
          teradataType: BLOB
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
