from pathlib import Path

from open_data_contract_standard.model import OpenDataContractStandard
from typer.testing import CliRunner

from datacontract.cli import app
from datacontract.data_contract import DataContract
from datacontract.export.confluence_exporter import to_csf
from datacontract.export.exporter import ExportFormat
from datacontract.model.data_contract_specification import DataContractSpecification

# logging.basicConfig(level=logging.DEBUG, force=True)


def test_cli():
    runner = CliRunner()
    result = runner.invoke(app, ["export", "./fixtures/export/datacontract.yaml", "--format", "confluence"])
    assert result.exit_code == 0
    assert "<ac:layout>" in result.output
    assert "Data Contract:" in result.output


def test_cli_with_output(tmp_path: Path):
    runner = CliRunner()
    output_file = tmp_path / "datacontract_confluence.html"
    result = runner.invoke(
        app,
        [
            "export",
            "./fixtures/export/datacontract.yaml",
            "--format",
            "confluence",
            "--output",
            str(output_file),
        ],
    )
    assert result.exit_code == 0
    assert output_file.exists()

    content = output_file.read_text()
    assert "<ac:layout>" in content
    assert "Data Contract:" in content


def test_to_csf():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Check basic structure
    assert "<ac:layout>" in result
    assert "</ac:layout>" in result

    # Check contract metadata is present
    assert "orders-unit-test" in result
    assert "Orders Unit Test" in result
    assert "1.0.0" in result

    # Check models section exists
    assert "<h2>Models</h2>" in result
    assert "orders" in result.lower()

    # Check fields are present
    assert "order_id" in result
    assert "order_total" in result
    assert "order_status" in result

    # Check servers section
    assert "<h2>Servers</h2>" in result
    assert "production" in result
    assert "snowflake" in result

    # Check Confluence-specific markup
    assert "<ac:layout-section" in result
    assert "<ac:layout-cell" in result


def test_to_csf_with_mermaid_diagram():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Mermaid diagram is no longer included in Confluence export
    # (Confluence has its own diagram rendering that doesn't use mermaid)
    # Full Data Contract YAML section has been removed
    assert "<h2>Models</h2>" in result


def test_to_csf_with_terms():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Check terms section
    assert "<h2>Terms" in result
    assert "This data contract serves to demo datacontract CLI export" in result
    assert "Not intended to use in production" in result


def test_to_csf_with_quality():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Quality information was previously in the Full YAML section which has been removed
    # Check that basic structure is still present
    assert "<h2>Models</h2>" in result
    assert "order_total" in result


def test_to_csf_with_definitions():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Check definitions section
    assert "<h2>Definitions</h2>" in result
    assert "Customer ID" in result  # Title is rendered, not the key


def test_to_csf_with_full_yaml():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Full YAML section has been removed due to Confluence Cloud paste limitations
    # Check that other sections are still present instead
    assert "<h2>Models</h2>" in result
    assert "<h2>Servers</h2>" in result


def test_to_csf_field_properties():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Check field properties are rendered
    assert "varchar" in result.lower()
    assert "bigint" in result.lower()
    assert "text" in result.lower()

    # Check field attributes
    assert "Required" in result
    assert "Primary Key" in result


def test_to_csf_server_details():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Server details are present in basic format
    assert "snowflake" in result
    assert "production" in result


def test_export_via_data_contract():
    data_contract = DataContract(data_contract_file="fixtures/export/datacontract.yaml")
    result = data_contract.export(ExportFormat.confluence)

    assert isinstance(result, str)
    assert "<ac:layout>" in result
    assert "Data Contract:" in result


def test_to_csf_metadata_footer():
    data_contract = DataContractSpecification.from_file("fixtures/export/datacontract.yaml")
    result = to_csf(data_contract)

    # Check metadata footer
    assert "Generated on" in result
    assert "datacontract-cli" in result


# ODCS (OpenDataContractStandard) Tests


def test_cli_odcs():
    runner = CliRunner()
    result = runner.invoke(app, ["export", "./fixtures/excel/shipments-odcs.yaml", "--format", "confluence"])
    assert result.exit_code == 0
    assert "<ac:layout>" in result.output
    assert "Data Contract:" in result.output


def test_cli_odcs_with_output(tmp_path: Path):
    runner = CliRunner()
    output_file = tmp_path / "datacontract_odcs_confluence.html"
    result = runner.invoke(
        app,
        [
            "export",
            "./fixtures/excel/shipments-odcs.yaml",
            "--format",
            "confluence",
            "--output",
            str(output_file),
        ],
    )
    assert result.exit_code == 0
    assert output_file.exists()

    content = output_file.read_text()
    assert "<ac:layout>" in content
    assert "Data Contract:" in content


def test_to_csf_odcs():
    odcs = OpenDataContractStandard.from_file("fixtures/excel/shipments-odcs.yaml")
    result = to_csf(odcs)

    # Check basic structure
    assert "<ac:layout>" in result
    assert "</ac:layout>" in result

    # Check ODCS contract metadata
    assert "fulfillment_shipments_v1" in result
    assert "Shipments" in result or "shipments" in result

    # Check ODCS schema section exists
    assert "<h2>Schema</h2>" in result

    # Check ODCS properties are present
    assert "shipment_id" in result
    assert "order_id" in result
    assert "delivery_date" in result

    # Check Confluence-specific markup
    assert "<ac:layout-section" in result
    assert "<ac:layout-cell" in result


def test_to_csf_odcs_with_servers():
    odcs = OpenDataContractStandard.from_file("fixtures/excel/shipments-odcs.yaml")
    result = to_csf(odcs)

    # Check servers section
    assert "<h2>Servers</h2>" in result
    assert "production" in result
    assert "bigquery" in result


def test_to_csf_odcs_with_properties():
    odcs = OpenDataContractStandard.from_file("fixtures/excel/shipments-odcs.yaml")
    result = to_csf(odcs)

    # Check that properties/fields are rendered
    assert "Properties" in result or "properties" in result.lower()
    assert "uuid" in result or "text" in result or "timestamp" in result.lower()

    # Check property attributes
    assert "Business Name" in result or "businessName" in result


def test_to_csf_odcs_metadata_footer():
    odcs = OpenDataContractStandard.from_file("fixtures/excel/shipments-odcs.yaml")
    result = to_csf(odcs)

    # Check metadata footer
    assert "Generated on" in result
    assert "datacontract-cli" in result


def test_to_csf_odcs_full_yaml():
    odcs = OpenDataContractStandard.from_file("fixtures/excel/shipments-odcs.yaml")
    result = to_csf(odcs)

    # Full YAML section has been removed due to Confluence Cloud paste limitations
    # Verify other sections are present
    assert "<h2>Schema</h2>" in result
    assert "<h2>Servers</h2>" in result
