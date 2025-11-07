"""Confluence Storage Format exporter for data contracts.

This module provides functionality to convert data contract specifications to Confluence Storage Format (*.csf) using
Jinja2 templates.
"""

from __future__ import annotations

import datetime
import logging
import warnings
from importlib.metadata import version
from typing import TYPE_CHECKING

import jinja_partials
import pytz
import yaml
from jinja2 import Environment, PackageLoader, Template, select_autoescape
from open_data_contract_standard.model import OpenDataContractStandard

from datacontract.export.exporter import Exporter
from datacontract.export.mermaid_exporter import to_mermaid
from datacontract.model.data_contract_specification import DataContractSpecification

if TYPE_CHECKING:
    from datacontract.model.data_contract_specification import (
        Model,
    )


class ConfluenceExporter(Exporter):
    def export(
        self,
        data_contract: DataContractSpecification,
        model: Model,
        server: str,
        sql_server_type: str,
        export_args: dict,
    ) -> dict | str:
        return to_csf(data_contract)


def to_csf(data_contract_spec: DataContractSpecification | OpenDataContractStandard) -> str:
    """Convert data contract specification to Confluence Storage Format (CSF)."""
    # Load the required template
    # needs to be included in /MANIFEST.in
    if isinstance(data_contract_spec, DataContractSpecification):
        template = get_j2_template("datacontract_confluence.html.j2")
    elif isinstance(data_contract_spec, OpenDataContractStandard):
        template = get_j2_template("datacontract_odcs_confluence.html.j2")
    else:
        raise ValueError(f"Unsupported data contract specification type: {type(data_contract_spec)}")

    quality_specification = None
    if isinstance(data_contract_spec, DataContractSpecification):
        # Suppress deprecation warnings for backward compatibility with v1.0.0 contracts
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            if data_contract_spec.quality is not None and isinstance(data_contract_spec.quality.specification, str):
                quality_specification = data_contract_spec.quality.specification
            elif data_contract_spec.quality is not None and isinstance(
                data_contract_spec.quality.specification, object
            ):
                if data_contract_spec.quality.type == "great-expectations":
                    quality_specification = yaml.dump(
                        data_contract_spec.quality.specification, sort_keys=False, default_style="|"
                    )
                else:
                    quality_specification = yaml.dump(data_contract_spec.quality.specification, sort_keys=False)

    datacontract_yaml = data_contract_spec.to_yaml()

    # Get the mermaid diagram
    mermaid_diagram = to_mermaid(data_contract_spec)

    # Render the template with necessary data
    return template.render(
        datacontract=data_contract_spec,
        quality_specification=quality_specification,
        # style=style_content,
        datacontract_yaml=datacontract_yaml,
        formatted_date=_formatted_date(),
        datacontract_cli_version=get_version(),
        mermaid_diagram=mermaid_diagram,
    )


def _formatted_date() -> str:
    """Get the current date formatted for Confluence."""
    tz = pytz.timezone("UTC")
    now = datetime.datetime.now(tz)
    return now.strftime("%d %b %Y %H:%M:%S UTC")


def get_version() -> str:
    """Get the current version of the datacontract_cli package."""
    try:
        return version("datacontract_cli")
    except Exception as e:
        logging.debug("Ignoring exception", e)
        return ""


def get_j2_template(template: str) -> Template:
    """Load Jinja2 template from package."""
    package_loader = PackageLoader("datacontract", "templates")
    env = Environment(
        loader=package_loader,
        autoescape=select_autoescape(
            enabled_extensions="html",
            default_for_string=True,
        ),
    )
    # Set up for partials
    jinja_partials.register_environment(env)

    return env.get_template(template)
