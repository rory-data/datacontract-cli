"""Confluence Storage Format exporter for data contracts.

This module provides functionality to convert data contract specifications to Confluence Storage Format (*.csf) using
Jinja2 templates.

The full data contract YAML section has been removed due to limitations whn pasting into Confluence Source Editor. This
could be reintroduced in future with an API-based publish approach.

The Mermaid diagram has also been removed as it is not guaranteed that a Confluence instance will have the appropriate
plugin installed to render it correctly.
"""

from __future__ import annotations

import datetime
import logging
from importlib.metadata import version
from typing import TYPE_CHECKING

import jinja_partials
import pytz
import yaml
from jinja2 import Environment, PackageLoader, Template, select_autoescape
from open_data_contract_standard.model import OpenDataContractStandard

from datacontract.export.exporter import Exporter
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
    if isinstance(data_contract_spec, DataContractSpecification):
        template = get_j2_template("datacontract_confluence.html.j2")
        quality_specification = get_dcs_quality_specification(data_contract_spec)
    elif isinstance(data_contract_spec, OpenDataContractStandard):
        template = get_j2_template("datacontract_odcs_confluence.html.j2")
        quality_specification = None
    else:
        raise ValueError(f"Unsupported data contract specification type: {type(data_contract_spec)}")

    # Render the template with necessary data
    return template.render(
        datacontract=data_contract_spec,
        quality_specification=quality_specification,
        formatted_date=_formatted_date(),
        datacontract_cli_version=get_version(),
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
        auto_reload=True,
        cache_size=0,
    )
    # Set up for partials
    jinja_partials.register_environment(env)

    return env.get_template(template)


def get_dcs_quality_specification(data_contract_spec: DataContractSpecification) -> str | None:
    """Extract quality specification from DataContractSpecification, handling deprecation warnings."""

    if data_contract_spec.quality is not None and isinstance(data_contract_spec.quality.specification, str):
        return data_contract_spec.quality.specification
    elif data_contract_spec.quality is not None and isinstance(data_contract_spec.quality.specification, object):
        if data_contract_spec.quality.type == "great-expectations":
            return yaml.dump(data_contract_spec.quality.specification, sort_keys=False, default_style="|")
        else:
            return yaml.dump(data_contract_spec.quality.specification, sort_keys=False)
    else:
        return None
