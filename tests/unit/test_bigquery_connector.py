"""Unit tests for BigQueryConnector."""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest
from google.cloud import bigquery

from ai_data_lab.connectors.bigquery import BigQueryConnector


def _make_field(name: str, field_type: str = "STRING", mode: str = "NULLABLE", *, description: str | None = None, fields: list | None = None):
    field = mock.Mock()
    field.name = name
    field.field_type = field_type
    field.mode = mode
    field.description = description
    field.fields = fields or []
    return field


def test_connector_uses_env_project(monkeypatch):
    env_project = "env-project"
    monkeypatch.setenv("BIGQUERY_PROJECT_ID", env_project)

    mock_client_instance = mock.Mock()
    mock_client_class = mock.Mock(return_value=mock_client_instance)
    monkeypatch.setattr(bigquery, "Client", mock_client_class)

    connector = BigQueryConnector()

    assert connector.project_id == env_project
    mock_client_class.assert_called_once_with(project=env_project, credentials=None, location=None)
    assert connector._client is mock_client_instance


def test_query_returns_dataframe(monkeypatch):
    df = pd.DataFrame({"value": [1]})
    mock_job = mock.Mock()
    mock_result = mock.Mock()
    mock_result.to_dataframe.return_value = df
    mock_job.result.return_value = mock_result

    mock_client = mock.Mock()
    mock_client.query.return_value = mock_job

    connector = BigQueryConnector(project_id="proj", client=mock_client)

    result = connector.query("SELECT 1")

    mock_client.query.assert_called_once_with("SELECT 1", job_config=None)
    assert result.equals(df)


def test_list_datasets_returns_structured_dicts():
    dataset = mock.Mock()
    dataset.project = "proj"
    dataset.dataset_id = "dataset"
    dataset.full_dataset_id = "proj:dataset"
    dataset.friendly_name = "Friendly dataset"

    mock_client = mock.Mock()
    mock_client.list_datasets.return_value = [dataset]

    connector = BigQueryConnector(project_id="proj", client=mock_client)

    datasets = connector.list_datasets()

    assert datasets == [
        {
            "project_id": "proj",
            "dataset_id": "dataset",
            "full_dataset_id": "proj:dataset",
            "friendly_name": "Friendly dataset",
        }
    ]


def test_list_tables_returns_structured_dicts():
    table = mock.Mock()
    table.project = "proj"
    table.dataset_id = "dataset"
    table.table_id = "table"
    table.full_table_id = "proj:dataset.table"
    table.friendly_name = "Friendly table"
    table.table_type = "TABLE"

    mock_client = mock.Mock()
    mock_client.list_tables.return_value = [table]

    connector = BigQueryConnector(project_id="proj", client=mock_client)
    tables = connector.list_tables("dataset")

    assert tables == [
        {
            "project_id": "proj",
            "dataset_id": "dataset",
            "table_id": "table",
            "full_table_id": "proj:dataset.table",
            "friendly_name": "Friendly table",
            "table_type": "TABLE",
        }
    ]

    mock_client.list_tables.assert_called_once()


def test_get_table_schema_handles_nested_fields():
    child_field = _make_field("nested_field", "INTEGER", "REQUIRED")
    parent_field = _make_field("parent", "RECORD", "REPEATED", fields=[child_field])

    mock_table = mock.Mock()
    mock_table.schema = [parent_field]

    mock_client = mock.Mock()
    mock_client.get_table.return_value = mock_table

    connector = BigQueryConnector(project_id="proj", client=mock_client)
    schema = connector.get_table_schema("dataset", "table")

    assert schema == [
        {
            "name": "parent",
            "field_type": "RECORD",
            "mode": "REPEATED",
            "description": None,
            "fields": [
                {
                    "name": "nested_field",
                    "field_type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": None,
                    "fields": [],
                }
            ],
        }
    ]


def test_get_table_info_returns_expected_structure():
    schema_field = _make_field("value", "FLOAT")

    mock_table = mock.Mock()
    mock_table.project = "proj"
    mock_table.dataset_id = "dataset"
    mock_table.table_id = "table"
    mock_table.full_table_id = "proj:dataset.table"
    mock_table.description = "Sample table"
    mock_table.num_rows = 100
    mock_table.num_bytes = 2048
    mock_table.schema = [schema_field]
    mock_table.time_partitioning = None
    mock_table.range_partitioning = None
    mock_table.clustering_fields = ["value"]
    mock_table.labels = {"env": "dev"}
    mock_table.table_type = "TABLE"

    mock_client = mock.Mock()
    mock_client.get_table.return_value = mock_table

    connector = BigQueryConnector(project_id="proj", client=mock_client)
    info = connector.get_table_info("dataset", "table")

    assert info["project_id"] == "proj"
    assert info["dataset_id"] == "dataset"
    assert info["table_id"] == "table"
    assert info["schema"][0]["name"] == "value"
    assert info["num_rows"] == 100
    assert info["num_bytes"] == 2048
    assert info["clustering_fields"] == ["value"]
    assert info["labels"] == {"env": "dev"}
    assert info["table_type"] == "TABLE"


