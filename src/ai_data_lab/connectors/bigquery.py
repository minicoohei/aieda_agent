"""BigQuery connector that provides pandas-friendly utilities."""

from __future__ import annotations

import os
from typing import Any, Iterable

from google.cloud import bigquery


class BigQueryConnector:
    """Helper class for executing queries and inspecting table metadata."""

    def __init__(
        self,
        *,
        project_id: str | None = None,
        credentials: Any | None = None,
        location: str | None = None,
        client: bigquery.Client | None = None,
    ) -> None:
        env_project = os.getenv("BIGQUERY_PROJECT_ID")
        self.project_id = project_id or env_project
        self._credentials = credentials
        self.location = location
        self._client = client or bigquery.Client(
            project=self.project_id,
            credentials=self._credentials,
            location=self.location,
        )

    def query(self, sql: str, *, job_config: bigquery.QueryJobConfig | None = None, **kwargs: Any):
        """Execute a SQL query and return the result as a pandas DataFrame."""
        if not sql or not isinstance(sql, str):
            raise ValueError("sql must be a non-empty string.")

        job = self._client.query(sql, job_config=job_config, **kwargs)
        result = job.result()
        return result.to_dataframe()

    def list_datasets(self, project_id: str | None = None):
        """Return dataset metadata for the specified (or default) project."""
        project = project_id or self.project_id or self._client.project
        datasets = self._client.list_datasets(project=project)
        return [
            {
                "project_id": dataset.project,
                "dataset_id": dataset.dataset_id,
                "full_dataset_id": dataset.full_dataset_id,
                "friendly_name": getattr(dataset, "friendly_name", None),
            }
            for dataset in datasets
        ]

    def list_tables(self, dataset_id: str, *, project_id: str | None = None):
        """Return table metadata within a dataset."""
        if not dataset_id:
            raise ValueError("dataset_id must be provided.")

        project = project_id or self.project_id or self._client.project
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        tables: Iterable[Any] = self._client.list_tables(dataset_ref)

        formatted_tables = []
        for table in tables:
            formatted_tables.append(
                {
                    "project_id": table.project,
                    "dataset_id": table.dataset_id,
                    "table_id": table.table_id,
                    "full_table_id": table.full_table_id,
                    "friendly_name": getattr(table, "friendly_name", None),
                    "table_type": getattr(table, "table_type", None),
                }
            )
        return formatted_tables

    def get_table_schema(self, dataset_id: str, table_id: str, *, project_id: str | None = None):
        """Return the schema definition for a table in a serializable format."""
        table = self._get_table(dataset_id, table_id, project_id=project_id)
        return self._serialize_schema(table.schema)

    def get_table_info(self, dataset_id: str, table_id: str, *, project_id: str | None = None):
        """Return comprehensive table metadata for agent consumption."""
        table = self._get_table(dataset_id, table_id, project_id=project_id)

        partitioning = self._extract_partitioning(table)

        return {
            "project_id": table.project,
            "dataset_id": table.dataset_id,
            "table_id": table.table_id,
            "full_table_id": table.full_table_id,
            "description": getattr(table, "description", None),
            "num_rows": getattr(table, "num_rows", None),
            "num_bytes": getattr(table, "num_bytes", None),
            "schema": self._serialize_schema(table.schema),
            "partitioning": partitioning,
            "clustering_fields": getattr(table, "clustering_fields", None),
            "labels": getattr(table, "labels", None),
            "table_type": getattr(table, "table_type", None),
        }

    # Helper methods -----------------------------------------------------------------
    def _get_table(self, dataset_id: str, table_id: str, *, project_id: str | None = None):
        if not dataset_id or not table_id:
            raise ValueError("dataset_id and table_id must be provided.")

        project = project_id or self.project_id or self._client.project
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project, dataset_id),
            table_id,
        )
        return self._client.get_table(table_ref)

    def _serialize_schema(self, schema):
        serialized = []
        for field in schema:
            serialized.append(
                {
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": getattr(field, "description", None),
                    "fields": self._serialize_schema(field.fields) if field.fields else [],
                }
            )
        return serialized

    def _extract_partitioning(self, table):
        if getattr(table, "time_partitioning", None):
            tp = table.time_partitioning
            return {
                "type": getattr(tp, "type_", getattr(tp, "type", None)),
                "field": getattr(tp, "field", None),
                "require_partition_filter": getattr(tp, "require_partition_filter", None),
            }
        if getattr(table, "range_partitioning", None):
            rp = table.range_partitioning
            range_def = getattr(rp, "range_", None)
            return {
                "field": getattr(rp, "field", None),
                "range": {
                    "start": getattr(range_def, "start", None),
                    "end": getattr(range_def, "end", None),
                    "interval": getattr(range_def, "interval", None),
                }
                if range_def
                else None,
            }
        return None

