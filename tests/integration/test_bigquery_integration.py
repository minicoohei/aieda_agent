"""Integration tests for BigQueryConnector."""

from __future__ import annotations

import pytest

from ai_data_lab.connectors.bigquery import BigQueryConnector

try:
    import google.auth
except ImportError:  # pragma: no cover - defensive
    google = None


pytestmark = pytest.mark.integration


def _adc_available() -> bool:
    if google is None:
        return False
    try:
        credentials, _ = google.auth.default()
    except Exception:
        return False
    return credentials is not None


@pytest.mark.integration
def test_query_select_one_returns_dataframe():
    if not _adc_available():
        pytest.skip("Application Default Credentials are not configured.")

    connector = BigQueryConnector()
    df = connector.query("SELECT 1 AS value")

    assert not df.empty
    assert "value" in df.columns
    assert df.iloc[0]["value"] == 1


@pytest.mark.integration
def test_list_datasets_returns_list():
    if not _adc_available():
        pytest.skip("Application Default Credentials are not configured.")

    connector = BigQueryConnector()
    datasets = connector.list_datasets()

    assert isinstance(datasets, list)


