import json
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from core.labeling import validate_targets


@pytest.fixture
def sample_data(tmpdir):
    """Create a temporary parquet dataset for CLI testing."""
    df = pd.DataFrame(
        {
            "time": pd.to_datetime(pd.date_range("2023-01-01", periods=100, freq="min")),
            "open": list(range(100)),
            "high": [i + 0.5 for i in range(100)],
            "low": [i - 0.5 for i in range(100)],
            "close": list(range(100)),
            "volume": [1000] * 100,
        }
    )
    file_path = tmpdir.join("sample_data.parquet")
    df.to_parquet(file_path)
    return str(file_path)


def test_labeling_cli_script(sample_data, tmpdir):
    """Run the CLI with the same Python interpreter used by pytest."""
    report_output_path = tmpdir.join("report.json")
    labeled_data_output_path = tmpdir.join("labeled_data.parquet")

    command = [
        sys.executable,
        "scripts/labeling_cli.py",
        "--input-path",
        sample_data,
        "--output-path",
        str(labeled_data_output_path),
        "--report-output-path",
        str(report_output_path),
        "--horizon",
        "10",
        "--atr-period",
        "14",
    ]

    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()

    result = subprocess.run(command, capture_output=True, text=True, env=env)

    assert result.returncode == 0, f"Script failed with error: {result.stderr}"
    assert report_output_path.exists(), "Output JSON report was not created."
    assert labeled_data_output_path.exists(), "Labeled data parquet was not created."

    with open(str(report_output_path), "r", encoding="utf-8") as file:
        report = json.load(file)

    assert report["total_rows"] == 100
    assert report["horizon"] == 10
    assert report["missing_values"] == 10
    assert "up_atr_stats" in report
    assert "down_atr_stats" in report
    assert "mean" in report["up_atr_stats"]
    assert "mean" in report["down_atr_stats"]
    assert "up_atr_share_gt_1" in report
    assert "up_atr_share_gt_2" in report
    assert "down_atr_share_gt_1" in report
    assert "down_atr_share_gt_2" in report
    assert "input_path" in report
    assert "labeled_output_path" in report
    assert "report_output_path" in report
    assert "timestamp" in report
    assert "python_version" in report


def test_validate_targets():
    horizon = 5
    valid = pd.DataFrame(
        {
            "up_atr": [1, 2, 3, 4, 5, np.nan, np.nan, np.nan, np.nan, np.nan],
            "down_atr": [1, 2, 3, 4, 5, np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )
    validate_targets(valid, horizon)

    invalid = pd.DataFrame(
        {
            "up_atr": [1, 2, 3, 4, 5, np.nan, np.nan, np.nan],
            "down_atr": [1, 2, 3, 4, 5, np.nan, np.nan, np.nan],
        }
    )
    with pytest.raises(ValueError):
        validate_targets(invalid, horizon)
