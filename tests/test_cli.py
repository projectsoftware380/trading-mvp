
import json
import subprocess
import pandas as pd
import pytest
import os

@pytest.fixture
def sample_data(tmpdir):
    """Create a sample parquet file for testing."""
    df = pd.DataFrame({
        'time': pd.to_datetime(pd.date_range('2023-01-01', periods=100, freq='min')),
        'open': [i for i in range(100)],
        'high': [i + 0.5 for i in range(100)],
        'low': [i - 0.5 for i in range(100)],
        'close': [i for i in range(100)],
        'volume': [1000 for _ in range(100)]
    })
    file_path = tmpdir.join("sample_data.parquet")
    df.to_parquet(file_path)
    return str(file_path)

def test_labeling_cli_script(sample_data, tmpdir):
    """Test the labeling_cli.py script."""
    output_path = tmpdir.join("report.json")
    
    # Command to execute the script
    command = [
        r".venv\Scripts\python.exe",
        "scripts/labeling_cli.py",
        "--input-path", sample_data,
        "--output-path", str(output_path),
        "--horizon", "10",
        "--atr-period", "14"
    ]
    
    # Set PYTHONPATH to include the project root
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()

    # Run the script
    result = subprocess.run(command, capture_output=True, text=True, env=env)
    
    # Assert the script ran successfully
    assert result.returncode == 0, f"Script failed with error: {result.stderr}"
    assert output_path.exists(), "Output JSON report was not created."
    
    # Load the generated report and verify its contents
    with open(str(output_path), 'r') as f:
        report = json.load(f)
        
    assert report['total_rows'] == 100
    assert report['horizon'] == 10
    assert report['missing_values'] == 10 # horizon
    assert 'up_atr_stats' in report
    assert 'down_atr_stats' in report
    assert 'mean' in report['up_atr_stats']
    assert 'mean' in report['down_atr_stats']
