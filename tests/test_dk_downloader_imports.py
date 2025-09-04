"""Basic import test for dk_downloader."""
from pathlib import Path
import sys

def test_import() -> None:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    import tools.dk_downloader as dk

    assert hasattr(dk, "__version__")
