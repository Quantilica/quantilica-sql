"""O pacote precisa importar e montar a CLI sem o extra `analysis` (polars)."""

from __future__ import annotations

import subprocess
import sys

_CODE = (
    "import sys; sys.modules['polars'] = None; sys.modules['pyarrow'] = None\n"
    "import quantilica_sql.adapters\n"
    "from quantilica_sql.cli import app\n"
    "assert app is not None\n"
)


def test_importa_sem_polars() -> None:
    cp = subprocess.run([sys.executable, "-c", _CODE], capture_output=True, text=True)
    assert cp.returncode == 0, cp.stderr
