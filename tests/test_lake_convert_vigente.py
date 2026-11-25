"""`lake convert` entrega ao conversor só a versão vigente de cada arquivo."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from quantilica_sql.cli import app

runner = CliRunner()

TWO_VERSIONS = ["a_2025@20260101.csv", "a_2025@20261021.csv", "b_2025@20261021.csv"]
CURRENT = ["a_2025@20261021.csv", "b_2025@20261021.csv"]


def _files(root: Path) -> list[str]:
    return sorted(p.name for p in root.rglob("*") if p.is_file())


@pytest.fixture
def scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "scratch"
    monkeypatch.setenv("QUANTILICA_LAKE_SCRATCH", str(path))
    return path


def _source(tmp_path: Path, names: list[str]) -> Path:
    pasta = tmp_path / "fonte"
    pasta.mkdir()
    for name in names:
        (pasta / name).write_text(name, encoding="utf-8")
    return pasta


def _install(monkeypatch: pytest.MonkeyPatch, seen: list[dict], fail: bool = False):
    class Conversor:
        def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
            seen.append({"input": Path(input_dir), "files": _files(Path(input_dir))})
            if fail:
                raise ValueError("falhou")
            return type("R", (), {"converted": 1, "skipped": 0, "errors": []})()

    monkeypatch.setattr(
        "quantilica.core.lake.load_lake_converter", lambda source: Conversor()
    )


def _run(pasta: Path, tmp_path: Path):
    return runner.invoke(
        app,
        ["lake", "convert", "fonte-x", "-s", str(pasta), "--lake", str(tmp_path / "l")],
    )


def test_converter_sees_only_the_current_version(
    tmp_path: Path, scratch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pasta = _source(tmp_path, TWO_VERSIONS)
    seen: list[dict] = []
    _install(monkeypatch, seen)
    first, second = _run(pasta, tmp_path), _run(pasta, tmp_path)
    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert [s["files"] for s in seen] == [CURRENT, CURRENT]
    assert all(s["input"] != pasta for s in seen)
    assert list(scratch.iterdir()) == []
    assert _files(pasta) == sorted(TWO_VERSIONS)  # bruto intacto


def test_single_version_keeps_the_original_folder(
    tmp_path: Path, scratch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pasta = _source(tmp_path, CURRENT)
    seen: list[dict] = []
    _install(monkeypatch, seen)
    assert _run(pasta, tmp_path).exit_code == 0
    assert seen[0]["input"] == pasta


def test_converter_error_cleans_the_view(
    tmp_path: Path, scratch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pasta = _source(tmp_path, TWO_VERSIONS)
    seen: list[dict] = []
    _install(monkeypatch, seen, fail=True)
    result = _run(pasta, tmp_path)
    assert result.exit_code != 0
    assert seen and not seen[0]["input"].exists()
    assert list(scratch.iterdir()) == []
