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


def _install(
    monkeypatch: pytest.MonkeyPatch,
    seen: list[dict],
    fail: bool = False,
    selects_versions: bool | None = None,
):
    class Conversor:
        def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
            seen.append({"input": Path(input_dir), "files": _files(Path(input_dir))})
            if fail:
                raise ValueError("falhou")
            return type("R", (), {"converted": 1, "skipped": 0, "errors": []})()

    if selects_versions is not None:
        Conversor.selects_versions = selects_versions  # type: ignore[attr-defined]
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


def test_converter_that_selects_its_own_versions_gets_the_original_folder(
    tmp_path: Path, scratch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pasta = _source(tmp_path, TWO_VERSIONS)
    seen: list[dict] = []
    _install(monkeypatch, seen, selects_versions=True)
    result = _run(pasta, tmp_path)
    assert result.exit_code == 0, result.output
    assert seen[0]["input"] == pasta
    assert seen[0]["files"] == sorted(TWO_VERSIONS)
    assert not scratch.exists() or list(scratch.iterdir()) == []


def test_refuses_to_copy_when_links_are_not_possible(
    tmp_path: Path, scratch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pasta = _source(tmp_path, TWO_VERSIONS)
    seen: list[dict] = []
    _install(monkeypatch, seen)

    def no_link(*_args: object, **_kwargs: object) -> None:
        raise OSError("sem link")

    monkeypatch.setattr("os.link", no_link)
    monkeypatch.setattr("os.symlink", no_link)
    result = _run(pasta, tmp_path)
    assert result.exit_code == 1
    assert "lake select" in " ".join(result.output.split())
    assert not seen
    assert list(scratch.iterdir()) == []


def test_orphan_views_are_removed_on_the_next_run(
    tmp_path: Path, scratch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json
    import socket

    from quantilica_sql import vigente

    scratch.mkdir()
    orfa = scratch / "quantilica-vigente-orfa"
    orfa.mkdir()
    (orfa / "owner.json").write_text(
        json.dumps({"pid": 999999, "hostname": socket.gethostname()})
    )
    monkeypatch.setattr(vigente, "_pid_alive", lambda pid: False)
    pasta = _source(tmp_path, TWO_VERSIONS)
    _install(monkeypatch, [])
    assert _run(pasta, tmp_path).exit_code == 0
    assert list(scratch.iterdir()) == []


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
