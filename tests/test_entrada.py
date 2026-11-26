"""Resolvedor de entrada compartilhado (``adapters/_entrada.py``)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from quantilica_sql.adapters._entrada import arquivos_vigentes

_RE = re.compile(r"^arq_(\d+)@")


def _chave(p: Path) -> str | None:
    m = _RE.match(p.name)
    return m.group(1) if m else None


def _resolve(raiz: Path, **kw):
    kw.setdefault("all_partitions", True)
    kw.setdefault("table", None)
    return arquivos_vigentes(
        raiz,
        eh_dado=lambda p: p.name.startswith("arq_"),
        chave_de=_chave,
        fonte="TESTE",
        esperado="arq_<id>@AAAAMMDD.json",
        **kw,
    )


@pytest.fixture
def pasta(tmp_path: Path) -> Path:
    (tmp_path / "sub").mkdir()
    for nome in (
        "arq_1@20260101.json",
        "arq_1@20260201.json",
        "arq_2@20260101T100000.json",
        "arq_2@20260101T120000.json",
        "arq_2@20260101T120000.json.manifest.json",  # sidecar
        "arq_3@20260101.json.tmp",  # escrita interrompida
        "indice@20260101.json",  # auxiliar
    ):
        (tmp_path / nome).write_text("[]")
    return tmp_path


def test_so_a_versao_vigente_sem_sidecar_tmp_nem_auxiliar(pasta):
    nomes = [p.name for p in _resolve(pasta)]
    assert nomes == ["arq_1@20260201.json", "arq_2@20260101T120000.json"]


def test_table_filtra_pela_chave(pasta):
    assert [p.name for p in _resolve(pasta, all_partitions=False, table="2")] == [
        "arq_2@20260101T120000.json"
    ]


def test_normaliza_chave_nos_dois_lados(pasta):
    nomes = _resolve(
        pasta,
        all_partitions=False,
        table="00002",
        normaliza_chave=lambda c: str(int(c)),
    )
    assert [p.name for p in nomes] == ["arq_2@20260101T120000.json"]


def test_pasta_sem_all_nem_table_da_erro_claro(pasta):
    with pytest.raises(ValueError, match="--all.*--table <id>"):
        _resolve(pasta, all_partitions=False)


def test_pasta_sem_arquivo_de_dados_da_erro_claro(tmp_path):
    (tmp_path / "indice@20260101.json").write_text("[]")
    with pytest.raises(ValueError, match="nenhum arquivo de dados do TESTE"):
        _resolve(tmp_path)


def test_table_sem_correspondencia_nomeia_a_chave(pasta):
    with pytest.raises(ValueError, match="nenhum arquivo de dados da tabela 9"):
        _resolve(pasta, all_partitions=False, table="9")


def test_todas_as_versoes_devolve_cada_versao_sem_sidecar_tmp_nem_auxiliar(pasta):
    nomes = [p.name for p in _resolve(pasta, todas_as_versoes=True)]
    assert nomes == [
        "arq_1@20260101.json",
        "arq_1@20260201.json",
        "arq_2@20260101T100000.json",
        "arq_2@20260101T120000.json",
    ]


def test_todas_as_versoes_respeita_table_e_exige_all_ou_table(pasta):
    nomes = _resolve(pasta, all_partitions=False, table="1", todas_as_versoes=True)
    assert [p.name for p in nomes] == ["arq_1@20260101.json", "arq_1@20260201.json"]
    with pytest.raises(ValueError, match="--all.*--table <id>"):
        _resolve(pasta, all_partitions=False, todas_as_versoes=True)
