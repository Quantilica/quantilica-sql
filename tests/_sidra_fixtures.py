"""Auxiliares dos testes do SIDRA: fixtures reais e variações montadas a partir delas."""

from __future__ import annotations

import atexit
import json
import re
import shutil
import tempfile
from pathlib import Path

from quantilica_sql.sidra import Papel, papeis_por_url

DADOS = Path(__file__).parent / "data" / "sidra"

T, V, P = Papel("territorio"), Papel("variavel"), Papel("periodo")


def C(i: int) -> Papel:
    return Papel("classificacao", i)


# arquivo (relativo a tests/data/sidra) -> papéis de D1..Dn pela URL da consulta
REAIS: dict[str, list[Papel]] = {
    "t-1086/t-1086_p-202601_f-a_n1-all_v-all_c12716-all_c12529-all@20260915.json": [
        T,
        V,
        P,
        C(12716),
        C(12529),
    ],
    "t-1612/t-1612_p-2025_f-a_n3-all_v-all_c81-all@20261023.json": [T, V, P, C(81)],
    "t-1737/t-1737_p-202610_f-a_n1-all_v-all@20261112.json": [T, V, P],
    "t-6381/t-6381_p-202609_f-a_n1-all_v-all@20261030.json": [T, V, P],
    "t-5932/t-5932_p-202602_f-a_n1-all_v-all_c11255-all@20260901.json": [
        T,
        V,
        P,
        C(11255),
    ],
    "t-7060/t-7060_p-202610_f-a_n1-all_v-all_c315-all@20261112.json": [T, V, P, C(315)],
    "t-6579/t-6579_p-2026_f-a_n6-all_v-all@20260828.json": [T, V, P],
    "ordens-de-url/t-1086/t-1086_p-202601_f-a_n1-all_v-all_c12716-all_c12529-all_o-npvc@20260915.json": [
        T,
        P,
        V,
        C(12716),
        C(12529),
    ],
    "ordens-de-url/t-1086/t-1086_p-202601_f-a_n1-all_v-all_c12716-all_c12529-all_o-ccnvp@20260915.json": [
        C(12716),
        C(12529),
        T,
        V,
        P,
    ],
    "ordens-de-url/t-1086/t-1086_p-202601_f-a_n1-all_n3-all_v-all_c12716-all_c12529-all_o-n1n3@20260915.json": [
        T,
        V,
        P,
        C(12716),
        C(12529),
    ],
    "ordens-de-url/t-1086/t-1086_p-202601_f-a_n1-all_v-all_c12716-all_c12529-all_o-pnvc@20260915.json": [
        P,
        T,
        V,
        C(12716),
        C(12529),
    ],
}

# (nome, arquivo-base, ordem nova das dimensões, em índices 0-based do original)
PERMUTADOS = [
    (
        "1086-c2-c1-p-v-n",
        "t-1086/t-1086_p-202601_f-a_n1-all_v-all_c12716-all_c12529-all@20260915.json",
        [4, 3, 2, 1, 0],
    ),
    (
        "1612-p-c-v-n",
        "t-1612/t-1612_p-2025_f-a_n3-all_v-all_c81-all@20261023.json",
        [2, 3, 1, 0],
    ),
    ("6579-v-n-p", "t-6579/t-6579_p-2026_f-a_n6-all_v-all@20260828.json", [1, 0, 2]),
]


def copia_pasta(origem: Path, destino: Path) -> Path:
    """Copia a pasta da tabela (dados, sidecars e metadados) para ``destino``."""
    shutil.copytree(origem, destino)
    return destino


def segmentos_da_url(url: str) -> list[tuple[str, str]]:
    segs = [s for s in url.split("/values/", 1)[1].split("/") if s]
    return list(zip(segs[::2], segs[1::2], strict=True))


def permuta(arquivo: Path, ordem: list[int], destino: Path) -> Path:
    """Regrava ``arquivo`` com as dimensões em outra ordem (como a API faria).

    ``ordem[k]`` é o índice (0-based) da dimensão original que vira ``D<k+1>``; o
    sidecar ganha a URL com os segmentos na mesma ordem.
    """
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    n = len(ordem)
    novas = []
    for r in linhas:
        nova = {k: v for k, v in r.items() if not re.fullmatch(r"D\d+[CN]", k)}
        for k, orig in enumerate(ordem, 1):
            nova[f"D{k}C"] = r[f"D{orig + 1}C"]
            nova[f"D{k}N"] = r[f"D{orig + 1}N"]
        novas.append(nova)
    assert n == len([k for k in linhas[0] if re.fullmatch(r"D\d+C", k)])
    destino.mkdir(parents=True, exist_ok=True)
    saida = destino / arquivo.name
    saida.write_text(json.dumps(novas, ensure_ascii=False, indent=2), encoding="utf-8")
    sidecar = json.loads(
        arquivo.with_name(arquivo.name + ".manifest.json").read_text(encoding="utf-8")
    )
    segs = segmentos_da_url(sidecar["url"])
    dims = [s for s in segs if s[0] not in ("t", "h", "f", "d")]
    # um segmento por dimensão (n<k> repetidos não ocorrem nos casos permutados)
    assert len(dims) == n
    t = [s for s in segs if s[0] == "t"]
    resto = [s for s in segs if s[0] in ("h", "f", "d")]
    novos = t + [dims[o] for o in ordem] + resto
    sidecar["url"] = "https://apisidra.ibge.gov.br/values/" + "/".join(
        f"{k}/{v}" for k, v in novos
    )
    saida.with_name(saida.name + ".manifest.json").write_text(
        json.dumps(sidecar, ensure_ascii=False), encoding="utf-8"
    )
    for m in arquivo.parent.glob("metadados@*"):
        shutil.copy(m, destino / m.name)
    return saida


def papeis_esperados_permutados(arquivo: Path, ordem: list[int]) -> list[Papel]:
    """Papéis da ordem original (pela URL real) reordenados."""
    sidecar = json.loads(
        arquivo.with_name(arquivo.name + ".manifest.json").read_text(encoding="utf-8")
    )
    originais = papeis_por_url(sidecar["url"])
    return [originais[o] for o in ordem]


def nome_do_caso(rel: str) -> str:
    """``1086-npvc``, ``1612``...: o id da tabela e, se houver, a ordem."""
    nome = Path(rel).name
    tabela = nome.split("_")[0].removeprefix("t-")
    ordem = re.search(r"_o-(\w+)@", nome)
    return f"{tabela}-{ordem.group(1)}" if ordem else tabela


def casos_catorze() -> dict[str, tuple[Path, list[Papel]]]:
    """As 11 respostas reais e 3 permutações montadas a partir delas."""
    tmp = Path(tempfile.mkdtemp(prefix="sidra-casos-"))
    atexit.register(shutil.rmtree, tmp, ignore_errors=True)
    casos = {nome_do_caso(rel): (DADOS / rel, papeis) for rel, papeis in REAIS.items()}
    for nome, rel, ordem in PERMUTADOS:
        arq = DADOS / rel
        saida = permuta(arq, ordem, tmp / nome / Path(rel).parent.name)
        casos[nome] = (saida, papeis_esperados_permutados(arq, ordem))
    return casos


CASOS = casos_catorze()
