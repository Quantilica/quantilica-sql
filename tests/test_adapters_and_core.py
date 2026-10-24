"""Testes unitários para adaptadores, copy_stream e partições do quantilica-sql."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl

from quantilica_sql.adapters import get_adapter
from quantilica_sql.core.partitions import (
    ensure_monthly_partition,
    ensure_yearly_partition,
)


class MockDbState:
    """Simulador de estado do PostgreSQL para testar adaptadores e idempotência."""

    def __init__(self) -> None:
        self.loaded_files: set[tuple[str, str, str]] = set()
        self.recorded: list[tuple] = []
        self.sqls: list[tuple[str, tuple | None]] = []

    def make_conn(self, rowcount: int = 10) -> MagicMock:
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.rowcount = rowcount

        mock_copy = MagicMock()
        mock_cur.copy.return_value.__enter__.return_value = mock_copy

        def execute_side_effect(query, params=None):
            q_str = query.as_string() if hasattr(query, "as_string") else str(query)
            self.sqls.append((q_str, params))
            if "SELECT sha256 FROM" in q_str:
                fn = params[0] if params else ""
                ds = params[1] if len(params) > 1 else ""
                part = params[2] if len(params) > 2 else ""
                if (fn, ds, part) in self.loaded_files or (
                    fn,
                    "",
                    "",
                ) in self.loaded_files:
                    mock_cur.fetchone.return_value = ("mock_sha",)
                else:
                    mock_cur.fetchone.return_value = None
            elif "INSERT INTO" in q_str and "arquivo_carregado" in q_str:
                if params:
                    fn, ds, part = params[0], params[1], params[2]
                    self.loaded_files.add((fn, ds, part))
                    self.recorded.append(params)
            elif "SELECT EXISTS" in q_str or "SELECT 1 FROM pg_class" in q_str:
                mock_cur.fetchone.return_value = (True,)
            elif "SELECT count(*)" in q_str:
                mock_cur.fetchone.return_value = (1, 100)
            elif "SELECT nome_arquivo" in q_str:
                part = params[0] if params else ""
                mock_cur.fetchall.return_value = [
                    (fn,) for fn, ds, p in self.loaded_files if p == part
                ]
            elif "SELECT dataset, count(*)" in q_str:
                mock_cur.fetchall.return_value = [("caged", 1, 100)]
            elif "SELECT column_name FROM information_schema.columns" in q_str:
                mock_cur.fetchall.return_value = [("codigo",), ("descricao",)]

        mock_cur.execute.side_effect = execute_side_effect
        return mock_conn


def test_registered_adapters_domains():
    """Valida que adaptadores registrados possuem domain e schema válidos."""
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock()

    all_domains = [
        "rfb",
        "cvm",
        "anp",
        "inmet",
        "pdet",
        "bcb",
        "sidra",
        "tesouro_direto",
        "rtn",
        "comex",
        "anac",
        "b3",
        "datasus",
        "inep",
        "tse",
    ]
    for domain in all_domains:
        adapter = get_adapter(domain)
        assert adapter.domain == domain or domain in adapter.domain
        assert adapter.schema is not None


def test_sidra_adapter_load_and_idempotence():
    """Valida carga do SidraAdapter com sucesso, retorno de linhas e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=5)
    adapter = get_adapter("sidra")
    assert adapter.domain == "sidra"
    assert adapter.schema == "ibge_sidra"

    rows = [
        {
            "tabela_sidra_id": "1732",
            "localidade_id": 1,
            "dimensao_id": 2,
            "periodo_id": 2026,
            "modificacao": "2026-01-01",
            "ativo": True,
            "v": "100.5",
        }
    ]

    with tempfile.NamedTemporaryFile(suffix="_1732.json", mode="w", delete=False) as f:
        json.dump(rows, f)
        temp_path = Path(f.name)

    try:
        # 1ª Carga: deve processar e retornar o total de linhas
        loaded = adapter.load(mock_conn, source_path=temp_path, table_id="1732")
        assert loaded == 5
        assert len(state.recorded) == 1
        fn, ds, part, sha, linhas, meta = state.recorded[0]
        assert fn == temp_path.name
        assert ds == "sidra"
        assert part == "1732"
        assert linhas == 5

        # Status: deve retornar contagens
        status_info = adapter.status(mock_conn)
        assert status_info["domain"] == "sidra"
        assert status_info["schema"] == "ibge_sidra"
        assert status_info["tabelas_carregadas"] >= 0

        # 2ª Carga do mesmo arquivo: idempotência deve retornar 0 e não gravar duplicado
        loaded_second = adapter.load(mock_conn, source_path=temp_path, table_id="1732")
        assert loaded_second == 0
        assert len(state.recorded) == 1  # não houve novo registro
    finally:
        temp_path.unlink(missing_ok=True)


def test_bcb_adapter_load_and_idempotence():
    """Valida carga do BcbAdapter com sucesso, retorno de linhas e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=8)
    adapter = get_adapter("bcb")
    assert adapter.domain == "bcb"
    assert adapter.schema == "bcb_sgs"

    observations = [
        {"data": "01/01/2026", "valor": "12.50"},
        {"data": "01/02/2026", "valor": "13.10"},
    ]

    with tempfile.NamedTemporaryFile(suffix="_432.json", mode="w", delete=False) as f:
        json.dump(observations, f)
        temp_path = Path(f.name)

    try:
        # 1ª Carga
        loaded = adapter.load(mock_conn, source_path=temp_path, series_id=432)
        assert loaded == 8
        assert len(state.recorded) == 1
        fn, ds, part, sha, linhas, meta = state.recorded[0]
        assert fn == temp_path.name
        assert ds == "bcb"
        assert part == "432"
        assert linhas == 8

        # Status
        status_info = adapter.status(mock_conn)
        assert status_info["domain"] == "bcb"
        assert status_info["schema"] == "bcb_sgs"

        # 2ª Carga: idempotência
        loaded_second = adapter.load(mock_conn, source_path=temp_path, series_id=432)
        assert loaded_second == 0
        assert len(state.recorded) == 1
    finally:
        temp_path.unlink(missing_ok=True)


def test_pdet_adapter_load_and_idempotence():
    """Valida carga do PdetAdapter via DataFrame e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=12)
    adapter = get_adapter("pdet")
    assert adapter.domain == "pdet"
    assert adapter.schema == "pdet"

    df = pl.DataFrame({"codigo": ["A", "B"], "descricao": ["Desc A", "Desc B"]})

    # 1ª Carga
    loaded = adapter.load(
        mock_conn,
        df=df,
        dataset="caged",
        particao="2026",
        filename="caged_2026.csv",
    )
    assert loaded == 12
    assert len(state.recorded) == 1
    fn, ds, part, sha, linhas, meta = state.recorded[0]
    assert fn == "caged_2026.csv"
    assert ds == "caged"
    assert part == "2026"
    assert linhas == 12

    # Status
    status_info = adapter.status(mock_conn)
    assert status_info["domain"] == "pdet"
    assert "datasets" in status_info

    # 2ª Carga: idempotência
    loaded_second = adapter.load(
        mock_conn,
        df=df,
        dataset="caged",
        particao="2026",
        filename="caged_2026.csv",
    )
    assert loaded_second == 0
    assert len(state.recorded) == 1


def test_rfb_adapter_load_and_idempotence():
    """Valida carga do RfbAdapter via DataFrame e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=15)
    adapter = get_adapter("rfb")
    assert adapter.domain == "rfb"
    assert adapter.schema == "rfb_cnpj"

    df = pl.DataFrame({"codigo": ["101", "102"], "descricao": ["Ativ 1", "Ativ 2"]})

    # 1ª Carga
    loaded = adapter.load(
        mock_conn,
        df=df,
        table_name="cnaes",
        competencia="2026-10",
        filename="cnaes_2026-10.csv",
    )
    assert loaded == 15
    assert len(state.recorded) == 1
    fn, ds, part, sha, linhas, meta = state.recorded[0]
    assert fn == "cnaes_2026-10.csv"
    assert ds == "rfb_cnpj"
    assert part == "2026-10"
    assert linhas == 15

    # Status
    status_info = adapter.status(mock_conn)
    assert status_info["domain"] == "rfb"
    assert status_info["schema"] == "rfb_cnpj"

    # 2ª Carga: idempotência
    loaded_second = adapter.load(
        mock_conn,
        df=df,
        table_name="cnaes",
        competencia="2026-10",
        filename="cnaes_2026-10.csv",
    )
    assert loaded_second == 0
    assert len(state.recorded) == 1


def test_ensure_yearly_partition_sql():
    """Valida que ensure_yearly_partition emite PARTITION OF com range anual."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = None  # Partição não existe ainda

    part_name = ensure_yearly_partition(mock_conn, "pdet", "rais_vinculos", 2024)
    assert part_name == '"pdet"."rais_vinculos_2024"'

    create_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "CREATE TABLE IF NOT EXISTS" in str(call)
    ]
    assert len(create_calls) == 1
    call_sql = create_calls[0][0][0].as_string()
    assert 'PARTITION OF "pdet"."rais_vinculos"' in call_sql
    assert "FOR VALUES FROM (2024) TO (2025)" in call_sql


def test_ensure_monthly_partition_sql():
    """Valida que ensure_monthly_partition emite range mensal de datas."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = None  # Partição não existe ainda

    part_name = ensure_monthly_partition(
        mock_conn, "rfb_cnpj", "estabelecimentos", 2026, 10
    )
    assert part_name == '"rfb_cnpj"."estabelecimentos_2026_10"'

    create_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "CREATE TABLE IF NOT EXISTS" in str(call)
    ]
    assert len(create_calls) == 1
    call_sql = create_calls[0][0][0].as_string()
    assert 'PARTITION OF "rfb_cnpj"."estabelecimentos"' in call_sql
    assert "FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')" in call_sql


def test_rfb_adapter_directory_load_and_filter(tmp_path: Path):
    """Valida carga de diretório com filtro de grupo e staging no RfbAdapter."""
    import zipfile

    from quantilica_sql.adapters.rfb import RfbAdapter

    comp_dir = tmp_path / "2026-10"
    cnaes_dir = comp_dir / "cnaes"
    empresas_dir = comp_dir / "empresas"
    cnaes_dir.mkdir(parents=True)
    empresas_dir.mkdir(parents=True)

    # Cria ZIP dummy para cnaes
    cnaes_zip = cnaes_dir / "Cnaes1.zip"
    with zipfile.ZipFile(cnaes_zip, "w") as zf:
        zf.writestr(
            "cnaes.csv",
            '"0111301";"Cultivo de arroz"\n"0111302";"Cultivo de milho"\n'.encode(
                "latin-1"
            ),
        )

    # Cria ZIP dummy para empresas
    empresas_zip = empresas_dir / "Empresas1.zip"
    with zipfile.ZipFile(empresas_zip, "w") as zf:
        zf.writestr(
            "empresas.csv",
            '"12345678";"EMPRESA TESTE LTDA";"2062";"49";"10000,00";"01";""\n'.encode(
                "latin-1"
            ),
        )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=2)
    adapter = RfbAdapter()

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    # 1. Carrega apenas grupo 'cnaes'
    rows_cnaes = adapter.load(
        mock_conn,
        source_path=tmp_path,
        competencia="2026-10",
        table="cnaes",
        staging_dir=staging_dir,
    )
    assert rows_cnaes == 2
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "Cnaes1.zip"
    assert state.recorded[0][2] == "2026-10"

    # 2. Carrega tudo (cnaes deve ser pulado por idempotência, empresas deve carregar)
    rows_all = adapter.load(
        mock_conn,
        source_path=tmp_path,
        competencia="2026-10",
        staging_dir=staging_dir,
    )
    assert rows_all == 2
    assert len(state.recorded) == 2
    assert state.recorded[1][0] == "Empresas1.zip"


def test_cvm_adapter_load_csv_and_zip(tmp_path: Path):
    """Valida carga genérica da CVM com CSV, ZIP e idempotência."""
    import zipfile

    from quantilica_sql.adapters.cvm import CvmAdapter

    cvm_dir = tmp_path / "cvm"
    cad_dir = cvm_dir / "cia-cad"
    dfp_dir = cvm_dir / "dfp"
    cad_dir.mkdir(parents=True)
    dfp_dir.mkdir(parents=True)

    # 1. Cria CSV avulso
    csv_file = cad_dir / "cad_cia_aberta@20261017.csv"
    csv_file.write_text(
        "CNPJ_CIA;DENOM_SOCIAL;SIT\n"
        "00.000.000/0001-91;BANCO DO BRASIL S.A.;ATIVO\n"
        "33.000.167/0001-01;PETROLEO BRASILEIRO S.A.;ATIVO\n",
        encoding="latin-1",
    )

    # 2. Cria ZIP contendo CSV
    zip_file = dfp_dir / "dfp_cia_aberta_2026.zip"
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr(
            "dfp_cia_aberta_2026.csv",
            "CNPJ_CIA;DT_REFER;VERSAO\n00.000.000/0001-91;2026-12-31;1\n".encode(
                "latin-1"
            ),
        )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=2)
    adapter = CvmAdapter()

    # Primeira execução: deve carregar ambos os arquivos
    total = adapter.load(mock_conn, source_path=cvm_dir)
    assert total > 0
    assert len(state.recorded) == 2
    filenames = [r[0] for r in state.recorded]
    assert "cad_cia_aberta@20261017.csv" in filenames
    assert "dfp_cia_aberta_2026.zip" in filenames

    # Segunda execução: deve pular ambos por idempotência
    state.recorded.clear()
    total_repeat = adapter.load(mock_conn, source_path=cvm_dir)
    assert total_repeat == 0
    assert len(state.recorded) == 0


def test_anp_adapter_load_csv_and_skip(tmp_path: Path):
    """Valida carga genérica da ANP e skip idempotente."""
    from quantilica_sql.adapters.anp import AnpAdapter

    anp_dir = tmp_path / "anp"
    shpc_dir = anp_dir / "shpc-glp"
    shpc_dir.mkdir(parents=True)

    csv_file = shpc_dir / "precos_glp_2026.csv"
    csv_file.write_text(
        "Regiao - Sigla;Estado - Sigla;Municipio;Revenda;Preco de Venda\n"
        "SE;SP;SAO PAULO;POSTO EXEMPLO LTDA;110,00\n",
        encoding="latin-1",
    )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = AnpAdapter()

    rows = adapter.load(mock_conn, source_path=anp_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "precos_glp_2026.csv"
    assert state.recorded[0][1] == "anp"

    # Segunda execução (skip)
    state.recorded.clear()
    rows_skip = adapter.load(mock_conn, source_path=anp_dir)
    assert rows_skip == 0
    assert len(state.recorded) == 0


def test_inmet_adapter_load_station_csv(tmp_path: Path):
    """Valida carga genérica do INMET com extração de metadados e séries."""
    from quantilica_sql.adapters.inmet import InmetAdapter

    inmet_dir = tmp_path / "inmet"
    inmet_dir.mkdir(parents=True)

    station_csv = inmet_dir / "INMET_SE_SP_A701_SAO_PAULO_2026.CSV"
    station_content = (
        "REGIAO:;SE\n"
        "UF:;SP\n"
        "ESTACAO:;SAO PAULO - MIRANTE DE SANTANA\n"
        "CODIGO (WMO):;A701\n"
        "LATITUDE:;-23,496\n"
        "LONGITUDE:;-46,620\n"
        "ALTITUDE:;792,06\n"
        "DATA DE FUNDACAO:;01/01/2006\n"
        "Data;Hora UTC;PRECIPITACAO TOTAL;TEMPERATURA DO AR\n"
        "2026/01/01;0000 UTC;0,0;22,4\n"
        "2026/01/01;0100 UTC;0,0;21,8\n"
    )
    station_csv.write_text(station_content, encoding="latin-1")

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=2)
    adapter = InmetAdapter()

    rows = adapter.load(mock_conn, source_path=inmet_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == station_csv.name
    assert state.recorded[0][1] == "inmet"

    # Segunda execução (skip)
    state.recorded.clear()
    rows_skip = adapter.load(mock_conn, source_path=inmet_dir)
    assert rows_skip == 0
    assert len(state.recorded) == 0


def test_tesouro_direto_adapter_load_csv_and_skip(tmp_path: Path):
    """Valida carga genérica de títulos do Tesouro Direto e idempotência."""
    from quantilica_sql.adapters.tesouro_direto import TesouroDiretoAdapter

    td_dir = tmp_path / "tesouro_direto"
    td_dir.mkdir(parents=True)
    csv_file = td_dir / "tesouro_direto_2026.csv"
    csv_file.write_text(
        "Tipo Titulo;Data Vencimento;Data Base;Taxa Compra Manha;"
        "Taxa Venda Manha;PU Compra Manha\n"
        "Tesouro Selic;01/01/2029;24/10/2026;0,00;0,05;15000,00\n",
        encoding="latin-1",
    )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = TesouroDiretoAdapter()

    rows = adapter.load(mock_conn, source_path=td_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "tesouro_direto_2026.csv"
    assert state.recorded[0][1] == "tesouro_direto"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=td_dir) == 0
    assert len(state.recorded) == 0


def test_rtn_adapter_load_csv_and_skip(tmp_path: Path):
    """Valida carga genérica de relatórios do RTN e idempotência."""
    from quantilica_sql.adapters.rtn import RtnAdapter

    rtn_dir = tmp_path / "rtn"
    rtn_dir.mkdir(parents=True)
    csv_file = rtn_dir / "resultado_tesouro_nacional_2026.csv"
    csv_file.write_text(
        "Rubrica;Ano;Mes;Valor\nRECEITA TOTAL;2026;10;150000.50\n",
        encoding="latin-1",
    )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = RtnAdapter()

    rows = adapter.load(mock_conn, source_path=rtn_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "resultado_tesouro_nacional_2026.csv"
    assert state.recorded[0][1] == "rtn"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=rtn_dir) == 0
    assert len(state.recorded) == 0


def test_comex_adapter_load_csv_and_skip(tmp_path: Path):
    """Valida carga genérica de dados do Comex Stat e idempotência."""
    from quantilica_sql.adapters.comex import ComexAdapter

    comex_dir = tmp_path / "comex"
    comex_dir.mkdir(parents=True)
    csv_file = comex_dir / "EXP_2026.csv"
    csv_file.write_text(
        "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
        "2026;10;12019000;10;105;MT;01;0817800;1000;1000;500\n",
        encoding="latin-1",
    )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = ComexAdapter()

    rows = adapter.load(mock_conn, source_path=comex_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "EXP_2026.csv"
    assert state.recorded[0][1] == "comex"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=comex_dir) == 0
    assert len(state.recorded) == 0


def test_anac_adapter_load_csv_and_skip(tmp_path: Path):
    """Valida carga genérica de voos da ANAC e idempotência."""
    from quantilica_sql.adapters.anac import AnacAdapter

    anac_dir = tmp_path / "anac"
    anac_dir.mkdir(parents=True)
    csv_file = anac_dir / "VRA_202610.csv"
    csv_file.write_text(
        "ICAO Empresa Aerea;Numero Voo;Codigo DI;Codigo Tipo Linha;"
        "ICAO Aerodromo Origem;ICAO Aerodromo Destino\n"
        "GLO;1234;0;N;SBBR;SBSP\n",
        encoding="latin-1",
    )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = AnacAdapter()

    rows = adapter.load(mock_conn, source_path=anac_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "VRA_202610.csv"
    assert state.recorded[0][1] == "anac"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=anac_dir) == 0
    assert len(state.recorded) == 0


def test_b3_adapter_load_cotahist_and_skip(tmp_path: Path):
    """Valida carga e parsing posicional de arquivo COTAHIST da B3."""
    from quantilica_sql.adapters.b3 import B3Adapter

    b3_dir = tmp_path / "b3"
    b3_dir.mkdir(parents=True)
    cot_file = b3_dir / "COTAHIST_A2026.TXT"

    line_01 = (
        "01"
        "20261024"
        "02"
        "PETR4       "
        "010"
        "PETROBRAS   "
        "PN          "
        "   "
        "R$  "
        "0000000003500"
        "0000000003600"
        "0000000003450"
        "0000000003550"
        "0000000003580"
        "0000000003570"
        "0000000003580"
        "05000"
        "000000000010000000"
        "00000000358000000"
        "0000000000000"
        "0"
        "99991231"
        "0000001"
        "0000000000000"
        "BRPETRACNPR6"
        "100"
    )
    content = (
        f"00COTAHIST.2026BOVESPA 20261024\n{line_01}\n"
        f"99COTAHIST.2026BOVESPA 0000000001\n"
    )
    cot_file.write_text(content, encoding="latin-1")

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = B3Adapter()

    rows = adapter.load(mock_conn, source_path=b3_dir)
    assert rows == 1
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "COTAHIST_A2026.TXT"
    assert state.recorded[0][1] == "b3"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=b3_dir) == 0
    assert len(state.recorded) == 0


def test_datasus_adapter_load_csv_and_skip(tmp_path: Path):
    """Valida carga genérica de microdados do DATASUS e idempotência."""
    from quantilica_sql.adapters.datasus import DatasusAdapter

    datasus_dir = tmp_path / "datasus"
    datasus_dir.mkdir(parents=True)
    csv_file = datasus_dir / "DO2026.csv"
    csv_file.write_text(
        "NUMERODO;DTOBITO;CAUSABAS;IDADE\n12345678;24102026;I219;65\n",
        encoding="latin-1",
    )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = DatasusAdapter()

    rows = adapter.load(mock_conn, source_path=datasus_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "DO2026.csv"
    assert state.recorded[0][1] == "datasus"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=datasus_dir) == 0
    assert len(state.recorded) == 0


def test_inep_adapter_load_zip_and_skip(tmp_path: Path):
    """Valida carga genérica de microdados do INEP contidos em ZIP e idempotência."""
    import zipfile

    from quantilica_sql.adapters.inep import InepAdapter

    inep_dir = tmp_path / "inep"
    inep_dir.mkdir(parents=True)
    zip_file = inep_dir / "microdados_enem_2026.zip"
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr(
            "DADOS/MICRODADOS_ENEM_2026.csv",
            "NU_INSCRICAO;NU_ANO;TP_SEXO;NOTA_CN\n1001;2026;M;650.5\n".encode(
                "latin-1"
            ),
        )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = InepAdapter()

    rows = adapter.load(mock_conn, source_path=inep_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "microdados_enem_2026.zip"
    assert state.recorded[0][1] == "inep"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=inep_dir) == 0
    assert len(state.recorded) == 0


def test_tse_adapter_load_zip_and_skip(tmp_path: Path):
    """Valida carga genérica de dados eleitorais do TSE e idempotência."""
    import zipfile

    from quantilica_sql.adapters.tse import TseAdapter

    tse_dir = tmp_path / "tse"
    tse_dir.mkdir(parents=True)
    zip_file = tse_dir / "consulta_cand_2026_SP.zip"
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr(
            "consulta_cand_2026_SP.csv",
            "ANO_ELEICAO;SG_UF;DS_CARGO;NM_CANDIDATO\n"
            "2026;SP;GOVERNADOR;CANDIDATO TESTE\n".encode("latin-1"),
        )

    state = MockDbState()
    mock_conn = state.make_conn(rowcount=1)
    adapter = TseAdapter()

    rows = adapter.load(mock_conn, source_path=tse_dir)
    assert rows > 0
    assert len(state.recorded) == 1
    assert state.recorded[0][0] == "consulta_cand_2026_SP.zip"
    assert state.recorded[0][1] == "tse"

    # Segunda execução (skip)
    state.recorded.clear()
    assert adapter.load(mock_conn, source_path=tse_dir) == 0
    assert len(state.recorded) == 0
