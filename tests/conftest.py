"""Fixtures compartilhadas: lake Hive sintético com descritores ``_dataset.json``."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def write_hive(tmp_path):
    """Fábrica que grava um Parquet de texto em ``<lake>/<fonte>/<dataset>/<k>=<v>/``.

    Imita o que o conversor de um fetcher produz: colunas de texto com nomes
    normalizados, partição Hive e ``_dataset.json`` com o apelido da view.
    """
    duckdb = pytest.importorskip("duckdb")
    from quantilica.core.lake import DatasetDescriptor

    from quantilica_sql.lake.specs import normalize_name, unique_names

    def write(
        lake: Path,
        source: str,
        dataset: str,
        csv_text: str,
        *,
        key: str,
        partition: str,
        alias: str | None = None,
        partition_key: str = "periodo",
        partition_types: dict[str, str] | None = None,
        descriptor: bool = True,
        sep: str = ";",
    ) -> Path:
        csv = tmp_path / f"{key}.csv"
        csv.write_text(csv_text, encoding="utf-8")
        header = csv_text.splitlines()[0].split(sep)
        names = unique_names([normalize_name(h) for h in header])
        columns = ", ".join(f"'{h}': 'VARCHAR'" for h in header)
        select = ", ".join(
            f'"{h}" AS "{n}"' for h, n in zip(header, names, strict=True)
        )
        dataset_dir = lake / source / dataset
        out = dataset_dir / f"{partition_key}={partition}" / f"{key}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        duckdb.connect().execute(
            f"COPY (SELECT {select} FROM read_csv('{csv}', delim='{sep}', "
            f"header=true, columns={{{columns}}})) TO '{out}' (FORMAT parquet)"
        )
        if descriptor:
            DatasetDescriptor(
                source_id=source,
                dataset_id=dataset,
                alias=alias or normalize_name(dataset),
                partition_keys=[partition_key],
                partition_types=partition_types or {partition_key: "varchar"},
            ).save(dataset_dir)
        return out

    return write
