"""Motor unificado de ingestão, particionamento e pipelines PostgreSQL da Quantilica."""

__version__ = "0.1.0"

from quantilica_sql.database import get_connection, get_engine
from quantilica_sql.runner import run_pipeline

__all__ = ["__version__", "get_connection", "get_engine", "run_pipeline"]
