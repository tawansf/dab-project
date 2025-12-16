"""This file configures pytest.

This file is in the root since it can be used for tests in any place in this
project, including tests under resources/.
"""

import os
import sys
import pathlib
import pytest
import json
import csv
from pyspark.sql import SparkSession

# --- CORREÇÃO DE PATH (CRUCIAL PARA O SEU ERRO) ---
# Pega o diretório onde este arquivo está (pasta tests)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Pega o diretório pai (raiz do projeto)
project_root = os.path.dirname(current_dir)
# Adiciona a raiz ao sys.path para que o Python encontre a pasta 'src'
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.
    
    Usamos 'local[1]' para garantir que os testes unitários rodem rápido
    e isolados, sem depender de conexão de rede ou Databricks Connect.
    """
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TesteUnitario") \
        .getOrCreate()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory."""
    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader