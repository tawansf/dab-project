import pytest
from pyspark.sql import SparkSession
from collections import namedtuple
from src.taxis_etl.taxis_functions import limpar_dados_taxis

# Configuração do Spark para Teste Local (Fixture)
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TesteUnitario").getOrCreate()

def test_limpeza_remove_valores_negativos(spark):
    """Teste para garantir que a função remove valores <= 0"""
    
    # 1. ARRANGE (Prepara dados falsos)
    DadosTaxi = namedtuple("DadosTaxi", "fare_amount trip_distance tpep_pickup_datetime tpep_dropoff_datetime pickup_zip")
    data = [
        DadosTaxi(50.0, 10, "2023-01-01", "2023-01-01", 12345), # Bom
        DadosTaxi(-10.0, 5, "2023-01-01", "2023-01-01", 12345), # Ruim (Negativo)
        DadosTaxi(0.0, 5, "2023-01-01", "2023-01-01", 12345)    # Ruim (Zero)
    ]
    df_input = spark.createDataFrame(data)

    # 2. ACT (Executa a função)
    df_result = limpar_dados_taxis(df_input)

    # 3. ASSERT (Verifica o resultado)
    assert df_result.count() == 1 # Só deve sobrar 1 linha
    assert df_result.first()["valor_da_corrida"] == 50.0 # Verifica se renomeou certo