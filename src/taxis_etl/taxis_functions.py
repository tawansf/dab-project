from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def limpar_dados_taxis(df: DataFrame) -> DataFrame:
    # Etapa 1: Remover corridas com valores negativos ou zerados
    df_cleaned = df.filter(col("fare_amount") > 0)

    # Etapa 2: Renomeação de colunas
    return df_cleaned \
    .withColumnRenamed("trip_distance", "distancia_da_corrida") \
    .withColumnRenamed("fare_amount", "valor_da_corrida") \
    .withColumnRenamed("tpep_pickup_datetime", "data_hora_inicio_corrida") \
    .withColumnRenamed("tpep_dropoff_datetime", "data_hora_fim_corrida") \
    .withColumnRenamed("pickup_zip", "cep_origem")