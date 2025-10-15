# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

# --- Camada TRUSTED (Prata) ---
# O objetivo desta camada é consolidar o histórico da camada Raw,
# apresentando apenas o estado atual e válido dos dados (SCD Type 1),
# além de aplicar limpeza e padronização.

# --- CARGAS COMPLETAS (FULL) ---
# Estas tabelas são recriadas do zero a cada execução.

@dlt.table(
  name="hoteis_trusted",
  comment="Tabela de hoteis limpa e padronizada (Carga Full)."
)
def hoteis_trusted():
  df_raw = spark.read.table("production.raw.hoteis_raw")
  return (
    df_raw.select(
      col("hotel_id").cast("INT"),
      col("nome_hotel").cast("STRING"),
      col("endereco").cast("STRING"),
      col("cidade").cast("STRING"),
      col("estado").cast("STRING"),
      col("estrelas").cast("INT"),
      col("numero_quartos").cast("INT"),
      col("comodidades").cast("STRING")
    ).withColumn("data_carga_trusted", current_timestamp())
  )

@dlt.table(
  name="quartos_trusted",
  comment="Tabela de quartos limpa e com tipos de dados garantidos (Carga Full)."
)
def quartos_trusted():
  df_raw = spark.read.table("production.raw.quartos_raw")
  return (
    df_raw.select(
      col("quarto_id").cast("INT"),
      col("hotel_id").cast("INT"),
      col("numero_quarto").cast("STRING"),
      col("tipo_quarto").cast("STRING"),
      col("capacidade_maxima").cast("INT"),
      col("preco_diaria_base").cast("DOUBLE")
    ).withColumn("data_carga_trusted", current_timestamp())
  )

# --- BATCH INCREMENTAL COM UPSERT (SCD TYPE 1) ---
# Lemos o fluxo de alterações da camada Raw e aplicamos um MERGE (UPDATE + INSERT).

# Hóspedes (UPSERT)
dlt.apply_changes(
  target = "hospedes_trusted",
  source = dlt.read_stream("hospedes_raw"), # Lê apenas as alterações da Raw
  keys = ["hospede_id"],
  sequence_by = col("data_modificacao"),
  stored_as_scd_type = 1 # Garante que a Trusted tenha apenas o estado atual
)

# Reservas (UPSERT)
dlt.apply_changes(
  target = "reservas_trusted",
  source = dlt.read_stream("reservas_raw"),
  keys = ["reserva_id"],
  sequence_by = col("data_modificacao_reserva"),
  stored_as_scd_type = 1
)

# Canais de Reserva (UPSERT)
dlt.apply_changes(
  target = "reservas_canal_trusted",
  source = dlt.read_stream("reservas_canal_raw"),
  keys = ["reserva_canal_id"],
  sequence_by = col("reserva_id"),
  stored_as_scd_type = 1
)

# Consumos (UPSERT)
dlt.apply_changes(
  target = "consumos_trusted",
  source = dlt.read_stream("consumos_raw"),
  keys = ["consumo_id"],
  sequence_by = col("data_consumo"),
  stored_as_scd_type = 1
)

# Faturas (UPSERT)
dlt.apply_changes(
  target = "faturas_trusted",
  source = dlt.read_stream("faturas_raw"),
  keys = ["fatura_id"],
  sequence_by = col("data_emissao"),
  stored_as_scd_type = 1
)