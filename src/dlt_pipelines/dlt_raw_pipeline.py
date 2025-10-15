# Databricks notebook source
import dlt
from pyspark.sql.functions import col

# --- Camada RAW ---
# Este pipeline executa em modo BATCH INCREMENTAL (NÃO É STREAMING CONTÍNUO).

# --- CARGAS COMPLETAS (FULL) ---
@dlt.table(
  name="hoteis_raw",
  comment="Tabela de hoteis, recarregada completamente a cada execução (FULL)."
)
def hoteis_raw():
  return spark.read.table("production.transient.source_hoteis")

@dlt.table(
  name="quartos_raw",
  comment="Tabela de quartos, recarregada completamente a cada execução (FULL)."
)
def quartos_raw():
  return spark.read.table("production.transient.source_quartos")


# --- BATCH INCREMENTAL COM UPSERT E HISTÓRICO (SCD TYPE 2) ---
# A função dlt.apply_changes() executa a lógica de MERGE (UPSERT) com histórico.

dlt.apply_changes(
  target = "hospedes_raw",
  source = spark.readStream.table("production.transient.source_hospedes"),
  keys = ["hospede_id"],
  sequence_by = col("data_modificacao"),
  track_history_column_list = ["nome_completo", "cpf", "data_nascimento", "email", "estado"],
  stored_as_scd_type = 2
)

dlt.apply_changes(
  target = "reservas_raw",
  source = spark.readStream.table("production.transient.source_reservas"),
  keys = ["reserva_id"],
  sequence_by = col("data_modificacao_reserva"),
  track_history_column_list = ["data_checkin", "data_checkout", "valor_total_estadia"],
  stored_as_scd_type = 2
)

dlt.apply_changes(
  target = "consumos_raw",
  source = spark.readStream.table("production.transient.source_consumos"),
  keys = ["consumo_id"],
  sequence_by = col("data_consumo"),
  stored_as_scd_type = 2
)

dlt.apply_changes(
  target = "faturas_raw",
  source = spark.readStream.table("production.transient.source_faturas"),
  keys = ["fatura_id"],
  sequence_by = col("data_emissao"),
  stored_as_scd_type = 2
)

dlt.apply_changes(
  target = "reservas_canal_raw",
  source = spark.readStream.table("production.transient.source_reservas_canal"),
  keys = ["reserva_canal_id"],
  sequence_by = col("reserva_id"),
  stored_as_scd_type = 2
)