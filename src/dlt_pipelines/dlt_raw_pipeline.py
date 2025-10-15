# Databricks notebook source
import dlt
from pyspark.sql.functions import col

# --- Camada RAW ---
# Implementa cargas Full e Incrementais (SCD Type 2) de forma explícita e robusta.

# --- CARGAS COMPLETAS (FULL) ---
@dlt.table(name="hoteis_raw", comment="Tabela de hoteis, recarregada completamente (FULL).")
def hoteis_raw():
  return spark.read.table("production.transient.source_hoteis")

@dlt.table(name="quartos_raw", comment="Tabela de quartos, recarregada completamente (FULL).")
def quartos_raw():
  return spark.read.table("production.transient.source_quartos")

# --- CARGAS INCREMENTAIS COM HISTÓRICO (SCD TYPE 2) ---

# --- HÓSPEDES ---
# Passo 1: Definir a fonte como uma tabela de streaming interna
@dlt.table(name="source_hospedes_streaming", comment="Fonte de streaming para hóspedes.", table_properties={"dlt.table.name": "source_hospedes_streaming"}, temporary=True)
def source_hospedes_streaming():
  return spark.readStream.table("production.transient.source_hospedes")

# Passo 2: Aplicar as alterações a partir da fonte interna
dlt.apply_changes(
  target = "hospedes_raw",
  source = dlt.read_stream("source_hospedes_streaming"),
  keys = ["hospede_id"],
  sequence_by = col("data_modificacao"),
  track_history_column_list = ["nome_completo", "cpf", "data_nascimento", "email", "estado"],
  stored_as_scd_type = 2
)

# --- RESERVAS ---
@dlt.table(name="source_reservas_streaming", temporary=True)
def source_reservas_streaming():
  return spark.readStream.table("production.transient.source_reservas")

dlt.apply_changes(
  target = "reservas_raw",
  source = dlt.read_stream("source_reservas_streaming"),
  keys = ["reserva_id"],
  sequence_by = col("data_modificacao_reserva"),
  track_history_column_list = ["data_checkin", "data_checkout", "valor_total_estadia"],
  stored_as_scd_type = 2
)

# ... E assim por diante para as outras tabelas ...

# --- CONSUMOS ---
@dlt.table(name="source_consumos_streaming", temporary=True)
def source_consumos_streaming():
    return spark.readStream.table("production.transient.source_consumos")

dlt.apply_changes(
    target="consumos_raw",
    source=dlt.read_stream("source_consumos_streaming"),
    keys=["consumo_id"],
    sequence_by=col("data_consumo"),
    stored_as_scd_type=2
)

# --- FATURAS ---
@dlt.table(name="source_faturas_streaming", temporary=True)
def source_faturas_streaming():
    return spark.readStream.table("production.transient.source_faturas")

dlt.apply_changes(
    target="faturas_raw",
    source=dlt.read_stream("source_faturas_streaming"),
    keys=["fatura_id"],
    sequence_by=col("data_emissao"),
    stored_as_scd_type=2
)

# --- RESERVAS CANAL ---
@dlt.table(name="source_reservas_canal_streaming", temporary=True)
def source_reservas_canal_streaming():
    return spark.readStream.table("production.transient.source_reservas_canal")

dlt.apply_changes(
    target="reservas_canal_raw",
    source=dlt.read_stream("source_reservas_canal_streaming"),
    keys=["reserva_canal_id"],
    sequence_by=col("reserva_id"),
    stored_as_scd_type=2
)