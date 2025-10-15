# Databricks notebook source
import dlt
from pyspark.sql.functions import col

# --- Camada RAW ---
# Implementa uma arquitetura robusta com cargas completas para dados dimensionais
# e cargas incrementais com histórico completo (SCD Type 2) para dados transacionais.

# --- CARGAS COMPLETAS (FULL) ---
# Tabelas que são sempre recriadas.

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


# --- CARGAS INCREMENTAIS COM HISTÓRICO (SCD TYPE 2) ---
# Usamos dlt.apply_changes() para fazer o MERGE e manter o histórico.
# O DLT irá gerir automaticamente as colunas de versionamento (__START_DATE, __END_DATE, etc.).

# Tabela de Hóspedes com Histórico
dlt.apply_changes(
  target = "hospedes_raw",
  source = spark.readStream.table("production.transient.source_hospedes"),
  keys = ["hospede_id"],
  sequence_by = col("data_modificacao"), # Coluna que define a ordem das atualizações
  # DLT irá criar uma nova versão do registo se qualquer uma destas colunas mudar.
  track_history_column_list = ["nome_completo", "cpf", "data_nascimento", "email", "estado"],
  stored_as_scd_type = 2
)

# Tabela de Reservas com Histórico (útil se o status de uma reserva puder mudar)
dlt.apply_changes(
  target = "reservas_raw",
  source = spark.readStream.table("production.transient.source_reservas"),
  keys = ["reserva_id"],
  sequence_by = col("data_modificacao_reserva"), # Assumindo que a fonte tem esta coluna
  track_history_column_list = ["data_checkin", "data_checkout", "valor_total_estadia"],
  stored_as_scd_type = 2
)

# Tabela de Consumos (Geralmente apenas append, mas podemos usar a mesma lógica)
dlt.apply_changes(
  target = "consumos_raw",
  source = spark.readStream.table("production.transient.source_consumos"),
  keys = ["consumo_id"],
  sequence_by = col("data_consumo"),
  stored_as_scd_type = 2
)

# Tabela de Faturas (Geralmente apenas append)
dlt.apply_changes(
  target = "faturas_raw",
  source = spark.readStream.table("production.transient.source_faturas"),
  keys = ["fatura_id"],
  sequence_by = col("data_emissao"),
  stored_as_scd_type = 2
)

# Tabela de Canais de Reserva (Geralmente apenas append)
dlt.apply_changes(
  target = "reservas_canal_raw",
  source = spark.readStream.table("production.transient.source_reservas_canal"),
  keys = ["reserva_canal_id"],
  sequence_by = col("reserva_id"), # Usando reserva_id como proxy de sequência
  stored_as_scd_type = 2
)