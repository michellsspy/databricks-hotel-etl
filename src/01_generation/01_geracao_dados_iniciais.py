# Célula 1: Instalações e Imports
# !pip install Faker names

import random
import names
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DateType, DoubleType, BooleanType)

# Inicializa o Faker
fake = Faker('pt_BR')

# Inicializa a Sessão Spark (no Databricks, ela já existe como 'spark')
spark = SparkSession.builder.appName("GeracaoDadosHotelaria").getOrCreate()

print("Faker e Spark inicializados.")

# Célula 2: Funções de Geração - HOTEIS
def gerar_hoteis(num_hoteis=50):
    COMODIDADES = ['Wi-Fi Grátis', 'Piscina', 'Academia', 'Spa', 'Estacionamento', 'Restaurante', 'Bar', 'Serviço de Quarto']
    PREFIXOS = ['Grand', 'Royal', 'Plaza', 'Imperial', 'Golden', 'Paradise']
    SUFIXOS = ['Hotel', 'Resort', 'Palace', 'Inn']
    
    hoteis_data = []
    for i in range(num_hoteis):
        nome_hotel = f"{random.choice(PREFIXOS)} {fake.city_suffix()} {random.choice(SUFIXOS)}"
        num_comodidades = random.randint(3, 7)
        comodidades_selecionadas = random.sample(COMODIDADES, num_comodidades)
        
        hotel = (
            101 + i,
            nome_hotel,
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            random.randint(3, 5),
            random.randint(50, 250),
            ', '.join(comodidades_selecionadas)
        )
        hoteis_data.append(hotel)
        
    schema = StructType([
        StructField("hotel_id", IntegerType(), False),
        StructField("nome_hotel", StringType(), True),
        StructField("endereco", StringType(), True),
        StructField("cidade", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("estrelas", IntegerType(), True),
        StructField("numero_quartos", IntegerType(), True),
        StructField("comodidades", StringType(), True)
    ])
    
    df = spark.createDataFrame(hoteis_data, schema)
    return df

# Célula 3: Funções de Geração - QUARTOS
def gerar_quartos(df_hoteis):
    TIPOS_QUARTO = {
        'Standard': {'capacidade': [1, 2], 'percentual': 0.50, 'preco_base': 250.0},
        'Superior': {'capacidade': [2, 3], 'percentual': 0.25, 'preco_base': 400.0},
        'Suíte': {'capacidade': [2, 4], 'percentual': 0.25, 'preco_base': 650.0}
    }
    
    quartos_data = []
    quarto_id_global = 1001
    hoteis_info = df_hoteis.collect()
    
    for hotel in hoteis_info:
        total_quartos_hotel = hotel['numero_quartos']
        
        tipos_dist = []
        for tipo, config in TIPOS_QUARTO.items():
            num_quartos_tipo = int(total_quartos_hotel * config['percentual'])
            tipos_dist.extend([tipo] * num_quartos_tipo)
        while len(tipos_dist) < total_quartos_hotel:
            tipos_dist.append('Standard')
        
        for i in range(total_quartos_hotel):
            tipo_quarto_atual = tipos_dist[i]
            config = TIPOS_QUARTO[tipo_quarto_atual]
            preco_final = config['preco_base'] * (1 + (hotel['estrelas'] - 3) * 0.15)
            
            quarto = (
                quarto_id_global,
                hotel['hotel_id'],
                f"{(i // 20) + 1:02d}{(i % 20) + 1:02d}",
                tipo_quarto_atual,
                random.randint(config['capacidade'][0], config['capacidade'][1]),
                round(preco_final, 2)
            )
            quartos_data.append(quarto)
            quarto_id_global += 1
            
    schema = StructType([
        StructField("quarto_id", IntegerType(), False),
        StructField("hotel_id", IntegerType(), False),
        StructField("numero_quarto", StringType(), True),
        StructField("tipo_quarto", StringType(), True),
        StructField("capacidade_maxima", IntegerType(), True),
        StructField("preco_diaria_base", DoubleType(), True)
    ])
    
    return spark.createDataFrame(quartos_data, schema)

# Célula 4: Funções de Geração - HÓSPEDES
def gerar_hospedes(num_hospedes=8000):
    hospedes_data = []
    for i in range(num_hospedes):
        nome = fake.name()
        hospede = (
            2001 + i,
            nome,
            fake.cpf(),
            fake.date_of_birth(minimum_age=18, maximum_age=85),
            f"{nome.split(' ')[0].lower()}@email.com",
            fake.state_abbr()
        )
        hospedes_data.append(hospede)
        
    schema = StructType([
        StructField("hospede_id", IntegerType(), False),
        StructField("nome_completo", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("data_nascimento", DateType(), True),
        StructField("email", StringType(), True),
        StructField("estado", StringType(), True)
    ])
    
    return spark.createDataFrame(hospedes_data, schema)

# Célula 5: Funções de Geração - RESERVAS, CANAIS, CONSUMOS e FATURAS
def gerar_dependentes(df_hoteis, df_quartos, df_hospedes, num_reservas=15000):
    CANAIS = ['Booking.com', 'Expedia', 'Website Hotel', 'Telefone', 'Balcão']
    SERVICOS = {'Restaurante': (25.0, 150.0), 'Bar': (15.0, 80.0), 'Spa': (100.0, 400.0), 'Serviço de Quarto': (30.0, 90.0)}
    FORMAS_PAGAMENTO = ['Cartão de Crédito', 'Pix', 'Dinheiro', 'Cartão de Débito']

    # Coletar dados para manipulação em Python
    quartos_info = df_quartos.select("quarto_id", "hotel_id", "preco_diaria_base").collect()
    hospedes_ids = [row['hospede_id'] for row in df_hospedes.select("hospede_id").collect()]

    reservas_data = []
    canais_data = []
    consumos_data = []
    faturas_data = []
    
    consumo_id_global = 50001
    fatura_id_global = 9001

    for i in range(num_reservas):
        reserva_id = 10001 + i
        quarto_selecionado = random.choice(quartos_info)
        hospede_id = random.choice(hospedes_ids)
        
        # Datas da reserva
        dias_antecedencia = random.randint(1, 90)
        data_reserva = datetime.now() - timedelta(days=dias_antecedencia + random.randint(5, 365*2))
        data_checkin = data_reserva + timedelta(days=dias_antecedencia)
        num_noites = random.randint(1, 10)
        data_checkout = data_checkin + timedelta(days=num_noites)

        # Dados da Reserva
        valor_total_estadia = num_noites * quarto_selecionado['preco_diaria_base']
        reservas_data.append((
            reserva_id, hospede_id, quarto_selecionado['quarto_id'], quarto_selecionado['hotel_id'],
            data_reserva.date(), data_checkin.date(), data_checkout.date(), num_noites, valor_total_estadia
        ))

        # Dados do Canal
        canais_data.append((f"CAN-{reserva_id}", reserva_id, random.choice(CANAIS)))

        # Dados de Consumos (gerar de 0 a 5 consumos por reserva)
        subtotal_consumos = 0.0
        num_consumos_reserva = random.randint(0, 5)
        for _ in range(num_consumos_reserva):
            servico, (v_min, v_max) = random.choice(list(SERVICOS.items()))
            valor_consumo = round(random.uniform(v_min, v_max), 2)
            # Garante que o consumo ocorreu durante a estadia
            dias_na_estadia = random.randint(0, num_noites -1 if num_noites > 0 else 0)
            data_consumo = data_checkin + timedelta(days=dias_na_estadia)
            
            consumos_data.append((consumo_id_global, reserva_id, hospede_id, quarto_selecionado['hotel_id'], data_consumo.date(), servico, valor_consumo))
            subtotal_consumos += valor_consumo
            consumo_id_global += 1

        # Dados da Fatura
        valor_total_fatura = valor_total_estadia + subtotal_consumos
        data_emissao = data_checkout + timedelta(days=1)
        faturas_data.append((
            fatura_id_global, reserva_id, hospede_id, data_emissao.date(), valor_total_fatura, random.choice(FORMAS_PAGAMENTO)
        ))
        fatura_id_global += 1

    # Schemas
    schema_reservas = StructType([
        StructField("reserva_id", IntegerType(), False), StructField("hospede_id", IntegerType(), True),
        StructField("quarto_id", IntegerType(), True), StructField("hotel_id", IntegerType(), True),
        StructField("data_reserva", DateType(), True), StructField("data_checkin", DateType(), True),
        StructField("data_checkout", DateType(), True), StructField("numero_noites", IntegerType(), True),
        StructField("valor_total_estadia", DoubleType(), True)
    ])
    schema_canais = StructType([
        StructField("reserva_canal_id", StringType(), False), StructField("reserva_id", IntegerType(), True),
        StructField("canal_reserva", StringType(), True)
    ])
    schema_consumos = StructType([
        StructField("consumo_id", IntegerType(), False), StructField("reserva_id", IntegerType(), True),
        StructField("hospede_id", IntegerType(), True), StructField("hotel_id", IntegerType(), True),
        StructField("data_consumo", DateType(), True), StructField("tipo_servico", StringType(), True),
        StructField("valor", DoubleType(), True)
    ])
    schema_faturas = StructType([
        StructField("fatura_id", IntegerType(), False), StructField("reserva_id", IntegerType(), True),
        StructField("hospede_id", IntegerType(), True), StructField("data_emissao", DateType(), True),
        StructField("valor_total", DoubleType(), True), StructField("forma_pagamento", StringType(), True)
    ])

    # Criar DataFrames
    df_reservas = spark.createDataFrame(reservas_data, schema_reservas)
    df_canais = spark.createDataFrame(canais_data, schema_canais)
    df_consumos = spark.createDataFrame(consumos_data, schema_consumos)
    df_faturas = spark.createDataFrame(faturas_data, schema_faturas)

    return df_reservas, df_canais, df_consumos, df_faturas

# Célula 6: Execução Principal e Salvamento das Tabelas
def run_generation():
    CATALOG = "production"
    SCHEMA = "transient"
    
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

    print("--- Gerando Hoteis ---")
    df_hoteis = gerar_hoteis(num_hoteis=50)
    df_hoteis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_hoteis")
    print(f"{df_hoteis.count()} registros de hoteis salvos em {CATALOG}.{SCHEMA}.source_hoteis")

    print("\n--- Gerando Quartos ---")
    df_quartos = gerar_quartos(df_hoteis)
    df_quartos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_quartos")
    print(f"{df_quartos.count()} registros de quartos salvos em {CATALOG}.{SCHEMA}.source_quartos")

    print("\n--- Gerando Hóspedes ---")
    df_hospedes = gerar_hospedes(num_hospedes=8000)
    df_hospedes.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_hospedes")
    print(f"{df_hospedes.count()} registros de hóspedes salvos em {CATALOG}.{SCHEMA}.source_hospedes")

    print("\n--- Gerando Reservas, Canais, Consumos e Faturas ---")
    df_reservas, df_canais, df_consumos, df_faturas = gerar_dependentes(df_hoteis, df_quartos, df_hospedes, num_reservas=15000)
    
    df_reservas.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_reservas")
    print(f"{df_reservas.count()} registros de reservas salvos em {CATALOG}.{SCHEMA}.source_reservas")

    df_canais.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_reservas_canal")
    print(f"{df_canais.count()} registros de canais salvos em {CATALOG}.{SCHEMA}.source_reservas_canal")

    df_consumos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_consumos")
    print(f"{df_consumos.count()} registros de consumos salvos em {CATALOG}.{SCHEMA}.source_consumos")

    df_faturas.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.source_faturas")
    print(f"{df_faturas.count()} registros de faturas salvos em {CATALOG}.{SCHEMA}.source_faturas")
    
    print("\n--- Processo de Geração Concluído! ---")

# Executa a função principal
run_generation()