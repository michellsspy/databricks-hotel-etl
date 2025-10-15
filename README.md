Guia Completo de Implementação: Pipeline de Dados Medallion com Databricks, DLT e CI/CD
1. Objetivo
Este documento serve como um guia passo a passo para a criação de um projeto de engenharia de dados de ponta a ponta. O objetivo é construir um pipeline ETL robusto, seguindo a arquitetura Medallion (Transient, Raw, Trusted, Refined), utilizando as ferramentas mais modernas do ecossistema Databricks. Toda a infraestrutura será gerida como código (IaC) com Databricks Asset Bundles e automatizada por uma esteira de CI/CD com GitHub Actions.

2. Arquitetura Final
Plataforma de Dados: Databricks Free Trial (Serverless).

Motor de ETL: Delta Live Tables (DLT) com pipelines modulares por camada.

Orquestração: Databricks Workflows (Jobs).

Gestão de Código: Git e GitHub.

Infraestrutura como Código: Databricks Asset Bundles.

Automação (CI/CD): GitHub Actions.

3. Pré-requisitos
Uma conta Databricks (plano Free Trial).

Uma conta GitHub.

Python 3.8 ou superior e Git instalados na sua máquina local.

Fase 1: Configuração do Ambiente Local
Esta fase prepara a sua máquina para o desenvolvimento.

1.1. Criar a Estrutura do Projeto
Crie uma estrutura de diretórios limpa para organizar todos os nossos artefactos de código.

Bash

# Crie a pasta principal e a subpasta de trabalho
mkdir -p ~/databricks_hotel_etl/databricks
cd ~/databricks_hotel_etl/databricks

# Crie as pastas para os nossos scripts
mkdir -p src/01_generation
mkdir -p src/dlt_pipelines
1.2. Criar um Ambiente Virtual Python
Para isolar as dependências do nosso projeto e evitar conflitos, usaremos um ambiente virtual (venv).

Bash

# Crie o venv usando uma versão moderna de Python
python3 -m venv venv

# Ative o venv. Você deverá ver (venv) no seu terminal.
source venv/bin/activate
1.3. Instalar a CLI do Databricks
A CLI moderna do Databricks é a chave para os Asset Bundles. A instalação correta é via script, não pip.

Bash

curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
Verificação: Feche e reabra o seu terminal, reative o venv e verifique a instalação.

Bash

source venv/bin/activate
databricks --version
# A saída deve ser v0.2xx.x ou superior.
1.4. Autenticar a CLI com o Databricks
Conecte a sua CLI ao seu workspace Databricks.

Bash

databricks auth login
Siga as instruções: cole a URL do seu workspace (ex: https://dbc-....cloud.databricks.com) e autentique no navegador.

Fase 2: Controle de Versão e Automação (CI/CD)
Configuramos a automação desde o início, uma prática profissional.

2.1. Inicializar o Git e o Repositório GitHub
Crie um Repositório Vazio no GitHub: Vá ao site do GitHub, crie um novo repositório (ex: databricks-hotel-etl) sem adicionar README ou .gitignore.

Conecte o seu Projeto Local: Na pasta do projeto, execute:

Bash

git init -b main
git remote add origin https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git
2.2. Criar o Workflow do GitHub Actions
Crie a estrutura de pastas para o workflow:

Bash

mkdir -p .github/workflows
Crie o ficheiro de configuração do workflow: touch .github/workflows/deploy.yml.

Abra o ficheiro .github/workflows/deploy.yml e cole o código abaixo. Este script define os passos que o GitHub executará a cada push:

YAML

# .github/workflows/deploy.yml
name: Validar e Implementar Bundle no Databricks

on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout código
        uses: actions/checkout@v4

      - name: Instalar Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

      - name: Validar o Databricks Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle validate

      - name: Implementar o Databricks Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle deploy -t dev --auto-approve
2.3. Configurar os Segredos de Automação no GitHub
Gere um Token no Databricks: Em User Settings > Developer > Access tokens, gere um novo token com 90 dias de vida útil e copie-o.

No seu Repositório GitHub: Vá para Settings > Secrets and variables > Actions.

Crie dois "Repository secrets":

DATABRICKS_HOST: A URL do seu workspace.

DATABRICKS_TOKEN: O token que você acabou de gerar.

Fase 3: Criação dos Scripts do Projeto
Agora, vamos criar os ficheiros de código e configuração.

3.1. O Ficheiro de Geração de Dados (Transient)
Esta é uma tarefa única para criar os nossos dados de origem.

Ficheiro: src/01_generation/00_geracao_dados_iniciais.py

Conteúdo: Cole o nosso "Script Mestre" de geração de dados unificado aqui. Este script será executado manualmente no Databricks.

3.2. Os Scripts dos Pipelines DLT
Estes são os scripts que definem a nossa lógica de ETL para cada camada.

a) Camada Raw

Ficheiro: src/dlt_pipelines/dlt_raw_pipeline.py

Conteúdo:

Python

# Databricks notebook source
import dlt

@dlt.table(name="hoteis_raw", comment="Cópia inicial da tabela de hoteis.")
def hoteis_raw(): return spark.read.table("production.transient.source_hoteis")

@dlt.table(name="quartos_raw", comment="Cópia inicial da tabela de quartos.")
def quartos_raw(): return spark.read.table("production.transient.source_quartos")

@dlt.table(name="hospedes_raw", comment="Cópia inicial da tabela de hospedes.")
def hospedes_raw(): return spark.read.table("production.transient.source_hospedes")

@dlt.table(name="reservas_raw", comment="Cópia inicial da tabela de reservas.")
def reservas_raw(): return spark.read.table("production.transient.source_reservas")

@dlt.table(name="reservas_canal_raw", comment="Cópia inicial da tabela de reservas_canal.")
def reservas_canal_raw(): return spark.read.table("production.transient.source_reservas_canal")

@dlt.table(name="consumos_raw", comment="Cópia inicial da tabela de consumos.")
def consumos_raw(): return spark.read.table("production.transient.source_consumos")

@dlt.table(name="faturas_raw", comment="Cópia inicial da tabela de faturas.")
def faturas_raw(): return spark.read.table("production.transient.source_faturas")
b) Camada Trusted

Ficheiro: src/dlt_pipelines/dlt_trusted_pipeline.py

Conteúdo:

Python

# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(name="hoteis_trusted", comment="Tabela de hoteis limpa e padronizada.")
def hoteis_trusted():
  df_raw = spark.read.table("production.raw.hoteis_raw")
  df_trusted = df_raw.select(col("hotel_id").cast("INT"), col("nome_hotel").cast("STRING"), col("endereco").cast("STRING"), col("cidade").cast("STRING"), col("estado").cast("STRING"), col("estrelas").cast("INT"), col("numero_quartos").cast("INT"), col("comodidades").cast("STRING")).withColumn("data_carga_trusted", current_timestamp())
  return df_trusted

# ... (Cole o código completo com as transformações para todas as outras tabelas Trusted) ...
c) Camada Refined

Ficheiro: src/dlt_pipelines/dlt_refined_pipeline.py

Conteúdo:

Python

# Databricks notebook source
import dlt
from pyspark.sql.functions import col

@dlt.table(name="d_hoteis", comment="Dimensão de Hotéis.")
def d_hoteis(): return spark.read.table("production.trusted.hoteis_trusted")

@dlt.table(name="d_hospedes", comment="Dimensão de Hóspedes.")
def d_hospedes(): return spark.read.table("production.trusted.hospedes_trusted")

@dlt.table(name="d_quartos", comment="Dimensão de Quartos.")
def d_quartos(): return spark.read.table("production.trusted.quartos_trusted")

@dlt.table(name="f_reservas", comment="Tabela de factos de reservas.")
def f_reservas():
  reservas = spark.read.table("production.trusted.reservas_trusted")
  canais = spark.read.table("production.trusted.reservas_canal_trusted")
  f_reservas_joined = reservas.join(canais, reservas.reserva_id == canais.reserva_id, "left")
  return f_reservas_joined.select(reservas.reserva_id, reservas.hospede_id, reservas.quarto_id, reservas.hotel_id, reservas.data_reserva, reservas.data_checkin, reservas.data_checkout, canais.canal_reserva, reservas.numero_noites, reservas.valor_total_estadia)
3.3. O Ficheiro de Configuração do Bundle
Este é o cérebro do nosso projeto, que define todos os nossos recursos no Databricks.

Ficheiro: databricks.yml (na raiz do projeto)

Conteúdo:

YAML

# databricks.yml
bundle:
  name: databricks_hotel_etl

workspace:
  host: https://dbc-03ce1db1-207c.cloud.databricks.com

sync:
  include:
    - "src/**/*"

resources:
  pipelines:
    dlt_raw_pipeline:
      # ... (Cole aqui a definição completa do pipeline Raw) ...
    dlt_trusted_pipeline:
      # ... (Cole aqui a definição completa do pipeline Trusted) ...
    dlt_refined_pipeline:
      # ... (Cole aqui a definição completa do pipeline Refined) ...
  jobs:
    medallion_orchestrator_job:
      # ... (Cole aqui a definição completa do Job Orquestrador) ...

targets:
  dev:
    default: true
Fase 4: Implementação e Execução
Com todos os ficheiros criados, é hora de dar vida ao projeto.

4.1. O Primeiro Deploy via Git
Adicione todos os seus ficheiros ao Git e envie para o GitHub.

Bash

git add .
git commit -m "Commit inicial do projeto de ETL Medallion completo"
git push
Verificação: Vá para a aba "Actions" do seu repositório no GitHub. Você verá a sua esteira a ser executada. Ela irá validar e implementar todos os seus recursos (3 pipelines e 1 job) no Databricks.

4.2. Execução da Carga Inicial
Vá para o seu workspace Databricks. Crie um notebook, cole o conteúdo do script 00_geracao_dados_iniciais.py e execute-o uma única vez.

Isto irá criar e popular as tabelas no schema production.transient.

4.3. Execução do Pipeline Orquestrado
Na interface do Databricks, vá para "Data Engineering" > "Jobs & Pipelines" e clique no separador "Jobs".

Encontre o job "JOB - Orquestrador do Pipeline Medallion Completo".

Clique em "Run now".

O orquestrador irá executar os três pipelines DLT em sequência, populando as camadas Raw, Trusted e Refined de forma totalmente automatizada.