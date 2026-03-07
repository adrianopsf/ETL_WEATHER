# 🌦️ ETL Weather Pipeline

> Pipeline de dados orquestrado com **Apache Airflow** que extrai dados meteorológicos das **26 capitais brasileiras** via OpenWeather API, transforma e carrega em PostgreSQL com camadas Raw e Curated.

[![Python](https://img.shields.io/badge/Python-3.8+-blue?style=flat-square&logo=python)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=flat-square&logo=apacheairflow)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-latest-blue?style=flat-square&logo=postgresql)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-required-2496ED?style=flat-square&logo=docker)](https://docker.com)
[![Astronomer](https://img.shields.io/badge/Astronomer-Runtime%2011.7-purple?style=flat-square)](https://astronomer.io)

---

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Execução](#instalação-e-execução)
- [DAGs e Tarefas](#dags-e-tarefas)
- [Esquema do Banco de Dados](#esquema-do-banco-de-dados)
- [Avaliação End-to-End](#avaliação-end-to-end)
- [Problemas Conhecidos e Melhorias](#problemas-conhecidos-e-melhorias)

---

## 🎯 Visão Geral

Este projeto implementa um pipeline ETL completo para dados meteorológicos:

1. **Extrai** dados de clima atual, previsão (5 dias) e histórico (últimos 3 dias) para todas as 26 capitais brasileiras via [OpenWeather API](https://openweathermap.org/api)
2. **Transforma** os dados em tabelas estruturadas com separação entre camada Raw e Curated
3. **Carrega** em PostgreSQL com tratamento de duplicatas e controle temporal
4. **Orquestra** todo o processo via Apache Airflow com execução paralela por cidade

**Cobertura geográfica:** Todas as 26 capitais estaduais brasileiras (Rio Branco, Maceió, Macapá, Manaus, Salvador, Fortaleza, Brasília, Vitória, Goiânia, São Luís, Cuiabá, Campo Grande, Belo Horizonte, Belém, João Pessoa, Curitiba, Recife, Teresina, Rio de Janeiro, Natal, Porto Alegre, Porto Velho, Boa Vista, Florianópolis, São Paulo, Aracaju).

---

## 🏗️ Arquitetura

```
┌────────────────────────────────────────────────────────────┐
│                  Apache Airflow DAG: weather_data           │
│                                                            │
│   ┌─────────┐                                             │
│   │  hello  │                                             │
│   └────┬────┘                                             │
│        │                                                   │
│   ┌────┴──────────────────────┐                           │
│   │    Extração Paralela      │                           │
│   ├───────────┬───────────┬───┤                           │
│   │ download  │ download  │ download                      │
│   │ current   │ forecast  │ historical                    │
│   └─────┬─────┴─────┬─────┴────┬──┘                      │
│         │           │          │                          │
│   ┌─────▼─────┬─────▼──────┬───▼──────┐                  │
│   │ current   │ forecast   │ history  │                   │
│   │ →raw table│ →raw table │ →raw tbl │   (PostgreSQL)   │
│   └─────┬─────┴─────┬──────┴──────────┘                  │
│         │           │                                      │
│   ┌─────▼──────┬────▼──────────┐                         │
│   │ current    │ timeline      │                          │
│   │ →curated   │ →curated      │   (PostgreSQL)          │
│   └─────┬──────┴────────────────┘                        │
│         │                                                  │
│   ┌─────▼────────┐                                        │
│   │   bye_bye    │                                        │
│   └──────────────┘                                        │
└────────────────────────────────────────────────────────────┘

Fonte: OpenWeather API → CSV intermediário → PostgreSQL
Camadas: Raw (dados brutos) → Curated (dados transformados)
```

---

## 📁 Estrutura do Projeto

```
ETL_WEATHER/
├── dags/                           # Definições do Apache Airflow
│   ├── scripts/
│   │   ├── weather_data.py         # Extração da OpenWeather API
│   │   └── queries.py              # Queries SQL de transformação
│   ├── weather-etl.py              # DAG principal do pipeline
│   ├── connection_test.py          # DAG de teste de conexão PostgreSQL
│   ├── current_raw.csv             # Amostra de dados de clima atual
│   ├── forecast_raw.csv            # Amostra de dados de previsão
│   ├── history_raw.csv             # Amostra de dados históricos
│   └── .airflowignore
│
├── tests/
│   └── dags/
│       └── test_dag_example.py     # Testes de validação das DAGs
│
├── .astro/                         # Configuração Astronomer
├── .env.example                    # Template de variáveis de ambiente
├── Dockerfile                      # astro-runtime:11.7.0
├── docker-compose.yml              # PostgreSQL + Airflow local
├── requirements.txt                # Dependências Python adicionais
├── packages.txt                    # Pacotes de sistema
└── .gitignore
```

---

## ✅ Pré-requisitos

| Ferramenta | Versão | Uso |
|------------|--------|-----|
| Docker     | 20+    | Infraestrutura local |
| Docker Compose | 2+ | Orquestração de serviços |
| Astro CLI  | latest | Gerenciamento do Airflow local *(recomendado)* |
| OpenWeather API Key | — | Chave gratuita em [openweathermap.org](https://openweathermap.org/api) |

---

## 🚀 Instalação e Execução

### Opção A — Com Astro CLI *(recomendado)*

```bash
# 1. Instale o Astro CLI
curl -sSL install.astronomer.io | sudo bash -s

# 2. Clone o repositório
git clone https://github.com/adrianopsf/ETL_WEATHER.git
cd ETL_WEATHER

# 3. Configure as variáveis de ambiente
cp .env.example .env
# Edite .env com sua chave da OpenWeather API e credenciais do banco

# 4. Inicie o ambiente local
astro dev start

# 5. Acesse o Airflow
# URL: http://localhost:8080
# Usuário: admin | Senha: admin
```

### Opção B — Com Docker Compose diretamente

```bash
# 1. Clone o repositório
git clone https://github.com/adrianopsf/ETL_WEATHER.git
cd ETL_WEATHER

# 2. Configure variáveis de ambiente
cp .env.example .env

# 3. Inicie todos os serviços
docker compose up -d

# 4. Aguarde os serviços subirem (~60 segundos)
docker compose ps

# 5. Acesse o Airflow em http://localhost:8080
```

### Ativar e executar o DAG

1. Acesse o Airflow em `http://localhost:8080`
2. Ative o DAG `weather_data` (toggle ON)
3. Clique em **Trigger DAG** para executar manualmente
4. Acompanhe a execução na aba **Graph View**

---

## ⚙️ DAGs e Tarefas

### DAG: `weather_data`

| Tarefa | Tipo | Descrição |
|--------|------|-----------|
| `hello` | PythonOperator | Log de início do pipeline |
| `download_current_weather_data` | PythonOperator | Busca clima atual para 26 capitais |
| `download_forecast_weather_data` | PythonOperator | Busca previsão de 5 dias para 26 capitais |
| `download_historical_weather_data` | PythonOperator | Busca histórico dos últimos 3 dias para 26 capitais |
| `current_weather_to_raw` | PythonOperator | Carga do CSV atual → `current_weather_raw` |
| `forecast_weather_to_raw` | PythonOperator | Carga do CSV de previsão → `forecast_weather_raw` |
| `history_weather_to_raw` | PythonOperator | Carga do CSV histórico → `history_weather_raw` |
| `current_to_curated` | PythonOperator | Transformação → `current_weather` (curada) |
| `timeline_to_curated` | PythonOperator | União forecast+history → `timeline_weather` (curada) |
| `bye_bye` | PythonOperator | Log de conclusão |

**Configurações do DAG:**
- Schedule: `@once` (execução única, manual ou agendável)
- Retries: 1 tentativa com intervalo de 5 minutos
- Catchup: desabilitado

---

## 🗃️ Esquema do Banco de Dados

### Camada Raw

#### `current_weather_raw`
Dados brutos da API de clima atual, um registro por cidade por execução.

#### `forecast_weather_raw`
Dados brutos de previsão (JSON com array de horas futuras).

#### `history_weather_raw`
Dados brutos históricos (JSON com array de horas passadas).

### Camada Curated

#### `current_weather`
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| date_local | DATE | Data local da medição |
| timestamp_local | TIMESTAMP | Timestamp local |
| location | TEXT | "Cidade, Estado, Brasil" |
| city | TEXT | Nome da cidade |
| state | TEXT | Sigla do estado |
| country | TEXT | País |
| latitude | NUMERIC | Latitude |
| longitude | NUMERIC | Longitude |
| timezone | TEXT | Fuso horário |
| temperature | NUMERIC | Temperatura (°C) |
| thermal_sensation | NUMERIC | Sensação térmica (°C) |
| precipitation | NUMERIC | Precipitação (mm) |
| humidity | NUMERIC | Umidade relativa (%) |
| cloud_cover | NUMERIC | Cobertura de nuvens (%) |
| uv_radiation | NUMERIC | Índice UV |
| wind_speed | NUMERIC | Velocidade do vento (km/h) |
| wind_direction | NUMERIC | Direção do vento (°) |
| condition | TEXT | Descrição das condições |
| updatetime_utc | TIMESTAMP | Timestamp da atualização |

#### `timeline_weather`
Combina dados históricos e de previsão hora a hora.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| location | TEXT | Localização |
| city / state / country | TEXT | Dados geográficos |
| latitude / longitude | NUMERIC | Coordenadas |
| type | TEXT | `'historical'` ou `'forecast'` |
| time_reference | TEXT | Referência temporal |
| date | DATE | Data da medição |
| hour | TEXT | Hora da medição |
| temperature | NUMERIC | Temperatura (°C) |
| humidity | NUMERIC | Umidade (%) |
| wind_speed | NUMERIC | Velocidade do vento |
| condition | TEXT | Condição do tempo |
| *...demais métricas* | NUMERIC | Dados meteorológicos horários |

---

## 🔍 Avaliação End-to-End

> Análise da viabilidade de execução completa do pipeline — atualizada após correções.

### ✅ O que funciona

| Componente | Status | Observação |
|-----------|--------|-----------|
| Dockerfile (Astronomer Runtime) | ✅ | Imagem funcional, todas as dependências do Airflow incluídas |
| Docker Compose (infra) | ✅ | PostgreSQL + Airflow configurados corretamente |
| `weather_data.py` (extração) | ✅ | API key lida de variável de ambiente `OPENWEATHER_API_KEY` |
| `weather-etl.py` (DAG) | ✅ | Credenciais DB via env vars + helper `get_db_conn()` |
| `queries.py` (transformações SQL) | ✅ | Queries de curated layer bem estruturadas com JSON parsing |
| Estrutura do DAG | ✅ | Paralelismo correto entre tarefas de extração |
| `.env.example` | ✅ | Template com todas as variáveis necessárias |
| `test_dag_example.py` | ✅ | Testes básicos de validação de DAG presentes |

### ⚠️ Pontos de atenção (restantes)

| Problema | Severidade | Recomendação |
|---------|-----------|-------------|
| **Testes insuficientes** | 🟡 Média | Adicionar testes de integração para validar a extração de dados |
| **Schedule `@once`** | 🟢 Baixa | Considerar `@daily` ou cron para operação contínua |
| **CSV armazenado no dags/** | 🟢 Baixa | Salvar em `/tmp` ou pasta de dados dedicada em vez do diretório de DAGs |

---

## 🛠️ Melhorias Futuras

- [ ] Configurar rotina diária (`schedule_interval='@daily'`)
- [ ] Adicionar monitoramento com alertas por e-mail em caso de falha
- [ ] Implementar idempotência nas tabelas Raw (evitar duplicatas por re-execução)
- [ ] Adicionar testes de qualidade de dados (Great Expectations ou dbt tests)
- [ ] Criar dashboard no Metabase ou Grafana para visualização
- [ ] Expandir para mais cidades ou países
- [ ] Armazenar dados em formato Parquet para análise histórica

---

## 📊 Dados de Exemplo

**Capitais monitoradas (26 no total):**

| Região | Capitais |
|--------|---------|
| Norte | Rio Branco, Macapá, Manaus, Belém, Porto Velho, Boa Vista, Palmas |
| Nordeste | Maceió, Salvador, Fortaleza, São Luís, Natal, João Pessoa, Recife, Teresina, Aracaju |
| Centro-Oeste | Brasília, Cuiabá, Campo Grande, Goiânia |
| Sudeste | Vitória, Belo Horizonte, Rio de Janeiro, São Paulo |
| Sul | Curitiba, Porto Alegre, Florianópolis |

---

## 🤝 Contribuindo

1. Fork o repositório
2. Crie uma branch: `git checkout -b feature/minha-feature`
3. Commit: `git commit -m 'feat: adiciona minha feature'`
4. Push: `git push origin feature/minha-feature`
5. Abra um Pull Request

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

---

<div align="center">
  <sub>Feito com ❤️ por <a href="https://github.com/adrianopsf">adrianopsf</a></sub>
</div>
