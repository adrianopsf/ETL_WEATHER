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

**Cobertura geográfica:** Todas as 26 capitais estaduais brasileiras.

---

## 🏗️ Arquitetura

```
OpenWeather API → Extração Paralela (26 cidades) → CSV intermediário → PostgreSQL Raw → PostgreSQL Curated
```

Orquestrado pelo Apache Airflow com 10 tarefas em pipeline sequencial/paralelo.

---

## 📁 Estrutura do Projeto

```
ETL_WEATHER/
├── dags/
│   ├── scripts/
│   │   ├── weather_data.py         # Extração da OpenWeather API
│   │   └── queries.py              # Queries SQL de transformação
│   ├── weather-etl.py              # DAG principal do pipeline
│   ├── connection_test.py          # DAG de teste de conexão PostgreSQL
│   ├── current_raw.csv             # Amostra de dados de clima atual
│   ├── forecast_raw.csv            # Amostra de dados de previsão
│   ├── history_raw.csv             # Amostra de dados históricos
│   └── .airflowignore
├── tests/
│   └── dags/
│       └── test_dag_example.py     # Testes de validação das DAGs
├── Dockerfile                      # astro-runtime:11.7.0
├── docker-compose.yml              # PostgreSQL + Airflow local
├── requirements.txt
└── packages.txt
```

---

## ✅ Pré-requisitos

| Ferramenta | Versão | Uso |
|------------|--------|-----|
| Docker | 20+ | Infraestrutura local |
| Docker Compose | 2+ | Orquestração de serviços |
| Astro CLI | latest | Gerenciamento do Airflow local *(recomendado)* |
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

# 5. Acesse o Airflow em http://localhost:8080 (admin/admin)
```

### Opção B — Com Docker Compose

```bash
git clone https://github.com/adrianopsf/ETL_WEATHER.git
cd ETL_WEATHER
cp .env.example .env
docker compose up -d
# Acesse http://localhost:8080
```

### Executar o DAG

1. Acesse `http://localhost:8080`
2. Ative o DAG `weather_data` (toggle ON)
3. Clique em **Trigger DAG**
4. Acompanhe na aba **Graph View**

---

## ⚙️ DAGs e Tarefas

### DAG: `weather_data`

| Tarefa | Descrição |
|--------|-----------|
| `hello` | Log de início do pipeline |
| `download_current_weather_data` | Busca clima atual para 26 capitais |
| `download_forecast_weather_data` | Busca previsão de 5 dias |
| `download_historical_weather_data` | Busca histórico dos últimos 3 dias |
| `current_weather_to_raw` | Carga CSV → `current_weather_raw` |
| `forecast_weather_to_raw` | Carga CSV → `forecast_weather_raw` |
| `history_weather_to_raw` | Carga CSV → `history_weather_raw` |
| `current_to_curated` | Transforma → `current_weather` |
| `timeline_to_curated` | Une forecast+history → `timeline_weather` |
| `bye_bye` | Log de conclusão |

**Configurações:** Schedule `@once`, 1 retry com delay de 5min, catchup desabilitado.

---

## 🗃️ Esquema do Banco de Dados

### Camada Raw
- `current_weather_raw` — dados brutos do clima atual por cidade
- `forecast_weather_raw` — JSON com previsão por horas futuras
- `history_weather_raw` — JSON com histórico por horas passadas

### Camada Curated

#### `current_weather`
Campos: `date_local`, `timestamp_local`, `location`, `city`, `state`, `country`, `latitude`, `longitude`, `timezone`, `temperature`, `thermal_sensation`, `precipitation`, `humidity`, `cloud_cover`, `uv_radiation`, `wind_speed`, `wind_direction`, `condition`, `updatetime_utc`

#### `timeline_weather`
Une dados históricos e de previsão hora a hora com campo `type` (`'historical'` ou `'forecast'`).

---

## 🔍 Avaliação End-to-End

### ✅ O que funciona

| Componente | Status | Observação |
|-----------|--------|-----------|
| Dockerfile (Astronomer Runtime) | ✅ | Imagem funcional com dependências incluídas |
| Docker Compose | ✅ | PostgreSQL + Airflow configurados corretamente |
| `weather_data.py` | ✅ | Extração e geração de CSV funcionando |
| `queries.py` | ✅ | Transformações SQL com JSON parsing correto |
| Estrutura do DAG | ✅ | Paralelismo correto entre tarefas |
| Testes de DAG | ✅ | Validações básicas presentes |

### ⚠️ Pontos de atenção

| Problema | Severidade | Recomendação |
|---------|-----------|-------------|
| **API Key hardcoded** em `weather_data.py` | 🔴 Alta | Usar `os.getenv("OPENWEATHER_API_KEY")` |
| **Credenciais DB hardcoded** em `weather-etl.py` | 🔴 Alta | Usar Airflow Connections ou variáveis de ambiente |
| **Bug: connection string sem f-string** | 🔴 Alta | Ver correção abaixo — falha em runtime |
| **Sem `.env.example`** | 🟡 Média | Criar template de variáveis |
| **Schedule `@once`** | 🟢 Baixa | Considerar `@daily` para operação contínua |

### 🐛 Bug crítico — connection string

```python
# ❌ ATUAL — variáveis não são interpoladas (bug de runtime)
conn = psycopg2.connect("dbname={dbname} user={user} host= {host} password={password}")

# ✅ CORRETO
import os
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME", "postgres"),
    user=os.getenv("DB_USER", "postgres"),
    host=os.getenv("DB_HOST", "localhost"),
    password=os.getenv("DB_PASSWORD", "postgres")
)
```

---

## 🛠️ Melhorias Futuras

- [ ] Corrigir bug da connection string e remover credenciais hardcoded
- [ ] Criar `.env.example` com todas as variáveis necessárias
- [ ] Configurar rotina diária (`schedule_interval='@daily'`)
- [ ] Adicionar idempotência nas tabelas Raw
- [ ] Implementar monitoramento com alertas por e-mail
- [ ] Criar dashboard no Metabase ou Grafana
- [ ] Adicionar testes de qualidade de dados

---

## 📊 Capitais Monitoradas (26)

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
3. Commit: `git commit -m 'feat: minha feature'`
4. Push e abra um Pull Request

---

## 📄 Licença

MIT License — veja [LICENSE](LICENSE) para detalhes.

---

<div align="center">
  <sub>Feito com ❤️ por <a href="https://github.com/adrianopsf">adrianopsf</a></sub>
</div>
