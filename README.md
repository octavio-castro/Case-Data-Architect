# Case-Tecnico-Data-Architect-iFood
NYC Taxi Trip Data — Data Engineering Case — Ingestion, transformation and analysis with PySpark
Solução de engenharia de dados para ingestão, transformação e análise de dados de corridas de táxi de Nova York (jan-mai 2023), utilizando arquitetura medalhão (Bronze/Silver/Gold) com PySpark e Delta Lake no Databricks.

---
## Ambiente de Desenvolvimento

A solução foi desenvolvida no Databricks Free Edition. O código fonte está disponível nos arquivos `.py` deste repositório, organizado nas pastas `src/` e `analysis/`.

---

## Arquitetura

A solução segue o padrão de arquitetura medalhão, organizando os dados em camadas progressivas de qualidade:

| Camada | O que contém | Formato |
|---|---|---|
| **Landing Zone** | Arquivos Parquet originais (20 arquivos, 4 tipos de táxi) | Parquet |
| **Bronze** | Dados ingeridos sem modificação (4 tabelas) | Delta |
| **Silver** | Dados limpos, validados e com colunas padronizadas (yellow + green) | Delta |
| **Gold** | Dados prontos para consumo, unificados e particionados por mês | Delta |

**Fluxo:** Landing Zone → Bronze (ingestão) → Silver (limpeza + validação) → Gold (consumo + análises)

---

## Tecnologias

- **Databricks Free Edition** — ambiente de processamento
- **PySpark** — processamento distribuído
- **Delta Lake** — formato de armazenamento com ACID transactions
- **Unity Catalog** — governança e organização (catálogo: `taxi_case`)
- **Parquet** — formato dos arquivos originais

---

## Estrutura do Repositório

**src/**
- `00_setup_and_ingestion.py` — Criacao de catalogos, schemas e volumes
- `01_ingestion_bronze.py` — Leitura dos Parquet -> Bronze
- `02_transformation_silver.py` — Bronze -> Silver (limpeza + assertions)
- `03_consumption_gold_governance.py` — Silver -> Gold (uniao + particionamento)

**analysis/**
- `01_avg_total_by_month.py` — Analise 1: media de total_amount por mes
- `02_avg_passengers_by_hour.py` — Analise 2: media de passenger_count por hora

**README.md/**

**requirements.txt/**


---

## Dados

Os dados utilizados são públicos e disponibilizados pela NYC Taxi & Limousine Commission (TLC). Foram baixados 20 arquivos Parquet cobrindo janeiro a maio de 2023, para 4 tipos de veículo:

| Tipo | Descrição | Tem passenger_count | Tem total_amount |
|---|---|---|---|
| Yellow Taxi | Táxis amarelos tradicionais | TRUE | TRUE |
| Green Taxi | Táxis verdes (Street Hail Livery) | TRUE | TRUE |
| FHV | For-Hire Vehicle (despacho) | FALSE | FALSE |
| FHVHV | High Volume FHV (Uber, Lyft) | FALSE | FALSE |

**Decisão:** FHV e FHVHV foram mantidos na Bronze para rastreabilidade, mas não foram levados para Silver/Gold, pois não possuem as colunas `passenger_count` e `total_amount` necessárias para as análises solicitadas.

---

## Data Quality

Durante a validação da camada Gold, identificou-se registros com datas de 2008 e meses fora do período esperado (janeiro a maio de 2023). Isso motivou uma revisão da camada Silver com filtros de qualidade adicionais.

### Filtros aplicados na Silver

| Filtro | Condição | Justificativa |
|---|---|---|
| Período válido | `tpep_pickup_datetime BETWEEN '2023-01-01' AND '2023-05-31'` | Garante apenas dados do período solicitado |
| Pickup não nulo | `tpep_pickup_datetime IS NOT NULL` | Sem data não é possível agregar por mês/hora |
| Dropoff após pickup | `tpep_dropoff_datetime >= tpep_pickup_datetime` | Remove corridas impossíveis |
| Total amount positivo | `total_amount > 0` | Remove corridas canceladas/estornadas |
| Passenger count válido | `passenger_count BETWEEN 1 AND 5` | Conforme norma TLC Chapter 54 — máximo de 5 passageiros por veículo |

### Data Quality Assertions

Após a limpeza e antes do salvamento, a pipeline executa assertions que validam se os dados limpos atendem às regras definidas. Se qualquer assertion falhar, a execução é interrompida e o erro é reportado — nenhum dado inválido é salvo.

PASS [silver.yellow]: Pickup nulo apos limpeza
PASS [silver.yellow]: Datas antes de 2023-01-01
PASS [silver.yellow]: Datas depois de 2023-05-31
PASS [silver.yellow]: Total amount <= 0
PASS [silver.yellow]: Passenger count fora do range 1-5
ALL CHECKS PASSED for silver.yellow

**Backtest:** Para validar os assertions, foi executado um backtest rodando as validações diretamente na Bronze (dados sujos). O backtest confirmou que os assertions identificam corretamente os problemas existentes nos dados originais. Após a limpeza, todos os checks passam.

---

## Como Executar

1. Criar os catálogos, schemas e volumes (executar `00_setup_and_ingestion.py`)
2. Subir os arquivos Parquet para os volumes em `/Volumes/taxi_case/landing/{tipo}/`
3. Executar a ingestão (`01_ingestion_bronze.py`) — cria as tabelas Bronze
4. Executar a transformação (`02_transformation_silver.py`) — limpa, valida e cria as tabelas Silver
5. Executar o consumo (`03_consumption_gold_governance.py`) — unifica e cria a tabela Gold
6. Executar as análises (`analysis/01_avg_total_by_month.py` e `02_avg_passengers_by_hour.py`)

---

## Análises e Resultados

### Análise 1: Média de total_amount por mês (Yellow Taxi)

Pergunta: "Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota?"

| Mês | Média por corrida |
|---|---|
| Janeiro | $27.47 |
| Fevereiro | $27.37 |
| Março | $28.29 |
| Abril | $28.79 |
| Maio | $29.45 |

**Média geral do período:** $28.32 por corrida.

**Interpretação:** A média por corrida variou de $27,37 (fevereiro) a $29,45 (maio), com tendência de aumento ao longo dos meses. A média geral do período foi de $28,32 por corrida.

### Análise 2: Média de passenger_count por hora (Maio — Todos os táxis)

Pergunta: "Qual a média de passageiros por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?"

| Hora | Média de passageiros | Hora | Média de passageiros |
|---|---|---|---|
| 00h | 1.39 | 12h | 1.33 |
| 01h | 1.40 | 13h | 1.34 |
| 02h | 1.41 | 14h | 1.34 |
| 03h | 1.40 | 15h | 1.36 |
| 04h | 1.35 | 16h | 1.36 |
| 05h | 1.23 | 17h | 1.35 |
| 06h | 1.20 | 18h | 1.35 |
| 07h | 1.23 | 19h | 1.36 |
| 08h | 1.24 | 20h | 1.37 |
| 09h | 1.26 | 21h | 1.38 |
| 10h | 1.30 | 22h | 1.39 |
| 11h | 1.31 | 23h | 1.39 |

**Interpretação:** A média de passageiros por corrida varia pouco ao longo do dia (entre 1,20 e 1,41), indicando que a maioria das corridas leva 1 passageiro. Os horários de madrugada (00h-03h) têm a média ligeiramente mais alta (1.39-1.41), possivelmente por grupos voltando de eventos. Os horários de início de expediente (06h-08h) têm a média mais baixa (1.20-1.24), sugerindo viagens individuais de deslocamento ao trabalho.

Nota: "Todos os táxis" na prática significa yellow + green, pois são os únicos tipos que registram `passenger_count`. FHV e FHVHV não possuem essa coluna.

---

## Decisões Técnicas

| Decisão | Justificativa |
|---|---|
| Arquitetura medalhão (Bronze/Silver/Gold) | Separação de responsabilidades: raw → limpo → consumo |
| Apenas yellow e green na Silver/Gold | FHV e FHVHV não têm as colunas necessárias para as análises |
| Particionamento por mês na Gold | Acelera consultas que filtram por mês (partition pruning) |
| Colunas calculadas na Gold (month, hour) | Evita recálculo a cada consulta — performance |
| Data quality assertions | Fail-fast: se dados inválidos entrarem, o pipeline para antes de contaminar análises |
| Delta Lake em todas as camadas | ACID transactions, time travel e schema enforcement |
| Unity Catalog | Governança e organização em 3 níveis (catálogo.schema.tabela) |
| Comentários nas colunas da Gold | Documentação e governança de dados via Unity Catalog |

## Melhorias Futuras

- **Orquestração:** Agendamento automático da pipeline com Lakeflow Jobs ou Airflow
- **Incremental loading:** Carregar apenas novos dados a cada execução, em vez de overwrite completo
- **Catálogo de dados:** Documentação e linhagem automatizada via Unity Catalog


## Fontes

- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- TLC Rules and Local Laws - Chapter 54: capacidade máxima de passageiros
- Databricks Medallion Architecture: https://docs.databricks.com/en/lakehouse/medallion.html

