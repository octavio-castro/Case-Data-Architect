# ============================================
# GOLD — Unificar, adicionar colunas calculadas, particionar
# ============================================

# Ler as tabelas Silver
df_yellow = spark.table("taxi_case.silver.yellow")
df_green = spark.table("taxi_case.silver.green")
df_fhv = spark.table("taxi_case.silver.fhv")
df_fhvhv = spark.table("taxi_case.silver.fhvhv")

# Unir todas as tabelas Silver numa só (Gold)
df_all = df_yellow.unionByName(df_green, allowMissingColumns=True)
df_all = df_all.unionByName(df_fhv, allowMissingColumns=True)
df_all = df_all.unionByName(df_fhvhv, allowMissingColumns=True)

# Adicionar colunas calculadas para as análises
df_gold = df_all \
    .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

# Salvar como tabela Delta particionada por mês (Gold)
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_month") \
    .saveAsTable("taxi_case.gold.taxi_trips")

#### Verificação ###
total = spark.sql("SELECT COUNT(*) as total FROM taxi_case.gold.taxi_trips").collect()[0]["total"]
print(f"gold.taxi_trips: {total:,} registros totais")

#### Verificar distribuição por tipo e mês ###
#spark.sql("""
#    SELECT taxi_type, pickup_month, COUNT(*) as total
#    FROM taxi_case.gold.taxi_trips
#    GROUP BY taxi_type, pickup_month
#    ORDER BY taxi_type, pickup_month
#""").show()

### Verificar pickup_month ###
#spark.sql("""
#    SELECT pickup_month, 
#           MIN(tpep_pickup_datetime) as min_date, 
#           MAX(tpep_pickup_datetime) as max_date,
#           COUNT(*) as total
#    FROM taxi_case.gold.taxi_trips
#    WHERE taxi_type = 'green'
#    GROUP BY pickup_month
#    ORDER BY pickup_month
#""").show()

### Validacao meses
#spark.sql("""
#    SELECT pickup_month, 
#           MIN(tpep_pickup_datetime) as min_date, 
#           MAX(tpep_pickup_datetime) as max_date,
#           COUNT(*) as total
#    FROM taxi_case.gold.taxi_trips
#    WHERE taxi_type = 'green'
#    GROUP BY pickup_month
#    ORDER BY pickup_month
#""").show()

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_month") \
    .saveAsTable("taxi_case.gold.taxi_trips")

# Adicionar comentários nas colunas (governança de dados)
spark.sql("""
    ALTER TABLE taxi_case.gold.taxi_trips ALTER COLUMN
      VendorID COMMENT 'Identificador do fornecedor/empresa do equipamento',
      passenger_count COMMENT 'Quantidade de passageiros na corrida (1-5, conforme TLC Chapter 54)',
      total_amount COMMENT 'Valor total cobrado pela corrida em USD',
      tpep_pickup_datetime COMMENT 'Data e hora de inicio da corrida',
      tpep_dropoff_datetime COMMENT 'Data e hora de termino da corrida',
      taxi_type COMMENT 'Tipo do taxi: yellow ou green',
      pickup_month COMMENT 'Mes da corrida (1-5), extraido do pickup_datetime',
      pickup_hour COMMENT 'Hora do dia (0-23), extraida do pickup_datetime'
""")
