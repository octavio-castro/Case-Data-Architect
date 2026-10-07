# ============================================
# ANÁLISE 2: Média de passenger_count por hora (todos os táxis, maio)
# ============================================
# Pergunta do case:
# "Qual a média de passageiros por cada hora do dia que pegaram táxi
#  no mês de maio considerando todos os táxis da frota?"
# ============================================

# Lê a tabela Gold
df_gold = spark.table("taxi_case.gold.taxi_trips")

# Filtra maio, agrupa por hora, formata e ordena
analise_2 = df_gold.filter(col("pickup_month") == 5) \
    .groupBy("pickup_hour") \
    .agg(avg("passenger_count").alias("avg_passenger_count")) \
    .withColumn("avg_passenger_count", format_string("%.2f", col("avg_passenger_count"))) \
    .orderBy("pickup_hour")

print("=== Análise 2: Média de passenger_count por hora (Maio - Todos os táxis) ===")
analise_2.show(24)
