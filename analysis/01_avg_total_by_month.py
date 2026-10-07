# ============================================
# ANÁLISE 1: Média de total_amount por mês (yellow taxi)
# ============================================
# Pergunta do case:
# "Qual a média de valor total (total_amount) recebido em um mês
#  considerando todos os yellow táxis da frota?"
# ============================================

from pyspark.sql.functions import col, avg
# Le a tabela Gold
df_gold = spark.table("taxi_case.gold.taxi_trips")

# Filtra so yellow, agrupa por mes e calcula a media de total_amount
print("=== Média por mês (Yellow Taxi) ===")
analise_1_fmt = df_gold.filter(col("taxi_type") == "yellow") \
    .groupBy("pickup_month") \
    .agg(avg("total_amount").alias("avg_total_amount")) \
    .withColumn("avg_total_amount", format_string("$%.2f", col("avg_total_amount"))) \
    .orderBy("pickup_month")

analise_1_fmt.show()

# Média geral do período todo
media_geral = df_gold.filter(col("taxi_type") == "yellow") \
    .agg(avg("total_amount").alias("avg_total_period")) \
    .collect()[0]["avg_total_period"]

print(f"=== Média geral do período (Jan-Mai): ${media_geral:.2f} ===\n")
