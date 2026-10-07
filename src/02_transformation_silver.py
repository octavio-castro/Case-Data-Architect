# ============================================
# FUNÇÃO DE DATA QUALITY ASSERTIONS
# ============================================
def assert_data_quality(df, table_name, checks):
    errors = []
    for condition, message in checks:
        failed_count = df.filter(condition).count()
        if failed_count > 0:
            errors.append(f"FAIL [{table_name}]: {message} ({failed_count} registros)")
        else:
            print(f"PASS [{table_name}]: {message}")
    
    if errors:
        print("\n=== DATA QUALITY CHECK FAILED ===")
        for e in errors:
            print(f"  {e}")
        raise ValueError(f"Data quality check falhou para {table_name}")
    else:
        print(f"ALL CHECKS PASSED for {table_name}\n")


# ============================================
# SILVER v4 — Limpeza + Data Quality Assertions
# ============================================

bronze_tables = {
    "yellow": "taxi_case.bronze.yellow",
    "green": "taxi_case.bronze.green"
}

required_cols = [
    "VendorID", "passenger_count", "total_amount",
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "taxi_type"
]

valid_start = "2023-01-01 00:00:00"
valid_end = "2023-05-31 23:59:59"

for taxi_type, table_name in bronze_tables.items():
    print(f"Transformando {taxi_type}...")
    df = spark.table(table_name)
    
    # Padronizar nomes de colunas
    if "lpep_pickup_datetime" in df.columns:
        df = df.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime") \
               .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
    
    # Selecionar colunas
    available_cols = [c for c in required_cols if c in df.columns]
    df = df.select(*available_cols)
    
    # Converter datas
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
           .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    
    # ===== FILTROS DE DATA QUALITY =====
    df = df.filter(col("tpep_pickup_datetime").isNotNull())
    df = df.filter((col("tpep_pickup_datetime") >= valid_start) & 
                   (col("tpep_pickup_datetime") <= valid_end))
    df = df.filter(
        col("tpep_dropoff_datetime").isNull() | 
        (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    )
    df = df.filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    df = df.filter(col("passenger_count").isNotNull() & 
                   (col("passenger_count") >= 1) & 
                   (col("passenger_count") <= 5))
    
    # ===== ASSERTIONS: validar antes de salvar =====
    assert_data_quality(df, f"silver.{taxi_type}", [
        (col("tpep_pickup_datetime").isNull(), "Pickup nulo apos limpeza"),
        ((col("tpep_pickup_datetime") < valid_start), "Datas antes de 2023-01-01"),
        ((col("tpep_pickup_datetime") > valid_end), "Datas depois de 2023-05-31"),
        ((col("total_amount") <= 0), "Total amount <= 0"),
        ((col("passenger_count") < 1) | (col("passenger_count") > 5), "Passenger count fora do range 1-5"),
    ])
    
    # Só salva se passou em todos os checks
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"taxi_case.silver.{taxi_type}")
    
    print(f"  {taxi_type}: {df.count()} registros salvos (validados)\n")

# Verificação
print("=== Resumo Silver v4 ===")
for taxi_type in ["yellow", "green"]:
    count = spark.sql(f"SELECT COUNT(*) as total FROM taxi_case.silver.{taxi_type}").collect()[0]["total"]
    print(f"silver.{taxi_type}: {count:,} registros")
