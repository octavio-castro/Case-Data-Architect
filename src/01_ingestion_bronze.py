#Aqui tivemos um problema que possivelmente as tabelas tinham colunas, diferentes, o que impossibilitou uma leitura de uma unica vez dentro de cada tipo de taxi, com isso foi preciso criar uma estrutura que lesse mes a mes

base_path = "/Volumes/taxi_case/landing"
taxi_types = ["yellow", "green", "fhv", "fhvhv"]

for taxi_type in taxi_types:
    print(f"Ingerindo {taxi_type}...")
    
# Ler cada mês separadamente    
    dfs = []
    for mes in range(1, 6):
        fname = f"{taxi_type}_tripdata_2023-{mes:02d}.parquet"
        path = f"{base_path}/{taxi_type}/{fname}"
        print(f"  Lendo {fname}...")
        
        try:
            df = spark.read.parquet(path)
            df = df.withColumn("taxi_type", lit(taxi_type))
            dfs.append(df)
        except Exception as e:
            print(f"  Erro em {fname}: {e}")
    
    # Une todos os meses tolerando colunas diferentes
    if dfs:
        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.unionByName(df, allowMissingColumns=True)
        
        # Salva como tabela Delta (Bronze)
        df_final.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"taxi_case.bronze.{taxi_type}")
        
        print(f"  {taxi_type}: {df_final.count()} registros ingeridos")

# Verificação
for taxi_type in taxi_types:
    count = spark.sql(f"SELECT COUNT(*) as total FROM taxi_case.bronze.{taxi_type}").collect()[0]["total"]
    print(f"bronze.{taxi_type}: {count:,} registros")

# Para cada tipo de táxi:
df = spark.read.parquet(f"{base_path}/{taxi_type}/")
df = df.withColumn("taxi_type", lit(taxi_type))
df.write.format("delta").mode("overwrite").saveAsTable(f"taxi_case.bronze.{taxi_type}")
