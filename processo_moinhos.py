from pyspark.sql.functions import col, sha2, concat_ws, current_date, lit, expr
from delta.tables import DeltaTable

def processar_fornecedores_scd2(spark: SparkSession, path_raw: str, path_silver: str) -> None:
    """
    Processa a dimensão de fornecedores aplicando SCD Tipo 2 via Delta Lake.
    """
    # Schema das alterações recebidas (Staging)
    df_updates = spark.read.format("csv").option("header", "true").load(path_raw)
    
    # Gerar hash dos campos descritivos para detetar mudanças reais
    df_updates_hashed = df_updates.withColumn(
        "hash_atributos", 
        sha2(concat_ws("||", col("endereco"), col("status_contratual"), col("dados_bancarios")), 256)
    )

    if not DeltaTable.isDeltaTable(spark, path_silver):
        # Primeira Carga: Inicializa a tabela SCD Tipo 2
        df_initial = df_updates_hashed \
            .withColumn("start_date", current_date()) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True))
            
        df_initial.write.format("delta").save(path_silver)
        return

    delta_target = DeltaTable.forPath(spark, path_silver)

    # 1. Identificar registos existentes que mudaram (Hash diferente)
    staged_updates = df_updates_hashed.alias("updates").join(
        delta_target.toDF().alias("target"),
        "fornecedor_id"
    ).filter("target.is_current = true AND target.hash_atributos != updates.hash_atributos")

    # 2. Registos a inserir (novos fornecedores + novas versões de fornecedores alterados)
    new_rows = staged_updates.select("updates.*").withColumn("merge_key", lit(None))
    
    # Registos normais recebidos
    main_rows = df_updates_hashed.withColumn("merge_key", col("fornecedor_id"))
    
    # União para execução no MERGE
    records_to_merge = main_rows.union(new_rows)

    # 3. Delta MERGE INTO
    (delta_target.alias("target")
     .merge(
         records_to_merge.alias("source"),
         "target.fornecedor_id = source.merge_key AND target.is_current = true"
     )
     # Expira o registo antigo quando deteta alteração
     .whenMatchedAnd(
         "target.hash_atributos != source.hash_atributos"
     ).updateExpr({
         "end_date": "current_date()",
         "is_current": "false"
     })
     # Insere novas versões ou novos fornecedores
     .whenNotMatchedInsert({
         "fornecedor_id": "source.fornecedor_id",
         "nome": "source.nome",
         "endereco": "source.endereco",
         "status_contratual": "source.status_contratual",
         "dados_bancarios": "source.dados_bancarios",
         "hash_atributos": "source.hash_atributos",
         "start_date": "current_date()",
         "end_date": "NULL",
         "is_current": "true"
     })
     .execute())