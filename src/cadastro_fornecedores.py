import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, current_date, lit
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SCD2Fornecedores")

def processar_fornecedores_scd2(spark: SparkSession, path_raw: str, path_silver: str) -> None:
    """
    Processa o cadastro de fornecedores aplicando SCD Tipo 2 de forma
    idempotente e preservando o histórico de versões via Delta Lake.
    """
    logger.info("Iniciando leitura dos novos dados de fornecedores...")
    
    # 1. Leitura dos novos dados brutos
    df_raw = spark.read.format("csv").option("header", "true").load(path_raw)
    
    # 2. Geração de HASH SHA-256 para detecção eficiente de mudanças nos atributos descritivos
    df_updates = df_raw.withColumn(
        "hash_atributos", 
        sha2(concat_ws("||", 
                       col("endereco"), 
                       col("status_contratual"), 
                       col("dados_bancarios")), 256)
    )

    # 3. Se a tabela Silver não existir (Primeira Carga), cria a estrutura inicial
    if not DeltaTable.isDeltaTable(spark, path_silver):
        logger.info("Tabela Silver não encontrada. Executando Carga Inicial (Bootstrap)...")
        df_initial = df_updates \
            .withColumn("start_date", current_date()) \
            .withColumn("end_date", lit(None).cast("date")) \
            .withColumn("is_current", lit(True))
            
        df_initial.write.format("delta").mode("overwrite").save(path_silver)
        logger.info("Carga inicial concluída com sucesso.")
        return

    # 4. Se a tabela Silver já existe, executa o fluxo incremental de SCD Tipo 2
    logger.info("Tabela Silver detectada. Executando processamento de SCD Tipo 2...")
    delta_target = DeltaTable.forPath(spark, path_silver)

    # Identificar registros ativos existentes que mudaram no novo lote
    staged_updates = df_updates.alias("updates").join(
        delta_target.toDF().alias("target"),
        "fornecedor_id"
    ).filter("target.is_current = true AND target.hash_atributos != updates.hash_atributos")

    # Registros que serão inseridos como NOVAS versões (chave de merge forçada para NULL)
    new_versions = staged_updates.select("updates.*").withColumn("merge_key", lit(None))
    
    # Registros recebidos normalmente (chave de merge mantida)
    main_rows = df_updates.withColumn("merge_key", col("fornecedor_id"))
    
    # União dos registros para o lote do MERGE
    records_to_merge = main_rows.union(new_versions)

    # 5. Execução do MERGE atômico no Delta Lake
    (delta_target.alias("target")
     .merge(
         records_to_merge.alias("source"),
         "target.fornecedor_id = source.merge_key AND target.is_current = true"
     )
     # QUANDO ENCONTRAR E HOUVER MUDANÇA: Inativa a versão antiga
     .whenMatchedAnd(
         "target.hash_atributos != source.hash_atributos"
     ).updateExpr({
         "end_date": "current_date()",
         "is_current": "false"
     })
     # QUANDO NÃO ENCONTRAR (Novos fornecedores ou novas versões com merge_key = NULL): Insere a nova versão
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

    logger.info("Processamento SCD Tipo 2 finalizado com sucesso.")