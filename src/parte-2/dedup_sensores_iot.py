"""Parte 2 - Deduplicacao de leituras de sensores_iot.json que chegam fora de ordem.

Estrategia em duas camadas (detalhada no README):
  1. Caminho quase em tempo real: Bronze -> Silver em streaming, com watermark e
     dropDuplicatesWithinWatermark. O estado do Spark fica LIMITADO pelo watermark.
  2. Reconciliacao periodica: MERGE insert-only a partir do Bronze recupera leituras que chegaram
     mais tarde que o watermark (o stream as descarta) e garante completude.

Por que nao so o watermark? Evento mais atrasado que o watermark e descartado pelo stream. Como o
Bronze guarda tudo, a reconciliacao devolve esse dado sem exigir um watermark enorme (que faria o
estado crescer sem controle).

Premissas (ver README): a chave de identidade e (sensor_id, event_ts) ou, se existir,
(sensor_id, event_id); o atraso maximo tipico (p99) e medido no Bronze - o watermark padrao de
2 horas e provisorio.

Requer Databricks Runtime 13.3 LTS+ (Spark 3.5: dropDuplicatesWithinWatermark).
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import assert_required_columns, col_or_null, get_logger, log_event

logger = get_logger("dedup_sensores_iot")


@dataclass(frozen=True)
class ConfigSensores:
    landing_path: str = "/Volumes/prod_operacoes/landing/sensores_iot/"
    checkpoint_base: str = "/Volumes/prod_operacoes/_checkpoints/sensores_iot/"
    bronze_table: str = "prod_operacoes.bronze.sensores_iot"
    silver_table: str = "prod_operacoes.silver.sensores_iot"
    watermark: str = "2 hours"          # provisorio: dimensionar pelo p99 do atraso observado
    usar_event_id: bool = False         # True se a fonte trouxer um identificador unico por leitura
    trigger: str = "1 minute"
    reconciliacao_dias: int = 3         # janela de ingestao relida pela reconciliacao


def chave_dedup(cfg: ConfigSensores) -> list[str]:
    return ["sensor_id", "event_id"] if cfg.usar_event_id else ["sensor_id", "event_ts"]


def garantir_silver(spark: SparkSession, cfg: ConfigSensores) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.silver_table} (
            sensor_id        STRING    NOT NULL,
            planta_id        STRING,
            event_id         STRING,
            event_ts         TIMESTAMP NOT NULL,
            event_date       DATE      NOT NULL,
            temperatura      DOUBLE,
            vibracao         DOUBLE,
            consumo_eletrico DOUBLE,
            ingestion_ts     TIMESTAMP,
            source_file      STRING
        )
        CLUSTER BY (event_date, planta_id, sensor_id)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.deletedFileRetentionDuration' = 'interval 30 days'
        )
        """
    )


# --------------------------------------------------------------------------------------
# Landing -> Bronze (streaming). Guarda TUDO: e a fonte da verdade para a reconciliacao.
# --------------------------------------------------------------------------------------
def iniciar_bronze(spark: SparkSession, cfg: ConfigSensores):
    fluxo = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{cfg.checkpoint_base}bronze_schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Hints so para os tipos criticos; nao usamos .schema() completo porque, com schema
        # explicito, o Auto Loader deixa de inferir/evoluir o schema.
        .option(
            "cloudFiles.schemaHints",
            "timestamp TIMESTAMP, leitura.temperatura DOUBLE, "
            "leitura.vibracao DOUBLE, leitura.consumo_eletrico DOUBLE",
        )
        .load(cfg.landing_path)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return (
        fluxo.writeStream.option("checkpointLocation", f"{cfg.checkpoint_base}bronze")
        .option("mergeSchema", "true")
        .trigger(processingTime=cfg.trigger)
        .toTable(cfg.bronze_table)
    )


# --------------------------------------------------------------------------------------
# Transformacao comum (achatamento do JSON aninhado)
# --------------------------------------------------------------------------------------
def achatar(df: DataFrame) -> DataFrame:
    assert_required_columns(df, ["sensor_id", "timestamp", "leitura"])
    return (
        df.select(
            F.trim(F.col("sensor_id")).alias("sensor_id"),
            col_or_null(df, "planta_id").alias("planta_id"),
            col_or_null(df, "event_id").alias("event_id"),
            F.col("timestamp").cast("timestamp").alias("event_ts"),
            F.to_date(F.col("timestamp").cast("timestamp")).alias("event_date"),
            F.col("leitura.temperatura").cast("double").alias("temperatura"),
            F.col("leitura.vibracao").cast("double").alias("vibracao"),
            F.col("leitura.consumo_eletrico").cast("double").alias("consumo_eletrico"),
            F.col("_ingestion_ts").alias("ingestion_ts"),
            F.col("_source_file").alias("source_file"),
        )
        # Sem chave nao ha como deduplicar; essas linhas permanecem no Bronze para auditoria.
        .where(F.col("sensor_id").isNotNull() & (F.col("sensor_id") != "") & F.col("event_ts").isNotNull())
    )


# --------------------------------------------------------------------------------------
# Caminho 1: Bronze -> Silver em streaming com watermark
# --------------------------------------------------------------------------------------
def iniciar_silver_stream(spark: SparkSession, cfg: ConfigSensores):
    garantir_silver(spark, cfg)
    deduplicado = (
        achatar(spark.readStream.table(cfg.bronze_table))
        # Watermark sobre o EVENT TIME (quando a leitura ocorreu), nao sobre o horario de chegada.
        .withWatermark("event_ts", cfg.watermark)
        # dropDuplicatesWithinWatermark: nao exige o event time na lista de colunas e, como o
        # estado expira com o watermark, ele NAO cresce indefinidamente. Um dropDuplicates comum
        # sem o event time na chave manteria o estado para sempre.
        .dropDuplicatesWithinWatermark(chave_dedup(cfg))
    )
    return (
        deduplicado.writeStream.outputMode("append")
        .option("checkpointLocation", f"{cfg.checkpoint_base}silver")
        .trigger(processingTime=cfg.trigger)
        .toTable(cfg.silver_table)
    )


# --------------------------------------------------------------------------------------
# Caminho 2: reconciliacao (batch) a partir do Bronze
# --------------------------------------------------------------------------------------
def reconciliar_silver(spark: SparkSession, cfg: ConfigSensores, tentativas: int = 3) -> None:
    """MERGE insert-only: insere o que falta na Silver e NUNCA altera o que ja existe.

    Cobre leituras que chegaram depois do watermark. Nao usa estado em memoria, entao aceita
    qualquer atraso, ao custo de um MERGE (por isso roda periodicamente e nao a cada micro-batch).
    """
    garantir_silver(spark, cfg)
    inicio = time.time()
    chave = chave_dedup(cfg)

    limite = F.date_sub(F.current_date(), cfg.reconciliacao_dias)
    bronze = spark.table(cfg.bronze_table).where(F.col("_ingestion_date") >= limite)

    # Uma leitura por chave; se houver duplicatas, mantem a PRIMEIRA ingerida.
    janela = Window.partitionBy(*chave).orderBy(F.col("ingestion_ts").asc())
    fonte = (
        achatar(bronze)
        .withColumn("_rn", F.row_number().over(janela))
        .where("_rn = 1")
        .drop("_rn")
        .persist()
    )
    try:
        limites = fonte.agg(F.min("event_date").alias("mn"), F.max("event_date").alias("mx")).first()
        if limites["mn"] is None:
            log_event(logger, "reconciliacao_sem_dados")
            return

        # Predicado de poda: so olha o intervalo de event_date presente na origem. E seguro, pois uma
        # linha com a mesma chave tem obrigatoriamente o mesmo event_date.
        condicao = " AND ".join(f"t.{c} = s.{c}" for c in chave) + (
            f" AND t.event_date BETWEEN DATE'{limites['mn'].isoformat()}' "
            f"AND DATE'{limites['mx'].isoformat()}'"
        )

        for tentativa in range(1, tentativas + 1):
            try:
                (
                    DeltaTable.forName(spark, cfg.silver_table)
                    .alias("t")
                    .merge(fonte.alias("s"), condicao)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                break
            except Exception as exc:  # noqa: BLE001
                # O stream de Silver escreve na mesma tabela; conflito de concorrencia e esperado
                # de vez em quando e resolvido com nova tentativa.
                if "Concurrent" in str(exc) and tentativa < tentativas:
                    log_event(logger, "reconciliacao_conflito_retry", tentativa=tentativa)
                    time.sleep(5 * tentativa)
                    continue
                raise
        log_event(
            logger,
            "reconciliacao_concluida",
            janela_dias=cfg.reconciliacao_dias,
            de=limites["mn"],
            ate=limites["mx"],
            segundos=round(time.time() - inicio, 2),
        )
    finally:
        fonte.unpersist()


def main() -> None:
    d = ConfigSensores()
    p = argparse.ArgumentParser(description="Deduplicacao de sensores IoT")
    p.add_argument("--modo", choices=["stream", "reconciliar"], default="stream")
    p.add_argument("--landing-path", default=d.landing_path)
    p.add_argument("--checkpoint-base", default=d.checkpoint_base)
    p.add_argument("--bronze-table", default=d.bronze_table)
    p.add_argument("--silver-table", default=d.silver_table)
    p.add_argument("--watermark", default=d.watermark)
    p.add_argument("--usar-event-id", action="store_true")
    a = p.parse_args()
    cfg = ConfigSensores(
        landing_path=a.landing_path,
        checkpoint_base=a.checkpoint_base,
        bronze_table=a.bronze_table,
        silver_table=a.silver_table,
        watermark=a.watermark,
        usar_event_id=a.usar_event_id,
    )
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        if a.modo == "stream":
            iniciar_bronze(spark, cfg)
            iniciar_silver_stream(spark, cfg)
            # Se qualquer query falhar (inclusive por evolucao de schema), o Job termina com erro e
            # a politica de retry do Workflow reinicia; os checkpoints retomam do ponto exato.
            spark.streams.awaitAnyTermination()
        else:
            reconciliar_silver(spark, cfg)
    except Exception:
        logger.exception("Pipeline de sensores falhou (modo=%s)", a.modo)
        raise


if __name__ == "__main__":
    main()
