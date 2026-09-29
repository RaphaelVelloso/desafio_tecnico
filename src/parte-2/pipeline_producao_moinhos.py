"""Parte 2 - Pipeline de producao dos moinhos.

Fluxo:  Landing (CSV)  ->  Bronze (Auto Loader, append-only)  ->  Silver (MERGE idempotente)
                                                              \\->  Quarentena (rejeitados)

Requisitos do enunciado e onde sao atendidos:
  * Leitura escalavel (sem collect)  -> tudo e DataFrame/streaming; nenhuma acao traz dados ao driver.
  * Enforcement de schema            -> contrato de colunas no Bronze + try_cast tipado +
                                        constraints NOT NULL/CHECK no Delta da Silver.
  * Tratamento de erros e logging    -> log estruturado por micro-batch; excecao propagada (o Job
                                        falha, aciona retry/alerta e o checkpoint impede perda).
  * Carga incremental e idempotente  -> Auto Loader (checkpoint de arquivos) + Delta streaming +
                                        MERGE por chave de negocio.

Premissas (ver README): a mudanca planta_id -> id_planta ocorre entre arquivos; valores de tempo com
offset/'Z' sao UTC, e sem offset sao horario local da planta; ponto decimal '.' em toneladas.

Requer Databricks Runtime 14.3 LTS+ (try_cast, Liquid Clustering, Delta streaming).
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import (
    assert_required_columns,
    col_or_null,
    get_logger,
    log_event,
    run_with_schema_retry,
)

logger = get_logger("pipeline_producao_moinhos")

# Valor com offset explicito ('Z', '+03:00', '-0300') e tratado como UTC/instante absoluto.
_REGEX_OFFSET = r"(Z|[+-]\d{2}:?\d{2})$"


@dataclass(frozen=True)
class Config:
    """Parametros do pipeline. Nada e hardcoded no codigo: tudo vem de argumentos do Job."""

    landing_path: str = "/Volumes/prod_operacoes/landing/producao_moinhos/"
    checkpoint_base: str = "/Volumes/prod_operacoes/_checkpoints/producao_moinhos/"
    bronze_table: str = "prod_operacoes.bronze.producao_moinhos"
    silver_table: str = "prod_operacoes.silver.producao"
    quarentena_table: str = "prod_operacoes.quarentena.producao_moinhos"
    fuso_padrao: str = "America/Sao_Paulo"
    # Mapa opcional planta_id -> fuso IANA (ex.: {"P01": "America/Sao_Paulo"}).
    fuso_por_planta: dict = field(default_factory=dict)
    max_files_per_trigger: int = 1000


# --------------------------------------------------------------------------------------
# DDL: tabelas de destino (idempotente). Os schemas/catalogos sao provisionados por IaC.
# --------------------------------------------------------------------------------------
def garantir_tabelas(spark: SparkSession, cfg: Config) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.silver_table} (
            planta_id            STRING        NOT NULL,
            moinho_id            STRING        NOT NULL,
            data_producao_utc    TIMESTAMP     NOT NULL,
            data_producao_date   DATE          NOT NULL,
            toneladas_produzidas DECIMAL(18,3) NOT NULL,
            _ingestion_ts        TIMESTAMP,
            _source_file         STRING,
            _processed_ts        TIMESTAMP,
            CONSTRAINT toneladas_nao_negativas CHECK (toneladas_produzidas >= 0)
        )
        CLUSTER BY (data_producao_date, planta_id)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.deletedFileRetentionDuration' = 'interval 30 days'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.quarentena_table} (
            payload        STRING,
            motivos        ARRAY<STRING>,
            arquivo_origem STRING,
            ingestion_ts   TIMESTAMP,
            quarentena_ts  TIMESTAMP,
            batch_id       BIGINT
        )
        """
    )


# --------------------------------------------------------------------------------------
# Landing -> Bronze
# --------------------------------------------------------------------------------------
def ingerir_bronze(spark: SparkSession, cfg: Config) -> None:
    """Ingestao incremental via Auto Loader (availableNow: processa o pendente e encerra).

    O Bronze guarda tudo como string (padrao do Auto Loader para CSV): a ingestao nunca
    falha por tipagem e o dado original fica preservado para reprocessamento.
    """

    def _run() -> None:
        fluxo = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", f"{cfg.checkpoint_base}bronze_schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.maxFilesPerTrigger", cfg.max_files_per_trigger)
            .load(cfg.landing_path)
            # `_metadata` substitui input_file_name(), que nao e suportada no Unity Catalog.
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
        consulta = (
            fluxo.writeStream.option("checkpointLocation", f"{cfg.checkpoint_base}bronze")
            .option("mergeSchema", "true")  # permite que `id_planta` entre no Bronze
            .trigger(availableNow=True)
            .toTable(cfg.bronze_table)
        )
        consulta.awaitTermination()

    log_event(logger, "bronze_inicio", landing=cfg.landing_path, destino=cfg.bronze_table)
    run_with_schema_retry(_run, logger)
    log_event(logger, "bronze_fim", destino=cfg.bronze_table)


# --------------------------------------------------------------------------------------
# Bronze -> Silver: normalizacao, classificacao (valido/rejeitado) e deduplicacao
# --------------------------------------------------------------------------------------
def _expr_fuso(cfg: Config, planta: Column) -> Column:
    if not cfg.fuso_por_planta:
        return F.lit(cfg.fuso_padrao)
    pares = [F.lit(x) for kv in cfg.fuso_por_planta.items() for x in kv]
    return F.coalesce(F.create_map(*pares)[planta], F.lit(cfg.fuso_padrao))


def normalizar_e_classificar(df: DataFrame, cfg: Config) -> DataFrame:
    """Harmoniza schema/tipos/fuso e marca os motivos de rejeicao (array vazio = valido)."""
    assert_required_columns(
        df,
        required=["moinho_id", "data_producao", "toneladas_produzidas"],
        any_of=[["planta_id", "id_planta"]],
    )
    colunas_brutas = [c for c in df.columns if not c.startswith("_")]

    # planta_id/id_planta: uma unica coluna canonica, independentemente da data do arquivo.
    planta = F.trim(F.coalesce(col_or_null(df, "planta_id"), col_or_null(df, "id_planta")))

    # try_cast devolve NULL (em vez de lancar erro) para valor invalido -> vai para quarentena.
    bruto_ts = F.expr("try_cast(trim(data_producao) AS TIMESTAMP)")
    tem_offset = F.trim(F.col("data_producao")).rlike(_REGEX_OFFSET)
    # Com offset: o instante ja e absoluto. Sem offset: interpreta como horario local da planta.
    # Depende de spark.sql.session.timeZone = UTC (definido em main) para o parse "literal".
    data_utc = F.when(tem_offset, bruto_ts).otherwise(
        F.to_utc_timestamp(bruto_ts, _expr_fuso(cfg, planta))
    )
    toneladas = F.expr("try_cast(trim(toneladas_produzidas) AS DECIMAL(18,3))")
    ton_bruto = F.trim(F.col("toneladas_produzidas"))

    base = df.select(
        planta.alias("planta_id"),
        F.trim(F.col("moinho_id")).alias("moinho_id"),
        data_utc.alias("data_producao_utc"),
        toneladas.alias("toneladas_produzidas"),
        ton_bruto.alias("_ton_bruto"),
        F.col("_ingestion_ts"),
        F.col("_source_file"),
        F.to_json(F.struct(*[F.col(c) for c in colunas_brutas])).alias("_payload"),
    )

    motivos = F.filter(
        F.array(
            F.when(F.col("planta_id").isNull() | (F.col("planta_id") == ""), F.lit("planta_ausente")),
            F.when(F.col("moinho_id").isNull() | (F.col("moinho_id") == ""), F.lit("moinho_ausente")),
            F.when(F.col("data_producao_utc").isNull(), F.lit("timestamp_invalido_ou_ausente")),
            # Nulo NAO e zero: e decisao de negocio, entao vai para quarentena com motivo.
            F.when(F.col("_ton_bruto").isNull() | (F.col("_ton_bruto") == ""), F.lit("toneladas_nulas")),
            F.when(
                F.col("_ton_bruto").isNotNull()
                & (F.col("_ton_bruto") != "")
                & F.col("toneladas_produzidas").isNull(),
                F.lit("toneladas_invalidas"),
            ),
            F.when(F.col("toneladas_produzidas") < 0, F.lit("toneladas_negativas")),
        ),
        lambda x: x.isNotNull(),
    )
    return base.withColumn("_reason", motivos)


def deduplicar(validos: DataFrame) -> DataFrame:
    """Uma linha por (planta, moinho, instante UTC); vence a ingestao mais recente.

    A deduplicacao ocorre DEPOIS de normalizar para UTC: a mesma leitura escrita em UTC num
    arquivo e em horario local em outro so vira duplicata quando ambas estao no mesmo fuso.
    """
    janela = Window.partitionBy("planta_id", "moinho_id", "data_producao_utc").orderBy(
        F.col("_ingestion_ts").desc(), F.col("_source_file").desc()
    )
    return validos.withColumn("_rn", F.row_number().over(janela)).where("_rn = 1").drop("_rn")


def _montar_silver(deduplicado: DataFrame) -> DataFrame:
    return deduplicado.select(
        "planta_id",
        "moinho_id",
        "data_producao_utc",
        F.to_date("data_producao_utc").alias("data_producao_date"),
        "toneladas_produzidas",
        "_ingestion_ts",
        "_source_file",
        F.current_timestamp().alias("_processed_ts"),
    )


def _gravar_quarentena(rejeitados: DataFrame, cfg: Config, batch_id: int) -> None:
    (
        rejeitados.select(
            F.col("_payload").alias("payload"),
            F.col("_reason").alias("motivos"),
            F.col("_source_file").alias("arquivo_origem"),
            F.col("_ingestion_ts").alias("ingestion_ts"),
            F.current_timestamp().alias("quarentena_ts"),
            F.lit(batch_id).cast("bigint").alias("batch_id"),
        )
        .write.format("delta")
        .mode("append")
        # Escrita idempotente do Delta: se o foreachBatch for reexecutado apos uma falha,
        # este batch nao e gravado duas vezes na quarentena.
        .option("txnAppId", "producao_moinhos_quarentena")
        .option("txnVersion", batch_id)
        .saveAsTable(cfg.quarentena_table)
    )


def _merge_silver(spark: SparkSession, deduplicado: DataFrame, cfg: Config) -> None:
    alvo = DeltaTable.forName(spark, cfg.silver_table)
    (
        alvo.alias("t")
        .merge(
            _montar_silver(deduplicado).alias("s"),
            "t.planta_id = s.planta_id AND t.moinho_id = s.moinho_id "
            "AND t.data_producao_utc = s.data_producao_utc",
        )
        # So atualiza se o dado que chegou e mais novo: um arquivo reprocessado ou atrasado
        # nao sobrescreve informacao mais recente e nao reescreve linhas sem necessidade.
        .whenMatchedUpdateAll(condition="s._ingestion_ts > t._ingestion_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _processar_lote(spark: SparkSession, cfg: Config, batch_df: DataFrame, batch_id: int) -> None:
    inicio = time.time()
    classificado = deduplicado = None
    try:
        # persist: o lote e usado em varias acoes (metricas, quarentena, merge); evita recomputar.
        classificado = normalizar_e_classificar(batch_df, cfg).persist()
        totais = classificado.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.size("_reason") > 0, 1).otherwise(0)).alias("rejeitados"),
        ).first()
        total, rejeitados = int(totais["total"]), int(totais["rejeitados"] or 0)
        if total == 0:
            log_event(logger, "lote_vazio", batch_id=batch_id)
            return

        if rejeitados:
            _gravar_quarentena(classificado.where(F.size("_reason") > 0), cfg, batch_id)

        deduplicado = deduplicar(classificado.where(F.size("_reason") == 0)).persist()
        n_merge = deduplicado.count()
        if n_merge:
            _merge_silver(spark, deduplicado, cfg)

        log_event(
            logger,
            "lote_processado",
            batch_id=batch_id,
            lidos=total,
            rejeitados=rejeitados,
            duplicados_removidos=total - rejeitados - n_merge,
            enviados_ao_merge=n_merge,
            segundos=round(time.time() - inicio, 2),
        )
    except Exception:
        # Propaga: o stream falha, o Job aciona retry/alerta e o checkpoint garante que o
        # lote sera reprocessado (o MERGE e idempotente, entao reprocessar e seguro).
        logger.exception("Falha no micro-batch %s", batch_id)
        raise
    finally:
        for df in (classificado, deduplicado):
            if df is not None:
                df.unpersist()


def processar_silver(spark: SparkSession, cfg: Config) -> None:
    """Le o Bronze como stream (so o que e novo) e aplica o lote via foreachBatch + MERGE."""
    garantir_tabelas(spark, cfg)

    def _lote(batch_df: DataFrame, batch_id: int) -> None:
        _processar_lote(spark, cfg, batch_df, batch_id)

    log_event(logger, "silver_inicio", origem=cfg.bronze_table, destino=cfg.silver_table)
    consulta = (
        spark.readStream.table(cfg.bronze_table)
        .writeStream.foreachBatch(_lote)
        .option("checkpointLocation", f"{cfg.checkpoint_base}silver")
        .trigger(availableNow=True)
        .start()
    )
    consulta.awaitTermination()
    log_event(logger, "silver_fim", destino=cfg.silver_table)


# --------------------------------------------------------------------------------------
# Entrada
# --------------------------------------------------------------------------------------
def _parse_args() -> Config:
    d = Config()
    p = argparse.ArgumentParser(description="Pipeline de producao dos moinhos (Landing->Bronze->Silver)")
    p.add_argument("--landing-path", default=d.landing_path)
    p.add_argument("--checkpoint-base", default=d.checkpoint_base)
    p.add_argument("--bronze-table", default=d.bronze_table)
    p.add_argument("--silver-table", default=d.silver_table)
    p.add_argument("--quarentena-table", default=d.quarentena_table)
    p.add_argument("--fuso-padrao", default=d.fuso_padrao)
    a = p.parse_args()
    return Config(
        landing_path=a.landing_path,
        checkpoint_base=a.checkpoint_base,
        bronze_table=a.bronze_table,
        silver_table=a.silver_table,
        quarentena_table=a.quarentena_table,
        fuso_padrao=a.fuso_padrao,
    )


def main() -> None:
    cfg = _parse_args()
    spark = SparkSession.builder.getOrCreate()
    # Fixa UTC na sessao: o parse de strings sem offset passa a ser "literal", e a conversao
    # de horario local -> UTC fica explicita em to_utc_timestamp (sem depender do cluster).
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        garantir_tabelas(spark, cfg)
        ingerir_bronze(spark, cfg)
        processar_silver(spark, cfg)
    except Exception:
        logger.exception("Pipeline de producao falhou")
        raise


if __name__ == "__main__":
    main()
