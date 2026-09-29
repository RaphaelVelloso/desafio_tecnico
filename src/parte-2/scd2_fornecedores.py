"""Parte 2 - Dimensao de fornecedores como SCD Tipo 2.

Fluxo:  Landing (snapshots CSV) -> Bronze (append-only, com _snapshot_date)
                                -> Silver dim_fornecedores (historico de versoes)

Modelo da dimensao (intervalo semiaberto [valid_from, valid_to)):
    fornecedor_id | atributos... | attribute_hash | valid_from | valid_to | is_current

Por que MERGE unico?
    Fechar a versao antiga e inserir a nova na MESMA transacao Delta evita o estado
    intermediario "fornecedor sem versao corrente" que uma falha entre dois comandos deixaria.
    A tecnica: o lote de origem e duplicado para os fornecedores alterados; uma copia entra
    com merge_key = chave (casa com a versao corrente e a FECHA) e a outra com merge_key = NULL
    (nunca casa, entao e INSERIDA como nova versao corrente).

Premissas (ver README): cada snapshot e um retrato completo; a vigencia de negocio vem da coluna
`data_atualizacao` (se ausente, usa a data do snapshot); remocoes de fornecedores nao sao tratadas.
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import (
    assert_required_columns,
    get_logger,
    log_event,
    run_with_schema_retry,
)

logger = get_logger("scd2_fornecedores")


@dataclass(frozen=True)
class ConfigFornecedores:
    landing_path: str = "/Volumes/prod_financeiro/landing/cadastro_fornecedores/"
    checkpoint_base: str = "/Volumes/prod_financeiro/_checkpoints/cadastro_fornecedores/"
    bronze_table: str = "prod_financeiro.bronze.cadastro_fornecedores"
    dim_table: str = "prod_financeiro.silver.dim_fornecedores"
    chave: str = "fornecedor_id"
    # Atributos versionados: qualquer mudanca em um deles gera uma nova versao.
    colunas_atributos: tuple = ("nome", "endereco", "status_contratual", "dados_bancarios")
    coluna_vigencia: str = "data_atualizacao"
    # Sal opcional (vindo de secret scope) para dificultar reversao do hash por forca bruta.
    salt: str = ""


def garantir_dim(spark: SparkSession, cfg: ConfigFornecedores) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.dim_table} (
            fornecedor_id     STRING    NOT NULL,
            nome              STRING,
            endereco          STRING,
            status_contratual STRING,
            dados_bancarios   STRING,
            attribute_hash    STRING    NOT NULL,
            valid_from        TIMESTAMP NOT NULL,
            valid_to          TIMESTAMP,
            is_current        BOOLEAN   NOT NULL,
            _snapshot_date    DATE,
            _ingestion_ts     TIMESTAMP,
            CONSTRAINT vigencia_valida CHECK (valid_to IS NULL OR valid_to > valid_from)
        )
        CLUSTER BY (fornecedor_id)
        TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
        """
    )


# --------------------------------------------------------------------------------------
# Landing -> Bronze (snapshots)
# --------------------------------------------------------------------------------------
def ingerir_bronze(spark: SparkSession, cfg: ConfigFornecedores) -> None:
    def _run() -> None:
        data_no_nome = F.regexp_extract(F.col("_metadata.file_path"), r"(\d{4}-\d{2}-\d{2})", 1)
        fluxo = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", f"{cfg.checkpoint_base}bronze_schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(cfg.landing_path)
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            # Data do snapshot: extraida do nome do arquivo (AAAA-MM-DD); senao, data da ingestao.
            .withColumn(
                "_snapshot_date",
                F.when(data_no_nome != "", F.to_date(data_no_nome)).otherwise(F.current_date()),
            )
        )
        (
            fluxo.writeStream.option("checkpointLocation", f"{cfg.checkpoint_base}bronze")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(cfg.bronze_table)
            .awaitTermination()
        )

    log_event(logger, "bronze_fornecedores_inicio", destino=cfg.bronze_table)
    run_with_schema_retry(_run, logger)


# --------------------------------------------------------------------------------------
# Preparacao: limpeza, vigencia, hash e deduplicacao por chave
# --------------------------------------------------------------------------------------
def preparar_snapshot(df: DataFrame, cfg: ConfigFornecedores) -> DataFrame:
    assert_required_columns(df, [cfg.chave, *cfg.colunas_atributos])

    def limpar(c: str):
        # Trim e string vazia -> NULL: evita "versoes" espurias por espaco em branco.
        return F.when(F.trim(F.col(c)) == "", F.lit(None).cast("string")).otherwise(F.trim(F.col(c)))

    if cfg.coluna_vigencia in df.columns:
        vigencia = F.expr(f"try_cast(`{cfg.coluna_vigencia}` AS TIMESTAMP)")
    else:
        vigencia = F.lit(None).cast("timestamp")
    # Fallback: sem data de atualizacao na fonte, a vigencia passa a ser a data do snapshot.
    vigencia = F.coalesce(vigencia, F.col("_snapshot_date").cast("timestamp"))

    atributos = [limpar(c).alias(c) for c in cfg.colunas_atributos]
    base = df.select(
        F.trim(F.col(cfg.chave)).alias("fornecedor_id"),
        *atributos,
        vigencia.alias("vigencia_ts"),
        F.col("_snapshot_date"),
        F.col("_ingestion_ts"),
    ).where(F.col("fornecedor_id").isNotNull() & (F.col("fornecedor_id") != ""))

    # Hash sobre JSON dos atributos: to_json preserva o NOME de cada campo, entao valores nulos em
    # posicoes diferentes NAO colidem (o que acontece com concat_ws, que ignora NULLs).
    hash_attr = F.sha2(
        F.concat_ws("|", F.to_json(F.struct(*[F.col(c) for c in cfg.colunas_atributos])), F.lit(cfg.salt)),
        256,
    ).alias("attribute_hash")
    base = base.select("*", hash_attr)

    # Uma linha por fornecedor no snapshot (a mais recente); evita "multiple source rows" no MERGE.
    janela = Window.partitionBy("fornecedor_id").orderBy(
        F.col("vigencia_ts").desc(), F.col("_ingestion_ts").desc()
    )
    return base.withColumn("_rn", F.row_number().over(janela)).where("_rn = 1").drop("_rn")


# --------------------------------------------------------------------------------------
# SCD Tipo 2: MERGE unico
# --------------------------------------------------------------------------------------
def aplicar_scd2(spark: SparkSession, cfg: ConfigFornecedores, snapshot_df: DataFrame) -> dict:
    inicio = time.time()
    fonte = preparar_snapshot(snapshot_df, cfg).persist()
    try:
        atual = spark.table(cfg.dim_table).where("is_current = true").alias("t")

        # Fornecedores existentes cujos atributos MUDARAM. A guarda `vigencia_ts > valid_from`
        # impede que um snapshot antigo/atrasado regrida a dimensao.
        alterados = (
            fonte.alias("s")
            .join(atual, F.col("s.fornecedor_id") == F.col("t.fornecedor_id"), "inner")
            .where(
                (F.col("s.attribute_hash") != F.col("t.attribute_hash"))
                & (F.col("s.vigencia_ts") > F.col("t.valid_from"))
            )
            .select("s.*")
        )

        # merge_key = chave  -> casa com a versao corrente (fecha) ou, se nao existir, insere (novo).
        # merge_key = NULL   -> nunca casa -> insere a NOVA versao dos alterados.
        staged = fonte.withColumn("merge_key", F.col("fornecedor_id")).unionByName(
            alterados.withColumn("merge_key", F.lit(None).cast("string"))
        )

        n_fonte, n_alterados = fonte.count(), alterados.count()

        (
            DeltaTable.forName(spark, cfg.dim_table)
            .alias("t")
            .merge(staged.alias("s"), "t.fornecedor_id = s.merge_key AND t.is_current = true")
            .whenMatchedUpdate(
                condition="s.attribute_hash <> t.attribute_hash AND s.vigencia_ts > t.valid_from",
                # valid_to da versao antiga = valid_from da nova: intervalos contiguos, sem buraco.
                set={"valid_to": "s.vigencia_ts", "is_current": "false"},
            )
            .whenNotMatchedInsert(
                values={
                    "fornecedor_id": "s.fornecedor_id",
                    **{c: f"s.{c}" for c in cfg.colunas_atributos},
                    "attribute_hash": "s.attribute_hash",
                    "valid_from": "s.vigencia_ts",
                    "valid_to": "NULL",
                    "is_current": "true",
                    "_snapshot_date": "s._snapshot_date",
                    "_ingestion_ts": "s._ingestion_ts",
                }
            )
            .execute()
        )
        metricas = {
            "fornecedores_no_snapshot": n_fonte,
            "novas_versoes_por_alteracao": n_alterados,
            "segundos": round(time.time() - inicio, 2),
        }
        log_event(logger, "scd2_aplicado", **metricas)
        return metricas
    finally:
        fonte.unpersist()


def _processar_lote(spark: SparkSession, cfg: ConfigFornecedores, batch_df: DataFrame, batch_id: int) -> None:
    """Aplica os snapshots do lote em ORDEM CRONOLOGICA (um MERGE por snapshot).

    Dois snapshots no mesmo lote precisam ser aplicados na ordem para que as versoes
    intermediarias fiquem preservadas no historico.
    """
    try:
        datas = sorted(r[0] for r in batch_df.select("_snapshot_date").distinct().collect())
        for d in datas:
            log_event(logger, "snapshot_inicio", batch_id=batch_id, snapshot_date=d)
            aplicar_scd2(spark, cfg, batch_df.where(F.col("_snapshot_date") == F.lit(d)))
    except Exception:
        logger.exception("Falha no micro-batch %s do SCD2", batch_id)
        raise


def processar_dimensao(spark: SparkSession, cfg: ConfigFornecedores) -> None:
    garantir_dim(spark, cfg)

    def _lote(batch_df: DataFrame, batch_id: int) -> None:
        _processar_lote(spark, cfg, batch_df, batch_id)

    (
        spark.readStream.table(cfg.bronze_table)
        .writeStream.foreachBatch(_lote)
        .option("checkpointLocation", f"{cfg.checkpoint_base}silver")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


def main() -> None:
    d = ConfigFornecedores()
    p = argparse.ArgumentParser(description="SCD Tipo 2 - dimensao de fornecedores")
    p.add_argument("--landing-path", default=d.landing_path)
    p.add_argument("--checkpoint-base", default=d.checkpoint_base)
    p.add_argument("--bronze-table", default=d.bronze_table)
    p.add_argument("--dim-table", default=d.dim_table)
    p.add_argument("--salt", default=d.salt)
    a = p.parse_args()
    cfg = ConfigFornecedores(
        landing_path=a.landing_path,
        checkpoint_base=a.checkpoint_base,
        bronze_table=a.bronze_table,
        dim_table=a.dim_table,
        salt=a.salt,
    )
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        ingerir_bronze(spark, cfg)
        processar_dimensao(spark, cfg)
    except Exception:
        logger.exception("Pipeline SCD2 de fornecedores falhou")
        raise


if __name__ == "__main__":
    main()
