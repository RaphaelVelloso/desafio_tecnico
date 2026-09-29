"""Utilitarios compartilhados pelos pipelines da Parte 2.

Mantidos em um modulo unico para evitar duplicacao de logica entre os datasets
(logging, contrato de schema e tratamento da evolucao de schema do Auto Loader).
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def get_logger(name: str) -> logging.Logger:
    """Logger padrao. No Databricks, a saida vai para os logs do driver/Job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, **fields) -> None:
    """Log estruturado em JSON: facilita filtrar e alertar em ferramentas de observabilidade."""
    logger.info(json.dumps({"event": event, **fields}, default=str))


def col_or_null(df: DataFrame, name: str, dtype: str = "string") -> Column:
    """Retorna a coluna se ela existir; senao, NULL tipado.

    Necessario porque o schema do Bronze evolui (ex.: `id_planta` so aparece a partir
    de certa data), e referenciar uma coluna inexistente lancaria AnalysisException.
    """
    return F.col(name) if name in df.columns else F.lit(None).cast(dtype)


def assert_required_columns(
    df: DataFrame,
    required: Iterable[str],
    any_of: Iterable[Iterable[str]] = (),
) -> None:
    """Contrato minimo de schema: falha rapido se o Bronze perdeu colunas essenciais.

    `any_of` aceita grupos de nomes alternativos (ex.: planta_id OU id_planta).
    """
    missing = [c for c in required if c not in df.columns]
    for group in any_of:
        group = list(group)
        if not any(c in df.columns for c in group):
            missing.append(" | ".join(group))
    if missing:
        raise ValueError(f"Contrato de schema violado; colunas ausentes: {missing}")


def run_with_schema_retry(
    run: Callable[[], None],
    logger: logging.Logger,
    max_retries: int = 2,
) -> None:
    """Reexecuta o stream quando o Auto Loader interrompe para evoluir o schema.

    Com `cloudFiles.schemaEvolutionMode = addNewColumns`, ao detectar uma coluna nova o
    stream falha uma vez (UnknownFieldException) e, ao reiniciar, usa o schema evoluido.
    Sem este retry, a mudanca `planta_id` -> `id_planta` exigiria intervencao manual.
    """
    for attempt in range(max_retries + 1):
        try:
            run()
            return
        except Exception as exc:  # noqa: BLE001 - reavaliamos a causa abaixo
            if "UnknownFieldException" in str(exc) and attempt < max_retries:
                log_event(logger, "schema_evolution_detected", attempt=attempt + 1)
                continue
            raise
