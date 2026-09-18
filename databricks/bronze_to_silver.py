# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver
# MAGIC
# MAGIC Lê os arquivos JSON Lines enviados para o Volume de Bronze, agrega em
# MAGIC janelas de 1 minuto (médias móveis, volatilidade) e grava como tabela
# MAGIC Delta na camada Silver.
# MAGIC
# MAGIC Leitura em batch simples (não streaming): cada run relê todo o Volume da
# MAGIC fonte escolhida e substitui as linhas dessa fonte na tabela Silver
# MAGIC (`replaceWhere`), então rodar de novo depois de subir mais arquivos
# MAGIC recalcula tudo pra aquela fonte sem duplicar nem exigir checkpoint.
# MAGIC
# MAGIC Binance e Alpha Vantage têm payloads diferentes (trade a trade vs.
# MAGIC snapshot de cotação por polling); escolha a fonte no widget abaixo. O
# MAGIC schema de saída é o mesmo para as duas, para caber num único
# MAGIC `fct_price_ticks` no dbt.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

dbutils.widgets.dropdown("source", "binance", ["binance", "alphavantage"])
dbutils.widgets.text("catalog", "market_pulse")
dbutils.widgets.text("bronze_schema", "lakehouse")
dbutils.widgets.text("silver_schema", "silver")

source = dbutils.widgets.get("source")
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

bronze_path = f"/Volumes/{catalog}/{bronze_schema}/bronze/{source}"
silver_table = f"{catalog}.{silver_schema}.price_aggregates"

WINDOW_DURATION = "1 minute"

# COMMAND ----------

_PAYLOAD_SCHEMAS = {
    "binance": StructType(
        [
            StructField("symbol", StringType()),
            StructField("price", StringType()),
            StructField("quantity", StringType()),
            StructField("trade_id", LongType()),
            StructField("trade_time_ms", LongType()),
            StructField("is_buyer_maker", StringType()),
        ]
    ),
    "alphavantage": StructType(
        [
            StructField("symbol", StringType()),
            StructField("price", StringType()),
            StructField("volume", StringType()),
            StructField("latest_trading_day", StringType()),
            StructField("previous_close", StringType()),
            StructField("change_percent", StringType()),
        ]
    ),
}


def _bronze_schema(src: str) -> StructType:
    return StructType(
        [
            StructField("ingested_at", StringType()),
            StructField("source", StringType()),
            StructField("payload", _PAYLOAD_SCHEMAS[src]),
        ]
    )


def _extract_ticks(raw, src: str):
    if src == "binance":
        event_time = F.to_timestamp(F.col("payload.trade_time_ms") / 1000)
        quantity = F.col("payload.quantity").cast(DoubleType())
    else:
        # Alpha Vantage não dá timestamp por trade: usa o instante do polling.
        event_time = F.to_timestamp(F.col("ingested_at"))
        quantity = F.col("payload.volume").cast(DoubleType())

    return raw.select(
        event_time.alias("event_time"),
        F.col("payload.symbol").alias("symbol"),
        F.col("payload.price").cast(DoubleType()).alias("price"),
        quantity.alias("quantity"),
    ).where(F.col("symbol").isNotNull())


def build_silver_batch(spark, path: str, src: str):
    raw = spark.read.schema(_bronze_schema(src)).json(path)

    ticks = _extract_ticks(raw, src)

    # binance: soma o volume negociado na janela. alphavantage: "volume" é o
    # acumulado do pregão (não incremental), então o maior valor visto na
    # janela aproxima o volume do dia até aquele ponto.
    volume_agg = F.sum("quantity") if src == "binance" else F.max("quantity")

    return (
        ticks.groupBy(F.window("event_time", WINDOW_DURATION), "symbol")
        .agg(
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("price_volatility"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            volume_agg.alias("total_volume"),
            F.count("*").alias("tick_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "avg_price",
            "price_volatility",
            "min_price",
            "max_price",
            "total_volume",
            "tick_count",
            F.lit(src).alias("source"),
        )
    )

# COMMAND ----------

silver_batch = build_silver_batch(spark, bronze_path, source)

# Evita conflito de schema com a tabela vazia criada por uma versão anterior
# deste notebook (streaming, sem a coluna source). Sem custo: só roda quando
# a tabela ainda não existe ou está vazia.
existing_count = (
    spark.sql(f"SELECT count(*) AS c FROM {silver_table}").collect()[0]["c"]
    if spark.catalog.tableExists(silver_table)
    else 0
)
if existing_count == 0:
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

(
    silver_batch.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"source = '{source}'")
    .option("mergeSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table).orderBy(F.col("window_start").desc()).limit(20))
