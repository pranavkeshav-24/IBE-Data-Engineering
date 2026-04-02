import json
import sys
from typing import Dict

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def optional_arg(name: str, default: str = "") -> str:
    token = f"--{name}"
    if token in sys.argv:
        idx = sys.argv.index(token)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


required_args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET",
        "PROCESSED_BUCKET",
        "QUARANTINE_BUCKET",
        "CONFIG_BUCKET",
        "CONFIG_KEY",
        "CLIENT_ID",
        "BUCKET",
        "KEY",
        "FILE_HASH",
        "RUN_ID",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
# Avoid hard failures on malformed timestamp strings; invalid parses become null.
spark.conf.set("spark.sql.ansi.enabled", "false")
job = Job(glue_context)
job.init(required_args["JOB_NAME"], required_args)

s3 = boto3.client("s3")

RAW_BUCKET = required_args["RAW_BUCKET"]
PROCESSED_BUCKET = required_args["PROCESSED_BUCKET"]
QUARANTINE_BUCKET = required_args["QUARANTINE_BUCKET"]
CONFIG_BUCKET = required_args["CONFIG_BUCKET"]
CONFIG_KEY = required_args["CONFIG_KEY"]
CLIENT_ID = required_args["CLIENT_ID"].lower()
INPUT_BUCKET = required_args["BUCKET"]
INPUT_KEY = required_args["KEY"]
FILE_HASH = required_args["FILE_HASH"]
RUN_ID = required_args["RUN_ID"]

INPUT_FILES_JSON = optional_arg("INPUT_FILES_JSON", "{}")


def parse_client_config() -> Dict:
    obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_KEY)
    cfg = json.loads(obj["Body"].read().decode("utf-8"))

    # Backward compatibility: if an index file is passed, resolve to per-client config.
    if "clients" in cfg:
        client_idx = cfg.get("clients", {}).get(CLIENT_ID, {})
        client_key = client_idx.get("config_key")
        if not client_key:
            raise ValueError(f"Missing config_key for client_id={CLIENT_ID} in {CONFIG_KEY}")
        cfg_obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=client_key)
        cfg = json.loads(cfg_obj["Body"].read().decode("utf-8"))

    return cfg


def discover_input_files(client_config: Dict) -> Dict[str, str]:
    configured = json.loads(INPUT_FILES_JSON or "{}")
    if configured:
        return configured

    folder = INPUT_KEY.rsplit("/", 1)[0] + "/" if "/" in INPUT_KEY else ""
    required_patterns = [x.lower() for x in client_config.get("required_patterns", [])]
    optional_patterns = [x.lower() for x in client_config.get("optional_patterns", [])]
    listed = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=folder)
    objects = listed.get("Contents", [])

    out = {}
    for pattern in required_patterns + optional_patterns:
        matches = [obj for obj in objects if pattern in obj["Key"].lower()]
        if matches:
            latest = sorted(matches, key=lambda x: x["LastModified"], reverse=True)[0]
            out[pattern] = f"s3://{INPUT_BUCKET}/{latest['Key']}"
    if not out:
        out["source"] = f"s3://{INPUT_BUCKET}/{INPUT_KEY}"
    return out


def parse_date_expr(col_name: str):
    c = F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    c = F.when(c == "", None).otherwise(c)
    return F.to_date(
        F.coalesce(
            F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(c, "yyyy-MM-dd"),
            F.to_timestamp(c, "MM/dd/yyyy HH:mm:ss"),
            F.to_timestamp(c, "MM/dd/yyyy"),
            F.to_timestamp(c, "dd-MM-yyyy HH:mm:ss"),
            F.to_timestamp(c, "dd-MM-yyyy"),
            F.to_timestamp(c, "dd/MM/yyyy HH:mm:ss"),
            F.to_timestamp(c, "dd/MM/yyyy"),
            F.to_timestamp(c, "yyyy/MM/dd"),
        )
    )


def clean_numeric_expr(col_name: str):
    c = F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    c = F.regexp_replace(c, r"[₹$€£\s]", "")
    c = F.when(c == "", None).otherwise(c)
    c = F.when(c.rlike(r"^-?\d+,\d+$"), F.regexp_replace(c, ",", ".")).otherwise(c)
    c = F.when(c.rlike(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$"), F.regexp_replace(c, ",", "")).otherwise(c)
    c = F.regexp_replace(c, r"[^0-9\.\-]", "")
    return c.cast("double")


def to_common_schema(df: DataFrame, client_id: str, source_file: str) -> DataFrame:
    target_cols = {
        "date": T.DateType(),
        "client_id": T.StringType(),
        "discount_code": T.StringType(),
        "show_name": T.StringType(),
        "revenue": T.DoubleType(),
        "orders": T.DoubleType(),
        "new_orders": T.DoubleType(),
        "lapsed_orders": T.DoubleType(),
        "active_orders": T.DoubleType(),
        "impressions": T.DoubleType(),
        "revenue_per_order": T.DoubleType(),
        "revenue_per_impression": T.DoubleType(),
        "impressions_per_order": T.DoubleType(),
        "source_file": T.StringType(),
        "run_id": T.StringType(),
    }

    out = df
    for col_name, col_type in target_cols.items():
        if col_name not in out.columns:
            if col_name == "client_id":
                out = out.withColumn(col_name, F.lit(client_id))
            elif col_name == "source_file":
                out = out.withColumn(col_name, F.lit(source_file))
            elif col_name == "run_id":
                out = out.withColumn(col_name, F.lit(RUN_ID))
            else:
                out = out.withColumn(col_name, F.lit(None).cast(col_type))
        else:
            out = out.withColumn(col_name, F.col(col_name).cast(col_type))

    out = out.select(*target_cols.keys())
    return out


def empty_quarantine_df() -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("client_id", T.StringType(), True),
            T.StructField("source_file", T.StringType(), True),
            T.StructField("reason", T.StringType(), True),
            T.StructField("raw_payload", T.StringType(), True),
            T.StructField("run_id", T.StringType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


def with_quarantine_columns(df: DataFrame, client_id: str, source_file: str, reason: str) -> DataFrame:
    return df.select(F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("raw_payload")).withColumn(
        "client_id", F.lit(client_id)
    ).withColumn("source_file", F.lit(source_file)).withColumn("reason", F.lit(reason)).withColumn("run_id", F.lit(RUN_ID)).select(
        "client_id", "source_file", "reason", "raw_payload", "run_id"
    )


def process_alpha(input_files: Dict[str, str]):
    orders_uri = input_files.get("orders") or input_files.get("source")
    codes_uri = input_files.get("codes")
    if not orders_uri or not codes_uri:
        raise ValueError("Alpha processing requires both orders and codes files.")

    orders = spark.read.option("header", True).csv(orders_uri)
    codes = spark.read.option("header", True).csv(codes_uri)

    orders_clean = (
        orders.withColumn("date", parse_date_expr("order_date"))
        .withColumn("discount_code", F.lower(F.trim(F.col("code"))))
        .withColumn("revenue", clean_numeric_expr("subtotal_price"))
        .withColumn("orders", F.lit(1.0))
    )

    codes_clean = (
        codes.withColumn("discount_code", F.lower(F.trim(F.col("Promo Code"))))
        .filter(F.col("discount_code").isNotNull() & (F.col("discount_code") != ""))
        .select("discount_code")
        .dropDuplicates()
        .withColumn("code_match", F.lit(1))
    )

    joined = orders_clean.join(codes_clean, on="discount_code", how="left")

    loaded = (
        joined.filter(
            F.col("code_match").isNotNull() & F.col("date").isNotNull() & F.col("revenue").isNotNull() & (F.col("discount_code") != "")
        )
        .groupBy("date", "discount_code")
        .agg(F.sum("orders").alias("orders"), F.sum("revenue").alias("revenue"))
        .withColumn("show_name", F.lit(None).cast("string"))
        .withColumn("revenue_per_order", F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")))
    )

    quarantine = with_quarantine_columns(
        joined.filter(
            F.col("code_match").isNull() | F.col("date").isNull() | F.col("revenue").isNull() | (F.col("discount_code") == "")
        ),
        "alpha",
        orders_uri,
        "ALPHA_CODE_OR_DATA_INVALID",
    )

    return loaded, quarantine


def process_beta(input_files: Dict[str, str]):
    sales_uri = input_files.get("sales") or input_files.get("source")
    shows_uri = input_files.get("shows_and_codes")
    salesforce_uri = input_files.get("salesforce")
    if not sales_uri or not shows_uri:
        raise ValueError("Beta processing requires both sales and shows_and_codes files.")

    sales = spark.read.option("header", True).csv(sales_uri)
    shows = spark.read.option("header", True).csv(shows_uri)

    sales_clean = (
        sales.withColumn("date", parse_date_expr("date"))
        .withColumn("show_name", F.trim(F.col("Show")))
        .withColumn("show_key", F.lower(F.trim(F.col("Show"))))
        .withColumn("discount_code_input", F.lower(F.trim(F.col("Promo Code"))))
        .withColumn("revenue", clean_numeric_expr("Sales"))
        .withColumn("orders", F.coalesce(clean_numeric_expr("Orders"), F.lit(0.0)))
        .withColumn("new_sales", F.coalesce(clean_numeric_expr("New"), F.lit(0.0)))
        .withColumn("lapsed_sales", F.coalesce(clean_numeric_expr("Lapsed"), F.lit(0.0)))
        .withColumn("active_sales", F.coalesce(clean_numeric_expr("Active"), F.lit(0.0)))
    )

    shows_clean = (
        shows.withColumn("show_key", F.lower(F.trim(F.col("OMAHA SHOWNAME"))))
        .withColumn("discount_code_lookup", F.lower(F.trim(F.col("CODE"))))
        .withColumn("lookup_match", F.lit(1))
        .select("show_key", "discount_code_lookup", "lookup_match")
    )

    joined = sales_clean.join(shows_clean, on="show_key", how="left")
    unmatched_lookup = with_quarantine_columns(
        joined.filter(F.col("lookup_match").isNull()), "beta", sales_uri, "BETA_SHOWS_AND_CODES_NO_MATCH"
    )

    matched = joined.filter(F.col("lookup_match").isNotNull()).withColumn(
        "discount_code", F.coalesce(F.col("discount_code_lookup"), F.col("discount_code_input"))
    )

    if salesforce_uri:
        salesforce_df = spark.read.option("header", True).csv(salesforce_uri)
        sf_columns = {c.lower(): c for c in salesforce_df.columns}
        sf_col = sf_columns.get("show") or sf_columns.get("show_name") or sf_columns.get("omaha showname")
        if sf_col:
            sf = salesforce_df.withColumn("sf_show_key", F.lower(F.trim(F.col(sf_col)))).withColumn("sf_match", F.lit(1))
            matched = matched.join(sf.select("sf_show_key", "sf_match"), matched.show_key == sf.sf_show_key, "left")
            unmatched_sf = with_quarantine_columns(
                matched.filter(F.col("sf_match").isNull()), "beta", sales_uri, "BETA_SALESFORCE_NO_MATCH"
            )
            matched = matched.filter(F.col("sf_match").isNotNull())
        else:
            unmatched_sf = empty_quarantine_df()
    else:
        unmatched_sf = empty_quarantine_df()

    matched = (
        matched.withColumn("new_positive", F.col("new_sales") > 0)
        .withColumn("lapsed_positive", F.col("lapsed_sales") > 0)
        .withColumn("active_positive", F.col("active_sales") > 0)
        .withColumn(
            "active_segment_count",
            F.col("new_positive").cast("int") + F.col("lapsed_positive").cast("int") + F.col("active_positive").cast("int"),
        )
        .withColumn(
            "segment_share",
            F.when(F.col("active_segment_count") > 0, F.col("orders") / F.col("active_segment_count")).otherwise(F.lit(0.0)),
        )
        .withColumn("new_orders", F.when(F.col("new_positive"), F.col("segment_share")).otherwise(F.lit(0.0)))
        .withColumn("lapsed_orders", F.when(F.col("lapsed_positive"), F.col("segment_share")).otherwise(F.lit(0.0)))
        .withColumn("active_orders", F.when(F.col("active_positive"), F.col("segment_share")).otherwise(F.lit(0.0)))
    )

    invalid_split = with_quarantine_columns(
        matched.filter(F.col("active_segment_count") == 0), "beta", sales_uri, "BETA_NO_ACTIVE_SEGMENT_FOR_ORDER_SPLIT"
    )

    loaded = (
        matched.filter(F.col("active_segment_count") > 0)
        .filter(F.col("date").isNotNull() & F.col("revenue").isNotNull() & F.col("discount_code").isNotNull())
        .groupBy("date", "discount_code")
        .agg(
            F.sum("orders").alias("orders"),
            F.sum("revenue").alias("revenue"),
            F.sum("new_orders").alias("new_orders"),
            F.sum("lapsed_orders").alias("lapsed_orders"),
            F.sum("active_orders").alias("active_orders"),
            F.max("show_name").alias("show_name"),
        )
        .withColumn("revenue_per_order", F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")))
    )

    quarantine = unmatched_lookup.unionByName(unmatched_sf, allowMissingColumns=True).unionByName(
        invalid_split, allowMissingColumns=True
    )
    return loaded, quarantine


def process_gamma(input_files: Dict[str, str]):
    sales_uri = input_files.get("sales") or input_files.get("source")
    salesforce_uri = input_files.get("salesforce")
    if not sales_uri:
        raise ValueError("Gamma processing requires a sales input file.")

    sales = spark.read.option("header", True).csv(sales_uri)
    sales_clean = (
        sales.withColumn("date", parse_date_expr("sale_date"))
        .withColumn("join_id", F.lower(F.trim(F.col("id"))))
        .withColumn("discount_code", F.lower(F.trim(F.col("code"))))
        .withColumn("revenue", clean_numeric_expr("value_eur"))
        .withColumn("orders", F.coalesce(clean_numeric_expr("units"), F.lit(0.0)))
        .withColumn("show_name", F.trim(F.col("item_id")))
    )

    if salesforce_uri:
        sf = spark.read.option("header", True).csv(salesforce_uri)
        sf_columns = {c.lower(): c for c in sf.columns}
        id_col = sf_columns.get("id") or sf_columns.get("showid")
        if id_col:
            sf_clean = sf.withColumn("join_id_sf", F.lower(F.trim(F.col(id_col)))).withColumn("sf_match", F.lit(1))
            joined = sales_clean.join(sf_clean.select("join_id_sf", "sf_match"), sales_clean.join_id == sf_clean.join_id_sf, "left")
            quarantine_unmatched = with_quarantine_columns(
                joined.filter(F.col("sf_match").isNull()), "gamma", sales_uri, "GAMMA_SALESFORCE_NO_MATCH"
            )
            valid_rows = joined.filter(F.col("sf_match").isNotNull())
        else:
            quarantine_unmatched = empty_quarantine_df()
            valid_rows = sales_clean
    else:
        quarantine_unmatched = empty_quarantine_df()
        valid_rows = sales_clean

    loaded = (
        valid_rows.filter(F.col("date").isNotNull() & F.col("revenue").isNotNull())
        .groupBy("date", "discount_code")
        .agg(F.sum("orders").alias("orders"), F.sum("revenue").alias("revenue"), F.max("show_name").alias("show_name"))
        .withColumn("revenue_per_order", F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")))
    )

    invalid_rows = with_quarantine_columns(
        valid_rows.filter(F.col("date").isNull() | F.col("revenue").isNull()), "gamma", sales_uri, "GAMMA_INVALID_DATE_OR_REVENUE"
    )

    quarantine = quarantine_unmatched.unionByName(invalid_rows, allowMissingColumns=True)
    return loaded, quarantine


def process_adscribe(input_files: Dict[str, str]):
    source_uri = input_files.get("adscribe") or input_files.get("source")
    df = spark.read.option("header", True).csv(source_uri)
    cols = {c.lower(): c for c in df.columns}

    date_candidates = [cols[x] for x in ["date", "report_date", "period_start", "created_at", "sale_date"] if x in cols]
    revenue_col = cols.get("revenue") or cols.get("sales") or cols.get("earnings")
    orders_col = cols.get("orders") or cols.get("units")
    impressions_col = cols.get("impressions")
    discount_col = cols.get("discount_code") or cols.get("code")
    show_col = cols.get("show_name") or cols.get("show")

    if not date_candidates:
        df = df.withColumn("date", F.lit(None).cast("date"))
    else:
        parsed_dates = [parse_date_expr(c) for c in date_candidates]
        df = df.withColumn("date", F.coalesce(*parsed_dates))

    df = df.withColumn("client_id", F.lit("adscribe"))
    df = df.withColumn("discount_code", F.lower(F.trim(F.col(discount_col))) if discount_col else F.lit(None).cast("string"))
    df = df.withColumn("show_name", F.trim(F.col(show_col)) if show_col else F.lit(None).cast("string"))
    df = df.withColumn("revenue", clean_numeric_expr(revenue_col) if revenue_col else F.lit(0.0))
    df = df.withColumn("orders", F.coalesce(clean_numeric_expr(orders_col), F.lit(0.0)) if orders_col else F.lit(0.0))
    df = df.withColumn(
        "impressions", F.coalesce(clean_numeric_expr(impressions_col), F.lit(0.0)) if impressions_col else F.lit(0.0)
    )

    loaded = (
        df.filter(F.col("date").isNotNull())
        .groupBy("date", "discount_code", "show_name", "client_id")
        .agg(F.sum("revenue").alias("revenue"), F.sum("orders").alias("orders"), F.sum("impressions").alias("impressions"))
        .withColumn("revenue_per_order", F.when(F.col("orders") > 0, F.round(F.col("revenue") / F.col("orders"), 2)))
        .withColumn(
            "revenue_per_impression",
            F.when(F.col("impressions") > 0, F.round(F.col("revenue") / F.col("impressions"), 6)),
        )
        .withColumn("impressions_per_order", F.when(F.col("orders") > 0, F.round(F.col("impressions") / F.col("orders"), 6)))
    )

    quarantine = with_quarantine_columns(df.filter(F.col("date").isNull()), "adscribe", source_uri, "ADSCRIBE_MISSING_CANONICAL_DATE")
    return loaded, quarantine


def write_outputs(loaded: DataFrame, quarantine: DataFrame, client_id: str, source_file: str):
    common = to_common_schema(loaded, client_id, source_file)

    load_path = f"s3://{PROCESSED_BUCKET}/silver/load/client_id={client_id}/run_id={RUN_ID}/"
    curated_path = f"s3://{PROCESSED_BUCKET}/silver/curated/client_id={client_id}/"
    quarantine_path = f"s3://{QUARANTINE_BUCKET}/quarantine/client_id={client_id}/run_id={RUN_ID}/"

    common.write.mode("overwrite").parquet(load_path)
    common.write.mode("append").partitionBy("date").parquet(curated_path)

    if not quarantine.rdd.isEmpty():
        quarantine.write.mode("overwrite").parquet(quarantine_path)


def validate_inputs(input_files: Dict[str, str], client_cfg: Dict):
    required_patterns = [x.lower() for x in client_cfg.get("required_patterns", [])]
    missing = [p for p in required_patterns if p not in input_files]
    if missing:
        raise ValueError(f"Missing required input files for {CLIENT_ID}: {missing}")


SPECIAL_TRANSFORMS = {
    "alpha": process_alpha,
    "beta": process_beta,
    "gamma": process_gamma,
    "adscribe": process_adscribe,
}


def run_pipeline(client_cfg: Dict, input_files: Dict[str, str]):
    state = {
        "loaded_df": None,
        "quarantine_df": empty_quarantine_df(),
    }
    steps = client_cfg.get("pipeline", [])
    if not steps:
        raise ValueError(f"No pipeline defined in client config for {CLIENT_ID}")

    for step in steps:
        step_name = (step.get("step") or "").lower()
        if step_name == "validate_inputs":
            validate_inputs(input_files, client_cfg)
        elif step_name == "special_transform":
            transform_name = (step.get("name") or CLIENT_ID).lower()
            transform_fn = SPECIAL_TRANSFORMS.get(transform_name)
            if not transform_fn:
                raise ValueError(f"Unsupported transform '{transform_name}'")
            loaded_df, quarantine_df = transform_fn(input_files)
            state["loaded_df"] = loaded_df
            state["quarantine_df"] = quarantine_df
        elif step_name == "write_outputs":
            if state["loaded_df"] is None:
                raise ValueError("write_outputs step reached before a transform step")
            primary_source = list(input_files.values())[0]
            write_outputs(state["loaded_df"], state["quarantine_df"], CLIENT_ID, primary_source)
        else:
            raise ValueError(f"Unsupported pipeline step '{step_name}'")


client_config = parse_client_config()
input_files = discover_input_files(client_config)
run_pipeline(client_config, input_files)
job.commit()
