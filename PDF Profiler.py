# Databricks notebook source
!pip install pypdf==6.0.0

# COMMAND ----------

# To duplicate initial files:
files = dbutils.fs.ls("/Volumes/dbxmetagen/default/all_pfizer_files")
dest_volume = "/Volumes/dbxmetagen/default/all_pfizer_files_duplicated_test"
for i in range(100):
    for f in files:
        # Construct new destination path with iteration number to avoid overwriting
        dest_path = f"{dest_volume}/{i}_{f.name}"
        dbutils.fs.cp(f.path, dest_path)

# COMMAND ----------

import os
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pypdf import PdfReader, PdfWriter

# COMMAND ----------

dbutils.widgets.text("catalog", "dbxmetagen")
dbutils.widgets.text("source_schema", "default")
dbutils.widgets.text("source_volume", "all_pfizer_files")
dbutils.widgets.text("dest_schema", "default")
dbutils.widgets.text("dest_volume", "trimmed_pdfs")
dbutils.widgets.text("dest_metadadta_table", "silver_pdf_parse_metadata")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
source_volume = dbutils.widgets.get("source_volume")
dest_schema = dbutils.widgets.get("dest_schema")
dest_volume = dbutils.widgets.get("dest_volume")

dest_metadata_table = dbutils.widgets.get("dest_metadadta_table")

# COMMAND ----------

import os
import pandas as pd
from pypdf import PdfReader, PdfWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import pandas_udf, col

# Define output schema
profile_schema = StructType([
    StructField("bronze_path", StringType(), True),
    StructField("silver_path", StringType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("initial_pdf_size", LongType(), True),
    StructField("pages_written", IntegerType(), True),
])

@pandas_udf(profile_schema)
def profile_and_trim_pdf(pdf_paths: pd.Series, pdf_sizes: pd.Series) -> pd.DataFrame:
    results = []
    for pdf_path, pdf_size in zip(pdf_paths, pdf_sizes):
        try:
            local_path = pdf_path.replace("dbfs:", "/dbfs")
            fname = os.path.basename(pdf_path)
            silver_path = f"/Volumes/dbxmetagen/default/trimmed_pdfs/udf_tests/{fname}"

            reader = PdfReader(local_path)
            total_pages = len(reader.pages)
            writer = PdfWriter()

            for i in range(min(10, total_pages)):
                writer.add_page(reader.pages[i])

            with open(silver_path, "wb") as f:
                writer.write(f)

            results.append({
                "bronze_path": pdf_path,
                "silver_path": silver_path.replace("/dbfs", "dbfs:"),
                "total_pages": total_pages,
                "initial_pdf_size": int(pdf_size),
                "pages_written": min(10, total_pages),
            })
        except Exception:
            results.append({
                "bronze_path": pdf_path,
                "silver_path": None,
                "total_pages": None,
                "initial_pdf_size": int(pdf_size),
                "pages_written": None,
            })
    return pd.DataFrame(results)


# COMMAND ----------

volume_path = f"/Volumes/{catalog}/{source_schema}/{source_volume}/"
schema_path = f"/tmp/{catalog}_{source_volume}_schema"
silver_table = f"{catalog}.{dest_schema}.{dest_metadata_table}"

bronze_pdf_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "binaryFile")
         .load(f"/Volumes/{catalog}/{source_schema}/{source_volume}")
)

processed_stream = (
    bronze_pdf_stream
    .withColumn("profile", profile_and_trim_pdf(col("path"), col("length")))
    .select(
        col("profile.bronze_path"),
        col("profile.silver_path"),
        col("profile.total_pages"),
        col("profile.initial_pdf_size"),
        col("profile.pages_written"),
    )
)

query = (
    processed_stream.writeStream
        .format("delta")
        .option("checkpointLocation", f"/Volumes/{catalog}/{dest_schema}/_checkpoints/{silver_table}_5")
        .outputMode("append")
        .toTable(silver_table)
)

# COMMAND ----------

files = dbutils.fs.ls(volume_path)
results = []
for f in files:
    if f.path.endswith(".csv"):
        print(f"found csv: {f.path}")
        continue
    try:
        local_path = f.path.replace("dbfs:", "")
        size_of_file = f.size
        fname = os.path.basename(local_path)
        silver_path = f"/Volumes/dbxmetagen/default/trimmed_pdfs/{fname}"

        reader = PdfReader(local_path)
        total_pages = len(reader.pages)
        writer = PdfWriter()

        for i in range(min(10, total_pages)):
            writer.add_page(reader.pages[i])

        with open(silver_path, "wb") as f:
            writer.write(f)

        results.append({
            "bronze_path": local_path,
            "silver_path": silver_path.replace("/dbfs", "dbfs:"),
            "total_pages": total_pages,
            "initial_pdf_size": int(size_of_file),
            "pages_written": min(10, total_pages),
        })
    except Exception as e:
        print(f"File errored: {f}")
        results.append({
            "bronze_path": local_path,
            "silver_path": None,
            "total_pages": None,
            "initial_pdf_size": int(size_of_file),
            "pages_written": None,
        })


results

# COMMAND ----------

files[0].size
