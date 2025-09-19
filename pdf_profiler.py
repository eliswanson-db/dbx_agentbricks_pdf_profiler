# Databricks notebook source
# MAGIC %md 
# MAGIC # Install necessary libraries

# COMMAND ----------

!pip install pypdf==6.0.0

# COMMAND ----------

# MAGIC %md
# MAGIC # If getting module not found error for pypdf after installing, restart Python after library install:

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------
# Required
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("source_volume", "")
dbutils.widgets.text("dest_schema", "")
dbutils.widgets.text("dest_volume", "")
dbutils.widgets.text("dest_metadata_table", "")

# Optional
dbutils.widgets.text("trim_to_pages", "10")
dbutils.widgets.dropdown("mode", "profile", ["profile", "profile_and_trim"], "Mode")
dbutils.widgets.text("dest_subfolder", "")

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# DBTITLE 1,Imports
import os
from typing import Optional
import pandas as pd
from pyspark.sql.functions import pandas_udf, split, element_at, concat, lit, col, concat_ws, md5
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import DataFrame
from typing import Dict
from pypdf import PdfReader, PdfWriter
from dataclasses import dataclass, field
import logging

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup widgets and a configuration dictionary

# COMMAND ----------

def make_config():
    catalog = dbutils.widgets.get("catalog")
    source_schema = dbutils.widgets.get("source_schema")
    source_volume = dbutils.widgets.get("source_volume")
    dest_schema = dbutils.widgets.get("dest_schema")
    dest_volume = dbutils.widgets.get("dest_volume")
    dest_subfolder = dbutils.widgets.get("dest_subfolder")
    dest_metadata_table = dbutils.widgets.get("dest_metadata_table")
    trim_to_pages = int(dbutils.widgets.get("trim_to_pages"))
    mode = dbutils.widgets.get("mode")
    config_map = {
            "catalog": catalog,
            "source_schema": source_schema,
            "source_volume": source_volume,
            "dest_schema": dest_schema,
            "dest_volume": dest_volume,
            "dest_subfolder": dest_subfolder,
            "dest_metadata_table": dest_metadata_table,
            "trim_to_pages": trim_to_pages,
            "mode": mode,
        }
    return config_map


@dataclass
class ProfilerConfig:
    """Configuration dataclass for computed paths."""
    catalog: str
    source_schema: str
    source_volume: str
    dest_schema: str
    dest_volume: str
    dest_subfolder: str
    dest_metadata_table: str
    trim_to_pages: int
    mode: str
    checkpoint_folder: str = field(init=False)
    source_path: str = field(init=False)
    dest_table: str = field(init=False)
    full_dest_volume: str = field(init=False)
    dest_path: str = field(init=False)

    @classmethod
    def process_dbutils(cls, dbutil_map):
        obj = cls(
            catalog=dbutil_map["catalog"],
            source_schema=dbutil_map["source_schema"],
            source_volume=dbutil_map["source_volume"],
            dest_schema=dbutil_map["dest_schema"],
            dest_volume=dbutil_map["dest_volume"],
            dest_subfolder=dbutil_map["dest_subfolder"],
            dest_metadata_table=dbutil_map["dest_metadata_table"],
            trim_to_pages=int(dbutil_map["trim_to_pages"]),
            mode=dbutil_map["mode"]
        )
        obj.checkpoint_folder = f"/Volumes/{obj.catalog}/{obj.dest_schema}/_checkpoints/{obj.dest_metadata_table}"
        obj.source_path = f"/Volumes/{obj.catalog}/{obj.source_schema}/{obj.source_volume}"
        obj.dest_table = f"{obj.catalog}.{obj.dest_schema}.{obj.dest_metadata_table}"
        obj.full_dest_volume = f"{obj.catalog}.{obj.dest_schema}.{obj.dest_volume}"
        obj.dest_path = os.path.join(
            f"/Volumes/{obj.catalog}/{obj.dest_schema}/{obj.dest_volume}",
            obj.dest_subfolder
        )
        return obj
    

def validate_widgets(profiler_config: ProfilerConfig):
    """
    Validate that all required widgets are present and not empty. 
    If you are getting an error here, please make sure all widgets are populated in the notebook
    """
    
    required_widgets = [
        "catalog",
        "source_schema",
        "source_volume",
        "dest_schema",
        "dest_volume",
        "dest_metadata_table",
        "mode",
        "trim_to_pages"
    ]

    for widget in required_widgets:
        try:
            value = dbutils.widgets.get(widget)
            if not value:
                raise ValueError(f"Widget '{widget}' is empty.")
        except Exception as e:
            raise RuntimeError(f"Required widget '{widget}' is missing or invalid.") from e

    if not os.path.exists(profiler_config.dest_path):
        raise ValueError(f"Ensure the destination volume and folder, {profiler_config.dest_path}, exists for your trimmed PDFs. If it does not, you must create it.")

    if not os.path.exists(profiler_config.source_path):
        raise ValueError(f"Ensure the source volume, {profiler_config.source_path}, exists for your PDFs.")


# COMMAND ----------

# MAGIC %md 
# MAGIC # Define globals and validate widgets

# COMMAND ----------

PROFILE_SCHEMA = StructType([
    StructField("path", StringType(), True),
    StructField("size_bytes", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("trimmed", BooleanType(), True),
    StructField("trimmed_path", StringType(), True),
    StructField("pages_after_trim", IntegerType(), True),
    StructField("error", StringType(), True),
])
config_map = make_config()
PROFILER_CONFIG = ProfilerConfig.process_dbutils(config_map)
validate_widgets(PROFILER_CONFIG)

# COMMAND ----------

# MAGIC %md
# MAGIC # Define functions

# COMMAND ----------


class PDFProfiler:
    def __init__(self):
        """
        PDF Profiler to extract metadata and optionally trim PDFs.
        
        Args:
            subdir: subdirectory of a given volume where trimmed PDFs are written if
                    no explicit destination is provided.
        """
    
    def profile(self, pdf_path: str) -> Dict:
        """Profile only (no trimming)."""
        return self._profile_and_trim(pdf_path, trim_pdfs=False)

    def profile_and_trim(
        self,
        pdf_path: str,
        profiler_config: ProfilerConfig,
        trim_to_pages: int = 10
    ) -> Dict:
        """Profile and trim."""
        return self._profile_and_trim(
            pdf_path,
            trim_pdfs=True,
            trim_to_pages=trim_to_pages,
            destination_path=profiler_config.dest_path,
        )
    
    def _normalize_path(self, path: str) -> str:
        """Convert dbfs:/ to /dbfs for local access."""
        return path.replace("dbfs:", "")

    def _profile_and_trim(
        self,
        pdf_path: str,
        trim_pdfs: bool = False,
        trim_to_pages: int = 10,
        destination_path: str = None,
    ) -> Dict:
        """
        Internal worker: profile a PDF and optionally trim it.
        Returns dict with profile info and error if any.
        """
        result = {
            "path": pdf_path,
            "size_bytes": None,
            "total_pages": None,
            "trimmed": False,
            "trimmed_path": None,
            "pages_after_trim": None,
            "error": None,
        }

        file_name = os.path.basename(pdf_path)
        try:
            if not isinstance(pdf_path, str) or not pdf_path.lower().endswith(".pdf"):
                raise ValueError("Path is not a PDF file.")

            local_path = self._normalize_path(pdf_path)

            if not os.path.exists(local_path):
                raise FileNotFoundError(f"PDF not found: {local_path}")

            result["size_bytes"] = os.path.getsize(local_path)

            reader = PdfReader(local_path)
            total_pages = len(reader.pages)
            result["total_pages"] = total_pages

            if trim_pdfs:
                writer = PdfWriter()
                for i in range(min(trim_to_pages, total_pages)):
                    writer.add_page(reader.pages[i])

                if destination_path is None:
                    raise ValueError("Destination path unspecified")

                destination_file_path = os.path.join(destination_path, file_name)
                local_dest = self._normalize_path(destination_file_path)
                os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)

                with open(destination_file_path, "wb") as f:
                    writer.write(f)

                result["trimmed"] = True
                result["trimmed_path"] = destination_file_path
                result["pages_after_trim"] = trim_to_pages

        except Exception as e:
            result["error"] = str(e)

        return result

def make_profile_and_trim_udf(profiler_config, profile_schema):
    """Higher-order function returning pandas UDF to handle passing config."""
    @pandas_udf(profile_schema)
    def profile_and_trim_udf(pdf_paths: pd.Series) -> pd.DataFrame:
        profiler = PDFProfiler()
        return pd.DataFrame([
            profiler.profile_and_trim(path, profiler_config=profiler_config, trim_to_pages=profiler_config.trim_to_pages) 
            for path in pdf_paths
        ])

    return profile_and_trim_udf


def make_profile_udf(profile_schema):
    """Higher-order function returning pandas UDF to handle passing config."""
    @pandas_udf(profile_schema)
    def profile_udf(pdf_paths: pd.Series) -> pd.DataFrame:
        profiler = PDFProfiler()
        return pd.DataFrame([
            profiler.profile(path)
            for path in pdf_paths
        ])
    return profile_udf
    

def read_bronze_pdf_stream(source_path: str) -> DataFrame:
    """
    Read the bronze PDF stream.
    """
    return (spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "binaryFile")
         .load(source_path)
         .select("modificationTime", "path", "length")
         )


def process_files(profiler_config: ProfilerConfig, bronze_pdf_stream: DataFrame) -> DataFrame:
    """
    Process the files.
    """
    if profiler_config.mode == "profile":
        profile_udf = make_profile_udf(PROFILE_SCHEMA)
        return (bronze_pdf_stream
                .withColumn("profile", profile_udf(bronze_pdf_stream["path"]))
                .withColumn("id", md5(concat_ws("|", col("modificationTime"), col("path"))))
                .select("id", "modificationTime", "profile.*")
        )
    elif profiler_config.mode == "profile_and_trim":
        profile_and_trim_udf = make_profile_and_trim_udf(profiler_config, PROFILE_SCHEMA)
        return (bronze_pdf_stream
                .withColumn("profile", profile_and_trim_udf(bronze_pdf_stream["path"]))
                .withColumn("id", md5(concat_ws("|", col("modificationTime"), col("path"))))
                .select("id", "modificationTime", "profile.*")
        )
    else:
        raise ValueError(f"Invalid mode: {profiler_config.mode}. Expected 'profile' or 'profile_and_trim'.")


def write_stream(processed_stream: DataFrame, profiler_config: ProfilerConfig) -> DataFrame:
    return (
        processed_stream.writeStream
            .option("checkpointLocation", profiler_config.checkpoint_folder)
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(profiler_config.dest_table)
            .awaitTermination()
        )


def create_subfolder(folder_path: str) -> None:
    """
    Create a subfolder if it doesn't exist.
    """
    try:
        os.mkdir(folder_path)
    except FileExistsError:
        print("Folder already exists. Skipping make directory...")
    except Exception as e:
        print(f"{e}")


def main(profiler_config):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {profiler_config.full_dest_volume}")
    create_subfolder(profiler_config.dest_path)
    bronze_pdf_stream = read_bronze_pdf_stream(profiler_config.source_path)
    processed_stream = process_files(profiler_config, bronze_pdf_stream)
    write_stream(processed_stream, profiler_config)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run ingestion 

# COMMAND ----------

# NOTE: This is setup for streaming. As such, the checkpoints will prohibit you from running and re-running this. 
# If you would like to test this on multiple occasions in development, you'll have to delete the metadata table 
# as well as the checkpoint each time you want to re-run the notebook.
# NOTE: It is recommended to just the `profile_udf()` function first and check the metadata table to ensure you
# are getting the expected results before running the `profile_and_trim_udf()` function (commented out below)

if __name__ == "__main__":
    main(PROFILER_CONFIG)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check results for debug

# COMMAND ----------

spark.sql(f"SELECT * FROM {config_map.get('catalog')}.{config_map.get('dest_schema')}.{config_map.get('dest_metadata_table')} LIMIT 5").display()
