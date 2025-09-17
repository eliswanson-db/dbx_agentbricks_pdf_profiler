# Databricks notebook source
!pip install pypdf==6.0.0

# COMMAND ----------

# If getting module not found error for pypdf after installing, run the following:
#dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("source_volume", "")
dbutils.widgets.text("dest_schema", "")
dbutils.widgets.text("dest_volume", "")
dbutils.widgets.text("dest_metadata_table", "")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
source_volume = dbutils.widgets.get("source_volume")
dest_schema = dbutils.widgets.get("dest_schema")
dest_volume = dbutils.widgets.get("dest_volume")
dest_metadata_table = dbutils.widgets.get("dest_metadata_table")


checkpoint_folder = f"/Volumes/{catalog}/{dest_schema}/_checkpoints/{dest_metadata_table}"
source_path = f"/Volumes/{catalog}/{source_schema}/{source_volume}"
silver_table = f"{catalog}.{dest_schema}.{dest_metadata_table}"
dest_path = f"/Volumes/{catalog}/{dest_schema}/{dest_volume}"

# COMMAND ----------

# Validate that all required widgets are present and not empty
# If you are getting an error here, please make sure all widgets are populated in the notebook
required_widgets = [
    "catalog",
    "source_schema",
    "source_volume",
    "dest_schema",
    "dest_volume",
    "dest_metadata_table"
]

for widget in required_widgets:
    try:
        value = dbutils.widgets.get(widget)
        if not value:
            raise ValueError(f"Widget '{widget}' is empty.")
    except Exception as e:
        raise RuntimeError(f"Required widget '{widget}' is missing or invalid.") from e

if not os.path.exists(dest_path):
    raise ValueError(f"Ensure the destination volume, {dest_path}, exists for your trimmed PDFs. If it does not, you must create it.")

if not os.path.exists(source_path):
    raise ValueError(f"Ensure the source volume, {source_path}, exists for your PDFs.")

# COMMAND ----------

# DBTITLE 1,vfrkbccrlvhbujidvrkkduhednt
import os
from typing import Optional
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from typing import Dict
from pypdf import PdfReader, PdfWriter

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
        trim_to_pages: int = 10,
        destination_path: str = f"/Volumes/{catalog}/{dest_schema}/{dest_volume}/",
    ) -> Dict:
        """Profile and trim."""
        return self._profile_and_trim(
            pdf_path,
            trim_pdfs=True,
            trim_to_pages=trim_to_pages,
            destination_path=destination_path,
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


profiler = PDFProfiler()

profile_schema = StructType([
    StructField("path", StringType(), True),
    StructField("size_bytes", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("trimmed", BooleanType(), True),
    StructField("trimmed_path", StringType(), True),
    StructField("pages_after_trim", IntegerType(), True),
    StructField("error", StringType(), True),
])

@pandas_udf(profile_schema)
def profile_and_trim_udf(pdf_paths: pd.Series) -> pd.DataFrame:
    return pd.DataFrame([
        profiler.profile_and_trim(path) 
        for path in pdf_paths
    ])


@pandas_udf(profile_schema)
def profile_udf(pdf_paths: pd.Series) -> pd.DataFrame:
    return pd.DataFrame([
        profiler.profile(path) 
        for path in pdf_paths
    ])

# COMMAND ----------

# NOTE: This is setup for streaming. As such, the checkpoints will prohibit you from running and re-running this. 
# If you would like to test this on multiple occasions, you'll have to delete the metadata table as well as the checkpoint
# NOTE: It is recommended to just the `profile_udf()` function first and check the metadata table to ensure you
# are getting the expected results before running the `profile_and_trim_udf()` function (commented out below)
bronze_pdf_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "binaryFile")
         .load(source_path)
)

processed_stream = (
    bronze_pdf_stream
    .withColumn("profile", profile_udf(bronze_pdf_stream["path"]))
)

# processed_stream = (
#     bronze_pdf_stream
#     .withColumn("profile", profile_and_trim_udf(bronze_pdf_stream["path"]))
# )

query = (
    processed_stream.writeStream
        .option("checkpointLocation", checkpoint_folder)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(silver_table)
)
