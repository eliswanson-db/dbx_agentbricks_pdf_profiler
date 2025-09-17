# Databricks notebook source
!pip install pypdf==6.0.0

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

# DBTITLE 1,vfrkbccrlvhbujidvrkkduhednt
import os
from typing import Optional
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from typing import Dict
from pypdf import PdfReader, PdfWriter

class PDFProfiler:
    def __init__(self, subdir: str = "trimmed_pdfs"):
        """
        PDF Profiler to extract metadata and optionally trim PDFs.
        
        Args:
            subdir: subdirectory of a given volume where trimmed PDFs are written if
                    no explicit destination is provided.
        """
        self.subdir = subdir
    
    def profile(self, pdf_path: str) -> Dict:
        """Profile only (no trimming)."""
        return self._profile_and_trim(pdf_path, trim_pdfs=False)

    def profile_and_trim(
        self,
        pdf_path: str,
        trim_to_pages: int = 10,
        destination_path: Optional[str] = None,
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

    def _make_trimmed_path(self, pdf_path: str) -> str:
        """Return a sibling trimmed path under `self.subdir`."""
        base_dir = os.path.dirname(pdf_path)
        fname = os.path.basename(pdf_path)
        return os.path.join(base_dir, self.subdir, fname)

    def _profile_and_trim(
        self,
        pdf_path: str,
        trim_pdfs: bool = False,
        trim_to_pages: int = 10,
        destination_path: Optional[str] = None,
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
            "error": None,
        }

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
                    destination_path = self._make_trimmed_path(pdf_path)

                local_dest = self._normalize_path(destination_path)
                os.makedirs(os.path.dirname(local_dest), exist_ok=True)

                with open(local_dest, "wb") as f:
                    writer.write(f)

                result["trimmed"] = True
                result["trimmed_path"] = destination_path

        except Exception as e:
            result["error"] = str(e)

        return result


profiler = PDFProfiler()

@pandas_udf(profile_schema)
def profile_and_trim_udf(pdf_paths: pd.Series) -> pd.DataFrame:
    return pd.DataFrame([
        profiler.profile_and_trim(path, trim_to_pages=10) 
        for path in pdf_paths
    ])


@pandas_udf(profile_schema)
def profile_only_udf(pdf_paths: pd.Series) -> pd.DataFrame:
    return pd.DataFrame([
        profiler.profile(path) 
        for path in pdf_paths
    ])

# COMMAND ----------

silver_table = f"{catalog}.{dest_schema}.{dest_metadata_table}_4"
bronze_pdf_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "binaryFile")
         .load(f"/Volumes/{catalog}/{source_schema}/{source_volume}")
)

processed_stream = (
    bronze_pdf_stream
    .withColumn("profile", profile_and_trim_pdf(bronze_pdf_stream["path"]))
)

query = (
    processed_stream.writeStream
        .option("checkpointLocation", f"/Volumes/{catalog}/{dest_schema}/_checkpoints/{silver_table}_81")
        .outputMode("append")
        .trigger(availableNow=True)
        .outputMode("append")
        .toTable(silver_table)
)

# COMMAND ----------

spark.read.table(silver_table).display()
