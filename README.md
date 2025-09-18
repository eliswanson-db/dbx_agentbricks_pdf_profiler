# DBX AI PDF Profiler

A Databricks-based tool for profiling and trimming PDF files at scale using Spark structured streaming. This tool extracts metadata from PDFs and can optionally trim them to a specified number of pages, making them suitable for downstream AI processing. This can be particularly useful for AgentBricks, which will process all pages for files that it reads.

## Overview

The PDF Profiler provides:
- **PDF Metadata Extraction**: File size, page count, error handling
- **PDF Trimming**: Reduce PDFs to first N pages (default: 10)
- **Streaming Processing**: Process PDFs as they arrive in Unity Catalog volumes
- **Scalable Architecture**: Uses Spark for parallel processing across cluster

## Quick Start

### Prerequisites

- Databricks Runtime 13.3+ with Unity Catalog enabled
- Source volume containing PDF files
- Destination volume for trimmed PDFs (will be created if doesn't exist)
- Permissions to read from source and write to destination volumes

### Installation

This is a Databricks notebook - no separate installation required. Dependencies are installed via:

```python
%pip install pypdf==6.0.0
```

### Basic Usage

1. **Configure Widgets**: Set the required parameters in the Databricks notebook widgets:
   - `catalog`: Your Unity Catalog name
   - `source_schema`: Schema containing source volume
   - `source_volume`: Volume with PDF files
   - `dest_schema`: Schema for destination volume
   - `dest_volume`: Volume for trimmed PDFs
   - `dest_metadadta_table`: Table name for profiled metadata
   - `mode`: set profile or profile_and_trim
   - `trim_to_pages`: PDFs will be trimmed to this number of pages

2. **Run the Notebook**: Execute all cells to start the streaming job

## Architecture

### Core Components

- **`PDFProfiler` Class**: Core logic for PDF processing. Set this via mode widget
  - `profile()`: Extract metadata only
  - `profile_and_trim()`: Extract metadata and trim PDF
- **Pandas UDFs**: Spark-compatible functions for distributed processing
- **Structured Streaming**: Processes PDFs as they arrive using cloudFiles

## Configuration

### Path Structure

- **Source**: `/Volumes/{catalog}/{source_schema}/{source_volume}/`
- **Destination**: `/Volumes/{catalog}/{dest_schema}/{dest_volume}/`
- **Checkpoints**: `/Volumes/{catalog}/{dest_schema}/_checkpoints/`

## Streaming Gotchas & Best Practices

### Critical Streaming Considerations

#### 1. **availableNow=True Behavior**
```python
.trigger(availableNow=True)
```
- **What it does**: Processes all currently available files then stops
- **Use case**: Batch-like processing of existing files
- **Gotcha**: Stream will terminate after processing existing files - won't wait for new ones. Note 
that this will affect you trying to conduct multiple runs. If you would like to manually run this multiple times with no new data, one will have to remove the checkpoints and the table. If new data arrive, the checkpoints will allow autoloader to only process the new files.

#### 2. **Continuous Streaming Alternative**
For continuous processing of new files:
```python
.trigger(processingTime='10 seconds')  # Process every 10 seconds
# Remove availableNow=True
# DO NOT use this with autoloader
```

#### 3. **Checkpoint Management**
- **Location**: `/Volumes/{catalog}/{dest_schema}/_checkpoints/{table_name}`
- **Gotcha**: Changing schema or processing logic requires new checkpoint location
- **Best Practice**: Include version/iteration in checkpoint path for easy resets

### Memory Management

#### PDF Size Limitations
- **Large PDFs**: May cause OOM errors in pandas UDFs
- **Recommendation**: Monitor cluster memory usage
- **Mitigation**: Use smaller cluster nodes with more parallelism

#### Batch Size Tuning
```python
.option("cloudFiles.maxFilesPerTrigger", 100)  # Limit files per batch
```

## End-to-End Execution Guide

### Step 1: Environment Setup

Create schemas first if needed.

1. **Create or Ensure Volumes Exist**:
   ```sql
   -- Source volume (most likely this already exists and contains your PDFs)
   -- If not you can create and add files.
   CREATE VOLUME IF NOT EXISTS {catalog}.{source_schema}.{source_volume};
   
   -- Destination volume (will be created automatically)
   CREATE VOLUME IF NOT EXISTS {catalog}.{dest_schema}.{dest_volume};
   ```

2. **Upload PDFs** to source volume via Databricks UI or CLI

### Step 2: Configure and Run

1. **Set Widget Values** in the notebook:
   - Update all widget default values or set them interactively
   - Ensure all paths exist and are accessible

2. **Run Notebook Cells**:
   ```
   Install dependencies
   Set up widgets and variables  
   Define PDFProfiler class and UDFs
   Create and start streaming query
   Display results
   ```

### Step 3: Monitor Execution

1. **Streaming UI**: Check Spark Streaming tab for progress
2. **Query Status**: Monitor via `query.status` and `query.progress`
3. **Error Handling**: Check `error` column in results table

### Step 4: Verify Results

```python
# Check processed files
results = spark.read.table(f"{catalog}.{dest_schema}.{dest_metadata_table}")
results.display()

# Check for errors
errors = results.filter(col("profile.error").isNotNull())
errors.display()

# Verify trimmed files exist
trimmed_files = results.filter(col("profiel.trimmed") == True)
trimmed_files.display()
```

## Output Schema

The metadata table contains:

| Column | Type | Description |
|--------|------|-------------|
| `path` | string | Original PDF file path |
| `size_bytes` | int | Original file size in bytes |
| `total_pages` | int | Number of pages in original PDF |
| `trimmed` | boolean | Whether PDF was trimmed |
| `trimmed_path` | string | Path to trimmed PDF (if created) |
| `pages_after_trim` | int | Pages in trimmed PDF |
| `error` | string | Error message (null if successful) |

## Troubleshooting

1. **Permission Errors**
   - Ensure user or SPN has access to volumes and tables
   - Check that dest schema, checkpoint volume, and destination volume + subfolder are either created or the user has permissions to create.
   - Check volume permissions for read/write access

2. **File Not Found Errors**
   - Verify PDF files exist in source volume

3. **OOM Errors**
   - Reduce `maxFilesPerTrigger` to process smaller batches
   - Use cluster with more memory - driver or workers depending on where error is appearing

4. **Streaming Query Stuck**
   - Reset checkpoints (will re-ingest everything so use with caution)
