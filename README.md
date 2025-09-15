# dbx_ai_pdf_profiler
Profile and trim PDFs for parsing

Pull from an external volume, count pages in pdf, create a profiling report table.

Have an option to trim the pages and put them in a second volume.

## PDF Processing Library Options

When selecting a PDF processing library for this Databricks project, consider the following options based on your specific needs for page counting, profiling, and trimming:

### Core PDF Processing Libraries

#### 1. **PyPDF / PyPDF2 / PyPDF4**
- **Best for**: Basic PDF manipulation, page counting, splitting/merging
- **Pros**:
  - Pure Python implementation (no external dependencies)
  - Simple API for basic tasks like page counting
  - Good for splitting and merging PDFs
  - Lightweight and fast for basic operations
  - Excellent for extracting metadata (page count, document info)
- **Cons**:
  - Limited text extraction capabilities for complex layouts
  - No OCR support
  - May struggle with password-protected or corrupted PDFs
- **Use Case**: Ideal for page counting and basic PDF trimming operations

#### 2. **PyMuPDF (fitz)**
- **Best for**: Fast processing, comprehensive PDF analysis, image extraction
- **Pros**:
  - Extremely fast performance (C++ backend)
  - Excellent for page counting and document analysis
  - Supports text, image, and metadata extraction
  - Can handle complex PDF structures
  - Good for rendering PDF pages as images
  - Supports password-protected PDFs
- **Cons**:
  - Larger dependency footprint
  - Steeper learning curve for advanced features
- **Use Case**: Best overall choice for comprehensive PDF profiling with performance requirements

#### 3. **pdfplumber**
- **Best for**: Structured data extraction, detailed page analysis
- **Pros**:
  - User-friendly API
  - Excellent for extracting tables and structured data
  - Good text extraction with positional information
  - Built-in visualization tools for debugging
  - Great for analyzing page layouts
- **Cons**:
  - Slower than PyMuPDF for large documents
  - May be overkill for simple page counting
- **Use Case**: Good for detailed PDF content analysis and profiling

#### 4. **pdfminer.six**
- **Best for**: Robust text extraction, complex PDF structures
- **Pros**:
  - Very robust text extraction
  - Handles complex PDF structures well
  - Good for extracting detailed document metadata
  - Pure Python implementation
- **Cons**:
  - Steeper learning curve
  - Slower performance on large documents
  - Complex API for simple tasks
- **Use Case**: When you need reliable text extraction for profiling content

### Specialized Libraries

#### 5. **pdfrw**
- **Best for**: PDF manipulation, merging, splitting
- **Pros**:
  - Fast PDF manipulation
  - Good for merging and splitting operations
  - Works well with other libraries
  - Lightweight
- **Cons**:
  - Limited text extraction
  - Sparse documentation
- **Use Case**: Good for PDF trimming and reorganization tasks

#### 6. **Camelot**
- **Best for**: Table extraction from PDFs
- **Pros**:
  - Excellent table extraction accuracy
  - Outputs to pandas DataFrames
  - Multiple parsing methods (stream, lattice)
- **Cons**:
  - Limited to table extraction only
  - Requires clear table structures
- **Use Case**: If your PDFs contain tables that need profiling

#### 7. **Tabula-py**
- **Best for**: Table extraction with Java backend
- **Pros**:
  - Robust table extraction
  - Batch processing capabilities
  - Converts to pandas DataFrames
- **Cons**:
  - Requires Java runtime
  - Limited to table extraction
- **Use Case**: Alternative to Camelot for table-heavy PDFs

### Document Generation Libraries

#### 8. **ReportLab**
- **Best for**: Generating new PDF reports
- **Pros**:
  - Excellent for creating profiling reports
  - High-quality PDF generation
  - Great for charts and graphics
- **Cons**:
  - Not for reading existing PDFs
  - Steep learning curve
- **Use Case**: For generating your PDF profiling reports

## License Compatibility Analysis

**COPYLEFT LICENSE WARNING**: 
- **PyMuPDF (fitz)** uses **AGPL v3** (copyleft) - Requires commercial license for proprietary use
- **Camelot** and **Tabula-py** licenses need verification

**SAFE OPTIONS (Permissive Licenses)**:
- **PyPDF/PyPDF2**: BSD License ✅
- **pdfplumber**: MIT License ✅  
- **pdfminer.six**: MIT License ✅
- **pdfrw**: MIT License ✅
- **ReportLab**: BSD License (open source version) ✅

### Revised Recommendation for This Project (License-Safe)

**Primary Choice**: **pdfplumber** - MIT licensed, excellent for PDF analysis and profiling
**Secondary Choice**: **PyPDF** - BSD licensed, perfect for basic page counting and splitting
**Report Generation**: **ReportLab** - BSD licensed open source version for generating reports
**Advanced Text**: **pdfminer.six** - MIT licensed for robust text extraction

**Avoid**: PyMuPDF (fitz) unless you can obtain a commercial license

### Installation Commands (License-Safe)

```bash
# License-safe primary recommendations
pip install pdfplumber
pip install PyPDF4
```


### Logic for implementation:

1. For this process, we want to both (a) gather metadata for profiling and (b) write trimmed PDFs to a silver source
2. The metadata extraction should be done with a pandas UDF; however, side-effects should happen on the driver using forEachBatch
3. We could use a hybrid approach, where persist the PDFs via forEachBatch and then extract metadata via a UDF; however, this would require opening the PDFs twice, which will likely be slow
4. We should assess the speed differences between the hybrid approach versus running everything on the driver
