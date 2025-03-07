# Azure-EDI
ADF, Spark and SQL for 850 EDI end to end 

### **Project Highlights:**
- **Azure Data Factory Pipeline**: Moves raw EDI 850 data from Blob Storage to Azure Data Lake.
- **Databricks PySpark Script**: Transforms EDI 850 (X12 format) into structured JSON format
- **SQL Queries**: Loads and processes transformed JSON data into Azure Synapse Analytics.

  ## 1. Azure Data Factory Pipeline (pipeline.json)
```json
{
  "name": "Ingest_EDI_850",
  "activities": [
    {
      "name": "Copy_EDI_to_ADLS",
      "type": "Copy",
      "inputs": [{ "referenceName": "BlobStorage_EDI" }],
      "outputs": [{ "referenceName": "ADLS_EDI" }],
      "typeProperties": {
        "source": { "type": "DelimitedTextSource" },
        "sink": { "type": "DelimitedTextSink" }
      }
    }
  ]
}

## 2. Databricks Notebook: Transform EDI 850 to JSON (transform_edi.py)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EDI850_Processing").getOrCreate()

# Load EDI 850 file from ADLS

edi_df = spark.read.text("abfss://edi-container@storageaccount.dfs.core.windows.net/edi850.txt")

# Transform EDI to JSON (Basic Example)

def parse_edi850(row):
    segments = row.value.split("~")
    json_data = {seg[:2]: seg[3:] for seg in segments if len(seg) > 2}
    return json_data

edi_json_df = edi_df.rdd.map(parse_edi850).toDF()
edi_json_df.write.mode("overwrite").json("abfss://edi-container@storageaccount.dfs.core.windows.net/edi850_json/")

## 3. SQL Queries for Data Processing (data_processing.sql)
```sql
-- Create Table for Processed EDI 850 Data
CREATE TABLE EDI850_Processed (
    PO_Number VARCHAR(50),
    Order_Date DATE,
    Customer_ID VARCHAR(50),
    Total_Amount DECIMAL(10,2)
);

-- Load Data into SQL
INSERT INTO EDI850_Processed (PO_Number, Order_Date, Customer_ID, Total_Amount)
SELECT po_number, order_date, customer_id, total_amount
FROM OPENROWSET(
    BULK 'https://storageaccount.blob.core.windows.net/edi850_json/*.json',
    FORMAT='CSV'
) AS edi_json;


