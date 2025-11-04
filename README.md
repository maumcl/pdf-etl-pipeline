# 🧾 PDF / Image Invoice ETL Pipeline

## 📖 Overview
This project automates the extraction and analysis of structured information from **PDF or image invoices/receipts**.  
It uses **OCR (Optical Character Recognition)**, regex-based parsing, and metadata extraction to convert raw unstructured documents into clean, structured tabular data — which is then stored in a **PostgreSQL database** for further analytics.

---

## 🚀 Features

- 🔍 **OCR Text Extraction:** Converts scanned receipts and PDFs into text using Tesseract.  
- 🧠 **Entity Parsing:** Extracts supplier, TIN, invoice number, date, totals, taxes, and line items.  
- 🧮 **Data Normalization:** Cleans, validates, and standardizes all numerical and textual fields.  
- 🗃️ **Database Loading:** Inserts structured data into PostgreSQL (`invoices` table).  
- 📊 **Exploratory Analysis:** Performs basic analytics on loaded invoices and line items.

---

## 🧩 Pipeline Architecture

```text
Raw PDFs / JPGs
        ↓
OCR Processing (Tesseract / PyMuPDF)
        ↓
Text Parsing (Regex + Rules)
        ↓
Structured DataFrame
        ↓
PostgreSQL Databank
        ↓
Data Analysis & Visualization



---

## 🛠️ Tech Stack

| Component | Technology |
|------------|-------------|
| Language | Python 3.9+ |
| OCR | pytesseract, OpenCV |
| PDF Processing | PyMuPDF / pdf2image |
| Data Manipulation | pandas, numpy |
| Database | PostgreSQL + psycopg2 |
| Visualization | matplotlib / seaborn |
| Cloud Storage (optional) | AWS S3 / GCS (future integration) |

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository
```bash
git clone https://github.com/<your_username>/pdf-etl-pipeline.git
cd pdf-etl-pipeline

2️⃣ Create Virtual Environment
conda create -n pdf-etl python=3.9
conda activate pdf-etl

3️⃣ Install Dependencies
pip install -r requirements.txt

4️⃣ Configure Database Connection

Create a .env file or export environment variables:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=invoices
DB_USER=postgres
DB_PASSWORD=your_password

5️⃣ Run the Pipeline
python run_pipeline.py


This command will:

Extract text from invoices/images

Parse metadata and line items

Store structured results into the PostgreSQL database

📊 Example Output
company_id	supplier_name	invoice_number	invoice_date	subtotal_amount	tax_amount	total_amount
1	MR. D.I.Y. SDN BHD	000306020352	2018-03-12	42.45	2.55	45.00
1	PYEDRAIN SUPPLIER	R000183898	2018-03-12	42.45	2.55	45.00


📈 Next Steps — Data Analysis

Once the data is in PostgreSQL, you can perform the following analyses: