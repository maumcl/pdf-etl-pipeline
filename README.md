# PDF Invoice OCR & ETL Pipeline

An automated pipeline for extracting, parsing, and storing invoice data from raw PDFs and images into a structured PostgreSQL database.

---

## 🔄 Pipeline Flow
```
Raw PDFs / JPGs
        ↓
OCR Processing (Tesseract / PyMuPDF)
        ↓
Text Parsing (Regex + Rules)
        ↓
Structured DataFrame
        ↓
PostgreSQL Database
        ↓
Data Analysis & Visualization
```

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.9+ |
| OCR | pytesseract, OpenCV |
| PDF Processing | PyMuPDF / pdf2image |
| Data Manipulation | pandas, numpy |
| Database | PostgreSQL + psycopg2 |

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository
```bash
git clone https://github.com/maumcl/pdf-etl-pipeline.git
cd pdf-etl-pipeline
```

### 2️⃣ Create Virtual Environment
```bash
conda create -n pdf-etl python=3.9
conda activate pdf-etl
```

### 3️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```

### 4️⃣ Configure Database Connection

Create a `.env` file or export environment variables:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=invoices
DB_USER=postgres
DB_PASSWORD=your_password
```

### 5️⃣ Run the Pipeline
```bash
python run_pipeline.py
```

This command will:
- Extract text from invoices/images
- Parse metadata and line items
- Store structured results into the PostgreSQL database

---

## 📊 Example Output

| company_id | supplier_name | invoice_number | invoice_date | subtotal_amount | tax_amount | total_amount |
|------------|---------------|----------------|--------------|-----------------|------------|--------------|
| 1 | MR. D.I.Y. SDN BHD | 000306020352 | 2018-03-12 | 42.45 | 2.55 | 45.00 |
| 1 | PYEDRAIN SUPPLIER | R000183898 | 2018-03-12 | 42.45 | 2.55 | 45.00 |

---

## 📈 Next Steps — Data Analysis

Once the data is in PostgreSQL, you can perform the following analyses:

- **Spending trends** over time
- **Supplier analysis** and vendor comparisons
- **Tax compliance** reporting
- **Budget forecasting** and anomaly detection

---



## 📧 Contact

For questions or support, reach out at [your-email@example.com]
