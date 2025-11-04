import glob
import pandas as pd
import numpy as np
import inspect
import pdfplumber
import os
from pathlib import Path
import unidecode
import re
from glob import glob
from tqdm.notebook import tqdm
from functools import reduce
from datetime import datetime, date
from PIL import Image, ImageOps, ImageFilter, ImageDraw
import pytesseract
from typing import List, Optional
from pathlib import PosixPath
import pathlib



# ---------------------------
# PARSING HELPERS
# ---------------------------

def parse_float(value):
    """Convert string to float, remove commas or $ symbols."""
    if pd.isna(value):
        return None
    try:
        value = str(value).replace(",", "").replace("$", "").strip()
        return float(value)
    except ValueError:
        return None


def clean_currency(df, columns):
    """Remove $ symbols and convert to float for numeric columns."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_float)
    return df


def parse_date(date_str):
    """Normalize various date formats to YYYY-MM-DD (PostgreSQL compatible)."""
    if not date_str or pd.isna(date_str):
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# Matches: 28-03-18, 28/03/2018, 28.03.18, 28-03-18 18:05
DATE_REGEX = re.compile(
    r'(?<!\d)'
    r'(?P<d>\d{1,2})[-/.\s]'
    r'(?P<m>\d{1,2})[-/.\s]'
    r'(?P<y>\d{2,4})'
    r'(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2}))?'
    r'(?!\d)'
)

def _coerce_year(y: int) -> int:
    """Convert 2-digit year to 4-digit (prefer 2000s for <=30, else 1900s)."""
    if y < 100:
        return 2000 + y if y <= 30 else 1900 + y
    return y


def _find_invoice_date(lines: List[str]) -> Optional[str]:
    """Scan all lines for the first valid date in dd-mm-yy(yy) and return YYYY-MM-DD."""
    candidates = []
    for raw in lines:
        s = re.sub(r'(?<=\d)[Oo](?=\d)', '0', raw)
        s = re.sub(r'(?<=\d)[Ss](?=\d)', '5', s)
        s = re.sub(r'(?<=\d)[lI](?=\d)', '1', s)

        for m in DATE_REGEX.finditer(s):
            d = int(m.group('d'))
            mo = int(m.group('m'))
            y = _coerce_year(int(m.group('y')))
            try:
                dt = datetime(y, mo, d)
                candidates.append(dt)
            except ValueError:
                continue

    if not candidates:
        return None
    return min(candidates).strftime('%Y-%m-%d')


def extract_tax_rate(tax_label):
    """Extract numeric percentage from 'GST 8%' → 8.0"""
    if not tax_label:
        return None
    match = re.search(r"(\d+\.?\d*)\s*%", str(tax_label))
    return float(match.group(1)) if match else None


def fix_null_total(df):
    """If total_amount is missing, compute subtotal + tax."""
    if "total_amount" in df.columns:
        df["total_amount"] = df["total_amount"].fillna(
            df["subtotal_amount"].fillna(0) + df["tax_amount"].fillna(0)
        )
    return df


def standardize_columns(df):
    """Lowercase all column names and trim whitespace."""
    df.columns = [c.strip().lower() for c in df.columns]
    return df



# ---------------------------
# STEP 1 - METADATA
# ---------------------------

def extract_invoice_metadata(lines):
    supplier_name = None
    supplier_tin = None
    invoice_number = None
    invoice_date = None
    subtotal = tax_label = tax_amount = total_amount = None

    for line in lines:
        u = line.upper().strip()

        # Supplier TIN (avoid capturing GST)
        if not supplier_tin:
            m = re.search(r"(?<!G)TIN[:\s]+(\d+)", u)
            if m:
                supplier_tin = m.group(1)

        # Invoice number (GST ID fallback)
        if not invoice_number:
            m_inv = re.search(r"GST\s*ID\s*No\s*[:\-]?\s*(\d+)", u)
            if not m_inv:
                m_inv = re.search(r"(R\d{6,}|INV\d+|INVOICE\s*#?\s*\d+)", u)
            if m_inv:
                invoice_number = m_inv.group(1)

        # Invoice Date
        if not invoice_date:
            m_date = re.search(r"(\d{2}[-/]\d{2}[-/]\d{2,4})", u)
            if m_date:
                invoice_date = parse_date(m_date.group(1))

        # Totals
        if re.search(r"TOTAL", u) and re.search(r"\d+[.,]\d+", u):
            total_amount = parse_float(re.findall(r"[\d.,]+", u)[-1])

        if "GST" in u:
            nums = re.findall(r"\d+[.,]\d+", u)
            if len(nums) >= 2:
                subtotal = parse_float(nums[0])
                tax_amount = parse_float(nums[1])
                tax_label = "GST"

    return {
        "supplier_name": supplier_name,
        "supplier_tin": supplier_tin,
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "subtotal_amount": subtotal,
        "tax_label": tax_label,
        "tax_amount": tax_amount,
        "total_amount": total_amount,
    }



# ---------------------------
# STEP 2 - PRODUCTS
# ---------------------------

def extract_products(df_ocr):
    items = []
    lines = df_ocr["line"].tolist()

    # Match flexible product patterns (qty X price total, etc.)
    pattern = re.compile(
        r"(?:(\d{1,3})\s*[Xx]\s*)?(\d+[.,]\d+)\s+(\d+[.,]\d+)$"
    )

    for i, line in enumerate(lines):
        s = line.strip()
        m = pattern.search(s)
        if not m:
            continue

        qty, price, total = m.groups()

        # Fallbacks
        qty = qty or "1"
        desc = lines[i - 1].strip() if i > 0 else ""

        items.append({
            "description": desc,
            "qty": parse_float(qty),
            "price": parse_float(price),
            "total": parse_float(total),
        })

    return pd.DataFrame(items)



# ---------------------------
# STEP 3 - MERGE RESULTS
# ---------------------------

def extract_product_info(df_ocr, company_id, country, processed_date, file_path):
    lines = df_ocr["line"].tolist()

    meta = extract_invoice_metadata(lines)
    products = extract_products(df_ocr)

    # Compute subtotal and total fallback
    if not meta["subtotal_amount"] and not products.empty:
        meta["subtotal_amount"] = products["total"].sum()
    if not meta["total_amount"]:
        meta["total_amount"] = (meta["subtotal_amount"] or 0) + (meta["tax_amount"] or 0)

    products["company_id"] = company_id
    products["country"] = country
    products["processed_date"] = processed_date
    products["supplier_name"] = meta["supplier_name"]
    products["supplier_tin"] = meta["supplier_tin"]
    products["invoice_number"] = meta["invoice_number"]
    products["invoice_date"] = meta["invoice_date"]
    products["subtotal_amount"] = meta["subtotal_amount"]
    products["tax_label"] = meta["tax_label"]
    products["tax_amount"] = meta["tax_amount"]
    products["total_amount"] = meta["total_amount"]
    products["file"] = file_path

    return products



# ---------------------------
# EXTRACTION FUNCTIONS (optional legacy)
# ---------------------------

def parse_invoice_line(line):
    """Extract Description, Qty, Price, and Total from a text line."""
    line = line.strip()

    if "Sub Total" in line:
        total_value = re.findall(r"[\d,]+\.\d+", line)
        return {            
            "description": "Sub Total",
            "qty": None,
            "price": None,
            "total": float(total_value[0].replace(',', '')) if total_value else None
        }

    id_match = re.match(r"^(\d+)\.", line)
    if not id_match:
        return None

    rest = re.sub(r"^\d+\.\s*", "", line)
    numbers = re.findall(r"\d+(?:\.\d+)?", rest)
    if len(numbers) < 3:
        return None

    qty, price, total = numbers[-3:]
    desc_part = rest[: rest.rfind(qty)].strip().rstrip(".")

    return {
        "description": desc_part,
        "qty": float(qty),
        "price": float(price),
        "total": float(total)
    }


def extract_table_section(text_list):
    """Extract table lines between header and subtotal."""
    start_idx = end_idx = None
    for i, line in enumerate(text_list):
        if "ID DESCRIPTION QTY PRICE TOTAL" in line:
            start_idx = i + 1
        elif "Sub Total" in line and start_idx is not None:
            end_idx = i
            break
    return text_list[start_idx:end_idx] if (start_idx and end_idx) else []
