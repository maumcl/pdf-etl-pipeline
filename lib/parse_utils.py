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
#    """Remove $ symbols and convert to float for numeric columns."""
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


# matches: 28-03-18, 28/03/2018, 28.03.18, 28-03-18 18:05
DATE_REGEX = re.compile(
    r'(?<!\d)'                      # not preceded by a digit
    r'(?P<d>\d{1,2})[-/.\s]'        # day
    r'(?P<m>\d{1,2})[-/.\s]'        # month
    r'(?P<y>\d{2,4})'               # year (2 or 4 digits)
    r'(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2}))?'  # optional time
    r'(?!\d)'                       # not followed by a digit
)

def _coerce_year(y: int) -> int:
    # 2-digit year → prefer 2000s for <= 30, else 1900s
    if y < 100:
        return 2000 + y if y <= 30 else 1900 + y
    return y


def _find_invoice_date(lines: List[str]) -> Optional[str]:
    """
    Scan all lines for the first valid dd-mm-yy(yy) date (with optional time)
    and return as YYYY-MM-DD. Robust to -, /, . and a trailing time token.
    """
    candidates = []
    for raw in lines:
        # Fix common OCR confusions inside numeric groups (O→0, S→5, l→1)
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
    # choose the earliest seen in the receipt text
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
# EXTRACTION FUNCTIONS
# ---------------------------

def parse_invoice_line(line):
    """
    Extract Description, Qty, Price, and Total from a text line.
    Handles cases like: "03. FX100 Graphic Tablet - 01 1. 1300. 1300."
    """
    line = line.strip()

    # Handle subtotal lines separately
    if "Sub Total" in line:
        total_value = re.findall(r"[\d,]+\.\d+", line)
        return {            
            "description": "Sub Total",
            "qty": None,
            "price": None,
            "total": float(total_value[0].replace(',', '')) if total_value else None
        }

    # Match ID first
    id_match = re.match(r"^(\d+)\.", line)
    if not id_match:
        return None

    # Remove ID part
    rest = re.sub(r"^\d+\.\s*", "", line)

    # Find the last 3 numeric values (qty, price, total)
    numbers = re.findall(r"\d+(?:\.\d+)?", rest)
    if len(numbers) < 3:
        return None

    qty, price, total = numbers[-3:]

    # Extract description (everything before qty)
    desc_part = rest[: rest.rfind(qty)].strip().rstrip(".")

    return {
        "description": desc_part,
        "qty": float(qty),
        "price": float(price),
        "total": float(total)
    }


def extract_invoice_metadata(text_list):
    """Extract supplier, client, invoice details, and totals."""
    supplier_name = " ".join([s.strip() for s in text_list[:2] if s.strip()])
    invoice_number = invoice_date = due_date = None
    client_name = client_address = supplier_tin = client_tin = None
    subtotal = tax_label = tax_amount = total_amount = None

    for line in text_list:
        u = line.strip().upper()

        # --- Supplier TIN ---
        if not supplier_tin:
            m = re.search(r"(?<!G)TIN[:\s]+(\d+)", line, re.IGNORECASE)
            if m:
                supplier_tin = m.group(1)

        # --- Invoice Number (now robust for GST ID No :001092886528) ---
        if not invoice_number:
            # common direct forms
            m = re.search(r"\b(?:INVOICE|RECEIPT|TAX\s*INVOICE)\s*#?\s*[:\-]?\s*([A-Z]?\d{3,})\b", u)
            if m:
                invoice_number = m.group(1)
            else:
                # pattern like R0000183898 or T000011248
                m2 = re.search(r"\b[RT]\d{6,}\b", u)
                if m2:
                    invoice_number = m2.group(0)

        # --- Invoice & Due Dates ---
        if not invoice_date:
            m = re.search(r"Invoice Date[:\s]+(.+)", line, re.IGNORECASE)
            if m:
                invoice_date = parse_date(m.group(1))

        if not due_date:
            m = re.search(r"Due Date[:\s]+(.+)", line, re.IGNORECASE)
            if m:
                due_date = parse_date(m.group(1))

        # --- Client Info ---
        if "Bill to" in line:
            idx = text_list.index(line)
            client_name = text_list[idx + 2].strip() if len(text_list) > idx + 2 else None
            client_address = ", ".join(text_list[idx + 3 : idx + 6]).strip()

        if not client_tin:
            m = re.search(r"TIN[:\s]+(\d+)", line, re.IGNORECASE)
            if m:
                client_tin = m.group(1)

    # --- Subtotal ---
        if re.search(r"Sub\s*Total|TOTAL", line, re.IGNORECASE):
            # Find all numbers in the line
            numbers = re.findall(r"\d+[,.]?\d*", line)
            if numbers:
                # Take the last number found (usually the amount)
                subtotal = parse_float(numbers[-1])
                total_amount = parse_float(numbers[0])
                
            

               
        # --- GST / Tax ---
        if re.search(r"GST", line, re.IGNORECASE) and not re.search(r"\bTOTAL\b", line, re.IGNORECASE):
            # Normalize
            u = line.replace("％", "%").replace("°", "%").replace("‰", "%")

            # Extract all numeric values
            numbers = re.findall(r"\d+(?:[.,]\d+)?", u)
            values = [parse_float(x) for x in numbers if parse_float(x) is not None]

            # Skip invalid or single-value GST lines (like GST@6%)
            if len(values) >= 2:
                # Typical patterns: [2.55, 6.0, 42.45]  or  [0.22, 3.68, 6.0]
                values_sorted = sorted(values)

                # If one of the numbers is a "rate" (5–15 %), ignore it
                rates = [v for v in values_sorted if 4.0 <= v <= 15.0]  # GST/VAT rates are usually 4–15 %
                non_rates = [v for v in values_sorted if v not in rates]

                if len(non_rates) >= 2:
                    # Usually smaller number = tax, larger = subtotal
                    non_rates_sorted = sorted(non_rates)
                    tax_amount = non_rates_sorted[0]
                    subtotal = non_rates_sorted[-1]
                elif len(non_rates) == 1 and rates:
                    # one numeric + one rate → treat numeric as subtotal, tax = subtotal * rate/100
                    subtotal = non_rates[0]
                    tax_amount = round(subtotal * (rates[-1] / 100), 2)
                else:
                    # fallback: smallest = tax, largest = subtotal
                    tax_amount = values_sorted[0]
                    subtotal = values_sorted[-1]

                tax_label = f"GST {rates[-1]}%" if rates else "GST"

                if tax_label:
                    m = re.search(r"(\d+(?:\.\d+)?)", str(tax_label))
                    if m:
                        tax_label = float(m.group(1))





    try:
        if subtotal and tax_amount and tax_amount > subtotal:
            subtotal, tax_amount = tax_amount, subtotal
    except Exception:
        pass


    return {
        "supplier_name": supplier_name,
        "supplier_tin": supplier_tin,
        "client_name": client_name,
        "client_address": client_address,
        "client_tin": client_tin,
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "due_date": due_date,
        "tax_label": tax_label,
        "tax_amount": tax_amount,
        "total_amount": total_amount,
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





# ---------------------------
# OCD / JPG HELPERS
# ---------------------------

def extract_product_info(df_ocr: pd.DataFrame, company_id: int, country: str, processed_date, file_path: str):


    # ---------- CONFIG ----------
    NOISE_WORDS = (
        "TOTAL", "CASH", "CHANGE", "QTY(S)", "QTY", "ITEM(S)", "ITEM",
        "SUMMARY", "SUBTOTAL", "INCL", "EXCL", "AMT(RM)", "TAX(RM)", "RM"
    )
    HEADER_WORDS = (
        "INVOICE", "TAX INVOICE", "INVOICE#", "INVOICE #", "TOTAL INCL.", "TOTAL INCL",
        "GST SUMMARY", "GST@", "GST S@", "GST ID", "GST NO", "GST ID NO", "GST ID:"
    )

    def _is_noise(line: str) -> bool:
        u = line.upper()
        return any(w in u for w in NOISE_WORDS)

    def _is_header(line: str) -> bool:
        u = line.upper().strip()
        if any(w in u for w in HEADER_WORDS):
            return True
        if re.fullmatch(r"[-=]{4,}", u):
            return True
        return False

    def _looks_like_desc(line: str) -> bool:
        return bool(re.search(r"[A-Za-z]", line)) and not _is_noise(line)

    def _norm_num(x):
        if x is None:
            return None
        s = str(x).strip().replace(" ", "")
        s = s.replace(",", ".")  # unify separators

        try:
            val = float(s)
            # Heuristic: if looks too large to be realistic (like >10k) and has no decimal part
            if val > 10000 and val.is_integer():
                val = val / 100   # treat as cents
            return val
        except ValueError:
            return None

    # Normalize OCR text
    df_ocr["line"] = df_ocr["line"].apply(
        lambda t: re.sub(r"[^A-Za-z0-9\s:/().,&-]", "", str(t))
    )

    PRICE_PATTERNS = [

    # SKU qty X price total  → "9556268000210 3 X 15.00 45.00"
    re.compile(r"^(?P<sku>\d{5,})\s+(?P<qty>\d{1,3})\s*(?:X|x)\s*(?P<price>\d+(?:[.,]\d{1,2}))\s+(?P<total>\d+(?:[.,]\d{1,2}))"),    

    # SKU X price total  → "9072363 X 29.90 29.90"
    re.compile(r"^(?P<sku>\d{5,})\s*(?:X|x)\s*(?P<price>\d+(?:[.,]\d{1,2}))\s+(?P<total>\d+(?:[.,]\d{1,2}))"),
    
    # SKU price qty X total  → "9021937 3.90 1 X 3.90" / "9021937 3.90 1X 3.90"
    re.compile(r"^(?P<sku>\d{5,})\s+(?P<price>\d+(?:[.,]\d{1,2}))\s+(?P<qty>\d+)\s*(?:X|x)\s*(?P<total>\d+(?:[.,]\d{1,2}))"),

    # price qty X total       → "3.90 1 X 3.90"
    re.compile(r"^(?P<price>\d+(?:[.,]\d{1,2}))\s+(?P<qty>\d+)\s*(?:X|x)\s*(?P<total>\d+(?:[.,]\d{1,2}))"),

    # qty X price total       → "1 X 29.90 29.90" or "1X 8.90 8.90"  (qty max 3 digits)
    re.compile(r"^(?P<qty>\d{1,3})\s*(?:X|x)\s*(?P<price>\d+(?:[.,]\d{1,2}))\s*(?P<total>\d+(?:[.,]\d{1,2}))"),

    # price ... qty X total   → "8.98 6942131561408 1X 8.90"
    re.compile(r"^(?P<price>\d+(?:[.,]\d{1,2})).*?(?P<qty>\d+)\s*(?:X|x)\s*(?P<total>\d+(?:[.,]\d{1,2}))"),
    ]


    DATE_PATTERNS = [
    re.compile(r"(?P<invoice_date>\d{2}[-/]\d{2}[-/]\d{2,4})"),
    ]

    INVOICE_NUMBER_PATTERNS = [
    re.compile(r"(?:T\d+\s+)?(?P<invoice_number>[A-Z]?\d{6,})"),
    ]


    def _match_price_line(line: str):
        s = re.sub(r"\s+", " ", str(line).strip())

        # ignore totals/tenders
        if any(w in s.upper() for w in ["TOTAL", "CASH", "CHANGE"]):
            return None

        # --- Try regex patterns in order ---
        for pat in PRICE_PATTERNS:
            m = pat.match(s)
            if not m:
                continue

            g = m.groupdict()
            qty = (g.get("qty") or "").strip()
            price = g.get("price")
            total = g.get("total") or g.get("price")

            # guard: qty that looks like SKU → discard
            if qty and len(qty) >= 5:
                qty = ""

            # convert to floats
            try:
                p = float(price.replace(",", "."))
                t = float(total.replace(",", "."))
            except Exception:
                continue

            # infer qty when missing or inconsistent
            if not qty:
                # try to read "Qty(s) : N" nearby (global line check helps when only 1 item)
                # (optional — simple arithmetic fallback is usually enough)
                pass

            # If qty missing OR p * qty doesn't match total, infer qty from total/price
            qf = None
            if qty:
                try:
                    qf = float(qty)
                except Exception:
                    qf = None

            if (qf is None) or (p > 0 and abs(t - p * qf) > 0.02):
                if p > 0:
                    inferred = t / p
                    # only accept clean-ish integers
                    if abs(round(inferred) - inferred) < 0.02 and 0.5 <= inferred <= 999:
                        qf = float(int(round(inferred)))

            if qf is None:
                # still no reliable qty → try next pattern
                continue

            return {
                "sku": g.get("sku"),
                "qty": str(int(qf)),
                "unit_price": price,
                "line_total": total,
            }

        # --- Last resort: "SKU X price total" → qty=1,
        # but ONLY if there is no number between SKU and X
        m_sku_x = re.match(
            r"^(?P<sku>\d{5,})\s*(?:X|x)\s*(?P<price>\d+(?:[.,]\d{1,2}))\s+(?P<total>\d+(?:[.,]\d{1,2}))$",
            s,
        )
        if m_sku_x:
            g = m_sku_x.groupdict()
            return {
                "sku": g.get("sku"),
                "qty": "1",
                "unit_price": g.get("price"),
                "line_total": g.get("total"),
            }

        return None


    

    # ---------- MAIN EXTRACTION ----------
    def _extract(df_ocr: pd.DataFrame, lookback=6):
        lines = df_ocr["line"].tolist()

        # -------- SUPPLIER INFO --------
        supplier_name = None
        supplier_tin = None

        for i, line in enumerate(lines):
            u = line.upper().strip()

            # Detect supplier name (company header)
            if any(k in u for k in ["BHD SON", "SDN BHD", "SON BHD", "SON BHO", "LTD", "ENTERPRISE", "COMPANY"]):
                supplier_name = line.strip()

            # If not found yet, check if first few lines look like name + address
            if supplier_name is None and i < 3:
                # Often first line is the company name without suffix
                if re.search(r"^[A-Z][A-Z\s.&'-]{3,}$", u):
                    supplier_name = line.strip()

            # Detect registration or GST number for supplier
            if "REG" in u or "TIN" in u or "GST" in u:
                m = re.search(r"(\d{6,})", line)
                if m:
                    supplier_tin = m.group(1)



        # -------- CLIENT ADDRESS --------
        client_address = None

        #   - Address lines usually appear right after supplier name (company header)
        #   - Often contain commas, numbers, or keywords like LOT, JALAN, KAWASAN, etc.
        address_keywords = ["LOT", "JALAN", "KAWASAN", "TAMAN", "BANDAR", "SELANGOR", "MALAYSIA"]
        address_lines = []

        if supplier_name:
            supplier_idx = next(
                (i for i, l in enumerate(lines) if supplier_name.split()[0] in l), None
            )
            if supplier_idx is not None:
                # Scan next few lines for address patterns
                for j in range(supplier_idx + 1, min(len(lines), supplier_idx + 6)):
                    line = lines[j].strip()
                    u = line.upper()

                    # Stop scanning if we reach GST, INVOICE, or TAX section
                    if any(x in u for x in ["GST", "INVOICE", "TAX", "REG", "CO-REG"]):
                        break

                    # Accept line if it looks like address content
                    if (
                        any(k in u for k in address_keywords)
                        or re.search(r"\d{4,5}", u)  # e.g. postal code
                        or "," in line
                    ):
                        address_lines.append(line)

        if address_lines:
            client_address = " ".join(address_lines)



        # -------- TAX INFO --------
        tax_label = tax_amount = total_amount = None

        for line in lines:
            u = line.upper().strip()  
    

            # --- GST summary or tax line ---
                       
            # Match GST lines but ignore totals
            if re.search(r"GST\s*[S@]?\s*\d+", u, re.IGNORECASE) and not re.search(r"\bTOTAL\b", u, re.IGNORECASE):

                # Extract all numeric values
                nums = re.findall(r"\d+(?:[.,]\d+)?", u)
                clean_nums = [float(x.replace(",", ".")) for x in nums]


                # GST summary usually has ≥2 numbers (subtotal + tax) or keywords
                if len(clean_nums) >= 2 or re.search(r"SUMMARY|AMT|TAX", u, re.IGNORECASE):

                    # --- Extract GST rate (handles GST6 / GST@6% / GST S6 etc.) ---
                    m_rate = re.search(r"GST\s*[S@]?\s*(\d+(?:[.,]\d+)?)(?:\s*%?)", u, re.IGNORECASE)
                    if m_rate:
                        tax_label = float(m_rate.group(1))


                    # --- Determine subtotal vs tax ---
                    if len(clean_nums) >= 2:
                        # The smaller value is tax, larger is subtotal
                        vals = sorted(clean_nums)
                        tax_amount = vals[0]
                    elif len(clean_nums) == 1:
                        tax_amount = 0.0

                # Extract all numbers
                nums = re.findall(r"\d+[.,]?\d*", u)
                nums = [n for n in nums if n.strip()]

                # Skip the GST rate (first small number like 6 or 5)
                if len(nums) >= 3 and float(nums[0]) in (5.0, 6.0, 7.0):
                    nums = nums[1:]

                # Detect pattern: smallest number before 'GST' → tax amount
            if "GST" in u:
                parts = re.split(r"GST", u)
                before_gst = parts[0]
                nums_before_gst = re.findall(r"\d+[.,]?\d*", before_gst)
                

                if len(nums_before_gst) >= 2:
                    # pick smaller one as tax (e.g. 2.55 from "2.55 S@6% 42.45 GST")
                    vals = [_norm_num(x) for x in nums_before_gst]
                    tax_amount = min(vals)
                    tax_label = vals[1]
                elif len(nums_before_gst) == 1:
                    tax_amount = _norm_num(nums_before_gst[0])

                # detect "TOTAL INCL. GST@6%" or similar
            if "TOTAL INCL" in line.upper() and re.search(r"\d+[.,]\d+", line):
                nums = re.findall(r"\d+[.,]\d+", line)
                if nums:
                    total_amount = _norm_num(nums[-1])

                    


        # -------- INVOICE INFO --------
        invoice_number = None
        invoice_date = None
        payment_method = None

        def _bad_context(s: str) -> bool:
            u = s.upper()
            return any(k in u for k in ["GST", "GST ID", "CO-REG", "CO REG", "COREG", "REG ", "FREE", "ML", "DRILL", "CLAMP", "CLOG"])

        for i, line in enumerate(lines):
            u = line.upper().strip()

            # --- date (dd-mm-yy[yy] with optional time) ---
            if not invoice_date:
                m_date = re.search(r"\b(\d{2}[-/]\d{2}[-/]\d{2,4})(?:\s+\d{1,2}:\d{2})?\b", u)
                if m_date:
                    invoice_date = m_date.group(1)

            # --- payment hint ---
            if any(k in u for k in ["CASH", "CARD", "VISA", "MASTERCARD"]) and not payment_method:
                payment_method = line.strip()

            # --- Step 1: explicit GST or invoice ID formats ---
            if not invoice_number:
                # Handle patterns like ":000473792512) (GST ID No"
                m_gst = re.search(r"[:(]?\s*0*(\d{6,})\s*\)?\s*\(?\s*GST\s*ID", u, re.IGNORECASE)
                if m_gst:
                    invoice_number = m_gst.group(1)
                    continue

                # Standard GST ID formats
                m_gst2 = re.search(r"GST\s*ID\s*No\s*[:\-]?\s*0*(\d{6,})", u, re.IGNORECASE)
                if m_gst2:
                    invoice_number = m_gst2.group(1)
                    continue

            if not invoice_number:
                m = re.search(r"\b(?:INVOICE|INV|RECEIPT)\s*(?:NO|#|:)?\s*([A-Z0-9-]{5,})\b", u)
                if m:
                    candidate = m.group(1)
                    # sanity check: must contain ≥3 digits
                    if len(re.findall(r"\d", candidate)) >= 3:
                        invoice_number = candidate
                        continue

            if not invoice_number:
                m = re.search(r"\bNO[:#]?\s*([A-Z0-9-]{5,})\b", u)
                if m:
                    candidate = m.group(1)
                    if len(re.findall(r"\d", candidate)) >= 3:
                        invoice_number = candidate
                        continue

        # --- Step 2: cross-line fallback (only if still None) ---
        if not invoice_number:
            for i, line in enumerate(lines):
                u = line.upper().strip()
                if _bad_context(u):
                    continue
                # must clearly be an invoice header (avoid short "INV" fragments)
                if re.fullmatch(r"(INVOICE|INVOICE NO[:#]?|INV NO[:#]?)", u):
                    nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
                    un = nxt.upper()
                    # reject codes or descriptions
                    if not _bad_context(un) and re.match(r"^[A-Z0-9-]{5,}$", un):
                        invoice_number = nxt.strip()
                        break

        # final cleanup
        if invoice_number:
            m = re.search(r"\b([A-Z]?\d[A-Z0-9]+)\b", invoice_number)
            if m:
                invoice_number = m.group(1)

        # normalize date safely
        if invoice_date:
            try:
                invoice_date = pd.to_datetime(invoice_date, dayfirst=True, errors="coerce")
            except Exception:
                invoice_date = None
        if pd.isna(invoice_date):
            invoice_date = None

        # final cleaning: strip trailing decorations like "-W)" or ")"
        if invoice_number:
            # keep core token (letters/digits only, preserve leading R/T and digits; drop trailing -W, ), etc.)
            m = re.search(r"\b([A-Z]?\d[A-Z0-9]+)\b", invoice_number)
            if m:
                invoice_number = m.group(1)

        # normalize date safely
        if invoice_date:
            try:
                invoice_date = pd.to_datetime(invoice_date, dayfirst=True, errors="coerce")
            except Exception:
                invoice_date = None
        if pd.isna(invoice_date):
            invoice_date = None





        # -------- PRODUCTS --------
        
        items = []

        def _is_price_like(line: str) -> bool:
            s = re.sub(r"\s+", " ", line.strip())
            return any(pat.match(s) for pat in PRICE_PATTERNS)

        def _is_code_like(line: str) -> bool:
            t = line.strip()
            return bool(re.fullmatch(r"[0-9][0-9/\-\s]{5,}", t))

        def _looks_like_desc(line: str) -> bool:
            u = line.upper().strip()
            # allow alphanumeric lines (letters or digits), so we don’t drop codes like JH51/3-63 or LG12-24
            if not re.search(r"[A-Za-z0-9]", u):
                return False
            if _is_price_like(u):
                return False
            if _is_header(u) or _is_noise(u):
                return False
            return True


        for i, line in enumerate(lines):
            if _is_noise(line):
                continue
            m = _match_price_line(line)
            if not m:
                continue

            desc_lines = []
            for j in range(i - 1, max(0, i - lookback) - 1, -1):
                cand = lines[j].strip()
                u = cand.upper()

                # stop scanning if we hit totals or headers
                if _is_header(cand) or _is_price_like(cand):
                    break

                # Accept descriptive or semi-code lines (letters, numbers, dashes, plus signs)
                if re.search(r"[A-Za-z]", cand) or re.search(r"[\d\-+/]", cand):
                    desc_lines.insert(0, cand)
                else:
                    break

            # Merge multi-line product descriptions
            desc = " ".join(desc_lines).strip()
            desc = re.sub(r"\s{2,}", " ", desc)  # normalize spaces
            desc = re.sub(r"^\s*(TAX\s+INVOICE\b[–—-]?\s*)", "", desc, flags=re.I)
            desc = desc or "UNNAMED ITEM"


            items.append({
                "description": desc,
                "qty": _norm_num(m["qty"]),
                "price": _norm_num(m["unit_price"]),
                "total": _norm_num(m["line_total"]),
            })

        df_products = pd.DataFrame(items)
        
        if not df_products.empty:
            subtotal_amount = df_products["total"].sum()
            total_amount = subtotal_amount



        # -------- FINAL MERGED ROWS --------
        records = []
        for idx, row in df_products.iterrows():
            records.append({
                "company_id": company_id,
                "country": country,
                "processed_date": date.today().strftime("%Y-%m-%d"),
                "supplier_name": supplier_name,
                "supplier_tin": supplier_tin,
                "client_name": None,
                "client_address": client_address,
                "client_tin": None,
                "invoice_number": invoice_number,
                "invoice_date": invoice_date,
                "due_date": None,
                "description": row["description"],
                "qty": row["qty"],
                "price": row["price"],
                "total": row["total"],
                "tax_label": tax_label,
                "tax_amount": tax_amount,
                "total_amount": total_amount,
                "file": file_path
            })

        df_final = pd.DataFrame(records)
        return df_final
    
 
    return _extract(df_ocr)
