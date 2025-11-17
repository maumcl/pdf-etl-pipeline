from lib.ocr_utils import *
from lib.parse_utils import *

def detect_doc_type(path: Path, text_list=None) -> str:

    name = path.name.lower()
    text_joined = "\n".join(text_list).lower() if text_list else ""

    # Award Letter / Notification of Award
    if "award letter" in name or "notification of award" in text_joined:
        return "nc_award_letter"
    if "bids as read" in name:
        return "nc_bids_as_read"
    if "bid tabs" in name:
        return "nc_bid_tabs"
    if "invitation to bid" in name:
        return "nc_invitation_to_bid"
    if "item c" in name:
        return "nc_item_c"

    return "invoice"

def extract_lines_from_pdf(path: Path):

    lines = []

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                lines.extend(text.split("\n"))
                continue

            try:
                page_image = page.to_image(resolution=300).original  # PIL.Image
                ocr_text = pytesseract.image_to_string(page_image)
                if ocr_text:
                    lines.extend(ocr_text.split("\n"))
            except Exception as e:
                print(f"⚠️ OCR failed on page {page.page_number} of {path}: {e}")

    return lines


def run_extraction_pipeline(pdf_path, company_id, country, processed_date):
    """
    Unified ETL function to extract structured invoice data from PDFs and JPGs.
    """
    records = []

    for path in pdf_path:
        path = Path(path)
        ext = path.suffix.lower()
        print(f"\n📂 Processing file: {path}")

        # --- PDF extraction ---
        if ext == ".pdf":
            text_list = []
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        text_list.extend(text.split("\n"))
            print(f"📄 Extracted {len(text_list)} text lines from PDF.")
            df_img = None

        # --- JPG extraction with OCR ---
        elif ext in [".jpg", ".jpeg", ".png"]:
            df_img = read_bbox_and_words(path)
            if df_img.empty:
                print(f"⚠️ No valid OCR text found in {path}")
                continue

            df_img = df_img.dropna(subset=["line"])
            df_img = df_img[df_img["line"].str.strip() != ""]
            df_img = group_ocr_words(df_img, y_tolerance=10)
            df_img = df_img[df_img["line"].str.len() > 3]

            print(f"🖼️ Extracted {len(df_img)} full text lines from image: {path}")
            display(df_img.head(5))

            text_list = df_img["line"].astype(str).tolist()

              # 📦 Try to find and read a matching entity file (.json)
            entity_path = Path(str(path).replace(".jpg", ".json"))
            if entity_path.exists():
                df_entities = read_entities(entity_path)
                print(f"✅ Loaded entity data from {entity_path.name}:")
                
            else:
                df_entities = pd.DataFrame(columns=["company_id", "address", "date", "total"])

            text_list = df_img["line"].astype(str).tolist()

            # After df_img and text_list extraction
            
            df_items = extract_product_info(df_img, company_id=company_id, country=country, processed_date=processed_date, file_path=pdf_path)

            if not df_items.empty:
                df_items["company_id"] = company_id
                df_items["country"] = country
                df_items["processed_date"] = processed_date
                df_items["file"] = path
                
    #            Append parsed data to records list
                records.extend(df_items.to_dict(orient="records"))
            else:
                print(f"⚠️ No valid product data found in {path}")

        else:
            print(f"⚠️ Unsupported file type: {ext}")
            continue

        doc_type = detect_doc_type(path, text_list=text_list)
        print(f"🔎 Detected doc_type={doc_type}")

        # -------------------------------------------------
        # DOCS ESPECIALS (NC ...)
        # -------------------------------------------------
        if doc_type != "invoice":
            parsed_records = parse_document_by_type(
                doc_type=doc_type,
                text_list=text_list,
                df_img=df_img,
                company_id=company_id,
                country=country,
                processed_date=processed_date,
                file_path=path,
            )
            print(parsed_records)
            # debug
            print(
                f"   ➜ parse_document_by_type returned "
                f"{type(parsed_records)}"
                f"{' len=' + str(len(parsed_records)) if isinstance(parsed_records, list) else ''}"
            )

            if isinstance(parsed_records, dict):
                parsed_records = [parsed_records]

            if parsed_records:
                records.extend(parsed_records)

            continue


        # --- Extract metadata & table lines ---
        metadata = extract_invoice_metadata(text_list)
        table_lines = extract_table_section(text_list)
        print(metadata)
        print(table_lines)

        # --- Parse and append structured lines ---
        for line in table_lines:
            parsed = parse_invoice_line(line)
            if parsed:
                parsed.update(metadata)
                parsed.update({
                    "company_id": company_id,
                    "country": country,
                    "processed_date": processed_date,
                    "file": path
                })
                records.append(parsed)
                

    # --- Combine results ---
    df = pd.DataFrame(records)



    if df.empty:
        print("⚠️ No valid records found across all files.")
        return pd.DataFrame()
    else:
        print(f"\n✅ Final DataFrame created with {len(df)} total records.")
        display(df.head())
        return df
    