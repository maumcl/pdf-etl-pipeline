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
from datetime import datetime
from PIL import Image, ImageOps, ImageFilter, ImageDraw
import pytesseract



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


def read_bbox_and_words(path: Path):
    bbox_and_words_list = []
    path = Path(path)
    image = Image.open(path).convert("RGB")
    ocr_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DATAFRAME)

    if ocr_data.empty:
        print(f"❌ No OCR text extracted from {path.name}")
        return pd.DataFrame(columns=['filename', 'x0', 'y0', 'x2', 'y2', 'line'])

    # Rename columns for consistency
    ocr_data = ocr_data.rename(columns={"left": "x0", "top": "y0", "width": "w", "height": "h"})
    ocr_data["x2"] = ocr_data["x0"] + ocr_data["w"]
    ocr_data["y2"] = ocr_data["y0"] + ocr_data["h"]
    ocr_data["filename"] = path.stem
    ocr_data["line"] = ocr_data["text"]

    return ocr_data[["filename", "x0", "y0", "x2", "y2", "line"]]


def visualize_bboxes(image_path, df: pd.DataFrame,
                     color="red", width=2, limit=None,
                     max_width=1000, min_conf=None):
    """
    Draw bounding boxes over an image using coords in df (x0,y0,x2,y2).
    - Accepts str or Path for image_path.
    - Downscales wide images to max_width while keeping aspect ratio.
    - Scales bbox coordinates accordingly.
    """
    try:
        img_path = Path(image_path)            
        image = Image.open(img_path).convert("RGB")
        W, H = image.size

        # Optional: filter bad OCR rows
        draw_df = df.copy()
        if "line" in draw_df.columns:
            draw_df = draw_df.dropna(subset=["line"])
            draw_df = draw_df[draw_df["line"].astype(str).str.strip() != ""]
        if min_conf is not None and "conf" in draw_df.columns:
            draw_df = draw_df[pd.to_numeric(draw_df["conf"], errors="coerce") >= min_conf]

        # Downscale for preview only
        scale = 1.0
        if W > max_width:
            scale = max_width / float(W)
            new_h = int(H * scale)
            image = image.resize((int(max_width), new_h), Image.LANCZOS)

        # Scale coordinates if we resized
        if scale != 1.0:
            for col in ["x0", "y0", "x2", "y2"]:
                draw_df[col] = (pd.to_numeric(draw_df[col], errors="coerce") * scale).round().astype("Int64")

        # Draw
        draw = ImageDraw.Draw(image)
        subset = draw_df if limit is None else draw_df.head(limit)
        for _, r in subset.iterrows():
            try:
                draw.rectangle([(int(r["x0"]), int(r["y0"])),
                                (int(r["x2"]), int(r["y2"]))],
                               outline=color, width=width)
            except Exception:
                continue

        display(image)
        print(f"✅ Displayed {len(subset)} boxes from {img_path.name} (scale={scale:.3f})")

    except Exception as e:
        print(f"❌ Error visualizing {image_path}: {e}")



def group_ocr_words(df, y_tolerance=10):
    """
    Groups nearby words (same y range) into full text lines.
    This merges OCR word-level detections into coherent lines.
    """

    if "text" in df.columns:
        df = df.rename(columns={"text": "line"})
    df = df.dropna(subset=["line"])
    df = df[df["line"].str.strip() != ""].copy()
    df = df.sort_values(["y0", "x0"]).reset_index(drop=True)

    # Group words that are on the same text line (close y0)
    groups, current, last_y = [], [], None
    for _, row in df.iterrows():
        if last_y is None or abs(row["y0"] - last_y) <= y_tolerance:
            current.append(row)
        else:
            groups.append(current)
            current = [row]
        last_y = row["y0"]
    if current:
        groups.append(current)

    # Merge groups into single lines
    merged = []
    for g in groups:
        line_text = " ".join(w["line"] for _, w in pd.DataFrame(g).iterrows())
        merged.append({
            "filename": g[0]["filename"],
            "x0": min(w["x0"] for w in g),
            "y0": min(w["y0"] for w in g),
            "x2": max(w["x2"] for w in g),
            "y2": max(w["y2"] for w in g),
            "line": line_text.strip()
        })

    return pd.DataFrame(merged)


def read_entities(path: Path):
    """
    Read a JSON file and return a clean DataFrame with
    company, date, address, and total fields.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame([data])
        df = df.rename(columns=str.lower)  # normalize case
        return df
    except Exception as e:
        print(f"⚠️ Error reading entity file {path.name}: {e}")
        return pd.DataFrame(columns=["company", "address", "date", "total"])