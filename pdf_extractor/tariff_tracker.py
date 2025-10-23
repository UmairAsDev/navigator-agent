from unstructured.partition.pdf import partition_pdf
import re, hashlib, warnings, os
warnings.filterwarnings("ignore", message=".*No languages specified.*")


import re
import hashlib
import os
from unstructured.partition.pdf import partition_pdf

def extract_document(pdf_path: str):
    """Extracts structured text/tables with section grouping + metadata, skipping page 1."""
    # Extract all elements
    elements = partition_pdf(
        filename=pdf_path,
        strategy="hi_res",
        include_metadata=True,
        infer_table_structure=True,
        languages=["eng"]
    )

    blocks = []
    current_section = None
    current_country = None
    doc_source = os.path.basename(pdf_path)

    section_pattern = re.compile(
        r"^(Section\s*\d+(\.\d+)?|Chapter\s*\d+|Updates\s+and\s+relevant\s+publications|Back\s+to\s+top|Key\s+Tariff\s+Measures|[A-Z][A-Z ]{3,})",
        re.IGNORECASE
    )
    country_pattern = re.compile(
        r"\b(China|India|United\s+States|Vietnam|Mexico|Russia|Canada|Korea|Taiwan|Germany|Brazil|Japan|United\s+Kingdom|EU|European\s+Union)\b",
        re.IGNORECASE
    )
    header_categories = {"title", "heading", "heading1", "heading2", "section header", "header"}

    prev_page = None

    for i, el in enumerate(elements):
        text = (getattr(el, "text", "") or "").strip()
        if not text:
            continue

        meta = el.metadata.to_dict() if el.metadata else {}
        page = meta.get("page_number")

        # Skip all elements that come from page 1
        if page == 1:
            continue

        cat = getattr(el, "category", "")
        cat_lower = cat.lower()

        # Debug print if you need
        if i < 10:
            print(f"[DEBUG] #{i} page={page} cat={cat} text={repr(text)[:50]}")

        # Detect if this element is a header/section break
        is_header = False
        if section_pattern.match(text):
            is_header = True
        elif cat_lower in header_categories:
            is_header = True
        elif page is not None and prev_page is not None and page != prev_page:
            # new page start might indicate section
            is_header = True

        if is_header:
            # If the header mentions a country, update current_country
            m_country = country_pattern.search(text)
            if m_country:
                current_country = m_country.group(1)
            current_section = text.strip()
            prev_page = page
            continue

        # Otherwise treat as content block
        m2 = country_pattern.search(text)
        found_country = current_country
        if m2:
            found_country = m2.group(1)

        block = {
            "id": i,
            "text": text,
            "category": cat,
            "page": page,
            "checksum": hashlib.sha256(text.encode()).hexdigest(),
            "is_table": (cat_lower == "table"),
            "metadata": {
                "doc_source": doc_source,
                "section_title": current_section or "Unknown Section",
                "country_mentions": found_country,
                "page_number": page,
                "type": cat,
            },
        }
        blocks.append(block)
        prev_page = page

    return blocks
