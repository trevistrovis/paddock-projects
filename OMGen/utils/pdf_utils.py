# utils/pdf_utils.py
import tempfile
import fitz  # PyMuPDF
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os
from PyPDF2 import PdfMerger, PdfReader
import re
import logging
import time  # Added for timestamp generation

logger = logging.getLogger(__name__)

# Common equipment terms and their variations (expanded)
EQUIPMENT_TERMS = {
    "filter": ["filter", "filtration", "filtering", "horizontal filter", "vertical filter", "sand filter", "cartridge filter", "diatomaceous filter", "DE filter"],
    "grate": ["grate", "grating", "floor grating", "gutter grating", "deck grate", "pool grate"],
    "platform": ["platform", "walkway", "catwalk", "mezzanine", "starting platform", "starting block"],
    "bulkhead": ["bulkhead", "bulkhead door"],
    "pump": ["pump", "pumping", "circulation", "circulation pump", "variable speed pump", "vs pump"],
    "tank": ["tank", "vessel", "container", "fiberglass tank", "fiberglass vessel", "fiberglass container", "surge tank"],
    "valve": ["valve", "control valve", "check valve", "butterfly valve", "gate valve", "ball valve"],
    "strainer": ["strainer", "screen", "separator", "hair strainer", "lint strainer"],
    "meter": ["meter", "flow meter", "gauge", "flow gauge"],
    "sensor": ["sensor", "detector", "probe", "pressure sensor", "temperature sensor"],
    "gutter": ["gutter", "gutter grating", "gutter grate", "perimeter overflow", "recirculation", "recirculation system", "overflow gutter"],
    "main drain": ["main drain", "main drain grate", "sump pump", "MD", "md", "main drain cover"],
    "regenerator": ["regenerator", "regen", "regenerative", "regenerative filter", "regen filter", "regenerative filtration", "regen filtration"],
    "evacuator": ["evacuator", "evac", "evacuator system", "vacuum system"],
    "heater": ["heater", "heat pump", "gas heater", "electric heater"],
    "chlorinator": ["chlorinator", "salt chlorinator", "salt cell", "chlorine generator"],
    "light": ["light", "lighting", "led light", "pool light", "underwater light"],
    "skimmer": ["skimmer", "surface skimmer", "automatic skimmer"],
    "drain": ["drain", "floor drain", "bottom drain", "drain cover"]
}

# Model number patterns (more flexible)
MODEL_PATTERNS = [
    r'PPEC\d+S',  # PPEC model numbers
    r'\d{3,4}S',  # 3-4 digit numbers ending in S
    r'VSC\d+',    # VSC model numbers
    r'[A-Z]{2,4}\d{3,4}[A-Z]?',  # General model pattern (letters + numbers + optional letter)
]

# Known pool/section labels used as sales-order separators.
# Keep them specific to avoid matching equipment descriptions that happen to contain "pool".
DEFAULT_POOL_LABELS = [
    "main pool", "spa", "wading pool", "kiddie pool", "childrens pool",
    "splash pad", "plunge pool", "therapy pool", "lap pool", "dive pool",
    "competition pool", "recreational pool", "infinity pool",
    "activity pool", "leisure pool", "training pool"
]


def _extract_keywords_from_lines(lines):
    """Extract equipment keywords from a list of text lines, preserving order."""
    keywords = []
    seen = set()

    for line in lines:
        line_lower = line.lower().strip()
        if not line_lower:
            continue

        # Skip lines that are just numbers or dates
        if line_lower.replace('.', '').replace('/', '').replace('-', '').isdigit():
            logger.debug(f"Skipping line that's just numbers/date: {line_lower}")
            continue

        # Split line by multiple delimiters (commas, tabs, semicolons, pipes)
        delimiters = [',', '\t', ';', '|']
        parts = [line_lower]
        for delim in delimiters:
            new_parts = []
            for part in parts:
                new_parts.extend(part.split(delim))
            parts = new_parts

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Skip parts that are just numbers
            if part.replace('.', '').replace('-', '').isdigit():
                continue

            # Check for equipment terms and their variations (check all categories, don't break)
            for category, variations in EQUIPMENT_TERMS.items():
                if any(term in part for term in variations):
                    # Clean up the line by removing common prefixes and suffixes
                    cleaned_part = clean_product_line(part)
                    if cleaned_part and cleaned_part not in seen:
                        keywords.append(cleaned_part)
                        seen.add(cleaned_part)
                        logger.debug(f"Found keyword in category '{category}': {cleaned_part}")

            # Check for model numbers using patterns
            for pattern in MODEL_PATTERNS:
                if re.search(pattern, part, re.IGNORECASE):
                    if part not in seen:
                        keywords.append(part)
                        seen.add(part)
                        logger.debug(f"Found model number pattern: {part}")

    return keywords


def _identify_pool_header(line, pool_labels=None):
    """Return a clean pool name if the line looks like a pool separator, else None."""
    labels = pool_labels or DEFAULT_POOL_LABELS
    norm = re.sub(r'[^a-z0-9\s]', ' ', line.lower()).strip()

    # Direct exact label match, e.g. "Main Pool", "Spa"
    for label in labels:
        if norm == label:
            return label.title()

    # "Pool 1 - Main Pool", "Pool #2 Main Pool", "Pool 2: Spa", etc.
    m = re.match(r'^pool\s*#?\s*(\d+)\s*[-:\u2013\u2014]\s*(.+)$', norm)
    if m:
        tail = m.group(2).strip()
        # If the tail is a known label, use it; otherwise treat it as a custom name
        return tail.title()

    # "Pool 1" (no label) – use generic name
    m = re.match(r'^pool\s*#?\s*(\d+)\s*$', norm)
    if m:
        return f"Pool {m.group(1)}"

    return None


def sort_files_by_keyword_order(files, keywords):
    """
    Sort file paths by the earliest sales-order keyword that appears in each
    filename. Files that don't match any keyword are placed at the end while
    preserving their relative order.
    """
    if not files or not keywords:
        return list(files)

    def sort_key(path):
        base = os.path.basename(path).lower()
        for i, kw in enumerate(keywords):
            if kw.lower() in base:
                return (i, base)
        return (len(keywords), base)

    return sorted(files, key=sort_key)


def extract_items_from_sales_order(pdf_path):
    """Extract a flat list of equipment keywords from a sales order PDF."""
    logger.info(f"Processing PDF for keywords: {pdf_path}")
    doc = fitz.open(pdf_path)
    all_lines = []
    for page in doc:
        all_lines.extend(page.get_text().split('\n'))
    keywords = _extract_keywords_from_lines(all_lines)
    logger.info(f"Extracted {len(keywords)} keywords from {pdf_path}")
    return keywords


def extract_pools_from_sales_order(pdf_path, pool_labels=None):
    """
    Parse a sales order PDF that separates pools by headings/labels and return
    per-pool equipment keywords.

    Returns:
        tuple (pools_data, pool_keywords_map) where:
        - pools_data: list of {'pool_id': str, 'pool_name': str} dicts
        - pool_keywords_map: dict mapping pool_id -> list of keywords

    If no pool separators are found, a single default pool is returned containing
    all extracted keywords.
    """
    logger.info(f"Processing sales order for pool separators: {pdf_path}")
    doc = fitz.open(pdf_path)
    all_lines = []
    for page in doc:
        all_lines.extend(page.get_text().split('\n'))

    # Group lines by detected pool headers
    default_group = {'pool_name': 'Default', 'lines': []}
    groups = [default_group]
    current = default_group

    for line in all_lines:
        pool_name = _identify_pool_header(line, pool_labels=pool_labels)
        if pool_name:
            new_group = {'pool_name': pool_name, 'lines': []}
            groups.append(new_group)
            current = new_group
        else:
            current['lines'].append(line)

    # Build results; skip the default group if it's empty and we have real headers
    if len(groups) > 1 and not default_group['lines']:
        groups = groups[1:]

    pools_data = []
    pool_keywords_map = {}
    for i, group in enumerate(groups, start=1):
        pool_id = str(i)
        pool_name = group['pool_name']
        keywords = _extract_keywords_from_lines(group['lines'])
        pools_data.append({'pool_id': pool_id, 'pool_name': pool_name})
        pool_keywords_map[pool_id] = keywords
        logger.info(f"Pool '{pool_name}' (id={pool_id}) -> {len(keywords)} keywords")

    return pools_data, pool_keywords_map

def clean_product_line(line):
    """Clean up a product line by removing common prefixes, suffixes, and numbers."""
    # Remove common prefixes
    prefixes_to_remove = ['qty', 'quantity', 'item', 'no.', '#', 'sku', 'part']
    # Remove common suffixes
    suffixes_to_remove = ['ea', 'each', 'unit', 'pc', 'pcs', 'pieces']
    
    words = line.split()
    
    # Remove leading prefixes
    while words and any(words[0].lower().rstrip('.') == prefix for prefix in prefixes_to_remove):
        words.pop(0)
    
    # Remove trailing suffixes
    while words and any(words[-1].lower().rstrip('.') == suffix for suffix in suffixes_to_remove):
        words.pop()
    
    # Remove pure numbers at start or end
    while words and words[0].replace('.','').isdigit():
        words.pop(0)
    while words and words[-1].replace('.','').isdigit():
        words.pop()
    
    return ' '.join(words) if words else ''

def normalize_text(text):
    """Normalize text for better matching by removing special characters and converting to lowercase."""
    # Remove special characters and convert to lowercase
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())
    return ' '.join(text.split())  # Normalize whitespace

def get_associated_documents(equipment_type, template_dir):
    """
    Get associated care and maintenance documents for specific equipment types.
    Returns a list of paths to associated documents.
    """
    # Get the project root directory (one level up from template_dir)
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    maintenance_docs_dir = os.path.join(project_dir, "maintenance_docs")
    logger.info(f"Looking for maintenance docs in: {maintenance_docs_dir}")
    logger.info(f"Equipment type to match: {equipment_type}")
    
    if not os.path.exists(maintenance_docs_dir):
        logger.error(f"Maintenance docs directory not found: {maintenance_docs_dir}")
        return []

    # Get actual files in the maintenance docs directory
    available_docs = [f for f in os.listdir(maintenance_docs_dir) if f.lower().endswith('.pdf')]
    logger.info(f"Found {len(available_docs)} maintenance docs")
    logger.debug(f"Available docs: {available_docs}")

    # Variant-specific mappings. Each key is a specific product variant;
    # during matching the LONGEST key that appears as a substring of the
    # equipment_type wins, so "horizontal 2 cell linkage" beats "horizontal".
    # Shared/general docs (filter tips, etc.) use broad category keys that
    # only match when no more-specific key does.
    maintenance_mappings = {
        # ── Gutter ─────────────────────────────────────────────────────
        "gutter": [
            "Perimeter Overflow & Recirculation System -gutter dwg.pdf",
        ],
        # ── Filters (shared docs for any filter keyword) ───────────────
        "filter": [
            "Filters Notes and Tips  rev06-2026.pdf",
            "General Trouble-Shooting - Filter.pdf",
            "Pressure Filter Winterizing.pdf",
        ],
        # ── Horizontal variants ────────────────────────────────────────
        "horizontal manual linkage": [
            "Horizontal Manual Linkage.pdf",
        ],
        "horizontal manual valves": [
            "Horizontal Manual Valves.pdf",
        ],
        "horizontal no face piping": [
            "Horizontal No Face Piping.pdf",
        ],
        "horizontal stacked": [
            "Horizontal Stacked with Manual Linkage.pdf",
        ],
        "horizontal 2 cell linkage": [
            "Horizontal - 2 Cell Linkage Valves 2026.pdf",
        ],
        "horizontal 2 cell manual": [
            "Horizontal - 2 Cell Manual Valves 2026.pdf",
        ],
        # ── Verticel variants ──────────────────────────────────────────
        "2c verticel manual linkage": [
            "2C Verticel Manual Linkage.pdf",
        ],
        "2c verticel manual valves": [
            "2C Verticel Manual Valves.pdf",
        ],
        "3c verticel": [
            "3C Verticel Manual Valves.pdf",
        ],
        # ── Regen ──────────────────────────────────────────────────────
        "regen": [
            "PPEC REGEN Installation and Operations Manual Updated 8-26.pdf",
            "PPEC Regen O&M Manual Rev.04-11-24(10-2023)2.pdf",
        ],
        # ── Vacsand ────────────────────────────────────────────────────
        "vacsand": [
            "Vacuum Sand Filter Winterizing & Trouble Shooting.pdf",
        ],
        # ── Compak ─────────────────────────────────────────────────────
        "compak": [
            "Compak Manual VSC with Air Scour-Evacuator 2026.pdf",
        ],
        # ── High Flow variants ─────────────────────────────────────────
        "high flow 4 manual valve": [
            "High Flow With 4 Manual Valve 2026.pdf",
        ],
        "high flow linkage": [
            "High Flow with Linkage Valves 2026.pdf",
        ],
        # ── Bulkhead ───────────────────────────────────────────────────
        "bulkhead": [
            "Bulkhead HDPE O_M r6-2026.pdf",
        ],
        # ── Main Drain ─────────────────────────────────────────────────
        "main drain": [
            "Paddock IAPMO R&T Main Drain Manual rev 05-2024r1.pdf",
        ],
        # ── Evacuator ──────────────────────────────────────────────────
        "evacuator": [
            "EvacuatorSystems- Care&Maintenance.pdf",
        ],
        # ── Deck Drain ─────────────────────────────────────────────────
        "deck drain": [
            "Modular Deck Drain & or Deck Drain+Evacuator Care&Maintenance.pdf",
        ],
        # ── Grating ────────────────────────────────────────────────────
        "grating": [
            "Grating HDPE.pdf",
        ],
    }

    logger.info(f"Checking equipment type: {equipment_type}")
    associated_docs = []
    eq_lower = equipment_type.lower().strip()

    # Find the LONGEST mapping key whose words ALL appear in the equipment_type.
    # This ensures "horizontal 2 cell linkage" wins over "horizontal", while
    # still matching when extra words appear (e.g. "high flow with linkage valves"
    # matches key "high flow linkage").
    eq_words = set(eq_lower.split())
    best_key = None
    best_len = 0
    for key in maintenance_mappings:
        key_words = set(key.split())
        if key_words.issubset(eq_words) and len(key) > best_len:
            best_key = key
            best_len = len(key)

    if best_key:
        logger.info(f"Best mapping key for '{equipment_type}': '{best_key}'")
        doc_list = maintenance_mappings[best_key]
    else:
        doc_list = []

    logger.info(f"Looking for {len(doc_list)} mapped maintenance docs")
    for doc in doc_list:
        if doc in available_docs:
            full_path = os.path.join(maintenance_docs_dir, doc)
            associated_docs.append(full_path)
            logger.info(f"Found maintenance doc: {doc}")
        else:
            doc_lower = doc.lower()
            matches = [f for f in available_docs if f.lower() == doc_lower]
            if matches:
                full_path = os.path.join(maintenance_docs_dir, matches[0])
                associated_docs.append(full_path)
                logger.info(f"Found maintenance doc (case-insensitive): {matches[0]}")
            else:
                logger.warning(f"Maintenance document not found: {doc}")

    # Exact-name fallback: if the equipment_type closely matches a filename
    # on disk (e.g. "2C Verticel Manual Linkage" -> "2C Verticel Manual Linkage.pdf"),
    # include it.  Require the normalized strings to be EQUAL (not just substrings)
    # to avoid false positives across variants.
    if equipment_type and len(equipment_type.strip()) >= 10:
        norm_eq = normalize_text(equipment_type)
        for f in available_docs:
            base_no_ext = os.path.splitext(f)[0]
            norm_base = normalize_text(base_no_ext)
            if norm_base and norm_eq == norm_base:
                full_path = os.path.join(maintenance_docs_dir, f)
                if full_path not in associated_docs:
                    associated_docs.append(full_path)
                    logger.info(f"Exact-name maintenance doc match for '{equipment_type}': {f}")

    if not associated_docs:
        logger.warning(f"No maintenance docs found for equipment type: {equipment_type}")
    else:
        logger.info(f"Found {len(associated_docs)} maintenance docs")
        logger.debug(f"Full paths: {associated_docs}")

    return associated_docs

def parse_cover_sheets(cover_sheets_dir):
    """
    Parse the cover_sheets folder into distinct roles:
      - main_cover:   unnumbered sheet intended as the manual cover (prefers a
                      filename containing 'cover'; falls back to first unnumbered PDF)
      - attention_page: unnumbered sheet whose filename contains 'attention'
      - other_unnumbered: any remaining unnumbered PDFs, sorted alphabetically
      - numbered_covers: list of (number, path) tuples sorted by leading number
    """
    if not os.path.isdir(cover_sheets_dir):
        logger.warning(f"Cover sheets directory not found: {cover_sheets_dir}")
        return {
            'main_cover': None,
            'attention_page': None,
            'other_unnumbered': [],
            'numbered_covers': []
        }

    numbered = []
    unnumbered = []
    for f in sorted(os.listdir(cover_sheets_dir)):
        if not f.lower().endswith('.pdf'):
            continue
        full_path = os.path.join(cover_sheets_dir, f)
        m = re.match(r'^(\d+)', f)
        if m:
            numbered.append((int(m.group(1)), full_path))
        else:
            unnumbered.append(full_path)

    numbered.sort(key=lambda x: x[0])
    unnumbered.sort(key=lambda x: os.path.basename(x).lower())

    # Identify the attention page and the main cover from unnumbered sheets
    attention_page = None
    attention_candidates = [p for p in unnumbered if 'attention' in os.path.basename(p).lower()]
    if attention_candidates:
        attention_page = attention_candidates[0]

    # Prefer the specific "Operation & Maintenance cover page" as the main cover
    main_cover = None
    for p in unnumbered:
        if 'operation & maintenance cover page' in os.path.basename(p).lower():
            main_cover = p
            break

    if main_cover is None:
        cover_candidates = [p for p in unnumbered
                            if 'cover' in os.path.basename(p).lower() and p != attention_page]
        main_cover = cover_candidates[0] if cover_candidates else None

    # Fallback: use the first unnumbered sheet that is not the attention page
    if main_cover is None and unnumbered:
        non_attention = [p for p in unnumbered if p != attention_page]
        if non_attention:
            main_cover = non_attention[0]

    other_unnumbered = [p for p in unnumbered if p != main_cover and p != attention_page]
    # Suppress alternate "cover page" files so only the chosen main cover is used as a cover
    other_unnumbered = [p for p in other_unnumbered if 'cover page' not in os.path.basename(p).lower()]

    logger.info(
        f"Parsed cover sheets in {cover_sheets_dir}: "
        f"main_cover={os.path.basename(main_cover) if main_cover else None}, "
        f"attention_page={os.path.basename(attention_page) if attention_page else None}, "
        f"numbered={len(numbered)}, other_unnumbered={len(other_unnumbered)}"
    )
    return {
        'main_cover': main_cover,
        'attention_page': attention_page,
        'other_unnumbered': other_unnumbered,
        'numbered_covers': numbered
    }


def _basename_matches_keywords(path, keywords):
    """Check whether a file's basename contains any of the supplied keywords."""
    if not keywords:
        return False
    base = os.path.basename(path).lower()
    return any(k.lower() in base for k in keywords)


def _organize_with_cover_sheets(cover_page, cover_info, templates, maintenance_docs,
                                job_files, warranty_docs=None, equipment_list=None,
                                pools_data=None, pool_templates=None,
                                pool_maintenance=None, pool_gutter_care=None,
                                always_include_dir=None):
    """
    Build the manual using the cover_sheets folder structure:
      cover_page -> attention page -> unnumbered covers -> numbered section covers
      with matching maintenance docs/templates grouped under each numbered cover.

    When pools_data is provided, pool-specific items are further grouped under
    a generated "Pool: <name>" sub-header within each section, while the numbered
    cover sheets themselves are kept in their original order.
    """
    organized_files = []

    # 1. Main cover page
    if cover_page:
        organized_files.append(cover_page)
        logger.info(f"Added main cover page: {os.path.basename(cover_page)}")

    # 2. Attention page
    if cover_info.get('attention_page'):
        organized_files.append(cover_info['attention_page'])
        logger.info(f"Added attention page: {os.path.basename(cover_info['attention_page'])}")

    # 3. Any other unnumbered cover sheets
    for p in cover_info.get('other_unnumbered', []):
        organized_files.append(p)
        logger.info(f"Added extra unnumbered cover sheet: {os.path.basename(p)}")

    # 4. Always-include docs after the front matter (TOC, special instructions)
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    maint_dir = os.path.join(project_dir, "maintenance_docs")
    for p in [os.path.join(maint_dir, "table_of_contents.pdf"),
              os.path.join(maint_dir, "special_instructions.pdf")]:
        if os.path.exists(p) and p.lower().endswith('.pdf'):
            organized_files.append(p)
            logger.info(f"Inserted always-include doc after front matter: {os.path.basename(p)}")

    pool_names = {p['pool_id']: p['pool_name'] for p in (pools_data or [])}
    multi_pool = len(pool_names) > 1

    # Map each pool-specific item back to its pool id. Items that appear in
    # multiple pools are treated as shared and are not assigned to any pool.
    template_pool_counts = {}
    template_pool_map = {}
    for pid, pt_list in (pool_templates or {}).items():
        for t in pt_list:
            template_pool_counts[t] = template_pool_counts.get(t, 0) + 1
    for pid, pt_list in (pool_templates or {}).items():
        for t in pt_list:
            if template_pool_counts.get(t, 0) == 1:
                template_pool_map[t] = pid

    maintenance_pool_counts = {}
    maintenance_pool_map = {}
    for pid, pm_list in (pool_maintenance or {}).items():
        for m in pm_list:
            maintenance_pool_counts[m] = maintenance_pool_counts.get(m, 0) + 1
    for pid, gc_list in (pool_gutter_care or {}).items():
        for m in gc_list:
            maintenance_pool_counts[m] = maintenance_pool_counts.get(m, 0) + 1
    for pid, pm_list in (pool_maintenance or {}).items():
        for m in pm_list:
            if maintenance_pool_counts.get(m, 0) == 1:
                maintenance_pool_map[m] = pid
    for pid, gc_list in (pool_gutter_care or {}).items():
        for m in gc_list:
            if maintenance_pool_counts.get(m, 0) == 1:
                maintenance_pool_map[m] = pid

    # Combine shared items (templates/maintenance_docs) with pool-specific items
    all_templates = list(templates or [])
    if pool_templates:
        for pt_list in pool_templates.values():
            all_templates.extend(pt_list)

    all_maintenance = list(maintenance_docs or [])
    if pool_maintenance:
        for pm_list in pool_maintenance.values():
            all_maintenance.extend(pm_list)
    if pool_gutter_care:
        for gc_list in pool_gutter_care.values():
            all_maintenance.extend(gc_list)

    # Exclude docs placed explicitly elsewhere to avoid duplication
    always_exclude_basenames = {'equipment list.pdf'}
    all_maintenance = [m for m in all_maintenance
                       if os.path.basename(m).lower() not in always_exclude_basenames]

    # Deduplicate while preserving order
    seen = set()
    deduped_templates = []
    for t in all_templates:
        if t not in seen:
            deduped_templates.append(t)
            seen.add(t)
    all_templates = deduped_templates

    seen = set()
    deduped_maintenance = []
    for m in all_maintenance:
        if m not in seen:
            deduped_maintenance.append(m)
            seen.add(m)
    all_maintenance = deduped_maintenance

    # Keywords that decide which section a document belongs under
    section_keywords = {
        2: ['perimeter', 'overflow', 'recirculation', 'gutter'],
        3: ['filter'],
    }

    assigned_templates = set()
    assigned_maintenance = set()

    def _emit_grouped(items, pool_map, used_set, section_num, item_label):
        """Place items matching the current section keywords, grouped by pool."""
        by_pool = {pid: [] for pid in pool_names}
        shared = []
        keywords = section_keywords[section_num]

        for item in items:
            if item in used_set:
                continue
            if not _basename_matches_keywords(item, keywords):
                continue
            pid = pool_map.get(item)
            if multi_pool and pid:
                by_pool.setdefault(pid, []).append(item)
            else:
                shared.append(item)

        # Emit pool groups in the order the pools were defined
        for pool in (pools_data or []):
            pid = pool['pool_id']
            if by_pool.get(pid):
                organized_files.append(create_section_header(f"Pool: {pool_names[pid]}"))
                for item in by_pool[pid]:
                    organized_files.append(item)
                    used_set.add(item)
                    logger.info(f"  {item_label} under section {section_num} for pool {pool_names[pid]}: {os.path.basename(item)}")

        # Shared items (no pool assignment or single-pool mode)
        for item in shared:
            organized_files.append(item)
            used_set.add(item)
            logger.info(f"  {item_label} under section {section_num}: {os.path.basename(item)}")

    def _emit_remaining(items, pool_map, used_set, item_label):
        """Place remaining items, grouped by pool when a pool assignment exists."""
        by_pool = {pid: [] for pid in pool_names}
        shared = []

        for item in items:
            if item in used_set:
                continue
            pid = pool_map.get(item)
            if multi_pool and pid:
                by_pool.setdefault(pid, []).append(item)
            else:
                shared.append(item)

        for pool in (pools_data or []):
            pid = pool['pool_id']
            if by_pool.get(pid):
                organized_files.append(create_section_header(f"Pool: {pool_names[pid]}"))
                for item in by_pool[pid]:
                    organized_files.append(item)
                    used_set.add(item)
                    logger.info(f"Placed remaining {item_label} in section 4 for pool {pool_names[pid]}: {os.path.basename(item)}")

        for item in shared:
            organized_files.append(item)
            used_set.add(item)
            logger.info(f"Placed remaining {item_label} in section 4: {os.path.basename(item)}")

    numbered = sorted(cover_info.get('numbered_covers', []), key=lambda x: x[0])

    for num, cover_path in numbered:
        # Insert always-include docs right before the warranty/drawings section cover
        if num == 5 and always_include_dir and os.path.isdir(always_include_dir):
            for ai in sorted(os.listdir(always_include_dir)):
                if ai.lower().endswith('.pdf'):
                    ai_path = os.path.join(always_include_dir, ai)
                    organized_files.append(ai_path)
                    logger.info(f"Inserted always-include doc before warranty section: {ai}")

        organized_files.append(cover_path)
        logger.info(f"Added numbered section cover ({num}): {os.path.basename(cover_path)}")

        if num in section_keywords:
            _emit_grouped(all_templates, template_pool_map, assigned_templates, num, "Template")
            _emit_grouped(all_maintenance, maintenance_pool_map, assigned_maintenance, num, "Maintenance doc")

        elif num == 4:
            # Section 4 is the catch-all for remaining maintenance/product info
            _emit_remaining(all_templates, template_pool_map, assigned_templates, "template")
            _emit_remaining(all_maintenance, maintenance_pool_map, assigned_maintenance, "maintenance doc")

        elif num == 5:
            # Section 5: Warranty & Drawings -- project docs and warranty docs
            if equipment_list:
                organized_files.append(equipment_list)
                logger.info(f"Added equipment list: {os.path.basename(equipment_list)}")
            if job_files:
                organized_files.extend(job_files)
                logger.info(f"Added {len(job_files)} job files under warranty/drawings section")
            if warranty_docs:
                organized_files.extend(warranty_docs)
                logger.info(f"Added {len(warranty_docs)} warranty docs under warranty/drawings section")

    # If no #5 numbered cover exists, fall back to legacy project/warranty sections
    if not any(num == 5 for num, _ in numbered):
        if equipment_list or job_files:
            project_header = create_section_header("Project Documentation")
            organized_files.append(project_header)
            logger.info("Added project documentation header to organized files")
            if equipment_list:
                organized_files.append(equipment_list)
                logger.info(f"Added equipment list: {os.path.basename(equipment_list)}")
            organized_files.extend(job_files or [])
            logger.info(f"Added {len(job_files or [])} job files to organized files")

        if warranty_docs:
            warranty_header = create_section_header("Warranty Documents")
            organized_files.append(warranty_header)
            logger.info("Added warranty header to organized files")
            organized_files.extend(warranty_docs)
            logger.info(f"Added {len(warranty_docs)} warranty docs to organized files")

    logger.info(f"Total organized files: {len(organized_files)}")
    return organized_files


def find_warranty_documents(keywords):
    """
    Find warranty documents by matching sales order-derived keywords against
    filenames in the 'warranty_docs' folder. Matching is case-insensitive and
    uses normalized text to allow partial matches and model numbers.

    Each detected equipment category maps to one specific warranty filename
    pattern so that only the correct document is included, not every variant.

    Args:
        keywords: List of strings extracted from sales order/job PDFs

    Returns:
        Sorted list of absolute paths to matched warranty PDF files
    """
    try:
        # Resolve project directory and warranty docs folder
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        warranty_dir = os.path.join(project_dir, "warranty_docs")
        logger.info(f"Looking for warranty docs in: {warranty_dir}")

        if not os.path.exists(warranty_dir):
            logger.warning(f"Warranty docs directory not found: {warranty_dir}")
            return []

        # Collect available warranty PDFs
        warranty_files = [f for f in os.listdir(warranty_dir) if f.lower().endswith('.pdf')]
        if not warranty_files:
            logger.info("No warranty PDF files found.")
            return []

        # 1) Normalize all extracted keywords to lowercase for consistent matching
        norm_kws = {normalize_text(k) for k in (keywords or []) if k}

        # Model number cues for regenerator filters
        model_cues = {"ppec", "1400s", "1200s", "2100s", "500s", "700s", "225s", "900s", "350s"}
        # If the sales order explicitly references a Vacsand/Compak vacuum sand
        # filter, do NOT infer a regenerator from shared model-number cues.
        has_vacsand = any("vacuum" in k or "vacsand" in k or "compak" in k for k in norm_kws)
        has_regen_model = not has_vacsand and any(
            any(cue in k for cue in model_cues) for k in norm_kws
        )

        # 2) Detect specific equipment categories from keywords.
        #    Each category key maps to EXACT filename substrings (after normalization)
        #    so only the right warranty file is pulled in — not every variant.
        #    Categories with multiple possible filenames (e.g., gutter sub-types)
        #    are broken into distinct sub-categories so the match stays tight.
        detected_categories = set()

        # --- Filters ---
        # Require BOTH "horizontal"/"horiz" AND "filter" together to avoid false positives
        if any("horizontal" in k and "filter" in k for k in norm_kws) or \
           any("horiz" in k and "filter" in k for k in norm_kws):
            detected_categories.add("horiz sand filter")
        if any("high flow" in k and "filter" in k for k in norm_kws) or \
           any("high flow sand" in k for k in norm_kws):
            detected_categories.add("high flow sand filter")
        if any("fiberglass" in k and "filter" in k for k in norm_kws) or \
           any("fiberglass sand" in k for k in norm_kws):
            detected_categories.add("fiberglass sand filter")
        # "compak" is normalized lowercase; fix previous case-sensitive bug
        if any("vacuum" in k and "filter" in k for k in norm_kws) or \
           any("compak" in k for k in norm_kws):
            detected_categories.add("vacuum sand filter")
            detected_categories.add("compak")
        if any("verticel" in k and "filter" in k for k in norm_kws) or \
           any("verticel sand" in k for k in norm_kws):
            detected_categories.add("verticel sand filter")
        if has_regen_model or any("regenerator" in k or "regen" in k for k in norm_kws):
            detected_categories.add("regenerator")

        # --- Gutter: detect the most specific sub-type from the sales order ---
        # Each sub-type maps to exactly one warranty file to avoid pulling in all variants.
        has_gutter = any("gutter" in k for k in norm_kws)
        if has_gutter:
            has_hdpe_gutter = any("hdpe" in k and "gutter" in k for k in norm_kws)
            has_hdpe_grating = any("hdpe" in k and ("grat" in k or "grating" in k) for k in norm_kws)
            if has_hdpe_gutter:
                detected_categories.add("gutter_hdpe")
            elif has_hdpe_grating:
                detected_categories.add("gutter_hdpe_grating_only")
            else:
                detected_categories.add("gutter_std")

        # --- Main drain: require the full phrase, not just "MD" abbreviation ---
        if any("main drain" in k or "main drain" in k.replace("_", " ") for k in norm_kws):
            detected_categories.add("main_drain")

        # --- Starting platform ---
        if any("starting platform" in k or "starting platform" in k.replace("_", " ") for k in norm_kws):
            detected_categories.add("starting_platform")

        # --- Strainer ---
        if any("strainer" in k for k in norm_kws):
            detected_categories.add("strainer")

        # --- Evacuator ---
        if any("evacuator" in k for k in norm_kws):
            detected_categories.add("evacuator")

        # --- Bulkhead ---
        if any("bulkhead" in k for k in norm_kws):
            detected_categories.add("bulkhead")

        # NOTE: "pump" and "valve" are intentionally omitted — there are no
        # pump/valve-specific warranty files in warranty_docs; those are covered
        # by maintenance docs and the always-included Sales Bulletin.

        logger.info(f"Detected equipment categories for warranty: {sorted(detected_categories) if detected_categories else 'none'}")

        # 3) Map each detected category to the exact filename substring(s) it should match.
        #    Keep each list as narrow as possible — one entry per actual warranty file.
        category_patterns = {
            "horiz sand filter":       ["horiz sand filter"],
            "high flow sand filter":   ["high flow sand filter"],
            "fiberglass sand filter":  ["fiberglass sand filter"],
            "vacuum sand filter":      ["vacuum sand filter"],
            "compak":                  ["compak"],
            "verticel sand filter":    ["verticel sand filter"],
            "regenerator":             ["regenerator warranty", "regenerator"],
            # Gutter sub-types — the actual warranty file is
            # "GutterSTD 1yrHDPE-STD 10yrWARRANTY rev 6-2026.pdf"
            "gutter_hdpe":             ["gutterstd"],
            "gutter_hdpe_grating_only":["gutterstd"],
            "gutter_std":              ["gutterstd"],
            # Other equipment
            "strainer":                ["ss strainer"],
            "evacuator":               ["evac system", "evacuator"],
            "main_drain":              ["main drain installation warranty", "md installation"],
            "starting_platform":       ["starting platform"],
            "bulkhead":                ["bulkheadwhdpe", "pvc ibar", "bulkhead"],
        }

        # If nothing specific detected, be conservative: return empty to avoid bloat
        if not detected_categories:
            logger.info("No specific equipment categories detected for warranty; returning no warranty docs to avoid bloat.")
            return []

        # 4) Match only files whose normalized name contains one of the strict patterns
        matched_paths = []
        for fname in warranty_files:
            base_no_ext = os.path.splitext(fname)[0]
            norm_name = normalize_text(base_no_ext)
            if not norm_name:
                continue

            include = False
            for cat in detected_categories:
                patterns = category_patterns.get(cat, [])
                for pat in patterns:
                    pat_norm = normalize_text(pat)
                    if pat_norm and pat_norm in norm_name:
                        include = True
                        logger.info(f"Matched warranty '{fname}' via category '{cat}' pattern '{pat}'")
                        break
                if include:
                    break

            if include:
                matched_paths.append(os.path.join(warranty_dir, fname))

        # 5) Deduplicate and sort by filename for stability
        result = sorted(set(matched_paths), key=lambda p: os.path.basename(p).lower())
        logger.info(f"Total matched warranty docs (strict): {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Error searching warranty documents: {e}")
        return []

def match_templates(keywords, template_dir, flow_data=None, filters_data=None, template_mappings=None, gutter_data=None, use_only_selected=False, output_dir=None):
    """
    Match templates and include associated maintenance documents with improved matching algorithm.
    
    Args:
        keywords: List of keywords to match against templates
        template_dir: Directory containing templates
        flow_data: Optional dictionary containing flow rate information (for backward compatibility)
        filters_data: Optional list of dictionaries containing flow rate information for multiple filters
        
    Returns:
        tuple (templates, maintenance_docs) where:
        - templates: List of matched template file paths (with flow data filled if provided)
        - maintenance_docs: List of matched maintenance document paths
    """
    logger.info("========== STARTING TEMPLATE MATCHING ==========")
    logger.info(f"Keywords: {keywords}")
    logger.info(f"Template directory: {template_dir}")
    logger.info(f"Flow data provided: {flow_data is not None}")
    logger.info(f"Filters data provided: {filters_data is not None}")
    logger.info(f"Gutter data provided: {gutter_data is not None}")
    logger.info(f"Use only selected templates: {use_only_selected}")
    if filters_data:
        logger.info(f"Number of filters: {len(filters_data)}")
        for i, filter_data in enumerate(filters_data):
            logger.info(f"Filter {i+1}: {filter_data.get('filter_name', f'Filter {i+1}')}")
    matched_templates = set()
    matched_maintenance = set()
    
    # Ensure template directory exists
    if not os.path.exists(template_dir):
        logger.error(f"Template directory not found: {template_dir}")
        return [], []
    
    # Process keywords to create more flexible search terms
    search_terms = set()
    for keyword in keywords:
        # Add original keyword and its normalized version
        search_terms.add(keyword.lower())  # Keep original format for model numbers
        normalized = normalize_text(keyword)
        search_terms.add(normalized)
        
        # Add individual words from multi-word keywords
        words = normalized.split()
        for word in words:
            if len(word) > 3:  # Only add words longer than 3 characters to avoid noise
                search_terms.add(word)
                
        # Special handling for model numbers
        if any(model in keyword.upper() for model in ['PPEC', '1400S', '1200S', '2100S', '500S', '700S', '225S', '900S', '350S']):
            # Add variations of the model number
            parts = re.split(r'[^a-zA-Z0-9]+', keyword)
            search_terms.update(part.lower() for part in parts if part)
    
    logger.info(f"Generated {len(search_terms)} search terms from {len(keywords)} keywords")
    logger.debug(f"Search terms: {search_terms}")
    
    # If explicit template selections are provided, use ONLY those and skip keyword-based matching
    if use_only_selected and not template_mappings:
        logger.info("use_only_selected=True but no template mappings provided; returning no templates.")
        template_list = []
    elif template_mappings:
        logger.info("Template mappings provided; using only the explicitly selected templates.")
        template_list = []
        for selected_name in template_mappings.keys():
            path = os.path.join(template_dir, os.path.basename(selected_name))
            if os.path.exists(path):
                template_list.append(path)
                logger.info(f"Selected template: {os.path.basename(path)}")
            else:
                logger.warning(f"Selected template not found on disk: {selected_name}")
    else:
        # First handle templates (prioritized as per user requirement)
        template_files = [f for f in os.listdir(template_dir) if f.lower().endswith('.pdf')]
        logger.info(f"Found {len(template_files)} template files")
        
        flow_related_terms = {'flow', 'gpm', 'rate', 'pump', 'filter', 'circulation'}
        
        # First pass: Find flow-related templates
        flow_templates = set()
        for filename in template_files:
            file_path = os.path.join(template_dir, filename)
            normalized_filename = normalize_text(os.path.splitext(filename)[0])
            words_in_filename = set(normalized_filename.split())
            
            # Check if this is a flow-related document
            if bool(words_in_filename & flow_related_terms):
                flow_templates.add(file_path)
        
        # Second pass: Match all templates with improved precision
        for filename in template_files:
            file_path = os.path.join(template_dir, filename)
            normalized_filename = normalize_text(os.path.splitext(filename)[0])
            words_in_filename = set(normalized_filename.split())
            
            # Track match quality (higher is better)
            match_quality = 0
            matching_terms = set()
            has_exact_keyword_match = False
            
            # Give higher weight to specific equipment types over generic terms
            specific_terms = {'regen', 'regenerator', 'vertical', 'horizontal', 'vacsand', 'cell', 'stacked', 'linkage', 'actuated', 'manual', 'ppec'}
            generic_terms = {'filter', 'template', 'pdf', 'care', 'maintenance', 'guide'}
            
            for term in search_terms:
                term_lower = term.lower()
                # Check for exact matches first (highest priority)
                if term_lower in normalized_filename:
                    if term_lower in specific_terms:
                        match_quality += 3  # Highest weight for specific equipment types
                        has_exact_keyword_match = True
                    elif term_lower not in generic_terms:
                        match_quality += 2  # Medium weight for non-generic terms
                    else:
                        match_quality += 0.5  # Low weight for generic terms
                    matching_terms.add(term)
                # Then check for partial word matches (lower priority)
                elif any(term_lower in word for word in words_in_filename):
                    if term_lower in specific_terms:
                        match_quality += 2  # Good weight for partial specific matches
                    elif term_lower not in generic_terms:
                        match_quality += 1  # Regular weight for partial non-generic matches
                    # Ignore partial generic term matches entirely
                    matching_terms.add(term)
            
            # Only add file if we have an exact keyword match from the original keywords list
            # This prevents matching based on derived search terms like individual words
            if has_exact_keyword_match:
                matched_templates.add(file_path)
                logger.info(f"Matched template '{filename}' with terms: {matching_terms} (quality: {match_quality}, exact: {has_exact_keyword_match})")
            elif match_quality > 0:
                logger.debug(f"Skipped template '{filename}' - no exact keyword match (quality: {match_quality}, terms: {matching_terms})")
    
    # Then check for maintenance docs based on keywords
    logger.info(f"Checking keywords for maintenance docs")
    for keyword in keywords:
        docs = get_associated_documents(keyword, template_dir)
        if docs:
            matched_maintenance.update(docs)
            logger.info(f"Added {len(docs)} maintenance docs for keyword: {keyword}")
    
    # Convert sets to sorted lists for consistent ordering (only when no explicit selection)
    if not template_mappings:
        template_list = sorted(list(matched_templates))
    maintenance_list = sorted(list(matched_maintenance))
    
    # Prioritize flow-related templates by moving them to the front
    if not template_mappings:
        if flow_templates:
            non_flow = [t for t in template_list if t not in flow_templates]
            template_list = sorted(list(flow_templates)) + non_flow
    
    # NOTE: Filter flow-rate fill-out is disabled. Filter templates are now boilerplate
    # documents and are no longer auto-filled with flow data. The original block is kept
    # below (commented) in case management reverts this decision.
    #
    # # If we have flow data, fill it in the templates
    # if filters_data:
    #     logger.info(f"Filling flow data from {len(filters_data)} filters in matched templates...")
    #     filled_templates = []
    #
    #     # Check if each template has form fields that can be filled with flow data
    #     logger.info(f"Processing {len(template_list)} templates for {len(filters_data)} filters")
    #     logger.info(f"Template paths: {[os.path.basename(t) for t in template_list]}")
    #
    #     # Clear the filled directory ONCE to avoid using old files (only when not using isolated workspace)
    #     if not output_dir:
    #         filled_dir = os.path.join(template_dir, 'filled')
    #         if os.path.exists(filled_dir):
    #             logger.info(f"Clearing filled directory: {filled_dir}")
    #             try:
    #                 for file in os.listdir(filled_dir):
    #                     if file.endswith('.pdf'):
    #                         file_path = os.path.join(filled_dir, file)
    #                         os.remove(file_path)
    #                         logger.info(f"Removed old file: {file}")
    #             except Exception as e:
    #                 logger.error(f"Error clearing filled directory: {str(e)}")
    #
    #     # Log template mappings if provided
    #     if template_mappings:
    #         logger.info(f"Using template mappings: {template_mappings}")
    #
    #     for template_path in template_list:
    #         template_name = os.path.basename(template_path)
    #         logger.info(f"Checking template: {template_name}")
    #         has_flow_fields = check_template_for_flow_fields(template_path)
    #         has_gutter_fields = check_template_for_gutter_fields(template_path)
    #         logger.info(f"Template {template_name} has flow fields: {has_flow_fields}")
    #         logger.info(f"Template {template_name} has gutter fields: {has_gutter_fields}")
    #
    #         if has_flow_fields:
    #             # Check if we have a mapping for this template
    #             mapped_filter_id = None
    #             if template_mappings and template_name in template_mappings:
    #                 mapped_filter_id = template_mappings[template_name]
    #                 logger.info(f"Found mapping for template {template_name}: filter ID {mapped_filter_id}")
    #
    #             # If we have a mapping, use only that filter's data
    #             if mapped_filter_id and filters_data:
    #                 # Find the filter with the matching ID
    #                 matched_filter = None
    #                 for filter_data in filters_data:
    #                     if filter_data.get('filter_id') == mapped_filter_id:
    #                         matched_filter = filter_data
    #                         break
    #
    #                 if matched_filter:
    #                     filter_name = matched_filter.get('filter_name', f"Filter {mapped_filter_id}")
    #                     logger.info(f"Using mapped filter: {filter_name} for template {template_name}")
    #
    #                     # Create a unique name for this filter's copy of the template
    #                     fill_dir = output_dir or os.path.join(os.path.dirname(template_path), 'filled')
    #                     os.makedirs(fill_dir, exist_ok=True)
    #                     base_name = template_name
    #                     filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    #
    #                     # Create a unique identifier
    #                     timestamp = int(time.time() * 1000)
    #                     unique_id = f'{filter_name_safe}_{timestamp}'
    #
    #                     # Fill the template with this filter's data
    #                     filled_path = fill_pdf_form_fields(template_path, matched_filter, filter_name=unique_id, gutter_data=gutter_data, output_dir=output_dir)
    #
    #                     if filled_path:
    #                         logger.info(f"Successfully filled template for {filter_name}, path: {filled_path}")
    #                         filled_templates.append(filled_path)
    #                     else:
    #                         logger.warning(f"Could not fill fields for {filter_name}, using original template")
    #                         filled_templates.append(template_path)
    #                 else:
    #                     logger.warning(f"Mapped filter ID {mapped_filter_id} not found, using original template")
    #                     filled_templates.append(template_path)
    #
    #             # If no mapping or mapping failed, use the old approach (create a copy for each filter)
    #             elif filters_data and not mapped_filter_id:
    #                 logger.info(f"No mapping for template {template_name}, creating copies for filters")
    #
    #                 # Only create multiple copies if there are actually multiple filters
    #                 if len(filters_data) > 1:
    #                     # For templates with flow fields, create a copy for each filter
    #                     for i, filter_data in enumerate(filters_data):
    #                         filter_name = filter_data.get('filter_name', f'Filter {i+1}')
    #                         logger.info(f"Processing filter {i+1}/{len(filters_data)}: {filter_name}")
    #
    #                         # Create a unique name for this filter's copy of the template
    #                         fill_dir = output_dir or os.path.join(os.path.dirname(template_path), 'filled')
    #                         os.makedirs(fill_dir, exist_ok=True)
    #                         base_name = template_name
    #                         filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    #
    #                         # Create a unique identifier
    #                         timestamp = int(time.time() * 1000) + i
    #                         unique_id = f'{filter_name_safe}_{timestamp}'
    #
    #                         # Try to fill the template with this filter's data
    #                         filled_path = fill_pdf_form_fields(template_path, filter_data, filter_name=unique_id, gutter_data=gutter_data, output_dir=output_dir)
    #
    #                         if filled_path:
    #                             logger.info(f"Successfully filled template for {filter_name}, path: {filled_path}")
    #                             filled_templates.append(filled_path)
    #                             break  # Only use the first successful fill
    #
    #                     # If no filter worked, add the original template
    #                     if len(filled_templates) == 0 or filled_templates[-1] != filled_path:
    #                         logger.warning(f"Could not fill template with any filter data, using original")
    #                         filled_templates.append(template_path)
    #                 else:
    #                     # Only one filter, just fill once
    #                     filter_data = filters_data[0]
    #                     filter_name = filter_data.get('filter_name', 'Filter 1')
    #                     logger.info(f"Single filter detected: {filter_name}, filling template once")
    #
    #                     # Create a unique name for this filter's copy of the template
    #                     fill_dir = output_dir or os.path.join(os.path.dirname(template_path), 'filled')
    #                     os.makedirs(fill_dir, exist_ok=True)
    #                     base_name = template_name
    #                     filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    #
    #                     # Create a unique identifier
    #                     timestamp = int(time.time() * 1000)
    #                     unique_id = f'{filter_name_safe}_{timestamp}'
    #
    #                     # Try to fill the template with the filter's data
    #                     filled_path = fill_pdf_form_fields(template_path, filter_data, filter_name=unique_id, gutter_data=gutter_data, output_dir=output_dir)
    #
    #                     if filled_path:
    #                         logger.info(f"Successfully filled template for {filter_name}, path: {filled_path}")
    #                         filled_templates.append(filled_path)
    #                     else:
    #                         logger.warning(f"Could not fill template with filter data, using original")
    #                         filled_templates.append(template_path)
    #             else:
    #                 # No filters data available
    #                 logger.info(f"No filter data available for template {template_name}")
    #                 filled_templates.append(template_path)
    #         else:
    #             # If no flow fields, try gutter-only fill when gutter data is provided
    #             if gutter_data and has_gutter_fields and any(gutter_data.get(k) for k in ['inlet_count', 'inlet_size', 'drawing_number']):
    #                 logger.info(f"Template {os.path.basename(template_path)} has gutter fields and gutter_data; attempting gutter fill")
    #                 filled_path = fill_pdf_form_fields(template_path, {}, filter_name=None, gutter_data=gutter_data, output_dir=output_dir)
    #                 if filled_path:
    #                     filled_templates.append(filled_path)
    #                 else:
    #                     filled_templates.append(template_path)
    #             else:
    #                 # For templates without applicable fields, just add the original once
    #                 logger.info(f"Template {os.path.basename(template_path)} has no applicable fields, adding as-is")
    #                 filled_templates.append(template_path)
    #
    #     logger.info(f"Final template list contains {len(filled_templates)} templates: {[os.path.basename(t) for t in filled_templates]}")
    #
    #     # Ensure we're not losing any templates
    #     if len(filled_templates) < len(filters_data):
    #         logger.warning(f"WARNING: Expected at least {len(filters_data)} templates (one per filter), but only got {len(filled_templates)}")
    #
    #     template_list = filled_templates
    #     logger.info(f"Returning {len(template_list)} templates from match_templates function")
    #
    #     # Double check that the files actually exist
    #     for i, template in enumerate(template_list):
    #         if os.path.exists(template):
    #             logger.info(f"Template {i+1} exists: {os.path.basename(template)}")
    #         else:
    #             logger.error(f"Template {i+1} DOES NOT EXIST: {template}")
    #
    #     logger.info("========== FINISHED TEMPLATE MATCHING ==========")
    # elif flow_data:  # For backward compatibility
    #     logger.info("Filling flow data in matched templates using legacy flow_data...")
    #     filled_templates = []
    #     for template_path in template_list:
    #         filled_path = fill_pdf_form_fields(template_path, flow_data, output_dir=output_dir)
    #         if filled_path:
    #             logger.info(f"Filled template {os.path.basename(template_path)} with flow data")
    #             filled_templates.append(filled_path)
    #         else:
    #             filled_templates.append(template_path)
    #     template_list = filled_templates

    # Gutter-only fill path: templates with gutter form fields (and no flow fields) are still
    # filled with the supplied gutter data. This runs independently of the disabled filter flow-rate
    # fill-out path so gutter templates remain unaffected.
    if gutter_data:
        filled_templates = []
        for template_path in template_list:
            template_name = os.path.basename(template_path)
            has_flow_fields = check_template_for_flow_fields(template_path)
            has_gutter_fields = check_template_for_gutter_fields(template_path)
            if not has_flow_fields and has_gutter_fields and gutter_data.get('drawing_number'):
                logger.info(f"Template {template_name} has gutter fields and gutter_data; attempting gutter fill")
                filled_path = fill_pdf_form_fields(template_path, {}, filter_name=None, gutter_data=gutter_data, output_dir=output_dir)
                if filled_path:
                    filled_templates.append(filled_path)
                else:
                    filled_templates.append(template_path)
            else:
                filled_templates.append(template_path)
        template_list = filled_templates
    
    logger.info(f"Found {len(template_list)} templates and {len(maintenance_list)} maintenance docs")
    return template_list, maintenance_list

def generate_cover_page(customer, job_name, phone, flow_data=None, filters_data=None, output_dir=None, template_path=None):
    """
    Generate a filled cover page using a template PDF with form fields.
    If template_path is provided and exists, it is used; otherwise the function
    looks for a default cover template and falls back to generating a simple
    centered page if none is found.
    """
    import os

    # Resolve the template to use
    if template_path and os.path.exists(template_path):
        pass
    else:
        # Legacy/default template in project root
        default_template = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Cover Sheet Template.pdf")
        if os.path.exists(default_template):
            template_path = default_template
        else:
            logger.error(f"Cover page template not found at: {template_path or default_template}")
            raise FileNotFoundError("Cover page template not found")

    # Create output path
    if output_dir:
        cover_path = os.path.join(output_dir, f"cover_{job_name}.pdf")
    else:
        cover_path = os.path.join("output", f"cover_{job_name}.pdf")
    os.makedirs(os.path.dirname(cover_path), exist_ok=True)

    logger.info(f"Generating cover page for job: {job_name}")
    logger.info(f"Template path: {template_path}")
    logger.info(f"Output path: {cover_path}")

    try:
        from datetime import date as _date
        today = _date.today().strftime('%m/%d/%Y')

        # Open the template PDF
        doc = fitz.open(template_path)
        page = doc[0]  # Get first page

        logger.info(f"Cover values => customer='{customer}', job_name='{job_name}', phone='{phone}', date='{today}'")

        # Map normalized field names to their fill values.
        # Support both the older 'Date Created' label and a plain 'Date' field.
        field_map = {
            'date created': today,
            'date': today,
            'customer name': customer or '',
            'job name': job_name or '',
            'phone': phone or '',
        }

        # Fill form fields
        for widget in page.widgets():
            field_name = (widget.field_name or '').strip()
            key = field_name.lower()
            if key in field_map and widget.field_type_string == 'Text':
                widget.field_value = field_map[key]
                widget.update()
                logger.info(f"Filled cover field '{field_name}' with '{field_map[key]}'")

        # Save the modified PDF
        doc.save(cover_path)
        doc.close()

        logger.info(f"Successfully generated cover page at: {cover_path}")
        return cover_path

    except Exception as e:
        logger.error(f"Error generating cover page: {str(e)}")
        raise

def validate_pdf(pdf_path):
    """Validate if a PDF file can be opened and read properly."""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PdfReader(file)
            # Try to read the number of pages to verify the PDF is readable
            num_pages = len(reader.pages)
            return True
    except Exception as e:
        return False

def add_gutter_form_fields_in_pdf(pdf_path):
    """
    Scan a PDF for underscore placeholders following sentences about gutter information and
    create text form fields positioned over those underscores.

    Field names will be standardized: inlet_count, inlet_size, drawing_number.

    Returns True if any fields were added; otherwise False.
    """
    try:
        doc = fitz.open(pdf_path)
        added = 0

        for page_index in range(len(doc)):
            page = doc[page_index]
            words = page.get_text("words") or []
            lines = {}
            for w in words:
                key = (w[5], w[6])
                lines.setdefault(key, []).append(w)

            for key, words_in_line in lines.items():
                words_in_line.sort(key=lambda w: w[0])
                line_text = ' '.join(w[4] for w in words_in_line)
                placeholder_idx = _line_contains_placeholder(words_in_line)
                if placeholder_idx == -1:
                    continue

                field_key = _classify_gutter_field_by_context(line_text)
                if not field_key:
                    continue

                ux0, uy0, ux1, uy1, _, *_ = words_in_line[placeholder_idx]
                rect = fitz.Rect(ux0 - 1, uy0 - 1, ux1 + 1, uy1 + 1)

                field_name = field_key
                suffix = 1
                existing_names = {w.field_name for w in (page.widgets() or []) if w.field_name}
                while field_name in existing_names:
                    suffix += 1
                    field_name = f"{field_key}_{suffix}"

                try:
                    widget = page.new_widget(
                        rect=rect,
                        field_name=field_name,
                        field_type=fitz.PDF_WIDGET_TYPE_TEXT,
                    )
                    widget.text_fontsize = 10
                    widget.text_color = (0, 0, 1)
                    widget.border_color = (0, 0, 0)
                    widget.fill_color = (1, 1, 1)
                    widget.update()
                    added += 1
                    logger.info(f"Added gutter text field '{field_name}' on page {page_index+1} at {rect}")
                except Exception as e:
                    logger.error(f"Failed adding gutter widget on page {page_index+1}: {e}")

        if added:
            doc.save(pdf_path, incremental=True)
            logger.info(f"Added {added} gutter fields to {os.path.basename(pdf_path)}")
            doc.close()
            return True
        else:
            doc.close()
            logger.info(f"No gutter placeholders found in {os.path.basename(pdf_path)}")
            return False
    except Exception as e:
        logger.error(f"Error processing {pdf_path} for adding gutter form fields: {e}")
        return False

def fill_gutter_maintenance_doc(pdf_path, gutter_data, gutter_name=None, output_dir=None):
    """
    Ensure a gutter maintenance PDF has fields, then fill with gutter_data.
    Returns the filled path if filled, otherwise original path.
    """
    try:
        has_fields = check_template_for_gutter_fields(pdf_path)
        if not has_fields:
            logger.info(f"No gutter fields found in {os.path.basename(pdf_path)}, attempting to add.")
            add_gutter_form_fields_in_pdf(pdf_path)
        # Use gutter_name as filter_name to create unique output file
        filled = fill_pdf_form_fields(pdf_path, flow_data={}, filter_name=gutter_name, gutter_data=gutter_data, output_dir=output_dir)
        return filled or pdf_path
    except Exception as e:
        logger.error(f"Error filling gutter maintenance doc {pdf_path}: {e}")
        return pdf_path

def check_template_for_gutter_fields(pdf_path):
    """
    Check if a template has form fields for gutter information: inlet_count, inlet_size, drawing_number.
    """
    try:
        doc = fitz.open(pdf_path)
        widgets = []
        for page in doc:
            widgets.extend(page.widgets())
        if not widgets:
            doc.close()
            return False
        field_variations = {
            'inlet_count': ['inlet_count', 'inlet count', 'inletcount', 'gutter_inlet_count', 'gutter inlet count'],
            'inlet_size': ['inlet_size', 'inlet size', 'inletsize', 'gutter_inlet_size', 'gutter inlet size'],
            'drawing_number': ['drawing_number', 'drawing number', 'drawingnumber', 'gutter_drawing_number', 'gutter drawing number'],
            'gutter_option': ['gutter_option', 'gutter option', 'gutter_options', 'gutter options', 'gutter type', 'gutter_type'],
            'has_grating': ['has_grating', 'grating', 'gutter_grating', 'gutter grating', 'has grating'],
            'grating_others': ['grating_others', 'grating others', 'grating provided by others'],
            'gutter_features': ['gutter_features', 'gutter features', 'features']
        }
        for widget in widgets:
            field_name = (widget.field_name or '').strip()
            if not field_name:
                continue
            normalized_field = field_name.lower().replace(' ', '_')
            for variations in field_variations.values():
                normalized_variations = [v.lower().replace(' ', '_') for v in variations]
                if normalized_field in normalized_variations:
                    doc.close()
                    return True
        doc.close()
        return False
    except Exception as e:
        logger.error(f"Error checking template for gutter fields: {str(e)}")
        return False

def check_template_for_flow_fields(pdf_path):
    """
    Check if a template has form fields that can be filled with flow data.
    
    Args:
        pdf_path: Path to the PDF template
        
    Returns:
        Boolean indicating whether the template has flow-related form fields
    """
    try:
        # Open the PDF
        doc = fitz.open(pdf_path)
        
        # Get all widgets (form fields) from the PDF
        widgets = []
        for page in doc:
            widgets.extend(page.widgets())
            
        if not widgets:
            logger.debug(f"No form fields found in {pdf_path}")
            doc.close()
            return False
        
        logger.info(f"Found {len(widgets)} form fields in {os.path.basename(pdf_path)}")
            
        # Define field name variations to check for
        field_variations = {
            'primary_flow_rate': ['primary_flow_rate', 'Primary_Flow_Rate', 'primary flow rate', 'primaryflowrate'],
            'backwash_rate': ['backwash_rate', 'backwash rate', 'backwashrate'],
            'total_dynamic_head': ['total_dynamic_head', 'total dynamic head', 'totaldynamichead', 'tdh']
        }
        
        # Check if any field matches our flow field patterns
        for widget in widgets:
            field_name = (widget.field_name or '').strip()
            if not field_name:
                continue
            normalized_field = field_name.lower().replace(' ', '_')
            for variations in field_variations.values():
                normalized_variations = [v.lower().replace(' ', '_') for v in variations]
                if normalized_field in normalized_variations:
                    doc.close()
                    return True
        doc.close()
        return False
    except Exception as e:
        logger.error(f"Error checking template for flow fields: {str(e)}")
        return False

def fill_pdf_form_fields(pdf_path, flow_data, filter_name=None, gutter_data=None, output_dir=None):
    """
    Fill PDF form fields with flow rate data.
    
    Args:
        pdf_path: Path to the PDF template
        flow_data: Dictionary containing flow rate information
        filter_name: Optional name of the filter for naming the output file
    
    Returns:
        Path to the filled PDF or None if no fields were filled
    """
    logger.info(f"Attempting to fill fields in {os.path.basename(pdf_path)} for filter: {filter_name}")
    logger.info(f"Flow data: {flow_data}")
    logger.info(f"Gutter data: {gutter_data}")
    try:
        # Open the PDF
        doc = fitz.open(pdf_path)
        
        # Get all widgets (form fields) from the PDF
        widgets = []
        for page in doc:
            widgets.extend(page.widgets())
            
        if not widgets:
            logger.debug(f"No form fields found in {pdf_path}")
            doc.close()
            return None
            
        logger.info(f"Found {len(widgets)} form fields in {pdf_path}")
        
        # Track if we made any changes
        made_changes = False
        fields_modified = 0
        
        # Define field name variations (flow)
        field_variations = {
            'primary_flow_rate': ['primary_flow_rate', 'Primary_Flow_Rate', 'primary flow rate', 'primaryflowrate'],
            'backwash_rate': ['backwash_rate', 'backwash rate', 'backwashrate'],
            'total_dynamic_head': ['total_dynamic_head', 'total dynamic head', 'tdh', 'totaldynamichead']
        }
        # Define gutter field name variations
        gutter_variations = {
            'inlet_count': ['inlet_count', 'inlet count', 'inletcount', 'gutter_inlet_count', 'gutter inlet count'],
            'inlet_size': ['inlet_size', 'inlet size', 'inletsize', 'gutter_inlet_size', 'gutter inlet size'],
            'drawing_number': ['drawing_number', 'drawing number', 'drawingnumber', 'gutter_drawing_number', 'gutter drawing number'],
            'gutter_option': ['gutter_option', 'gutter option', 'gutter_options', 'gutter options', 'gutter type', 'gutter_type'],
            'has_grating': ['has_grating', 'grating', 'gutter_grating', 'gutter grating', 'has grating'],
            'grating_others': ['grating_others', 'grating others', 'grating provided by others'],
            'gutter_features': ['gutter_features', 'gutter features', 'features']
        }
        
        # Map flow data keys to their values, adding GPM where appropriate
        formatted_values = {
            'primary_flow_rate': f"{flow_data.get('primary_flow_rate', '')} GPM" if flow_data.get('primary_flow_rate') else '',
            'backwash_rate': f"{flow_data.get('backwash_rate', '')} GPM" if flow_data.get('backwash_rate') else '',
            'total_dynamic_head': flow_data.get('total_dynamic_head', '')
        }
        # Gutter formatted values (no suffixes)
        gutter_values = {
            'inlet_count': str(gutter_data.get('inlet_count', '')).strip() if gutter_data else '',
            'inlet_size': str(gutter_data.get('inlet_size', '')).strip() if gutter_data else '',
            'drawing_number': str(gutter_data.get('drawing_number', '')).strip() if gutter_data else '',
            'gutter_option': str(gutter_data.get('gutter_option', '')).strip() if gutter_data else '',
            'has_grating': str(gutter_data.get('has_grating', '')).strip() if gutter_data else '',
            'grating_others': str(gutter_data.get('grating_others', '')).strip() if gutter_data else '',
            'gutter_features': str(gutter_data.get('gutter_features_text', '')).strip() if gutter_data else ''
        }
        
        # Derive a safe base name from the PDF filename for field renaming
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        safe_base = re.sub(r"[^a-zA-Z0-9]+", "_", base_name).strip("_").lower() or "doc"

        def _make_unique_field_name(original_name: str, data_key: str) -> str:
            """Create a template-specific, per-field unique name to avoid collisions.

            Example: filter_template.pdf, data_key='primary_flow_rate'
            -> filter_template_primary_flow_rate or filter_template_primary_flow_rate_2
            """
            base = f"{safe_base}_{data_key}"
            new_name = base
            orig = (original_name or "").strip()
            if orig:
                parts = orig.split("_")
                if parts and parts[-1].isdigit():
                    new_name = f"{base}_{parts[-1]}"
            return new_name

        # Collect all widgets (including duplicates with same field_name)
        all_widgets = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            for widget in page.widgets() or []:
                field_name = widget.field_name or ''
                all_widgets.append((field_name, page, widget))
        
        # Try to fill in fields
        for field_name, page, widget in all_widgets:
            logger.info(f"Found field with name: {field_name}, type: {widget.field_type_string}")
            
            if not field_name:  # Skip if no field name
                logger.debug("Skipping field with no name")
                continue
                
            field_name = field_name.strip()  # Remove any whitespace
            
            # Check each type of flow data
            for data_key, variations in field_variations.items():
                # Normalize the current field name for comparison
                normalized_field = field_name.lower().replace(' ', '_')
                normalized_variations = [v.lower().replace(' ', '_') for v in variations]
                
                logger.info(f"Comparing field '{normalized_field}' with variations: {normalized_variations}")
                
                if normalized_field in normalized_variations and formatted_values[data_key]:
                    value = formatted_values[data_key]
                    logger.info(f"Match found! Filling field '{field_name}' with value '{value}'")
                    
                    try:
                        # Handle different field types
                        if widget.field_type_string == 'Text':
                            # Regular text field; rename to a template-specific name first
                            try:
                                original_name = widget.field_name or ''
                                new_name = _make_unique_field_name(original_name, data_key)
                                widget.field_name = new_name
                                logger.info(
                                    f"Renamed flow field '{original_name}' to '{new_name}' in filled PDF"
                                )
                            except Exception as rn_err:
                                logger.warning(f"Could not rename flow field '{field_name}': {rn_err}")

                            widget.field_value = value
                            widget.update()
                            made_changes = True
                            fields_modified += 1
                            logger.info(f"Successfully updated text field with value '{value}'")
                        elif widget.field_type_string == 'Choice':
                            # Dropdown/combo box field
                            options = widget.choice_values or []
                            logger.info(f"Found dropdown with options: {options}")

                            def _norm(s: str) -> str:
                                import re as _re
                                return _re.sub(r'[^a-z0-9]+', '', (s or '').lower())

                            v_norm = _norm(value)
                            found_match = False
                            # 1) Exact normalized match
                            for option in options:
                                if _norm(option) == v_norm and option is not None:
                                    widget.field_value = option
                                    widget.update()
                                    made_changes = True
                                    logger.info(f"Selected dropdown option by normalized exact match '{option}'")
                                    found_match = True
                                    fields_modified += 1
                                    break
                            # 2) Substring match (case-insensitive)
                            if not found_match:
                                for option in options:
                                    if value.lower() in (option or '').lower():
                                        widget.field_value = option
                                        widget.update()
                                        made_changes = True
                                        logger.info(f"Selected dropdown option by substring '{option}'")
                                        found_match = True
                                        fields_modified += 1
                                        break
                            # 3) Startswith match on normalized
                            if not found_match:
                                for option in options:
                                    if _norm(option).startswith(v_norm) or v_norm.startswith(_norm(option)):
                                        widget.field_value = option
                                        widget.update()
                                        made_changes = True
                                        logger.info(f"Selected dropdown option by startswith '{option}'")
                                        found_match = True
                                        fields_modified += 1
                                        break
                            # Fallback: select first option if any
                            if not found_match and options:
                                widget.field_value = options[0]
                                widget.update()
                                made_changes = True
                                fields_modified += 1
                                logger.info(f"No match found, selected first option '{options[0]}'")
                        else:
                            # Other field types
                            logger.info(f"Unsupported field type: {widget.field_type_string}")
                    except Exception as e:
                        logger.error(f"Error updating field: {str(e)}")
                    # Note: We removed the break statement here to allow all matching fields to be filled
            # Check gutter fields if provided
            if gutter_data:
                for data_key, variations in gutter_variations.items():
                    normalized_field = field_name.lower().replace(' ', '_')
                    normalized_variations = [v.lower().replace(' ', '_') for v in variations]
                    if normalized_field in normalized_variations and gutter_values[data_key]:
                        value = gutter_values[data_key]
                        logger.info(f"Gutter match found! Filling field '{field_name}' with value '{value}'")
                        try:
                            if widget.field_type_string == 'Text':
                                # Rename all gutter text fields to template-specific names
                                try:
                                    original_name = widget.field_name or ''
                                    new_name = _make_unique_field_name(original_name, data_key)
                                    widget.field_name = new_name
                                    logger.info(
                                        f"Renamed gutter field '{original_name}' to '{new_name}' in filled PDF"
                                    )
                                except Exception as rn_err:
                                    logger.warning(f"Could not rename gutter field '{field_name}': {rn_err}")

                                widget.field_value = value
                                widget.update()
                                made_changes = True
                                fields_modified += 1
                                logger.info(f"Successfully updated gutter text field with value '{value}'")
                            elif widget.field_type_string == 'Choice':
                                options = widget.choice_values or []
                                def _norm(s: str) -> str:
                                    import re as _re
                                    return _re.sub(r'[^a-z0-9]+', '', (s or '').lower())
                                v_norm = _norm(value)
                                found_match = False
                                for option in options:
                                    if _norm(option) == v_norm and option is not None:
                                        widget.field_value = option
                                        widget.update()
                                        made_changes = True
                                        fields_modified += 1
                                        found_match = True
                                        break
                                if not found_match:
                                    for option in options:
                                        if value.lower() in (option or '').lower():
                                            widget.field_value = option
                                            widget.update()
                                            made_changes = True
                                            fields_modified += 1
                                            found_match = True
                                            break
                                if not found_match:
                                    for option in options:
                                        if _norm(option).startswith(v_norm) or v_norm.startswith(_norm(option)):
                                            widget.field_value = option
                                            widget.update()
                                            made_changes = True
                                            fields_modified += 1
                                            found_match = True
                                            break
                                if not found_match and options:
                                    widget.field_value = options[0]
                                    widget.update()
                                    made_changes = True
                                    fields_modified += 1
                            elif widget.field_type_string == 'Button':
                                # Checkbox/radio: treat 'has_grating' specially
                                if data_key == 'has_grating':
                                    # Set checked for Yes, unchecked for No
                                    yes = value.strip().lower() in ('yes', 'true', '1', 'on')
                                    try:
                                        widget.set_on(yes)
                                    except Exception:
                                        # Fallback to setting field_value
                                        widget.field_value = 'Yes' if yes else 'Off'
                                    widget.update()
                                    made_changes = True
                                    fields_modified += 1
                                else:
                                    logger.info(f"Button field encountered for '{field_name}', no special handling applied")
                            else:
                                logger.info(f"Unsupported field type for gutter field: {widget.field_type_string}")
                        except Exception as e:
                            logger.error(f"Error updating gutter field: {str(e)}")

                # Additionally: toggle individual feature checkboxes if present
                try:
                    selected = set((gutter_data.get('gutter_features') or []))
                    feature_map = {
                        'tg': ['tg', 'feature_tg', 'tg_feature', 'top_grate', 'top grate'],
                        'di': ['di', 'feature_di', 'di_feature', 'drop_in', 'drop in'],
                        'tgec': ['tgec', 'feature_tgec', 'tgec_feature', 'top_grate_extra_chamber', 'top grate extra chamber'],
                        'tgdd': ['tgdd', 'feature_tgdd', 'tgdd_feature', 'top_grate_deck_drain', 'top grate deck drain']
                    }
                    nf = field_name.lower().replace(' ', '_')
                    for code, aliases in feature_map.items():
                        if nf in [a.replace(' ', '_') for a in aliases]:
                            is_on = code.upper() in selected
                            if widget.field_type_string == 'Button':
                                try:
                                    widget.set_on(is_on)
                                except Exception:
                                    widget.field_value = 'Yes' if is_on else 'Off'
                                widget.update()
                                made_changes = True
                                fields_modified += 1
                            elif widget.field_type_string == 'Text':
                                widget.field_value = 'Yes' if is_on else 'No'
                                widget.update()
                                made_changes = True
                                fields_modified += 1
                            break
                except Exception as e:
                    logger.error(f"Error updating individual feature checkbox: {e}")
        
        if made_changes:
            # Save to a new file
            save_dir = output_dir or os.path.join(os.path.dirname(pdf_path), 'filled')
            os.makedirs(save_dir, exist_ok=True)

            base_name = os.path.basename(pdf_path)

            # If filter name is provided, include it in the filename
            if filter_name:
                filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                # Ensure we don't have 'filled_' prefix if we're using filter name
                filled_path = os.path.join(save_dir, f'{filter_name_safe}_{base_name}')
            else:
                filled_path = os.path.join(save_dir, f'filled_{base_name}')

            logger.info(f"Generated filled path: {filled_path}")

            # Save the changes, handling Windows permission issues when overwriting
            try:
                doc.save(filled_path)
                logger.info(f"Saved filled PDF to: {filled_path} with {fields_modified} fields modified")
                final_path = filled_path
            except Exception as e:
                logger.error(f"Primary save failed for {filled_path}: {e}. Retrying with unique name.")
                try:
                    import time as _time
                    ts = int(_time.time() * 1000)
                    # For gutter_care.pdf this yields filled_gutter_care_<ts>.pdf
                    alt_name = f"filled_{os.path.splitext(base_name)[0]}_{ts}.pdf"
                    alt_path = os.path.join(save_dir, alt_name)
                    doc.save(alt_path)
                    logger.info(f"Saved filled PDF to alternate path: {alt_path}")
                    final_path = alt_path
                except Exception as e2:
                    logger.error(f"Alternate save also failed: {e2}")
                    doc.close()
                    return None

            doc.close()
            return final_path
        else:
            logger.warning(f"No fields were modified in {os.path.basename(pdf_path)} for filter {filter_name}")
            doc.close()
            return None
            
    except Exception as e:
        logger.error(f"Error filling PDF form fields: {str(e)}")
        return None

def create_section_header(title):
    """
    Create a section header page with the given title.
    
    Args:
        title: Title of the section
    Returns:
        Path to the generated section header PDF
    """
    # Create a temporary file for the section header
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
        header_path = tmp.name
        
    # Create the PDF
    c = canvas.Canvas(header_path, pagesize=letter)
    width, height = letter
    
    # Add the title
    c.setFont("Helvetica-Bold", 24)
    c.drawString(72, height - 144, title)
    
    # Add a line under the title
    c.setStrokeColorRGB(0, 0, 0)
    c.line(72, height - 156, width - 72, height - 156)
    
    c.save()
    return header_path

def organize_files_by_section(cover_page, templates, maintenance_docs, job_files, warranty_docs=None, pools_data=None, pool_templates=None, pool_maintenance=None, pool_gutter_care=None, equipment_list=None, cover_sheets=None, always_include_dir=None):
    """
    Organize files into sections with headers.

    Args:
        cover_page: Path to cover page
        templates: List of template file paths
        maintenance_docs: List of maintenance document paths
        job_files: List of job folder file paths
        warranty_docs: List of warranty document paths
        pools_data: Optional list of pool dicts with 'pool_id' and 'pool_name'
        pool_templates: Optional dict mapping pool_id -> list of template paths
        pool_maintenance: Optional dict mapping pool_id -> list of maintenance doc paths
        pool_gutter_care: Optional dict mapping pool_id -> list of gutter care paths
        cover_sheets: Either a list of extra cover sheets (legacy) or a dict
                      returned by parse_cover_sheets() that drives numbered
                      section-divider layout.
    Returns:
        List of organized file paths including section headers
    """
    organized_files = []

    # If cover_sheets is the parsed dict with numbered section dividers, use the
    # new cover-sheet-driven layout and skip the legacy section organization.
    if isinstance(cover_sheets, dict):
        return _organize_with_cover_sheets(
            cover_page, cover_info=cover_sheets,
            templates=templates, maintenance_docs=maintenance_docs,
            job_files=job_files, warranty_docs=warranty_docs,
            equipment_list=equipment_list,
            pools_data=pools_data,
            pool_templates=pool_templates,
            pool_maintenance=pool_maintenance,
            pool_gutter_care=pool_gutter_care,
            always_include_dir=always_include_dir
        )

    # Add cover page
    if cover_page:
        organized_files.append(cover_page)
        logger.info(f"Added cover page to organized files: {os.path.basename(cover_page)}")

        # Legacy: insert a flat list of extra cover sheets right after the cover
        if cover_sheets and isinstance(cover_sheets, list):
            for cs in cover_sheets:
                organized_files.append(cs)
                logger.info(f"Inserted cover sheet after cover page: {os.path.basename(cs)}")

        # Immediately after cover, include Table of Contents and Special Instructions if present
        try:
            project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            maint_dir = os.path.join(project_dir, "maintenance_docs")
            toc_path = os.path.join(maint_dir, "table_of_contents.pdf")
            spec_path = os.path.join(maint_dir, "special_instructions.pdf")
            # Append in defined order if they exist and are valid PDFs
            for p in [toc_path, spec_path]:
                if os.path.exists(p) and p.lower().endswith('.pdf'):
                    organized_files.append(p)
                    logger.info(f"Inserted always-include doc after cover: {os.path.basename(p)}")
                else:
                    logger.warning(f"Always-include doc missing or invalid: {p}")
        except Exception as e:
            logger.error(f"Error inserting always-include docs after cover: {e}")
    
    # Prepare Maintenance & Operation section and separate gutter care from other docs
    gutter_care_docs = []
    remaining_maintenance = []
    if maintenance_docs:
        try:
            always_include_basenames = {"table_of_contents.pdf", "special_instructions.pdf", "additional_info.pdf"}
            filtered = [p for p in maintenance_docs if os.path.basename(p).lower() not in always_include_basenames]
        except Exception:
            filtered = maintenance_docs

        # Identify all filled gutter care docs (e.g., filled_gutter_care*.pdf)
        for p in filtered:
            base = os.path.basename(p).lower()
            if "gutter_care" in base:
                gutter_care_docs.append(p)
            else:
                remaining_maintenance.append(p)

    if pools_data:
        # Per-pool organization: each pool gets its own section with templates and gutter care
        for pool in pools_data:
            pool_id = pool['pool_id']
            pool_name = pool['pool_name']
            pool_templates_list = (pool_templates or {}).get(pool_id, [])
            pool_gutters = (pool_gutter_care or {}).get(pool_id, [])

            if pool_templates_list or pool_gutters:
                pool_header = create_section_header(f"Pool: {pool_name}")
                organized_files.append(pool_header)
                logger.info(f"Added pool header: Pool: {pool_name}")

            if pool_templates_list:
                template_header = create_section_header("Equipment Templates")
                organized_files.append(template_header)
                logger.info(f"Added equipment templates header for pool {pool_name}")
                for i, template in enumerate(pool_templates_list):
                    organized_files.append(template)
                    logger.info(f"  {i+1}. Added pool template: {os.path.basename(template)}")

            if pool_gutters:
                gutter_header = create_section_header("Gutter Care")
                organized_files.append(gutter_header)
                logger.info(f"Added gutter care header for pool {pool_name}")
                for gutter_doc in pool_gutters:
                    organized_files.append(gutter_doc)
                    logger.info(f"  Placed gutter maintenance doc for pool {pool_name}: {os.path.basename(gutter_doc)}")

    if maintenance_docs:
        maintenance_header = create_section_header("Maintenance & Operation Guides")
        organized_files.append(maintenance_header)
        logger.info("Added maintenance header to organized files")

        # Place all gutter care docs first in Maintenance & Operation, if present (legacy path/no pools)
        if gutter_care_docs and not pools_data:
            for gutter_doc in gutter_care_docs:
                organized_files.append(gutter_doc)
                logger.info(f"Placed gutter maintenance doc: {os.path.basename(gutter_doc)}")

    # Add Equipment Templates section immediately after the primary gutter doc (legacy path/no pools)
    if templates and not pools_data:
        template_header = create_section_header("Equipment Templates")
        organized_files.append(template_header)
        logger.info("Added template header to organized files")
        logger.info(f"Adding {len(templates)} templates to organized files:")
        for i, template in enumerate(templates):
            organized_files.append(template)
            logger.info(f"  {i+1}. Added template: {os.path.basename(template)}")

    # After templates, append the remaining Maintenance & Operation docs
    if remaining_maintenance:
        organized_files.extend(remaining_maintenance)
        logger.info(f"Added {len(remaining_maintenance)} remaining maintenance docs after templates")

    # Always include docs from always_include folder before Project Documentation
    if always_include_dir and os.path.isdir(always_include_dir):
        for ai in sorted(os.listdir(always_include_dir)):
            if ai.lower().endswith('.pdf'):
                ai_path = os.path.join(always_include_dir, ai)
                organized_files.append(ai_path)
                logger.info(f"Inserted always-include doc before project docs: {ai}")
    
    # Add Project Documentation section
    if equipment_list or job_files:
        project_header = create_section_header("Project Documentation")
        organized_files.append(project_header)
        logger.info(f"Added project header to organized files")
        if equipment_list:
            organized_files.append(equipment_list)
            logger.info(f"Added equipment list: {os.path.basename(equipment_list)}")
        organized_files.extend(job_files)
        logger.info(f"Added {len(job_files)} job files to organized files")
    
    # Add Warranty Documents section LAST
    if warranty_docs:
        warranty_header = create_section_header("Warranty Documents")
        organized_files.append(warranty_header)
        logger.info("Added warranty header to organized files")
        organized_files.extend(warranty_docs)
        logger.info(f"Added {len(warranty_docs)} warranty docs to organized files")
    
    logger.info(f"Total organized files: {len(organized_files)}")
    return organized_files

def merge_pdfs(input_paths, output_path, organized=False, sections=None):
    """
    Merge multiple PDF files into a single PDF.
    
    Args:
        input_paths: List of PDF paths to merge
        output_path: Path for the output PDF
        organized: If True, add section headers (requires sections parameter)
        sections: Dictionary with keys 'cover', 'templates', 'maintenance', 'job_files', 'warranty'
                 containing lists of files for each section
    
    Returns:
        tuple (success: bool, error_message: str, skipped_files: list)
    """
    logger.info(f"Starting PDF merge with {len(input_paths)} files")
    
    # If organized mode is requested, reorganize files with sections
    if organized and sections:
        logger.info("Organizing files into sections with headers")
        logger.info(f"Templates before organization: {len(sections.get('templates', []))}")
        logger.info(f"Template filenames: {[os.path.basename(t) for t in sections.get('templates', [])]}")
        
        input_paths = organize_files_by_section(
            sections.get('cover'),
            sections.get('templates', []),
            sections.get('maintenance', []),
            sections.get('job_files', []),
            sections.get('warranty', []),
            sections.get('pools_data'),
            sections.get('pool_templates'),
            sections.get('pool_maintenance'),
            sections.get('pool_gutter_care'),
            sections.get('equipment_list'),
            sections.get('cover_sheets'),
            sections.get('always_include')
        )
        logger.info(f"Organized files into sections with headers, total files: {len(input_paths)}")
    
    logger.info("Files to merge:")
    for i, path in enumerate(input_paths, 1):
        if os.path.exists(path):
            logger.info(f"{i}. {path} (exists: True, size: {os.path.getsize(path)} bytes)")
        else:
            logger.error(f"{i}. {path} (exists: False)")

    merger = PdfMerger()
    skipped_files = []
    merged_count = 0
    temp_files = []

    try:
        for path in input_paths:
            if not path.lower().endswith(".pdf"):
                logger.warning(f"Skipping non-PDF file: {path}")
                skipped_files.append((path, "Not a PDF file"))
                continue
                
            if not os.path.exists(path):
                logger.error(f"File does not exist: {path}")
                skipped_files.append((path, "File not found"))
                continue
                
            if not validate_pdf(path):
                logger.error(f"Invalid or corrupted PDF file: {path}")
                skipped_files.append((path, "Invalid or corrupted PDF"))
                continue
                
            try:
                logger.info(f"Appending file: {path}")
                merger.append(path)
                logger.info(f"Successfully appended: {path}")
                merged_count += 1
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error processing {path}: {error_msg}")
                skipped_files.append((path, f"Error: {error_msg}"))
                continue
        
        if merged_count == 0:
            logger.error("No valid PDFs to merge")
            return False, "No valid PDFs found to merge", skipped_files
            
        logger.info(f"Writing merged PDF to: {output_path}")
        merger.write(output_path)
        logger.info(f"PDF merge completed successfully. Merged {merged_count} files, skipped {len(skipped_files)} files")
        
        # Clean up temporary section header files
        for temp_file in temp_files:
            try:
                os.unlink(temp_file)
            except Exception as e:
                logger.warning(f"Could not delete temporary file {temp_file}: {e}")
        
        if skipped_files:
            logger.warning("Skipped files during merge:")
            for file, reason in skipped_files:
                logger.warning(f"  - {os.path.basename(file)}: {reason}")
        
        return True, f"PDFs merged successfully ({merged_count} merged, {len(skipped_files)} skipped)", skipped_files
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error during PDF merge: {error_msg}")
        return False, f"Error during PDF merge: {error_msg}", skipped_files
    finally:
        merger.close()

def _line_contains_placeholder(words_in_line):
    """Return index of the underscore placeholder word in a line if present, else -1."""
    for i, w in enumerate(words_in_line):
        text = w[4]
        if text and ('____' in text or re.fullmatch(r'_+', text)):
            return i
    return -1

def _classify_field_by_context(line_text):
    """Classify which flow field a line refers to based on keywords."""
    t = line_text.lower()
    if 'backwash' in t:
        return 'backwash_rate'
    if 'dynamic head' in t or 'tdh' in t:
        return 'total_dynamic_head'
    if 'flow rate' in t or 'gpm' in t:
        return 'primary_flow_rate'
    return None

def add_flow_form_fields_in_pdf(pdf_path):
    """
    Scan a PDF for underscore placeholders following sentences about flow information and
    create text form fields positioned over those underscores.

    Field names will be standardized: primary_flow_rate, backwash_rate, total_dynamic_head.

    Returns True if any fields were added; otherwise False.
    """
    try:
        doc = fitz.open(pdf_path)
        added = 0

        for page_index in range(len(doc)):
            page = doc[page_index]
            # Get words: list of (x0, y0, x1, y1, word, block_no, line_no, word_no)
            words = page.get_text("words") or []
            # Group words by (block, line)
            lines = {}
            for w in words:
                key = (w[5], w[6])
                lines.setdefault(key, []).append(w)

            for key, words_in_line in lines.items():
                # Sort by x position
                words_in_line.sort(key=lambda w: w[0])
                line_text = ' '.join(w[4] for w in words_in_line)
                placeholder_idx = _line_contains_placeholder(words_in_line)
                if placeholder_idx == -1:
                    continue

                field_key = _classify_field_by_context(line_text)
                if not field_key:
                    continue

                # Use the bbox of the underscore word as rect
                ux0, uy0, ux1, uy1, _, *_ = words_in_line[placeholder_idx]
                # pad the rect a bit for better usability
                rect = fitz.Rect(ux0 - 1, uy0 - 1, ux1 + 1, uy1 + 1)

                # Ensure unique field name per page / occurrence
                field_name = field_key
                suffix = 1
                existing_names = {w.field_name for w in (page.widgets() or []) if w.field_name}
                while field_name in existing_names:
                    suffix += 1
                    field_name = f"{field_key}_{suffix}"

                try:
                    widget = page.new_widget(
                        rect=rect,
                        field_name=field_name,
                        field_type=fitz.PDF_WIDGET_TYPE_TEXT,
                    )
                    # Optional styling
                    widget.text_fontsize = 10
                    widget.text_color = (0, 0, 1)
                    widget.border_color = (0, 0, 0)
                    widget.fill_color = (1, 1, 1)
                    widget.update()
                    added += 1
                    logger.info(f"Added text field '{field_name}' on page {page_index+1} at {rect}")
                except Exception as e:
                    logger.error(f"Failed adding widget on page {page_index+1}: {e}")

        if added:
            # Save in-place (caller should have backed up). Use incremental save when possible.
            doc.save(pdf_path, incremental=True)
            logger.info(f"Added {added} flow fields to {os.path.basename(pdf_path)}")
            doc.close()
            return True
        else:
            doc.close()
            logger.info(f"No flow placeholders found in {os.path.basename(pdf_path)}")
            return False
    except Exception as e:
        logger.error(f"Error processing {pdf_path} for adding form fields: {e}")
        return False

def prepare_templates_add_flow_fields(template_dir):
    """
    Process all PDFs in a template directory, backing up originals and adding flow-related form fields.

    Returns a summary dict with counts and lists of modified files.
    """
    if not os.path.exists(template_dir):
        raise FileNotFoundError(f"Template directory not found: {template_dir}")

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(template_dir, f"_backup_{timestamp}")
    os.makedirs(backup_dir, exist_ok=True)

    pdfs = [os.path.join(template_dir, f) for f in os.listdir(template_dir) if f.lower().endswith('.pdf')]
    modified = []
    skipped = []

    for pdf in pdfs:
        try:
            # Backup
            base = os.path.basename(pdf)
            backup_path = os.path.join(backup_dir, base)
            with open(pdf, 'rb') as src, open(backup_path, 'wb') as dst:
                dst.write(src.read())

            if add_flow_form_fields_in_pdf(pdf):
                modified.append(base)
            else:
                skipped.append(base)
        except Exception as e:
            logger.error(f"Error preparing template {pdf}: {e}")
            skipped.append(os.path.basename(pdf))

    summary = {
        'processed': len(pdfs),
        'modified': len(modified),
        'skipped': len(skipped),
        'modified_files': modified,
        'skipped_files': skipped,
        'backup_dir': backup_dir,
    }
    logger.info(f"Preparation summary: {summary}")
    return summary