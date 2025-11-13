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

def extract_items_from_sales_order(pdf_path):
    doc = fitz.open(pdf_path)
    keywords = set()
    
    # Common equipment terms and their variations
    equipment_terms = {
        "filter": ["filter", "filtration", "filtering", "horizontal filter", "vertical filter"],
        "grate": ["grate", "grating", "floor grating", "gutter grating"],
        "platform": ["platform", "walkway", "catwalk", "mezzanine"],
        "bulkhead": ["bulkhead", "partition", "wall panel"],
        "pump": ["pump", "pumping", "circulation"],
        "tank": ["tank", "vessel", "container", "fiberglass tank", "fiberglass vessel", "fiberglass container"],
        "valve": ["valve", "control valve", "check valve"],
        "strainer": ["strainer", "screen", "separator"],
        "meter": ["meter", "flow meter", "gauge"],
        "sensor": ["sensor", "detector", "probe"],
        "gutter": ["gutter", "gutter grating", "gutter grate", "perimeter overflow", "recirculation", "recirculation system"],
        "main drain": ["main drain", "main drain grate", "sump pump", "MD", "md"],
        "regenerator": ["regenerator", "regen", "regenerative", "regenerative filter", "regen filter", "regenerative filtration", "regen filtration", "PPEC1400S", "PPEC1200S", "PPEC2100S", "PPEC500S", "PPEC700S", "PPEC225S", "PPEC900S", "PPEC350S", "PPEC"]
    }
    
    logger.info(f"Processing PDF for keywords: {pdf_path}")
    
    for page in doc:
        text = page.get_text()
        lines = text.split('\n')
        
        for line in lines:
            line_lower = line.lower().strip()
            if not line_lower:
                continue
                
            # Skip very long lines as they're likely not product names
            if len(line_lower) > 100:
                continue
                
            # Split line by commas to handle comma-separated product descriptions
            parts = line_lower.split(',')
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                    
                # Skip parts that are just numbers
                if part.replace('.','').isdigit():
                    continue
                
                # Check for equipment terms and their variations
                for category, variations in equipment_terms.items():
                    if any(term in part for term in variations):
                        # Clean up the line by removing common prefixes and suffixes
                        cleaned_part = clean_product_line(part)
                        if cleaned_part:
                            keywords.add(cleaned_part)
                            # Also add the original part if it contains a model number
                            if any(model in part for model in ['PPEC', '1400S', '1200S', '2100S', '500S', '700S', '225S', '900S', '350S']):
                                keywords.add(part)
                            logger.debug(f"Found keyword in category '{category}': {cleaned_part}")
                        break
    
    logger.info(f"Extracted {len(keywords)} keywords from {pdf_path}")
    return list(keywords)

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

    # Define mappings of equipment types to their required care/maintenance docs
    maintenance_mappings = {
        "gutter": [
            "stainless_steel_care_maintenance.pdf",
            "gutter_depth_marker_installation.pdf",
            "gutter_grating_care.pdf",
            "radius_vs_miter_gutter.pdf",
            "Prevent-p-poster.pdf",
            "Safety_Information.pdf"
        ],
        "filter": [
            "PPEC OperationHorizontal Manual-reviseFinal 12-20-17R.pdf",
            "VSC Auto to Manual conversion.pdf",
            "VSC Backwash Procedure.pdf",
            "winterizing.pdf",
            "vsc_instructions.pdf"
        ],
        "pump": [
            "strainer_install.pdf"
        ],
        "strainer": [
            "strainer_install.pdf"
        ],
        "bulkhead": [
            "Bulkhead_Manual.pdf"
        ],
        "main drain": [
            "main drain installation warranty.pdf",
            "Main_Drain_Manual.pdf"
        ],
        "starting platform": [
            "Starting Platforms Install,Care&Maint rev.12-2022 wLogo.pdf",
            "Powder Coated Metal Care 05-2023-2.pdf",
            "Non-skid Change & or Replace material-platforms.pdf"
        ],
        "evacuator": [
            "Install EvacWall-Mount Sys.pdf",
            "DD+E Install Manual.pdf",
            "Gutter Evac Manual.pdf",
            "Install EvacPVC Bench Sys.pdf",
            "Installation Manual for the Paddock Evacuator 7-24 Rev03.pdf"
        ]
    }
    
    logger.info(f"Checking equipment type: {equipment_type}")
    associated_docs = []
    
    # Try exact match first
    if equipment_type in maintenance_mappings:
        logger.info(f"Found exact match for equipment type: {equipment_type}")
        doc_list = maintenance_mappings[equipment_type]
    else:
        # Try substring match
        matched_types = [k for k in maintenance_mappings.keys() if k in equipment_type.lower()]
        if matched_types:
            logger.info(f"Found substring matches: {matched_types}")
            doc_list = []
            for matched_type in matched_types:
                doc_list.extend(maintenance_mappings[matched_type])
        else:
            logger.info(f"No matches found for equipment type: {equipment_type}")
            return []

    logger.info(f"Looking for {len(doc_list)} maintenance docs")
    for doc in doc_list:
        # Try to find an exact match first
        if doc in available_docs:
            full_path = os.path.join(maintenance_docs_dir, doc)
            associated_docs.append(full_path)
            logger.info(f"Found maintenance doc: {doc}")
        else:
            # Try case-insensitive match
            doc_lower = doc.lower()
            matches = [f for f in available_docs if f.lower() == doc_lower]
            if matches:
                full_path = os.path.join(maintenance_docs_dir, matches[0])
                associated_docs.append(full_path)
                logger.info(f"Found maintenance doc (case-insensitive): {matches[0]}")
            else:
                logger.warning(f"Maintenance document not found: {doc}")
    
    if not associated_docs:
        logger.warning(f"No maintenance docs found for equipment type: {equipment_type}")
    else:
        logger.info(f"Found {len(associated_docs)} maintenance docs")
        logger.debug(f"Full paths: {associated_docs}")
    
    return associated_docs

def find_warranty_documents(keywords):
    """
    Find warranty documents by matching sales order-derived keywords against
    filenames in the 'warranty_docs' folder. Matching is case-insensitive and
    uses normalized text to allow partial matches and model numbers.

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

        # 1) Determine equipment categories from keywords (strictly)
        kws = [k.lower() for k in (keywords or []) if k]
        norm_kws = {normalize_text(k) for k in kws if k}

        # Model number cues for regenerator filters
        model_cues = {"ppec", "1400s", "1200s", "2100s", "500s", "700s", "225s", "900s", "350s"}
        has_regen_model = any(any(cue in k for cue in model_cues) for k in norm_kws)

        detected_categories = set()

        # Detect categories by specific phrases, avoid generic 'filter' only
        if any("horizontal" in k and "filter" in k for k in norm_kws) or any("horiz" in k for k in norm_kws):
            detected_categories.add("horiz sand filter")
        if any("high flow" in k and "filter" in k for k in norm_kws) or any("high flow sand" in k for k in norm_kws):
            detected_categories.add("High Flow Sand Filter")
        if any("fiberglass" in k and "filter" in k for k in norm_kws) or any("fiberglass sand" in k for k in norm_kws):
            detected_categories.add("fiberglass sand filter")
        if any("vacuum" in k and "filter" in k for k in norm_kws) or any("Compak" in k for k in norm_kws):
            detected_categories.add("vavuum sand filter") and detected_categories.add("Compak -STD")
        if any("verticel" in k and "filter" in k for k in norm_kws) or any("verticel sand" in k for k in norm_kws):
            detected_categories.add("verticel sand filter")
        if has_regen_model or any("regenerator" in k or "regen" in k for k in norm_kws):
            detected_categories.add("regenerator")
        if any("gutter" in k for k in norm_kws):
            detected_categories.add("gutter")
        if any("main drain" in k or "main_drain" in k for k in norm_kws) or any("MD" in k for k in norm_kws):
            detected_categories.add("main_drain")
        if any("starting platform" in k or "starting_platform" in k for k in norm_kws):
            detected_categories.add("starting_platform")
        if any("pump" in k for k in norm_kws):
            detected_categories.add("pump")
        if any("valve" in k for k in norm_kws):
            detected_categories.add("valve")
        if any("strainer" in k for k in norm_kws):
            detected_categories.add("strainer")
        if any("evacuator" in k for k in norm_kws) or any("evac" in k for k in norm_kws):
            detected_categories.add("evacuator")
        if any("bulkhead" in k for k in norm_kws):
            detected_categories.add("bulkhead")

        logger.info(f"Detected equipment categories for warranty: {sorted(detected_categories) if detected_categories else 'none'}")

        # 2) Map categories to strict filename patterns expected in warranty_docs
        # Patterns are normalized and compared as substrings in normalized filename
        category_patterns = {
            # User examples
            "horizontal_filter": ["horiz sand filter", "horizontal filter"],
            "regenerator": ["regenerator warranty", "regen warranty", "regenerator"],
            "vacuum sand filter": ["vacuum sand filter", "Compak"],
            "verticel sand filter": ["verticel sand filter"],
            "horizontal sand filter": ["horiz sand filter"],
            "high flow sand filter": ["high flow sand filter"],
            "fiberglass sand filter": ["fiberglass sand filter"],
            "gutter": ["gutter only", "gutterhdpe", "gutterstd", "gutter warranty", "HDPE Grating"],
            "strainer": ["strainer"],
            "evacuator": ["evacuator", "evac"],
            # Additional reasonable mappings
            "main_drain": ["main drain", "MD"],
            "starting_platform": ["starting platform"],
            "pump": ["pump"],
            "valve": ["valve"],
            "bulkhead": ["bulkhead", "bulkheadwHDPE", "PVC I-Bar"],
        }

        # If nothing specific detected, be conservative: return empty to avoid bloat
        if not detected_categories:
            logger.info("No specific equipment categories detected for warranty; returning no warranty docs to avoid bloat.")
            return []

        # 3) Match only files that contain one of the strict patterns for detected categories
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
                        break
                if include:
                    break

            if include:
                path = os.path.join(warranty_dir, fname)
                logger.info(f"Included warranty by pattern: {fname}")
                matched_paths.append(path)

        # 4) Deduplicate and sort by filename for stability
        result = sorted(set(matched_paths), key=lambda p: os.path.basename(p).lower())
        logger.info(f"Total matched warranty docs (strict): {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Error searching warranty documents: {e}")
        return []

def match_templates(keywords, template_dir, flow_data=None, filters_data=None, template_mappings=None, gutter_data=None, use_only_selected=False):
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
        
        # Second pass: Match all templates
        for filename in template_files:
            file_path = os.path.join(template_dir, filename)
            normalized_filename = normalize_text(os.path.splitext(filename)[0])
            words_in_filename = set(normalized_filename.split())
            
            # Track match quality (higher is better)
            match_quality = 0
            matching_terms = set()
            
            for term in search_terms:
                # Check for exact matches first
                if term in normalized_filename:
                    match_quality += 2
                    matching_terms.add(term)
                # Then check for partial word matches
                elif any(term in word for word in words_in_filename):
                    match_quality += 1
                    matching_terms.add(term)
            
            # Add file if we have any matches
            if match_quality > 0:
                matched_templates.add(file_path)
                logger.info(f"Matched template '{filename}' with terms: {matching_terms} (quality: {match_quality})")
    
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
    
    # If we have flow data, fill it in the templates
    if filters_data:
        logger.info(f"Filling flow data from {len(filters_data)} filters in matched templates...")
        filled_templates = []
        
        # Check if each template has form fields that can be filled with flow data
        logger.info(f"Processing {len(template_list)} templates for {len(filters_data)} filters")
        logger.info(f"Template paths: {[os.path.basename(t) for t in template_list]}")
        
        # Clear the filled directory ONCE to avoid using old files
        filled_dir = os.path.join(template_dir, 'filled')
        if os.path.exists(filled_dir):
            logger.info(f"Clearing filled directory: {filled_dir}")
            try:
                for file in os.listdir(filled_dir):
                    if file.endswith('.pdf'):
                        file_path = os.path.join(filled_dir, file)
                        os.remove(file_path)
                        logger.info(f"Removed old file: {file}")
            except Exception as e:
                logger.error(f"Error clearing filled directory: {str(e)}")
        
        # Log template mappings if provided
        if template_mappings:
            logger.info(f"Using template mappings: {template_mappings}")
        
        for template_path in template_list:
            template_name = os.path.basename(template_path)
            logger.info(f"Checking template: {template_name}")
            has_flow_fields = check_template_for_flow_fields(template_path)
            has_gutter_fields = check_template_for_gutter_fields(template_path)
            logger.info(f"Template {template_name} has flow fields: {has_flow_fields}")
            logger.info(f"Template {template_name} has gutter fields: {has_gutter_fields}")
            
            if has_flow_fields:
                # Check if we have a mapping for this template
                mapped_filter_id = None
                if template_mappings and template_name in template_mappings:
                    mapped_filter_id = template_mappings[template_name]
                    logger.info(f"Found mapping for template {template_name}: filter ID {mapped_filter_id}")
                
                # If we have a mapping, use only that filter's data
                if mapped_filter_id and filters_data:
                    # Find the filter with the matching ID
                    matched_filter = None
                    for filter_data in filters_data:
                        if filter_data.get('filter_id') == mapped_filter_id:
                            matched_filter = filter_data
                            break
                    
                    if matched_filter:
                        filter_name = matched_filter.get('filter_name', f"Filter {mapped_filter_id}")
                        logger.info(f"Using mapped filter: {filter_name} for template {template_name}")
                        
                        # Create a unique name for this filter's copy of the template
                        output_dir = os.path.join(os.path.dirname(template_path), 'filled')
                        os.makedirs(output_dir, exist_ok=True)
                        base_name = template_name
                        filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                        
                        # Create a unique identifier
                        timestamp = int(time.time() * 1000)
                        unique_id = f'{filter_name_safe}_{timestamp}'
                        
                        # Fill the template with this filter's data
                        filled_path = fill_pdf_form_fields(template_path, matched_filter, filter_name=unique_id, gutter_data=gutter_data)
                        
                        if filled_path:
                            logger.info(f"Successfully filled template for {filter_name}, path: {filled_path}")
                            filled_templates.append(filled_path)
                        else:
                            logger.warning(f"Could not fill fields for {filter_name}, using original template")
                            filled_templates.append(template_path)
                    else:
                        logger.warning(f"Mapped filter ID {mapped_filter_id} not found, using original template")
                        filled_templates.append(template_path)
                
                # If no mapping or mapping failed, use the old approach (create a copy for each filter)
                elif filters_data and not mapped_filter_id:
                    logger.info(f"No mapping for template {template_name}, creating copies for all filters")
                    
                    # For templates with flow fields, create a copy for each filter
                    for i, filter_data in enumerate(filters_data):
                        filter_name = filter_data.get('filter_name', f'Filter {i+1}')
                        logger.info(f"Processing filter {i+1}/{len(filters_data)}: {filter_name}")
                        
                        # Create a unique name for this filter's copy of the template
                        output_dir = os.path.join(os.path.dirname(template_path), 'filled')
                        os.makedirs(output_dir, exist_ok=True)
                        base_name = template_name
                        filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                        
                        # Create a unique identifier
                        timestamp = int(time.time() * 1000) + i
                        unique_id = f'{filter_name_safe}_{timestamp}'
                        
                        # Try to fill the template with this filter's data
                        filled_path = fill_pdf_form_fields(template_path, filter_data, filter_name=unique_id, gutter_data=gutter_data)
                        
                        if filled_path:
                            logger.info(f"Successfully filled template for {filter_name}, path: {filled_path}")
                            filled_templates.append(filled_path)
                            break  # Only use the first successful fill
                    
                    # If no filter worked, add the original template
                    if len(filled_templates) == 0 or filled_templates[-1] != filled_path:
                        logger.warning(f"Could not fill template with any filter data, using original")
                        filled_templates.append(template_path)
                else:
                    # No filters data available
                    logger.info(f"No filter data available for template {template_name}")
                    filled_templates.append(template_path)
            else:
                # If no flow fields, try gutter-only fill when gutter data is provided
                if gutter_data and has_gutter_fields and any(gutter_data.get(k) for k in ['inlet_count', 'inlet_size', 'drawing_number']):
                    logger.info(f"Template {os.path.basename(template_path)} has gutter fields and gutter_data; attempting gutter fill")
                    filled_path = fill_pdf_form_fields(template_path, {}, filter_name=None, gutter_data=gutter_data)
                    if filled_path:
                        filled_templates.append(filled_path)
                    else:
                        filled_templates.append(template_path)
                else:
                    # For templates without applicable fields, just add the original once
                    logger.info(f"Template {os.path.basename(template_path)} has no applicable fields, adding as-is")
                    filled_templates.append(template_path)
                
        logger.info(f"Final template list contains {len(filled_templates)} templates: {[os.path.basename(t) for t in filled_templates]}")
        
        # Ensure we're not losing any templates
        if len(filled_templates) < len(filters_data):
            logger.warning(f"WARNING: Expected at least {len(filters_data)} templates (one per filter), but only got {len(filled_templates)}")
                
        template_list = filled_templates
        logger.info(f"Returning {len(template_list)} templates from match_templates function")
        
        # Double check that the files actually exist
        for i, template in enumerate(template_list):
            if os.path.exists(template):
                logger.info(f"Template {i+1} exists: {os.path.basename(template)}")
            else:
                logger.error(f"Template {i+1} DOES NOT EXIST: {template}")
        
        logger.info("========== FINISHED TEMPLATE MATCHING ==========")
    elif flow_data:  # For backward compatibility
        logger.info("Filling flow data in matched templates using legacy flow_data...")
        filled_templates = []
        for template_path in template_list:
            filled_path = fill_pdf_form_fields(template_path, flow_data)
            if filled_path:
                logger.info(f"Filled template {os.path.basename(template_path)} with flow data")
                filled_templates.append(filled_path)
            else:
                filled_templates.append(template_path)
        template_list = filled_templates
    
    logger.info(f"Found {len(template_list)} templates and {len(maintenance_list)} maintenance docs")
    return template_list, maintenance_list

def generate_cover_page(customer, job_name, phone, flow_data=None, filters_data=None):
    """
    Generate a simplified, centered cover page using the template.
    Shows only customer, job name, and phone in larger, centered text.
    """
    import os

    # Get the template path
    template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Cover Sheet Template.pdf")
    if not os.path.exists(template_path):
        logger.error(f"Cover page template not found at: {template_path}")
        raise FileNotFoundError("Cover page template not found")

    # Create output path
    cover_path = os.path.join("output", f"cover_{job_name}.pdf")
    os.makedirs(os.path.dirname(cover_path), exist_ok=True)

    logger.info(f"Generating centered cover page for job: {job_name}")
    logger.info(f"Template path: {template_path}")
    logger.info(f"Output path: {cover_path}")

    try:
        # Open the template PDF
        doc = fitz.open(template_path)
        page = doc[0]  # Get first page

        # Page geometry
        rect = page.rect
        # Move block further down the page (~70% from top)
        start_y = max(240, rect.height * 0.70)
        margin_x = 72
        box_width = rect.width - 2 * margin_x

        logger.info(f"Cover values => customer='{customer}', job_name='{job_name}', phone='{phone}'")

        def insert_centered_box(text, fontsize, y_top, fontname="helv"):
            if not text:
                return y_top
            text = str(text)
            height = fontsize * 1.8
            box = fitz.Rect(margin_x, y_top, margin_x + box_width, y_top + height)
            logger.info(f"Cover text '{text}' at box {box}")
            try:
                page.insert_textbox(
                    box,
                    text,
                    fontsize=fontsize,
                    fontname=fontname,
                    color=(0, 0, 0),
                    align=1,
                    overlay=True,
                )
            except Exception as e:
                logger.warning(f"insert_textbox with font '{fontname}' failed: {e}; retrying with Times-Roman")
                page.insert_textbox(
                    box,
                    text,
                    fontsize=fontsize,
                    fontname="Times-Roman",
                    color=(0, 0, 0),
                    align=1,
                    overlay=True,
                )
            return box.y1

        # Draw main lines (bigger and centered) using text boxes for reliable alignment
        y = start_y
        y = insert_centered_box(customer, 30, y, fontname="Times-Bold")
        y += 10
        y = insert_centered_box(job_name, 26, y)
        y += 6
        y = insert_centered_box(phone, 18, y)

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

def fill_gutter_maintenance_doc(pdf_path, gutter_data):
    """
    Ensure a gutter maintenance PDF has fields, then fill with gutter_data.
    Returns the filled path if filled, otherwise original path.
    """
    try:
        has_fields = check_template_for_gutter_fields(pdf_path)
        if not has_fields:
            logger.info(f"No gutter fields found in {os.path.basename(pdf_path)}, attempting to add.")
            add_gutter_form_fields_in_pdf(pdf_path)
        filled = fill_pdf_form_fields(pdf_path, flow_data={}, filter_name=None, gutter_data=gutter_data)
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
            'drawing_number': ['drawing_number', 'drawing number', 'drawingnumber', 'gutter_drawing_number', 'gutter drawing number']
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

def fill_pdf_form_fields(pdf_path, flow_data, filter_name=None, gutter_data=None):
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
            'drawing_number': ['drawing_number', 'drawing number', 'drawingnumber', 'gutter_drawing_number', 'gutter drawing number']
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
            'drawing_number': str(gutter_data.get('drawing_number', '')).strip() if gutter_data else ''
        }
        
        # Create a page-to-widgets mapping
        page_widgets = {}
        for page_num in range(len(doc)):
            page = doc[page_num]
            for widget in page.widgets():
                if widget.field_type_string == 'Text':
                    page_widgets[widget.field_name] = (page, widget)
        
        # Create a mapping for all form fields, not just text fields
        all_widgets = {}
        for page_num in range(len(doc)):
            page = doc[page_num]
            for widget in page.widgets():
                all_widgets[widget.field_name] = (page, widget)
        
        # Try to fill in fields
        for field_name, (page, widget) in all_widgets.items():
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
                            # Regular text field
                            widget.field_value = value
                            widget.update()
                            made_changes = True
                            fields_modified += 1
                            logger.info(f"Successfully updated text field with value '{value}'")
                        elif widget.field_type_string == 'Choice':
                            # Dropdown/combo box field
                            options = widget.choice_values
                            logger.info(f"Found dropdown with options: {options}")
                            
                            # Try to find a matching option
                            found_match = False
                            for option in options:
                                if value.lower() in option.lower():
                                    widget.field_value = option
                                    widget.update()
                                    made_changes = True
                                    logger.info(f"Selected dropdown option '{option}'")
                                    found_match = True
                                    fields_modified += 1
                                    break
                            
                            # If no match found but options exist, use the first one
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
                                widget.field_value = value
                                widget.update()
                                made_changes = True
                                fields_modified += 1
                                logger.info(f"Successfully updated gutter text field with value '{value}'")
                            elif widget.field_type_string == 'Choice':
                                options = widget.choice_values
                                found_match = False
                                for option in options:
                                    if value.lower() in option.lower():
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
                            else:
                                logger.info(f"Unsupported field type for gutter field: {widget.field_type_string}")
                        except Exception as e:
                            logger.error(f"Error updating gutter field: {str(e)}")
        
        if made_changes:
            # Save to a new file
            output_dir = os.path.join(os.path.dirname(pdf_path), 'filled')
            os.makedirs(output_dir, exist_ok=True)
            
            base_name = os.path.basename(pdf_path)
            
            # If filter name is provided, include it in the filename
            if filter_name:
                filter_name_safe = filter_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
                # Ensure we don't have 'filled_' prefix if we're using filter name
                filled_path = os.path.join(output_dir, f'{filter_name_safe}_{base_name}')
            else:
                filled_path = os.path.join(output_dir, f'filled_{base_name}')
                
            logger.info(f"Generated filled path: {filled_path}")
            
            # Save the changes
            doc.save(filled_path)
            logger.info(f"Saved filled PDF to: {filled_path} with {fields_modified} fields modified")
            
            doc.close()
            return filled_path
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

def organize_files_by_section(cover_page, templates, maintenance_docs, job_files, warranty_docs=None):
    """
    Organize files into sections with headers.
    
    Args:
        cover_page: Path to cover page
        templates: List of template file paths
        maintenance_docs: List of maintenance document paths
        job_files: List of job folder file paths
    Returns:
        List of organized file paths including section headers
    """
    organized_files = []
    
    # Add cover page
    if cover_page:
        organized_files.append(cover_page)
        logger.info(f"Added cover page to organized files: {os.path.basename(cover_page)}")
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
    
    # Add Equipment Templates section
    if templates:
        template_header = create_section_header("Equipment Templates")
        organized_files.append(template_header)
        logger.info(f"Added template header to organized files")
        
        # Log each template being added
        logger.info(f"Adding {len(templates)} templates to organized files:")
        for i, template in enumerate(templates):
            organized_files.append(template)
            logger.info(f"  {i+1}. Added template: {os.path.basename(template)}")
    
    # Add Maintenance & Operation section
    if maintenance_docs:
        try:
            always_include_basenames = {"table_of_contents.pdf", "special_instructions.pdf", "additional_info.pdf"}
            maintenance_docs = [p for p in maintenance_docs if os.path.basename(p).lower() not in always_include_basenames]
        except Exception:
            pass
        maintenance_header = create_section_header("Maintenance & Operation Guides")
        organized_files.append(maintenance_header)
        logger.info(f"Added maintenance header to organized files")
        organized_files.extend(maintenance_docs)
        logger.info(f"Added {len(maintenance_docs)} maintenance docs to organized files")

    # Always include additional_info.pdf after Maintenance section and before Project Documentation
    try:
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        maint_dir = os.path.join(project_dir, "maintenance_docs")
        additional_info_path = os.path.join(maint_dir, "additional_info.pdf")
        if os.path.exists(additional_info_path) and additional_info_path.lower().endswith('.pdf'):
            organized_files.append(additional_info_path)
            logger.info("Inserted always-include doc before project docs: additional_info.pdf")
        else:
            logger.warning(f"Always-include doc missing or invalid: {additional_info_path}")
    except Exception as e:
        logger.error(f"Error inserting additional_info.pdf: {e}")
    
    # Add Project Documentation section
    if job_files:
        project_header = create_section_header("Project Documentation")
        organized_files.append(project_header)
        logger.info(f"Added project header to organized files")
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
            sections.get('warranty', [])
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