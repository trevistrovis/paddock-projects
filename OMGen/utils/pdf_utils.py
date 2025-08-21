# utils/pdf_utils.py
import tempfile
import fitz  # PyMuPDF
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os
from PyPDF2 import PdfMerger, PdfReader
import re
import logging

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

def match_templates(keywords, template_dir, flow_data=None):
    """
    Match templates and include associated maintenance documents with improved matching algorithm.
    
    Args:
        keywords: List of keywords to match against templates
        template_dir: Directory containing templates
        flow_data: Optional dictionary containing flow rate information
        
    Returns:
        tuple (templates, maintenance_docs) where:
        - templates: List of matched template file paths (with flow data filled if provided)
        - maintenance_docs: List of matched maintenance document paths
    """
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
    
    # Convert sets to sorted lists for consistent ordering
    template_list = sorted(list(matched_templates))
    maintenance_list = sorted(list(matched_maintenance))
    
    # Prioritize flow-related templates by moving them to the front
    if flow_templates:
        non_flow = [t for t in template_list if t not in flow_templates]
        template_list = sorted(list(flow_templates)) + non_flow
    
    # If we have flow data, fill it in the templates
    if flow_data:
        logger.info("Filling flow data in matched templates...")
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

def generate_cover_page(customer, job_name, phone, flow_data=None):
    """
    Generate a cover page using the template and adding text at specific coordinates.
    Args:
        customer: Customer name
        job_name: Job name, city, state, zip
        phone: Phone number
        flow_data: Optional dictionary containing flow rate information
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
    
    logger.info(f"Generating cover page from template for job: {job_name}")
    logger.info(f"Template path: {template_path}")
    logger.info(f"Output path: {cover_path}")
    
    try:
        # Open the template PDF
        doc = fitz.open(template_path)
        page = doc[0]  # Get first page
        
        # Define text positions and properties
        text_fields = [
            {"text": customer, "x": 200, "y": 660, "fontsize": 18, "color": (0, 0, 0)},  # Black color
            {"text": job_name, "x": 200, "y": 680, "fontsize": 16, "color": (0, 0, 0)},
            {"text": phone, "x": 200, "y": 700, "fontsize": 16, "color": (0, 0, 0)}
        ]
        
        # Add flow rate information if provided
        if flow_data:
            y_offset = 720  # Start position for flow rate info
            if flow_data.get('primary_flow_rate'):
                text_fields.append({
                    "text": f"Primary Flow Rate: {flow_data['primary_flow_rate']} GPM",
                    "x": 200, "y": y_offset, "fontsize": 14, "color": (0, 0, 0)
                })
                y_offset += 20
            
            if flow_data.get('backwash_rate'):
                text_fields.append({
                    "text": f"Backwash Rate: {flow_data['backwash_rate']} GPM",
                    "x": 200, "y": y_offset, "fontsize": 14, "color": (0, 0, 0)
                })
                y_offset += 20
                
            if flow_data.get('total_dynamic_head'):
                # Split notes into multiple lines if needed
                notes = flow_data['total_dynamic_head'].split('\n')
                for note in notes:
                    text_fields.append({
                        "text": f"Total Dynamic Head: {note}",
                        "x": 200, "y": y_offset, "fontsize": 12, "color": (0, 0, 0)
                    })
                    y_offset += 15  # Smaller offset for notes
        
        # Insert text for each field
        for field in text_fields:
            # Create a new insertion point
            point = fitz.Point(field["x"], field["y"])
            
            # Insert the text
            page.insert_text(
                point,
                field["text"],
                fontsize=field["fontsize"],
                color=field["color"]
            )
        
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

def fill_pdf_form_fields(pdf_path, flow_data):
    """
    Fill PDF form fields with flow rate data.
    
    Args:
        pdf_path: Path to the PDF template
        flow_data: Dictionary containing flow rate information
    
    Returns:
        Path to the filled PDF or None if no fields were filled
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
            return None
            
        logger.info(f"Found {len(widgets)} form fields in {pdf_path}")
        
        # Track if we made any changes
        made_changes = False
        
        # Define field name variations
        field_variations = {
            'primary_flow_rate': ['primary_flow_rate', 'Primary_Flow_Rate', 'primary flow rate'],
            'backwash_rate': ['backwash_rate', 'backwash rate'],
            'total_dynamic_head': ['total_dynamic_head', 'total dynamic head']
        }
        
        # Map flow data keys to their values, adding GPM where appropriate
        formatted_values = {
            'primary_flow_rate': f"{flow_data.get('primary_flow_rate', '')} GPM" if flow_data.get('primary_flow_rate') else '',
            'backwash_rate': f"{flow_data.get('backwash_rate', '')} GPM" if flow_data.get('backwash_rate') else '',
            'total_dynamic_head': flow_data.get('total_dynamic_head', '')
        }
        
        # Create a page-to-widgets mapping
        page_widgets = {}
        for page_num in range(len(doc)):
            page = doc[page_num]
            for widget in page.widgets():
                if widget.field_type_string == 'Text':
                    page_widgets[widget.field_name] = (page, widget)
        
        # Try to fill in fields
        for field_name, (page, widget) in page_widgets.items():
            logger.info(f"Found text field with name: {field_name}")
            
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
                        # Update the form field
                        widget.field_value = value
                        widget.update()
                        made_changes = True
                        logger.info("Successfully updated field value")
                    except Exception as e:
                        logger.error(f"Error updating field: {str(e)}")
                    break  # Stop checking other variations once we find a match
        
        if made_changes:
            # Save to a new file
            output_dir = os.path.join(os.path.dirname(pdf_path), 'filled')
            os.makedirs(output_dir, exist_ok=True)
            
            base_name = os.path.basename(pdf_path)
            filled_path = os.path.join(output_dir, f'filled_{base_name}')
            
            # Save the changes
            doc.save(filled_path)
            logger.info(f"Saved filled PDF to: {filled_path}")
            
            doc.close()
            return filled_path
        else:
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

def organize_files_by_section(cover_page, templates, maintenance_docs, job_files):
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
    
    # Add Equipment Templates section
    if templates:
        template_header = create_section_header("Equipment Templates")
        organized_files.append(template_header)
        organized_files.extend(templates)
    
    # Add Maintenance & Operation section
    if maintenance_docs:
        maintenance_header = create_section_header("Maintenance & Operation Guides")
        organized_files.append(maintenance_header)
        organized_files.extend(maintenance_docs)
    
    # Add Project Documentation section
    if job_files:
        project_header = create_section_header("Project Documentation")
        organized_files.append(project_header)
        organized_files.extend(job_files)
    
    return organized_files

def merge_pdfs(input_paths, output_path, organized=False, sections=None):
    """
    Merge multiple PDF files into a single PDF.
    
    Args:
        input_paths: List of PDF paths to merge
        output_path: Path for the output PDF
        organized: If True, add section headers (requires sections parameter)
        sections: Dictionary with keys 'cover', 'templates', 'maintenance', 'job_files'
                 containing lists of files for each section
    
    Returns:
        tuple (success: bool, error_message: str, skipped_files: list)
    """
    logger.info(f"Starting PDF merge with {len(input_paths)} files")
    
    # If organized mode is requested, reorganize files with sections
    if organized and sections:
        input_paths = organize_files_by_section(
            sections.get('cover'),
            sections.get('templates', []),
            sections.get('maintenance', []),
            sections.get('job_files', [])
        )
        logger.info("Organized files into sections with headers")
    
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