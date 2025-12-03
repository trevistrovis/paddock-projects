from flask import Flask, render_template, request, send_file, jsonify
import os
import tempfile
import zipfile
from io import BytesIO
from utils.pdf_utils import (
    generate_cover_page,
    extract_items_from_sales_order,
    match_templates,
    merge_pdfs,
    find_warranty_documents,
    fill_gutter_maintenance_doc,
)
from utils.excel_utils import extract_job_metadata
from werkzeug.utils import secure_filename
import logging
import fitz  # PyMuPDF for thumbnails

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get absolute paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
TEMPLATE_FOLDER = os.path.join(BASE_DIR, 'template_cache')
MAINTENANCE_DOCS = os.path.join(BASE_DIR, 'maintenance_docs')
WARRANTY_DOCS = os.path.join(BASE_DIR, 'warranty_docs')
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)
os.makedirs(MAINTENANCE_DOCS, exist_ok=True)
os.makedirs(WARRANTY_DOCS, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
THUMBNAIL_FOLDER = os.path.join(TEMPLATE_FOLDER, '.thumbnails')
os.makedirs(THUMBNAIL_FOLDER, exist_ok=True)

logger.info(f"Template folder: {TEMPLATE_FOLDER}")
logger.info(f"Maintenance docs folder: {MAINTENANCE_DOCS}")

def clear_upload_folder():
    """
    Clear all files from the upload folder to prevent bloating and overlapping documents.
    """
    logger.info("Clearing upload folder...")
    for filename in os.listdir(UPLOAD_FOLDER):
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                import shutil
                shutil.rmtree(file_path)
            logger.info(f"Removed {file_path}")
        except Exception as e:
            logger.error(f"Error while deleting {file_path}: {e}")

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Clear upload folder before processing new files
        clear_upload_folder()
        customer = request.form['customer']
        job_name = request.form['job_name']
        phone = request.form['phone']

        # Get flow rate information from multiple filters
        filter_count = int(request.form.get('filter_count', '1'))
        
        # Create a list to store flow data for each filter
        filters_data = []
        
        for i in range(1, filter_count + 1):
            filter_data = {
                'filter_name': request.form.get(f'filter_name_{i}', f'Filter {i}'),
                'primary_flow_rate': request.form.get(f'primary_flow_rate_{i}', ''),
                'backwash_rate': request.form.get(f'backwash_rate_{i}', ''),
                'total_dynamic_head': request.form.get(f'total_dynamic_head_{i}', ''),
                'filter_id': str(i)  # Store the filter ID for mapping
            }
            # Only add filters that have at least one value filled
            if any(value for key, value in filter_data.items() if key not in ['filter_name', 'filter_id']):
                filters_data.append(filter_data)
                logger.info(f"Added flow data for {filter_data['filter_name']}")
        
        # Process template-filter mappings
        template_mappings = {}
        template_count = int(request.form.get('template_count', '0'))
        
        for i in range(template_count):
            template_name = request.form.get(f'template_name_{i}', '')
            filter_id = request.form.get(f'template_filter_map_{i}', '')
            
            if template_name and filter_id:
                template_mappings[template_name] = filter_id
                logger.info(f"Mapped template '{template_name}' to filter ID {filter_id}")
        
        logger.info(f"Template mappings: {template_mappings}")
        
        # For backward compatibility, use the first filter's data as the main flow_data
        flow_data = filters_data[0] if filters_data else {
            'filter_name': 'Filter 1',
            'primary_flow_rate': '',
            'backwash_rate': '',
            'total_dynamic_head': '',
        }

        # Collect gutter information (optional)
        gutter_features = request.form.getlist('gutter_features') or []
        raw_has_grating = request.form.get('has_grating')
        # Only record a value when the checkbox is actually checked; otherwise treat as blank
        has_grating = 'Yes' if raw_has_grating == 'yes' else ''

        gutter_data = {
            'inlet_count': request.form.get('inlet_count', ''),
            'inlet_size': request.form.get('inlet_size', ''),
            'drawing_number': request.form.get('drawing_number', ''),
            'gutter_option': request.form.get('gutter_option', ''),
            'has_grating': has_grating,
            'gutter_features': gutter_features,
            'gutter_features_text': ", ".join(gutter_features) if gutter_features else ''
        }

        # If all gutter fields are effectively empty, disable gutter_data entirely
        if not any(gutter_data.get(k) for k in [
            'inlet_count',
            'inlet_size',
            'drawing_number',
            'gutter_option',
            'has_grating',
            'gutter_features_text',
        ]):
            gutter_data = None

        sales_order = request.files['sales_order']
        ot_file = request.files.get('ot_file')
        job_folder = request.files.getlist('job_folder')

        # Log the files being processed
        logger.info(f"Processing sales order: {sales_order.filename}")
        if ot_file:
            logger.info(f"Processing OT file: {ot_file.filename}")
        
        logger.info(f"Number of files in job folder: {len(job_folder)}")
        
        so_path = os.path.join(UPLOAD_FOLDER, secure_filename(sales_order.filename))
        sales_order.save(so_path)

        if ot_file:
            ot_path = os.path.join(UPLOAD_FOLDER, secure_filename(ot_file.filename))
            ot_file.save(ot_path)
        else:
            ot_path = None

        job_folder_paths = []
        for f in job_folder:

            if '/void/' in f.filename.lower() or '\\void\\' in f.filename.lower():
                logger.info(f"Skipping files in VOID folder: {f.filename}")
                continue

            if f.filename.lower().endswith('.pdf'):
                logger.info(f"Processing job folder PDF: {f.filename}")
                # Extract the relative path to maintain folder structure
                relative_path = secure_filename(f.filename)
                full_path = os.path.join(UPLOAD_FOLDER, relative_path)
                
                # Create necessary subdirectories
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                f.save(full_path)
                job_folder_paths.append(full_path)
            else:
                logger.warning(f"Skipping non-PDF file in job folder: {f.filename}")

        # Extract items from sales order
        logger.info("Extracting keywords from sales order...")
        item_keywords = extract_items_from_sales_order(so_path)
        logger.info(f"Keywords from sales order: {item_keywords}")
        
        # Also look for keywords in the uploaded job folder PDFs
        logger.info("Processing job folder PDFs for additional keywords...")
        for pdf_path in job_folder_paths:
            try:
                additional_keywords = extract_items_from_sales_order(pdf_path)
                logger.info(f"Keywords from {os.path.basename(pdf_path)}: {additional_keywords}")
                item_keywords.extend(additional_keywords)
            except Exception as e:
                logger.error(f"Error processing {pdf_path}: {str(e)}")
        
        # Remove duplicates and normalize keywords
        item_keywords = list(set(item_keywords))
        logger.info(f"Final deduplicated keywords: {item_keywords}")

        # Match templates and maintenance docs
        logger.info(f"Looking for templates and maintenance docs in {TEMPLATE_FOLDER}")
        logger.info(f"Maintenance docs directory: {os.path.join(TEMPLATE_FOLDER, 'maintenance_docs')}")
        logger.info(f"Maintenance docs exist: {os.path.exists(os.path.join(TEMPLATE_FOLDER, 'maintenance_docs'))}")
        logger.info(f"Processing with {len(filters_data) if filters_data else 0} filters")
        
        use_only_selected = template_count > 0
        templates, maintenance_docs = match_templates(
            item_keywords,
            TEMPLATE_FOLDER,
            flow_data=flow_data,
            filters_data=filters_data,
            template_mappings=template_mappings,
            gutter_data=gutter_data,
            use_only_selected=use_only_selected
        )
        logger.info(f"Matched templates: {[os.path.basename(t) for t in templates]}")
        logger.info(f"Total templates returned: {len(templates)}")
        logger.info(f"Matched maintenance docs: {[os.path.basename(d) for d in maintenance_docs]}")

        # If any gutter data provided, ensure gutter_care.pdf is filled with those fields
        if gutter_data and any(gutter_data.get(k) for k in [
            'inlet_count',
            'inlet_size',
            'drawing_number',
            'gutter_option',
            'has_grating',
            'gutter_features_text',
        ]):
            try:
                gutter_care_path = os.path.join(MAINTENANCE_DOCS, 'gutter_care.pdf')
                if os.path.exists(gutter_care_path):
                    filled_gutter_care = fill_gutter_maintenance_doc(gutter_care_path, gutter_data)
                    # Replace existing occurrence or append
                    replaced = False
                    for i, p in enumerate(maintenance_docs):
                        if os.path.basename(p).lower() == 'gutter_care.pdf':
                            maintenance_docs[i] = filled_gutter_care
                            replaced = True
                            break
                    if not replaced:
                        maintenance_docs.append(filled_gutter_care)
                    logger.info("Processed gutter_care.pdf with gutter data")
                else:
                    logger.warning(f"gutter_care.pdf not found in maintenance docs folder: {gutter_care_path}")
            except Exception as e:
                logger.error(f"Error preparing gutter_care.pdf: {e}")

        # Find warranty docs based on keywords and append as last section
        warranty_docs = find_warranty_documents(item_keywords)
        logger.info(f"Matched warranty docs: {[os.path.basename(d) for d in warranty_docs]}")

        # Always-include documents
        def _append_unique(seq, item):
            if item and item not in seq:
                seq.append(item)

        # 1) Always append Prevent-p-poster.pdf to end of Maintenance section
        prevent_p_path = os.path.join(MAINTENANCE_DOCS, "Prevent-p-poster.pdf")
        if os.path.exists(prevent_p_path):
            _append_unique(maintenance_docs, prevent_p_path)
            logger.info("Appended required maintenance doc: Prevent-p-poster.pdf")
        else:
            logger.warning(f"Required maintenance doc missing: {prevent_p_path}")

        # Determine if project contains a filter
        keywords_lower = [k.lower() for k in item_keywords]
        has_filter = any(any(term in k for term in ["filter", "regenerator"]) for k in keywords_lower)
        if not has_filter:
            has_filter = bool(filters_data)  # flow data implies filters present
        if not has_filter:
            try:
                has_filter = any("filter" in os.path.basename(t).lower() for t in templates)
            except Exception:
                has_filter = has_filter
        logger.info(f"Project contains filter: {has_filter}")

        # 2) If project contains a filter, append Valve Series 30/31 PDF to Maintenance
        valve_doc_path = os.path.join(MAINTENANCE_DOCS, "Valve Series 30 Wafer and Series 31-416 standard.pdf")
        if has_filter:
            if os.path.exists(valve_doc_path):
                _append_unique(maintenance_docs, valve_doc_path)
                logger.info("Appended valve document for filter projects: Valve Series 30 Wafer and Series 31-416 standard.pdf")
            else:
                logger.warning(f"Valve document missing (expected for filter projects): {valve_doc_path}")

        # 3) Always append Sales Bulletin to end of Warranty section
        sales_bulletin_path = os.path.join(WARRANTY_DOCS, "SALES BULLETIN 84-4-R W-LOGO revformat7-2021.pdf")
        if os.path.exists(sales_bulletin_path):
            _append_unique(warranty_docs, sales_bulletin_path)
            logger.info("Appended required warranty doc: SALES BULLETIN 84-4-R W-LOGO revformat7-2021.pdf")
        else:
            logger.warning(f"Required warranty doc missing: {sales_bulletin_path}")

        # After maintenance_docs list is finalized, replace any items with their
        # filled counterparts from MAINTENANCE_DOCS/filled when present.
        try:
            filled_dir = os.path.join(MAINTENANCE_DOCS, 'filled')
            if os.path.isdir(filled_dir):
                # Build mapping from original basename -> filled path
                filled_map = {}
                for fname in os.listdir(filled_dir):
                    if not fname.lower().endswith('.pdf'):
                        continue
                    # For files like filled_gutter_care.pdf, infer original name
                    lower = fname.lower()
                    if lower.startswith('filled_') and len(fname) > len('filled_'):
                        orig = fname[len('filled_'):]
                        filled_map[orig.lower()] = os.path.join(filled_dir, fname)
                if filled_map:
                    new_maintenance = []
                    for p in maintenance_docs:
                        base = os.path.basename(p).lower()
                        if base in filled_map:
                            logger.info(f"Using filled maintenance doc for {base}: {os.path.basename(filled_map[base])}")
                            new_maintenance.append(filled_map[base])
                        else:
                            new_maintenance.append(p)
                    maintenance_docs = new_maintenance
        except Exception as e:
            logger.error(f"Error swapping in filled maintenance docs: {e}")

        # Create cover page with flow data from all filters
        cover_pdf_path = generate_cover_page(customer, job_name, phone, filters_data=filters_data)
        logger.info(f"Generated cover page: {cover_pdf_path}")

        # Organize files into sections
        output_pdf_path = os.path.join(OUTPUT_FOLDER, f'{job_name}_Manual.pdf')
        logger.info(f"Merging PDFs into: {output_pdf_path}")
        
        # Log organization of files
        logger.info("Files organized by section:")
        logger.info(f"1. Cover page: {cover_pdf_path}")
        logger.info("2. Equipment Templates:")
        for i, template in enumerate(templates, 1):
            logger.info(f"   {i}. {os.path.basename(template)}")
        logger.info("3. Maintenance & Operation Guides:")
        for i, doc in enumerate(maintenance_docs, 1):
            logger.info(f"   {i}. {os.path.basename(doc)}")
        logger.info("4. Project Documentation:")
        for i, pdf in enumerate(job_folder_paths, 1):
            logger.info(f"   {i}. {os.path.basename(pdf)}")
        
        # Create sections dictionary for organized merging
        sections = {
            'cover': cover_pdf_path,
            'templates': templates,
            'maintenance': maintenance_docs,
            # job_files will be set after filtering
            'job_files': [],
            'warranty': warranty_docs,
        }
        
        # Log detailed information about templates
        logger.info("Templates to be included in the manual:")
        for i, template in enumerate(templates):
            logger.info(f"  {i+1}. {os.path.basename(template)}")
        
        # When users upload their Job Folder, it may contain extra template PDFs.
        # To ensure ONLY explicitly selected templates are included, exclude any
        # job folder PDFs that appear to be templates (e.g., filename contains 'template').
        filtered_job_files = []
        for p in job_folder_paths:
            base = os.path.basename(p).lower()
            if 'template' in base:
                logger.info(f"Excluding job folder file that looks like a template: {base}")
                continue
            filtered_job_files.append(p)

        # Update sections with filtered job files
        sections['job_files'] = filtered_job_files

        # For backward compatibility, keep a list of all PDFs
        all_pdfs = [cover_pdf_path] + templates + maintenance_docs + filtered_job_files + warranty_docs
        logger.info(f"Total PDFs to merge: {len(all_pdfs)}")
        logger.info(f"Templates count: {len(templates)}")
        logger.info(f"Maintenance docs count: {len(maintenance_docs)}")
        logger.info(f"Warranty docs count: {len(warranty_docs)}")
        logger.info(f"Job files count: {len(filtered_job_files)} (filtered out {len(job_folder_paths) - len(filtered_job_files)} template-like files)")
        
        success, message, skipped_files = merge_pdfs(all_pdfs, output_pdf_path, organized=True, sections=sections)
        
        if not success:
            logger.error("Failed to create output PDF")
            return f"Error creating manual: {message}", 500
        
        # If we have skipped files but still created a PDF, show a warning to the user
        if skipped_files:
            skipped_msg = "Warning: Some documents were skipped:\n"
            for file, reason in skipped_files:
                skipped_msg += f"- {os.path.basename(file)}: {reason}\n"
            logger.warning(skipped_msg)
            
            # Create a warning file next to the PDF
            warning_path = os.path.splitext(output_pdf_path)[0] + "_warnings.txt"
            with open(warning_path, "w") as f:
                f.write(skipped_msg)
            
            # Return both files in a zip
            memory_file = BytesIO()
            with zipfile.ZipFile(memory_file, 'w') as zf:
                zf.write(output_pdf_path, os.path.basename(output_pdf_path))
                zf.write(warning_path, os.path.basename(warning_path))
            
            memory_file.seek(0)
            return send_file(
                memory_file,
                mimetype='application/zip',
                as_attachment=True,
                download_name=f'{job_name}_Manual.zip'
            )
        
        logger.info(f"Successfully created manual at: {output_pdf_path}")
        return send_file(output_pdf_path, as_attachment=True)

    return render_template('index.html')

@app.get('/api/templates')
def api_list_templates():
    """List PDFs in template_cache with basic metadata and thumbnail URLs."""
    try:
        items = []
        for fname in sorted(os.listdir(TEMPLATE_FOLDER)):
            if not fname.lower().endswith('.pdf'):
                continue
            fpath = os.path.join(TEMPLATE_FOLDER, fname)
            try:
                stat = os.stat(fpath)
                items.append({
                    'name': fname,
                    'size': stat.st_size,
                    'mtime': stat.st_mtime,
                    'thumbnail': f'/template_thumbnail?name={fname}'
                })
            except Exception:
                continue
        return jsonify({'templates': items})
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        return jsonify({'error': str(e)}), 500

def _ensure_thumbnail(pdf_path, thumb_path):
    """Create a thumbnail PNG for the first page of pdf_path at thumb_path if missing or stale."""
    try:
        if os.path.exists(thumb_path):
            pdf_mtime = os.path.getmtime(pdf_path)
            thumb_mtime = os.path.getmtime(thumb_path)
            if thumb_mtime >= pdf_mtime:
                return True
        doc = fitz.open(pdf_path)
        if len(doc) == 0:
            doc.close()
            return False
        page = doc[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(0.8, 0.8), alpha=False)
        pix.save(thumb_path)
        doc.close()
        return True
    except Exception as e:
        logger.error(f"Thumbnail generation failed for {pdf_path}: {e}")
        return False

@app.get('/template_thumbnail')
def template_thumbnail():
    """Serve cached or on-the-fly thumbnail for a template PDF by name."""
    name = request.args.get('name', '')
    if not name or not name.lower().endswith('.pdf'):
        return 'Invalid name', 400
    safe_name = os.path.basename(name)
    pdf_path = os.path.join(TEMPLATE_FOLDER, safe_name)
    if not os.path.exists(pdf_path):
        return 'Not found', 404
    thumb_name = safe_name + '.png'
    thumb_path = os.path.join(THUMBNAIL_FOLDER, thumb_name)
    ok = _ensure_thumbnail(pdf_path, thumb_path)
    if not ok or not os.path.exists(thumb_path):
        return 'Thumbnail error', 500
    return send_file(thumb_path, mimetype='image/png')

@app.route('/regenerate_cover', methods=['POST'])
def regenerate_cover():
    try:
        customer = request.form['customer']
        job_name = request.form['job_name']
        phone = request.form['phone']
        
        logger.info(f"Regenerating cover page for job: {job_name}")
        
        # Get flow rate information for multiple filters
        filter_count = int(request.form.get('filter_count', '1'))
        logger.info(f"Processing {filter_count} filters for cover page")
        
        # Create a list to store flow data for each filter
        filters_data = []
        
        for i in range(1, filter_count + 1):
            filter_data = {
                'filter_name': request.form.get(f'filter_name_{i}', f'Filter {i}'),
                'primary_flow_rate': request.form.get(f'primary_flow_rate_{i}', ''),
                'backwash_rate': request.form.get(f'backwash_rate_{i}', ''),
                'total_dynamic_head': request.form.get(f'total_dynamic_head_{i}', ''),
            }
            # Only add filters that have at least one value filled
            if any(value for key, value in filter_data.items() if key != 'filter_name'):
                filters_data.append(filter_data)
                logger.info(f"Added flow data for {filter_data['filter_name']}")
        
        # Generate new cover page with flow data from all filters
        cover_path = generate_cover_page(customer, job_name, phone, filters_data=filters_data)
        
        if not os.path.exists(cover_path):
            return "Failed to generate cover page", 500
            
        # Return the cover page
        return send_file(
            cover_path,
            as_attachment=True,
            download_name=f"cover_{job_name}.pdf"
        )
        
    except Exception as e:
        logger.error(f"Error regenerating cover page: {str(e)}")
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    # Only use debug mode when running directly
    is_debug = os.environ.get('FLASK_ENV') == 'development'
    port = int(os.environ.get('PORT', 5000))
    app.run(host='localhost', port=port, debug=is_debug)
