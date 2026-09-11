from flask import Flask, render_template, request, send_file, jsonify
import os
import tempfile
import zipfile
import shutil
import uuid
from io import BytesIO
from utils.pdf_utils import (
    generate_cover_page,
    extract_items_from_sales_order,
    extract_pools_from_sales_order,
    sort_files_by_keyword_order,
    match_templates,
    merge_pdfs,
    find_warranty_documents,
    fill_gutter_maintenance_doc,
    parse_cover_sheets,
)
from utils.excel_utils import create_equipment_list_pdf, extract_job_metadata
from werkzeug.utils import secure_filename
import logging

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
COVER_SHEETS = os.path.join(BASE_DIR, 'cover_sheets')
WARRANTY_DOCS = os.path.join(BASE_DIR, 'warranty_docs')
ALWAYS_INCLUDE = os.path.join(BASE_DIR, 'always_include')
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')
WORKSPACES_DIR = os.path.join(BASE_DIR, 'workspaces')

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)
os.makedirs(MAINTENANCE_DOCS, exist_ok=True)
os.makedirs(COVER_SHEETS, exist_ok=True)
os.makedirs(WARRANTY_DOCS, exist_ok=True)
os.makedirs(ALWAYS_INCLUDE, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(WORKSPACES_DIR, exist_ok=True)
# Clean up stale workspaces from previous runs
for _stale in os.listdir(WORKSPACES_DIR):
    _stale_path = os.path.join(WORKSPACES_DIR, _stale)
    if os.path.isdir(_stale_path):
        try:
            shutil.rmtree(_stale_path)
        except Exception:
            pass

logger.info(f"Template folder: {TEMPLATE_FOLDER}")
logger.info(f"Maintenance docs folder: {MAINTENANCE_DOCS}")
logger.info(f"Cover sheets folder: {COVER_SHEETS}")

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
                shutil.rmtree(file_path)
            logger.info(f"Removed {file_path}")
        except Exception as e:
            logger.error(f"Error while deleting {file_path}: {e}")

def _create_request_workspace():
    """Create an isolated temporary workspace for the current request."""
    request_id = uuid.uuid4().hex[:12]
    workspace = os.path.join(WORKSPACES_DIR, request_id)
    upload_dir = os.path.join(workspace, 'uploads')
    output_dir = os.path.join(workspace, 'output')
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created request workspace: {workspace}")
    return workspace, upload_dir, output_dir

def _cleanup_workspace(workspace):
    """Remove a request workspace after the response is sent."""
    try:
        if os.path.isdir(workspace):
            shutil.rmtree(workspace)
            logger.info(f"Cleaned up workspace: {workspace}")
    except Exception as e:
        logger.error(f"Error cleaning up workspace {workspace}: {e}")

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Create isolated workspace for this request
        workspace, req_upload_dir, req_output_dir = _create_request_workspace()
        customer = request.form['customer']
        job_name = request.form['job_name']
        phone = request.form['phone']

        # Collect pool information (at least one default pool must exist)
        pool_count = int(request.form.get('pool_count', '1'))
        pools_data = []
        for i in range(1, pool_count + 1):
            pool_name = request.form.get(f'pool_name_{i}', f'Pool {i}').strip()
            if pool_name:
                pools_data.append({'pool_id': str(i), 'pool_name': pool_name})
        if not pools_data:
            pools_data.append({'pool_id': '1', 'pool_name': 'Default'})
        pool_id_set = {p['pool_id'] for p in pools_data}
        default_pool_id = pools_data[0]['pool_id']
        logger.info(f"Pools: {pools_data}")

        # NOTE: Filter flow-rate data collection is disabled. Filter templates are now
        # boilerplate documents and are no longer auto-filled. Keeping the original code
        # intact (commented) in case management reverts this decision.
        #
        # # Get flow rate information from multiple filters
        # filter_count = int(request.form.get('filter_count', '1'))
        #
        # # Create a list to store flow data for each filter
        # filters_data = []
        #
        # for i in range(1, filter_count + 1):
        #     pool_id = request.form.get(f'filter_pool_{i}', default_pool_id).strip()
        #     if pool_id not in pool_id_set:
        #         pool_id = default_pool_id
        #     filter_data = {
        #         'filter_name': request.form.get(f'filter_name_{i}', f'Filter {i}'),
        #         'pool_id': pool_id,
        #         'primary_flow_rate': request.form.get(f'primary_flow_rate_{i}', ''),
        #         'backwash_rate': request.form.get(f'backwash_rate_{i}', ''),
        #         'total_dynamic_head': request.form.get(f'total_dynamic_head_{i}', ''),
        #         'filter_id': str(i)  # Store the filter ID for mapping
        #     }
        #     # Only add filters that have at least one value filled
        #     if any(value for key, value in filter_data.items() if key not in ['filter_name', 'filter_id', 'pool_id']):
        #         filters_data.append(filter_data)
        #         logger.info(f"Added flow data for {filter_data['filter_name']} (pool {pool_id})")
        #
        # Filter flow-rate fill-out and template mapping are disabled.
        # Templates are matched automatically by keyword.
        filters_data = None
        flow_data = None

        # Collect gutter information for multiple gutters (optional)
        gutter_count = int(request.form.get('gutter_count', '1'))
        
        # Create a list to store gutter data for each gutter
        gutters_data = []
        
        for i in range(1, gutter_count + 1):
            drawing_number = request.form.get(f'drawing_number_{i}', '').strip()
            gutter_data = {
                'gutter_name': f'Gutter {i}',
                'pool_id': default_pool_id,
                'drawing_number': drawing_number,
                'gutter_id': str(i)
            }
            if drawing_number:
                gutters_data.append(gutter_data)
                logger.info(f"Added drawing number for {gutter_data['gutter_name']}")
        
        # For backward compatibility, use the first gutter's data as the main gutter_data if any exist
        gutter_data = gutters_data[0] if gutters_data else None

        sales_order = request.files['sales_order']
        ot_file = request.files.get('ot_file')
        job_folder = request.files.getlist('job_folder')

        # Log the files being processed
        logger.info(f"Processing sales order: {sales_order.filename}")
        if ot_file:
            logger.info(f"Processing OT file: {ot_file.filename}")
        
        logger.info(f"Number of files in job folder: {len(job_folder)}")
        
        so_path = os.path.join(req_upload_dir, secure_filename(sales_order.filename))
        sales_order.save(so_path)

        equipment_list_pdf = None
        if ot_file and ot_file.filename:
            spreadsheet_name = secure_filename(ot_file.filename)
            if not spreadsheet_name.lower().endswith(('.xls', '.xlsx')):
                _cleanup_workspace(workspace)
                return "Equipment list must be an Excel file (.xls or .xlsx).", 400
            ot_path = os.path.join(req_upload_dir, spreadsheet_name)
            ot_file.save(ot_path)
            try:
                equipment_list_pdf = create_equipment_list_pdf(
                    ot_path,
                    os.path.join(req_output_dir, "equipment_list.pdf"),
                    title=f"Equipment List - {job_name}",
                )
                logger.info(f"Created equipment list PDF: {equipment_list_pdf}")
            except Exception as e:
                logger.error(f"Error creating equipment list PDF: {e}")
                _cleanup_workspace(workspace)
                return f"Error converting equipment list spreadsheet: {e}", 400
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
                full_path = os.path.join(req_upload_dir, relative_path)
                
                # Create necessary subdirectories
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                f.save(full_path)
                job_folder_paths.append(full_path)
            else:
                logger.warning(f"Skipping non-PDF file in job folder: {f.filename}")

        # Extract items from sales order, detecting pool separators if present.
        logger.info("Extracting keywords and pools from sales order...")
        parsed_pools_data, pool_keywords_map = extract_pools_from_sales_order(so_path)
        use_parsed_pools = bool(
            parsed_pools_data and
            (len(parsed_pools_data) > 1 or parsed_pools_data[0]['pool_name'].lower() != 'default')
        )
        if use_parsed_pools:
            pools_data = parsed_pools_data
            default_pool_id = pools_data[0]['pool_id']
            logger.info(f"Detected {len(pools_data)} pools from sales order: {[p['pool_name'] for p in pools_data]}")
        else:
            logger.info("No pool separators detected in sales order; using form pools")

        # The sales order is the authoritative source of equipment keywords.
        # We intentionally do NOT scan the uploaded job-folder PDF contents for
        # keywords, because drawings/cutsheets often mention unrelated equipment
        # (e.g. "Bulkhead" in a general note or title block) and pull unwanted
        # maintenance or warranty documents.
        additional_keywords = []

        # Build per-pool and global keyword lists
        if use_parsed_pools:
            item_keywords = []
            for pid, kws in pool_keywords_map.items():
                kws = list(set(kws + additional_keywords))
                pool_keywords_map[pid] = kws
                item_keywords.extend(kws)
            item_keywords = list(set(item_keywords))
        else:
            item_keywords = extract_items_from_sales_order(so_path)
            item_keywords = list(set(item_keywords + additional_keywords))
        logger.info(f"Final deduplicated keywords: {item_keywords}")

        # Sort the uploaded cutsheets/drawings to follow the order they appear
        # in the sales order based on filename matching.
        job_folder_paths = sort_files_by_keyword_order(job_folder_paths, item_keywords)
        logger.info(f"Reordered job folder files by sales-order keyword order: {[os.path.basename(p) for p in job_folder_paths]}")

        # Match templates and maintenance docs (per pool if pools were parsed)
        logger.info(f"Looking for templates and maintenance docs in {TEMPLATE_FOLDER}")
        logger.info(f"Maintenance docs directory: {os.path.join(TEMPLATE_FOLDER, 'maintenance_docs')}")
        logger.info(f"Maintenance docs exist: {os.path.exists(os.path.join(TEMPLATE_FOLDER, 'maintenance_docs'))}")

        pool_templates = {p['pool_id']: [] for p in pools_data}
        pool_maintenance = {p['pool_id']: [] for p in pools_data}
        if use_parsed_pools:
            maintenance_docs = []
            for pool in pools_data:
                pid = pool['pool_id']
                kws = pool_keywords_map.get(pid, [])
                templates, maint = match_templates(
                    kws,
                    TEMPLATE_FOLDER,
                    gutter_data=gutter_data,
                    output_dir=req_output_dir
                )
                pool_templates[pid] = templates
                pool_maintenance[pid] = maint
                maintenance_docs.extend(maint)
                logger.info(f"Pool '{pool['pool_name']}' matched {len(templates)} templates, {len(maint)} maintenance docs")
            templates = [t for ts in pool_templates.values() for t in ts]
            maintenance_docs = list(set(maintenance_docs))
        else:
            templates, maintenance_docs = match_templates(
                item_keywords,
                TEMPLATE_FOLDER,
                gutter_data=gutter_data,
                output_dir=req_output_dir
            )
            pool_templates.setdefault(default_pool_id, []).extend(templates)
        logger.info(f"Matched templates: {[os.path.basename(t) for t in templates]}")
        logger.info(f"Total templates returned: {len(templates)}")
        logger.info(f"Matched maintenance docs: {[os.path.basename(d) for d in maintenance_docs]}")

        # If any gutter data provided, fill gutter_care.pdf and the Perimeter Overflow
        # & Recirculation gutter doc for each gutter, grouping by pool.
        pool_gutter_care = {p['pool_id']: [] for p in pools_data}
        if gutters_data:
            try:
                # 1) Legacy gutter_care.pdf if still present
                gutter_care_path = os.path.join(MAINTENANCE_DOCS, 'gutter_care.pdf')
                if os.path.exists(gutter_care_path):
                    # Remove any existing gutter_care.pdf references from maintenance_docs
                    maintenance_docs = [p for p in maintenance_docs if 'gutter_care.pdf' not in os.path.basename(p).lower()]

                    for gutter in gutters_data:
                        if gutter.get('drawing_number'):
                            # Create a safe name for the output file
                            gutter_name_safe = gutter.get('gutter_name', f'Gutter_{gutter.get("gutter_id", 0)}')
                            gutter_name_safe = gutter_name_safe.replace(' ', '_').replace('/', '_').replace('\\', '_')

                            filled_gutter_care = fill_gutter_maintenance_doc(gutter_care_path, gutter, gutter_name=gutter_name_safe, output_dir=req_output_dir)
                            if filled_gutter_care:
                                pool_id = gutter.get('pool_id', default_pool_id)
                                if pool_id not in pool_gutter_care:
                                    pool_id = default_pool_id
                                pool_gutter_care[pool_id].append(filled_gutter_care)
                                logger.info(f"Processed gutter_care.pdf for {gutter['gutter_name']} (pool {pool_id})")
                else:
                    logger.info(f"gutter_care.pdf not found in maintenance docs folder; skipping")

                # 2) Fill the Perimeter Overflow & Recirculation System -gutter dwg.pdf
                #    with each gutter's drawing number and replace the original in
                #    the maintenance docs list.
                perimeter_base = "Perimeter Overflow & Recirculation System -gutter dwg.pdf"
                perimeter_path = None
                perimeter_index = None
                for i, p in enumerate(maintenance_docs):
                    if os.path.basename(p).lower() == perimeter_base.lower():
                        perimeter_path = p
                        perimeter_index = i
                        break

                if perimeter_path:
                    filled_perimeter = []
                    for gutter in gutters_data:
                        if gutter.get('drawing_number'):
                            gutter_name_safe = gutter.get('gutter_name', f'Gutter_{gutter.get("gutter_id", 0)}')
                            gutter_name_safe = gutter_name_safe.replace(' ', '_').replace('/', '_').replace('\\', '_')

                            filled_gutter_doc = fill_gutter_maintenance_doc(perimeter_path, gutter, gutter_name=gutter_name_safe, output_dir=req_output_dir)
                            if filled_gutter_doc:
                                filled_perimeter.append(filled_gutter_doc)
                                logger.info(f"Filled {perimeter_base} for {gutter['gutter_name']}")

                    if filled_perimeter and perimeter_index is not None:
                        maintenance_docs = maintenance_docs[:perimeter_index] + filled_perimeter + maintenance_docs[perimeter_index + 1:]
            except Exception as e:
                logger.error(f"Error preparing gutter maintenance docs: {e}")

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

        # Determine cover page: prefer an unnumbered cover sheet from the
        # cover_sheets folder; fill its form fields if it has them.
        cover_info = parse_cover_sheets(COVER_SHEETS)
        if cover_info.get('main_cover'):
            cover_pdf_path = generate_cover_page(
                customer, job_name, phone,
                filters_data=filters_data,
                output_dir=req_output_dir,
                template_path=cover_info['main_cover']
            )
            logger.info(f"Using filled folder cover sheet as main cover: {os.path.basename(cover_pdf_path)}")
        else:
            cover_pdf_path = generate_cover_page(customer, job_name, phone, filters_data=filters_data, output_dir=req_output_dir)
            logger.info(f"Generated cover page: {cover_pdf_path}")

        cover_sheets = cover_info

        # Organize files into sections
        output_pdf_path = os.path.join(req_output_dir, f'{job_name}_Manual.pdf')
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
        
        # pool_templates and pool_maintenance are already populated above.
        logger.info(f"Templates grouped by pool: { {k: [os.path.basename(p) for p in v] for k,v in pool_templates.items()} }")

        # Create sections dictionary for organized merging
        sections = {
            'cover': cover_pdf_path,
            # Only use the flat lists as shared items when not using parsed pools.
            'templates': [] if use_parsed_pools else templates,
            'maintenance': [] if use_parsed_pools else maintenance_docs,
            'equipment_list': equipment_list_pdf,
            # job_files will be set after filtering
            'job_files': [],
            'warranty': warranty_docs,
            'pools_data': pools_data,
            'pool_templates': pool_templates,
            'pool_maintenance': pool_maintenance,
            'pool_gutter_care': pool_gutter_care,
            'cover_sheets': cover_sheets,
            'always_include': ALWAYS_INCLUDE,
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
        all_pdfs = [cover_pdf_path] + templates + maintenance_docs + ([equipment_list_pdf] if equipment_list_pdf else []) + filtered_job_files + warranty_docs
        logger.info(f"Total PDFs to merge: {len(all_pdfs)}")
        logger.info(f"Templates count: {len(templates)}")
        logger.info(f"Maintenance docs count: {len(maintenance_docs)}")
        logger.info(f"Warranty docs count: {len(warranty_docs)}")
        logger.info(f"Job files count: {len(filtered_job_files)} (filtered out {len(job_folder_paths) - len(filtered_job_files)} template-like files)")
        
        success, message, skipped_files = merge_pdfs(all_pdfs, output_pdf_path, organized=True, sections=sections)
        
        if not success:
            logger.error("Failed to create output PDF")
            _cleanup_workspace(workspace)
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
            
            # Return both files in a zip (in memory so workspace can be cleaned)
            memory_file = BytesIO()
            with zipfile.ZipFile(memory_file, 'w') as zf:
                zf.write(output_pdf_path, os.path.basename(output_pdf_path))
                zf.write(warning_path, os.path.basename(warning_path))
            
            memory_file.seek(0)
            _cleanup_workspace(workspace)
            return send_file(
                memory_file,
                mimetype='application/zip',
                as_attachment=True,
                download_name=f'{job_name}_Manual.zip'
            )
        
        logger.info(f"Successfully created manual at: {output_pdf_path}")
        # Read output into memory so workspace can be cleaned up
        output_data = BytesIO()
        with open(output_pdf_path, 'rb') as f:
            output_data.write(f.read())
        output_data.seek(0)
        _cleanup_workspace(workspace)
        return send_file(
            output_data,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'{job_name}_Manual.pdf'
        )

    return render_template('index.html')

@app.route('/regenerate_cover', methods=['POST'])
def regenerate_cover():
    try:
        customer = request.form['customer']
        job_name = request.form['job_name']
        phone = request.form['phone']
        
        logger.info(f"Regenerating cover page for job: {job_name}")
        
        # NOTE: Filter flow-rate data is no longer collected for cover pages. Cover page
        # filling (customer / job / phone) remains active. Keeping the original code below
        # commented in case management reverts this decision.
        #
        # # Get flow rate information for multiple filters
        # filter_count = int(request.form.get('filter_count', '1'))
        # logger.info(f"Processing {filter_count} filters for cover page")
        #
        # # Create a list to store flow data for each filter
        # filters_data = []
        #
        # for i in range(1, filter_count + 1):
        #     filter_data = {
        #         'filter_name': request.form.get(f'filter_name_{i}', f'Filter {i}'),
        #         'primary_flow_rate': request.form.get(f'primary_flow_rate_{i}', ''),
        #         'backwash_rate': request.form.get(f'backwash_rate_{i}', ''),
        #         'total_dynamic_head': request.form.get(f'total_dynamic_head_{i}', ''),
        #     }
        #     # Only add filters that have at least one value filled
        #     if any(value for key, value in filter_data.items() if key != 'filter_name'):
        #         filters_data.append(filter_data)
        #         logger.info(f"Added flow data for {filter_data['filter_name']}")
        
        filters_data = None
        # Use the main cover sheet from cover_sheets folder if available, otherwise fall back
        cover_info = parse_cover_sheets(COVER_SHEETS)
        cover_path = generate_cover_page(
            customer, job_name, phone,
            filters_data=filters_data,
            template_path=cover_info.get('main_cover')
        )
        
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

# File Management API Endpoints

# Map folder names to their paths
FOLDER_MAP = {
    'maintenance': MAINTENANCE_DOCS,
    'warranty': WARRANTY_DOCS,
    'cover_sheets': COVER_SHEETS
}

@app.get('/api/files/<folder_type>')
def api_list_files(folder_type):
    """List PDF files in the specified folder."""
    if folder_type not in FOLDER_MAP:
        return jsonify({'error': f'Invalid folder type: {folder_type}'}), 400
    
    folder_path = FOLDER_MAP[folder_type]
    try:
        files = []
        for fname in sorted(os.listdir(folder_path)):
            if not fname.lower().endswith('.pdf'):
                continue
            fpath = os.path.join(folder_path, fname)
            try:
                stat = os.stat(fpath)
                files.append({
                    'name': fname,
                    'size': stat.st_size,
                    'mtime': stat.st_mtime,
                    'size_human': _format_file_size(stat.st_size)
                })
            except Exception:
                continue
        return jsonify({'files': files, 'folder': folder_type})
    except Exception as e:
        logger.error(f"Error listing files in {folder_type}: {e}")
        return jsonify({'error': str(e)}), 500


def _format_file_size(size_bytes):
    """Convert bytes to human readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


@app.post('/api/upload/<folder_type>')
def api_upload_file(folder_type):
    """Upload a PDF file to the specified folder."""
    if folder_type not in FOLDER_MAP:
        return jsonify({'error': f'Invalid folder type: {folder_type}'}), 400
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files are allowed'}), 400
    
    folder_path = FOLDER_MAP[folder_type]
    filename = secure_filename(file.filename)
    
    # Check if file already exists
    file_path = os.path.join(folder_path, filename)
    if os.path.exists(file_path):
        return jsonify({'error': f'File "{filename}" already exists. Delete it first to replace.'}), 409
    
    try:
        file.save(file_path)
        logger.info(f"Uploaded {filename} to {folder_type}")
        return jsonify({
            'success': True,
            'message': f'File "{filename}" uploaded successfully',
            'filename': filename
        })
    except Exception as e:
        logger.error(f"Error uploading file to {folder_type}: {e}")
        return jsonify({'error': str(e)}), 500


@app.get('/api/view/<folder_type>')
def api_view_file(folder_type):
    """Serve a PDF file from the specified folder for viewing."""
    if folder_type not in FOLDER_MAP:
        return jsonify({'error': f'Invalid folder type: {folder_type}'}), 400
    
    filename = request.args.get('filename', '')
    if not filename:
        return jsonify({'error': 'No filename provided'}), 400
    
    # Basic security validation without using secure_filename (which strips special chars)
    if '..' in filename or filename.startswith('/') or filename.startswith('\\'):
        return jsonify({'error': 'Invalid filename'}), 400
    
    folder_path = FOLDER_MAP[folder_type]
    file_path = os.path.join(folder_path, filename)
    
    # Security check: ensure the file is within the target folder
    if not os.path.realpath(file_path).startswith(os.path.realpath(folder_path)):
        return jsonify({'error': 'Invalid file path'}), 403
    
    if not os.path.exists(file_path):
        return jsonify({'error': f'File "{filename}" not found'}), 404
    
    # Ensure it's a PDF file
    if not filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files can be viewed'}), 400
    
    try:
        logger.info(f"Serving file for viewing: {filename} from {folder_type}")
        return send_file(file_path, mimetype='application/pdf')
    except Exception as e:
        logger.error(f"Error serving file {filename}: {e}")
        return jsonify({'error': str(e)}), 500


@app.delete('/api/delete/<folder_type>')
def api_delete_file(folder_type):
    """Delete a PDF file from specified folder."""
    if folder_type not in FOLDER_MAP:
        return jsonify({'error': f'Invalid folder type: {folder_type}'}), 400
    
    data = request.get_json()
    if not data or 'filename' not in data:
        return jsonify({'error': 'No filename provided'}), 400
    
    filename = secure_filename(data['filename'])
    folder_path = FOLDER_MAP[folder_type]
    file_path = os.path.join(folder_path, filename)
    
    # Security check: ensure the file is within the target folder
    if not os.path.realpath(file_path).startswith(os.path.realpath(folder_path)):
        return jsonify({'error': 'Invalid file path'}), 403
    
    if not os.path.exists(file_path):
        return jsonify({'error': f'File "{filename}" not found'}), 404
    
    try:
        os.remove(file_path)
        logger.info(f"Deleted {filename} from {folder_type}")
        
        return jsonify({
            'success': True,
            'message': f'File "{filename}" deleted successfully'
        })
    except Exception as e:
        logger.error(f"Error deleting file from {folder_type}: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Only use debug mode when running directly
    is_debug = os.environ.get('FLASK_ENV') == 'development'
    port = int(os.environ.get('PORT', 5000))
    app.run(host='localhost', port=port, debug=is_debug)
