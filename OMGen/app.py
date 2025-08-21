from flask import Flask, render_template, request, send_file
import os
import tempfile
import zipfile
from io import BytesIO
from utils.pdf_utils import generate_cover_page, extract_items_from_sales_order, match_templates, merge_pdfs
from utils.excel_utils import extract_job_metadata
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
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)
os.makedirs(MAINTENANCE_DOCS, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

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

        # Get flow rate information
        flow_data = {
            'primary_flow_rate': request.form.get('primary_flow_rate', ''),
            'backwash_rate': request.form.get('backwash_rate', ''),
            'total_dynamic_head': request.form.get('total_dynamic_head', ''),
        }

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
        
        templates, maintenance_docs = match_templates(item_keywords, TEMPLATE_FOLDER, flow_data=flow_data)
        logger.info(f"Matched templates: {[os.path.basename(t) for t in templates]}")
        logger.info(f"Matched maintenance docs: {[os.path.basename(d) for d in maintenance_docs]}")

        # Create cover page (without flow data)
        cover_pdf_path = generate_cover_page(customer, job_name, phone)
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
            'job_files': job_folder_paths
        }
        
        # For backward compatibility, keep a list of all PDFs
        all_pdfs = [cover_pdf_path] + templates + maintenance_docs + job_folder_paths
        logger.info(f"Total PDFs to merge: {len(all_pdfs)}")
        
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

@app.route('/regenerate_cover', methods=['POST'])
def regenerate_cover():
    try:
        customer = request.form['customer']
        job_name = request.form['job_name']
        phone = request.form['phone']
        
        logger.info(f"Regenerating cover page for job: {job_name}")
        
        # Generate new cover page with flow data
        cover_path = generate_cover_page(customer, job_name, phone)
        
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
    app.run(host='0.0.0.0', port=port, debug=is_debug)
