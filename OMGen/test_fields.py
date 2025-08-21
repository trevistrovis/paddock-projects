import fitz
import os

def inspect_pdf_fields(pdf_path):
    print(f"\nInspecting fields in: {os.path.basename(pdf_path)}")
    try:
        doc = fitz.open(pdf_path)
        # Get all widgets (form fields) from the PDF
        widgets = []
        for page in doc:
            widgets.extend(page.widgets())
        
        print(f"Found {len(widgets)} form fields:")
        for widget in widgets:
            print(f"  Field type: {widget.field_type_string}")
            print(f"  Field name: {widget.field_name if hasattr(widget, 'field_name') else 'No name'}")
            print(f"  Field label: {widget.field_label if hasattr(widget, 'field_label') else 'No label'}")
            print(f"  Current value: {widget.field_value if hasattr(widget, 'field_value') else 'No value'}")
            print(f"  Field flags: {widget.field_flags if hasattr(widget, 'field_flags') else 'No flags'}")
            print()
        
        doc.close()
    except Exception as e:
        print(f"Error inspecting {os.path.basename(pdf_path)}: {str(e)}")

# Directory containing templates
template_dir = "template_cache"

# Check each PDF in the directory
for filename in os.listdir(template_dir):
    if filename.lower().endswith('.pdf'):
        pdf_path = os.path.join(template_dir, filename)
        inspect_pdf_fields(pdf_path)
