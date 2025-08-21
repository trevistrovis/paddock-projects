import os
from utils.pdf_utils import fill_pdf_form_fields
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

# Test data
flow_data = {
    'primary_flow_rate': '1500',
    'backwash_rate': '750',
    'total_dynamic_head': '45 feet'
}

# Template to test
template_name = "2 Cell Central Header Verticel Template.pdf"
template_path = os.path.join("template_cache", template_name)

print(f"\nTesting flow rate filling with template: {template_name}")
print(f"Flow data to insert: {flow_data}")

# Try to fill the template
filled_path = fill_pdf_form_fields(template_path, flow_data)

if filled_path:
    print(f"\nSuccess! Filled template saved to: {filled_path}")
    print("You can open this PDF to verify the flow rates were inserted correctly.")
else:
    print("\nNo fields were filled in the template.")
