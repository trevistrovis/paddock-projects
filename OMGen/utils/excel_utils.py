# utils/excel_utils.py
import pandas as pd

def extract_job_metadata(excel_path):
    try:
        df = pd.read_excel(excel_path)
        metadata = {}

        # Example logic: look for known columns
        for col in df.columns:
            if 'project' in col.lower():
                metadata['project'] = df[col].iloc[0]
            elif 'customer' in col.lower():
                metadata['customer'] = df[col].iloc[0]
            elif 'ship' in col.lower():
                metadata['ship_date'] = df[col].iloc[0]
            elif 'job' in col.lower():
                metadata['job_number'] = df[col].iloc[0]

        return metadata
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return {}