# utils/excel_utils.py
from xml.sax.saxutils import escape

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

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


def create_equipment_list_pdf(excel_path, output_path, title="Equipment List"):
    sheets = pd.read_excel(excel_path, sheet_name=None, header=None, dtype=object)
    document = SimpleDocTemplate(
        output_path,
        pagesize=landscape(letter),
        leftMargin=0.35 * inch,
        rightMargin=0.35 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
        title=title,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "EquipmentListTitle", parent=styles["Heading1"], alignment=TA_CENTER,
        fontName="Helvetica-Bold", fontSize=16, leading=20, spaceAfter=10,
    )
    sheet_style = ParagraphStyle(
        "EquipmentListSheet", parent=styles["Heading2"], fontName="Helvetica-Bold",
        fontSize=11, leading=14, spaceAfter=6,
    )
    cell_style = ParagraphStyle(
        "EquipmentListCell", parent=styles["BodyText"], fontName="Helvetica",
        fontSize=7, leading=8.5, wordWrap="CJK",
    )
    header_style = ParagraphStyle(
        "EquipmentListHeader", parent=cell_style, fontName="Helvetica-Bold", textColor=colors.white,
    )
    story = [Paragraph(escape(title), title_style)]
    usable_width = landscape(letter)[0] - document.leftMargin - document.rightMargin

    for sheet_index, (sheet_name, dataframe) in enumerate(sheets.items()):
        if sheet_index:
            story.append(PageBreak())
        story.append(Paragraph(f"Sheet: {escape(str(sheet_name))}", sheet_style))
        dataframe = dataframe.dropna(axis=0, how="all").dropna(axis=1, how="all").fillna("")
        if dataframe.empty:
            story.append(Paragraph("No equipment data found on this sheet.", cell_style))
            continue

        rows = dataframe.astype(str).values.tolist()
        column_count = max(len(row) for row in rows)
        normalized_rows = [row + [""] * (column_count - len(row)) for row in rows]
        table_data = []
        for row_index, row in enumerate(normalized_rows):
            style = header_style if row_index == 0 else cell_style
            table_data.append([
                Paragraph(escape(str(value)).replace("\n", "<br/>"), style)
                for value in row
            ])

        table = Table(
            table_data,
            colWidths=[usable_width / column_count] * column_count,
            repeatRows=1,
            hAlign="LEFT",
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E78")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#B7C9D6")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#EAF2F8")]),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.extend([table, Spacer(1, 0.1 * inch)])

    document.build(story)
    return output_path