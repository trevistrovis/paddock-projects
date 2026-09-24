# utils/excel_utils.py
import os
from xml.sax.saxutils import escape

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle

# Paddock house style, taken from cover_sheets/Equipment List Example.pdf
PADDOCK_BLUE = colors.HexColor('#0067B1')
INTRO_GREY = colors.HexColor('#404040')


def _register_equipment_fonts():
    """Register Calibri/Arial from Windows fonts to match the house style."""
    mapping = {
        'title': ('calibrib.ttf', 'Helvetica-Bold'),
        'intro': ('calibrib.ttf', 'Helvetica-Bold'),
        'header': ('arialbd.ttf', 'Helvetica-Bold'),
        'cell': ('arial.ttf', 'Helvetica'),
        'foot': ('calibri.ttf', 'Helvetica'),
        'foot_bold': ('calibrib.ttf', 'Helvetica-Bold'),
    }
    font_dir = os.path.join(os.environ.get('WINDIR', r'C:\Windows'), 'Fonts')
    resolved = {}
    for key, (ttf, fallback) in mapping.items():
        try:
            pdfmetrics.registerFont(TTFont(f'EL-{key}', os.path.join(font_dir, ttf)))
            resolved[key] = f'EL-{key}'
        except Exception:
            resolved[key] = fallback
    return resolved


def _draw_equipment_list_footer(canvas, doc, fonts):
    """Paddock footer, drawn on every page (matches Equipment List Example)."""
    canvas.saveState()
    width, _ = letter
    canvas.setFont(fonts['foot'], 10)
    canvas.setFillColor(colors.black)
    canvas.drawCentredString(
        width / 2, 54,
        '© Paddock Pool Equipment Co. |  555 Paddock Parkway, Rock Hill, SC 29730'
    )
    # Contact line: black text with blue links/phone
    segments = [
        ('Phone (803)324-1111  |  ', 'foot', colors.black),
        ('www.paddockpoolequipment.com', 'foot', PADDOCK_BLUE),
        ('  |  ', 'foot', colors.black),
        ('800-849-2729', 'foot_bold', PADDOCK_BLUE),
        ('  |  Email:', 'foot', colors.black),
        ('info@paddockindustries.com', 'foot_bold', PADDOCK_BLUE),
    ]
    total = sum(pdfmetrics.stringWidth(t, fonts[k], 10) for t, k, _ in segments)
    x = (width - total) / 2
    for text, fkey, color in segments:
        canvas.setFont(fonts[fkey], 10)
        canvas.setFillColor(color)
        canvas.drawString(x, 40, text)
        x += pdfmetrics.stringWidth(text, fonts[fkey], 10)
    canvas.setFont(fonts['foot_bold'], 8)
    canvas.setFillColor(PADDOCK_BLUE)
    canvas.drawString(doc.leftMargin, 26, 'Rev. 07/2024')
    canvas.restoreState()

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


def _find_column(header_row, keywords):
    """Return the index of the first column whose header contains a keyword."""
    for idx, cell in enumerate(header_row):
        text = str(cell).strip().lower()
        if text and any(k in text for k in keywords):
            return idx
    return None


def _clean_cell(value):
    """Normalize a spreadsheet cell for display; '' for empty/NaN."""
    if value is None:
        return ''
    text = str(value).strip()
    if text.lower() in ('nan', 'nat', 'none'):
        return ''
    # Trim trailing '.0' left over from float-valued cells
    if text.endswith('.0') and text[:-2].replace(',', '').isdigit():
        text = text[:-2]
    return text


def _sheet_to_item_rows(dataframe):
    """
    Convert one spreadsheet into (qty, description, drawing) row tuples.

    A header row is located by matching Qty/Description/Drawing-ish column
    names. Columns for price/cost/total are dropped; part-number columns are
    rendered as "Part  <no>" inside the description, matching the house
    equipment-list layout. Without a recognizable header, each row's cells are
    joined into the description column so nothing is silently dropped.
    """
    dataframe = dataframe.dropna(axis=0, how="all").dropna(axis=1, how="all").fillna("")
    if dataframe.empty:
        return []

    rows = dataframe.astype(str).values.tolist()
    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]

    header_idx = None
    for i, row in enumerate(rows):
        lowered = [str(c).strip().lower() for c in row]
        if any(('qty' in c or 'quantity' in c) for c in lowered) and \
           any(('desc' in c or 'description' in c) for c in lowered):
            header_idx = i
            break

    item_rows = []
    if header_idx is None:
        # No headers: keep everything as description lines
        for row in rows:
            desc = ' '.join(_clean_cell(c) for c in row).strip()
            if desc:
                item_rows.append(('', desc, ''))
        return item_rows

    header = rows[header_idx]
    qty_col = _find_column(header, ['qty', 'quantity', 'qnty'])
    desc_col = _find_column(header, ['description', 'desc'])
    drawing_col = _find_column(header, ['drawing', 'dwg', 'drw'])
    part_col = _find_column(header, ['part no', 'part#', 'part number', 'model', 'item no', 'part'])
    skip_cols = {i for i, h in enumerate(header)
                 if any(k in str(h).lower() for k in ['price', 'cost', 'total', 'amount', '$'])}
    mapped_cols = {c for c in (qty_col, desc_col, drawing_col, part_col) if c is not None}

    for row in rows[header_idx + 1:]:
        qty = _clean_cell(row[qty_col]) if qty_col is not None else ''
        drawing = _clean_cell(row[drawing_col]) if drawing_col is not None else ''
        desc_lines = []
        if part_col is not None and _clean_cell(row[part_col]):
            desc_lines.append(f"Part  {_clean_cell(row[part_col])}")
        if desc_col is not None and _clean_cell(row[desc_col]):
            desc_lines.append(_clean_cell(row[desc_col]))
        for i, cell in enumerate(row):
            if i in mapped_cols or i in skip_cols:
                continue
            text = _clean_cell(cell)
            if text:
                desc_lines.append(text)
        desc = '\n'.join(desc_lines).strip()
        if qty or desc or drawing:
            item_rows.append((qty, desc, drawing))
    return item_rows


def create_equipment_list_pdf(excel_path, output_path, title="Equipment List"):
    """Render the uploaded equipment spreadsheet in the Paddock house style:
    portrait page, blue EQUIPMENT LIST title, intro line, Qty/Description/
    Drawing table, and the company footer on every page."""
    sheets = pd.read_excel(excel_path, sheet_name=None, header=None, dtype=object)
    fonts = _register_equipment_fonts()

    document = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.95 * inch,
        rightMargin=0.95 * inch,
        topMargin=0.9 * inch,
        bottomMargin=1.0 * inch,
        title=title,
    )

    title_style = ParagraphStyle(
        'ELTitle', fontName=fonts['title'], fontSize=24, leading=28,
        alignment=TA_CENTER, textColor=PADDOCK_BLUE, spaceAfter=8,
    )
    intro_style = ParagraphStyle(
        'ELIntro', fontName=fonts['intro'], fontSize=12, leading=15,
        textColor=INTRO_GREY, spaceAfter=14,
    )
    head_style = ParagraphStyle(
        'ELHead', fontName=fonts['header'], fontSize=9, leading=11,
    )
    head_center = ParagraphStyle('ELHeadCenter', parent=head_style, alignment=TA_CENTER)
    cell_style = ParagraphStyle(
        'ELCell', fontName=fonts['cell'], fontSize=10, leading=13,
    )
    cell_center = ParagraphStyle('ELCellCenter', parent=cell_style, alignment=TA_CENTER)

    story = [
        Paragraph('EQUIPMENT LIST', title_style),
        Paragraph('The following items have been provided by Paddock Pool Equipment Company, Inc.', intro_style),
    ]

    usable_width = letter[0] - document.leftMargin - document.rightMargin
    qty_w = 0.75 * inch
    draw_w = 1.65 * inch
    col_widths = [qty_w, usable_width - qty_w - draw_w, draw_w]

    table_data = [[
        Paragraph('Qty', head_style),
        Paragraph('Description', head_style),
        Paragraph('Drawing', head_center),
    ]]
    # Only pull the tab(s) containing "O&M"; if none exist, fall back to all
    # sheets rather than producing an empty list.
    om_sheets = [(name, df) for name, df in sheets.items() if 'o&m' in str(name).lower()]
    if not om_sheets:
        print('No sheet containing "O&M" found; using all sheets')
        om_sheets = list(sheets.items())

    for sheet_name, dataframe in om_sheets:
        for qty, desc, drawing in _sheet_to_item_rows(dataframe):
            table_data.append([
                Paragraph(escape(qty).replace('\n', '<br/>'), cell_center),
                Paragraph(escape(desc).replace('\n', '<br/>'), cell_style),
                Paragraph(escape(drawing).replace('\n', '<br/>'), cell_center),
            ])

    if len(table_data) > 1:
        table = Table(table_data, colWidths=col_widths, repeatRows=1, hAlign='CENTER')
        table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.6, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('VALIGN', (1, 1), (1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ]))
        story.append(table)
    else:
        story.append(Paragraph('No equipment data found.', cell_style))

    document.build(
        story,
        onFirstPage=lambda c, d: _draw_equipment_list_footer(c, d, fonts),
        onLaterPages=lambda c, d: _draw_equipment_list_footer(c, d, fonts),
    )
    return output_path