"""
Generate professional PDFs from markdown files.
Usage: python generate_pdfs.py
"""
import re
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Preformatted,
    Table, TableStyle, HRFlowable, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

# ── Colour palette ────────────────────────────────────────────────────────────
NAVY       = colors.HexColor("#1a2332")
BLUE       = colors.HexColor("#2563eb")
LIGHT_BLUE = colors.HexColor("#dbeafe")
DARK_GRAY  = colors.HexColor("#374151")
MID_GRAY   = colors.HexColor("#6b7280")
LIGHT_GRAY = colors.HexColor("#f3f4f6")
CODE_BG    = colors.HexColor("#1e293b")
CODE_FG    = colors.HexColor("#e2e8f0")
WHITE      = colors.white

PAGE_W, PAGE_H = A4
MARGIN = 20 * mm


def build_styles():
    base = getSampleStyleSheet()

    styles = {}

    styles["h1"] = ParagraphStyle(
        "h1", fontSize=22, leading=28, textColor=WHITE,
        fontName="Helvetica-Bold", spaceAfter=4, spaceBefore=0,
    )
    styles["h1_sub"] = ParagraphStyle(
        "h1_sub", fontSize=10, leading=14, textColor=colors.HexColor("#93c5fd"),
        fontName="Helvetica", spaceAfter=0,
    )
    styles["h2"] = ParagraphStyle(
        "h2", fontSize=14, leading=18, textColor=NAVY,
        fontName="Helvetica-Bold", spaceBefore=14, spaceAfter=4,
        borderPad=0,
    )
    styles["h3"] = ParagraphStyle(
        "h3", fontSize=11, leading=15, textColor=BLUE,
        fontName="Helvetica-Bold", spaceBefore=10, spaceAfter=3,
    )
    styles["h4"] = ParagraphStyle(
        "h4", fontSize=10, leading=14, textColor=DARK_GRAY,
        fontName="Helvetica-Bold", spaceBefore=8, spaceAfter=2,
    )
    styles["body"] = ParagraphStyle(
        "body", fontSize=9.5, leading=14, textColor=DARK_GRAY,
        fontName="Helvetica", spaceAfter=4,
    )
    styles["bullet"] = ParagraphStyle(
        "bullet", fontSize=9.5, leading=14, textColor=DARK_GRAY,
        fontName="Helvetica", spaceAfter=2, leftIndent=12,
        bulletIndent=0, bulletFontName="Helvetica",
    )
    styles["code"] = ParagraphStyle(
        "code", fontSize=8, leading=11, textColor=CODE_FG,
        fontName="Courier", spaceAfter=0, spaceBefore=0,
        backColor=CODE_BG, leftIndent=0,
    )
    styles["meta"] = ParagraphStyle(
        "meta", fontSize=8.5, leading=12, textColor=MID_GRAY,
        fontName="Helvetica-Oblique", spaceAfter=2,
    )
    styles["table_header"] = ParagraphStyle(
        "table_header", fontSize=8.5, leading=11, textColor=WHITE,
        fontName="Helvetica-Bold",
    )
    styles["table_cell"] = ParagraphStyle(
        "table_cell", fontSize=8.5, leading=11, textColor=DARK_GRAY,
        fontName="Helvetica",
    )
    styles["table_code"] = ParagraphStyle(
        "table_code", fontSize=7.5, leading=10, textColor=DARK_GRAY,
        fontName="Courier",
    )

    return styles


def header_footer(canvas, doc):
    canvas.saveState()
    # Top bar
    canvas.setFillColor(NAVY)
    canvas.rect(0, PAGE_H - 12*mm, PAGE_W, 12*mm, fill=1, stroke=0)
    canvas.setFillColor(WHITE)
    canvas.setFont("Helvetica-Bold", 8)
    canvas.drawString(MARGIN, PAGE_H - 7.5*mm, doc.title or "")
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 7.5*mm, "Lebogang Mphaga")
    # Bottom bar
    canvas.setFillColor(LIGHT_GRAY)
    canvas.rect(0, 0, PAGE_W, 10*mm, fill=1, stroke=0)
    canvas.setFillColor(MID_GRAY)
    canvas.setFont("Helvetica", 7.5)
    canvas.drawString(MARGIN, 3.5*mm, "Orders Data Pipeline — Technical Assessment")
    canvas.drawRightString(PAGE_W - MARGIN, 3.5*mm, f"Page {doc.page}")
    canvas.restoreState()


def parse_markdown(md_text: str, styles: dict) -> list:
    """Convert markdown to ReportLab flowables."""
    flowables = []
    lines = md_text.split("\n")

    in_code = False
    code_lines = []
    code_lang = ""
    in_table = False
    table_rows = []
    skip_next_separator = False

    i = 0
    while i < len(lines):
        line = lines[i]

        # ── Code block ────────────────────────────────────────────────────
        if line.startswith("```"):
            if not in_code:
                in_code = True
                code_lang = line[3:].strip()
                code_lines = []
            else:
                in_code = False
                code_text = "\n".join(code_lines)
                # One row per line so the table can split across pages
                col_w = PAGE_W - 2*MARGIN - 4*mm
                code_line_style = ParagraphStyle(
                    "code_line", fontSize=7.5, leading=10.5,
                    fontName="Courier", textColor=CODE_FG,
                    backColor=CODE_BG,
                    leftIndent=8, rightIndent=8,
                    spaceBefore=0, spaceAfter=0,
                )
                rows = []
                for ci, cl in enumerate(code_lines):
                    safe_line = cl.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    if not safe_line:
                        safe_line = " "
                    top_pad = 5 if ci == 0 else 0
                    bot_pad = 5 if ci == len(code_lines)-1 else 0
                    rows.append([Paragraph(safe_line, code_line_style)])
                if rows:
                    t = Table(rows, colWidths=[col_w], splitByRow=True)
                    ts_cmds = [
                        ("BACKGROUND",    (0,0), (-1,-1), CODE_BG),
                        ("LEFTPADDING",   (0,0), (-1,-1), 10),
                        ("RIGHTPADDING",  (0,0), (-1,-1), 10),
                        ("TOPPADDING",    (0,0), (0,0),   6),
                        ("TOPPADDING",    (0,1), (-1,-1), 0),
                        ("BOTTOMPADDING", (0,-1),(-1,-1), 6),
                        ("BOTTOMPADDING", (0,0), (-1,-2), 0),
                    ]
                    t.setStyle(TableStyle(ts_cmds))
                    flowables.append(Spacer(1, 3))
                    flowables.append(t)
                    flowables.append(Spacer(1, 6))
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        # ── Markdown table ─────────────────────────────────────────────────
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            # Separator row?
            if all(re.match(r"^[-:]+$", c) for c in cells if c):
                i += 1
                continue
            table_rows.append(cells)
            in_table = True
            i += 1
            continue
        else:
            if in_table and table_rows:
                flowables.append(_build_table(table_rows, styles))
                flowables.append(Spacer(1, 6))
                table_rows = []
                in_table = False

        # ── Headings ──────────────────────────────────────────────────────
        if line.startswith("#### "):
            flowables.append(Paragraph(_clean(line[5:]), styles["h4"]))
        elif line.startswith("### "):
            flowables.append(Paragraph(_clean(line[4:]), styles["h3"]))
        elif line.startswith("## "):
            flowables.append(HRFlowable(width="100%", thickness=0.5,
                                         color=LIGHT_BLUE, spaceAfter=2))
            flowables.append(Paragraph(_clean(line[3:]), styles["h2"]))
        elif line.startswith("# "):
            # Title block — only once at top
            title_text = _clean(line[2:])
            # Look ahead for subtitle lines (bold metadata)
            meta_lines = []
            j = i + 1
            while j < len(lines) and lines[j].startswith("**"):
                meta_lines.append(lines[j].strip("*").strip())
                j += 1
            i = j - 1  # will be incremented at end of loop

            title_block = Table(
                [[Paragraph(title_text, styles["h1"])]],
                colWidths=[PAGE_W - 2*MARGIN]
            )
            title_block.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,-1), NAVY),
                ("LEFTPADDING",  (0,0), (-1,-1), 14),
                ("RIGHTPADDING", (0,0), (-1,-1), 14),
                ("TOPPADDING",   (0,0), (-1,-1), 14),
                ("BOTTOMPADDING",(0,0), (-1,-1), 14),
            ]))
            flowables.append(title_block)
            for m in meta_lines:
                flowables.append(Paragraph(m, styles["meta"]))
            flowables.append(Spacer(1, 8))

        # ── Horizontal rule ───────────────────────────────────────────────
        elif line.strip() == "---":
            flowables.append(HRFlowable(width="100%", thickness=1,
                                         color=LIGHT_BLUE, spaceBefore=4, spaceAfter=4))

        # ── Bullet points ─────────────────────────────────────────────────
        elif re.match(r"^(\s*)[-*]\s", line):
            indent = len(line) - len(line.lstrip())
            text = re.sub(r"^(\s*)[-*]\s", "", line)
            bullet_style = ParagraphStyle(
                "bullet_dyn", parent=styles["bullet"],
                leftIndent=12 + indent * 3,
            )
            flowables.append(Paragraph(f"• {_clean(text)}", bullet_style))

        # ── Numbered list ─────────────────────────────────────────────────
        elif re.match(r"^\d+\.\s", line):
            text = re.sub(r"^\d+\.\s", "", line)
            flowables.append(Paragraph(_clean(text), styles["bullet"]))

        # ── Blank line ────────────────────────────────────────────────────
        elif line.strip() == "":
            flowables.append(Spacer(1, 4))

        # ── Normal paragraph ──────────────────────────────────────────────
        else:
            if line.strip():
                flowables.append(Paragraph(_clean(line), styles["body"]))

        i += 1

    # Flush any trailing table
    if table_rows:
        flowables.append(_build_table(table_rows, styles))

    return flowables


def _clean(text: str) -> str:
    """Convert inline markdown to ReportLab XML."""
    # Bold+italic
    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"<b><i>\1</i></b>", text)
    # Bold
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    # Italic
    text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
    # Inline code
    text = re.sub(r"`([^`]+)`",
                  r'<font name="Courier" color="#1d4ed8">\1</font>', text)
    # Strip raw links [text](url) → text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    # Escape bare & < > that aren't our tags
    # (simple approach: only escape & that aren't already &amp;)
    text = re.sub(r"&(?!amp;|lt;|gt;|nbsp;)", "&amp;", text)
    return text


def _build_table(rows: list, styles: dict) -> Table:
    """Build a styled reportlab Table from list-of-lists rows."""
    if not rows:
        return Spacer(1, 1)

    header = rows[0]
    body_rows = rows[1:]

    def cell(text, is_header=False):
        s = styles["table_header"] if is_header else styles["table_cell"]
        # Use code style for cells that look like code
        if not is_header and (text.startswith("python") or "/" in text or
                               text.startswith("`") or "_" in text):
            s = styles["table_code"]
        return Paragraph(_clean(text), s)

    data = [[cell(c, True) for c in header]]
    for row in body_rows:
        data.append([cell(c) for c in row])

    col_count = len(header)
    avail = PAGE_W - 2 * MARGIN - 4*mm
    col_w = avail / col_count

    t = Table(data, colWidths=[col_w] * col_count, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  WHITE),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [WHITE, LIGHT_GRAY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def md_to_pdf(md_path: Path, pdf_path: Path, doc_title: str):
    print(f"  {md_path.name} -> {pdf_path.name}")
    styles = build_styles()
    md_text = md_path.read_text(encoding="utf-8")

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=18*mm,
        bottomMargin=16*mm,
        title=doc_title,
    )
    doc.title = doc_title

    flowables = parse_markdown(md_text, styles)
    doc.build(flowables, onFirstPage=header_footer, onLaterPages=header_footer)
    print(f"    OK Saved: {pdf_path}")


if __name__ == "__main__":
    docs_dir = Path(__file__).parent
    root_dir = docs_dir.parent

    conversions = [
        (docs_dir / "HIGH_LEVEL_DESIGN.md",  docs_dir / "HIGH_LEVEL_DESIGN.pdf",  "High-Level Design"),
        (docs_dir / "LOW_LEVEL_DESIGN.md",   docs_dir / "LOW_LEVEL_DESIGN.pdf",   "Low-Level Design"),
        (root_dir / "SOLUTION.md",            docs_dir / "SOLUTION.pdf",            "Solution Notes"),
    ]

    print("Generating PDFs...\n")
    for md, pdf, title in conversions:
        md_to_pdf(md, pdf, title)

    print("\nAll done.")
