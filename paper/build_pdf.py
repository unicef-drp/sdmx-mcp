#!/usr/bin/env python3
"""master.md -> SDMX-MCP-Report.pdf, via HTML and headless Chrome.

The document half of master.md only: everything from "# Presentation outline"
down belongs to the deck and is cut here, so the two deliverables stay one
file without the reader meeting a slide outline at the end of a report.
"""
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import markdown

import context

DIR = Path(__file__).resolve().parent
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

CSS = """
@page { size: A4; margin: 17mm 15mm 15mm 15mm; }
* { box-sizing: border-box; }
body { font-family: Calibri, "Segoe UI", system-ui, sans-serif; color: #10263a;
       font-size: 10.4pt; line-height: 1.52; margin: 0; }
h1 { font-family: Cambria, Georgia, serif; font-size: 25pt; line-height: 1.15;
     margin: 0 0 3pt; color: #0B1F33; }
h2 { font-family: Cambria, Georgia, serif; font-size: 15pt; color: #0B1F33;
     margin: 19pt 0 6pt; padding-top: 3pt; page-break-after: avoid;
     border-top: 2px solid #1CABE2; }
h3 { font-size: 11.5pt; color: #0B6E99; margin: 13pt 0 4pt; page-break-after: avoid; }
p { margin: 0 0 7pt; }
.sub { font-family: Cambria, Georgia, serif; font-size: 13pt; color: #0B6E99;
       margin: 0 0 12pt; font-style: italic; }
.by { font-size: 9.3pt; color: #5b7183; border-top: 1px solid #d8e3ea;
      border-bottom: 1px solid #d8e3ea; padding: 7pt 0; margin-bottom: 15pt; }
.lead { font-size: 11.4pt; }
table { width: 100%; border-collapse: collapse; margin: 8pt 0 11pt;
        font-size: 8.9pt; page-break-inside: avoid; }
th { text-align: left; background: #0B1F33; color: #fff; padding: 5pt 6pt;
     font-weight: 600; font-size: 8.4pt; }
td { padding: 4.5pt 6pt; border-bottom: 1px solid #e3ebf0; vertical-align: top; }
tbody tr:nth-child(even) td { background: #f6fafc; }
code { font-family: "Courier New", monospace; font-size: 8.8pt;
       background: #eef4f8; padding: 0 2pt; }
pre { background: #0B1F33; color: #dceaf2; padding: 8pt 10pt; font-size: 8.5pt;
      overflow-x: auto; page-break-inside: avoid; }
pre code { background: none; color: inherit; font-size: 8.5pt; }
ul, ol { margin: 0 0 8pt; padding-left: 16pt; }
li { margin-bottom: 3.5pt; }
blockquote { margin: 0; }
hr { border: 0; border-top: 1px solid #d8e3ea; margin: 14pt 0; }
strong { color: #0B1F33; }
footer { margin-top: 15pt; padding-top: 7pt; border-top: 1px solid #d8e3ea;
         font-size: 8.4pt; color: #7a8d9c; }
"""


def main() -> int:
    ctx = context.build()
    raw = (DIR / "master.md").read_text(encoding="utf-8")

    # Document half only. Strip comments first and anchor the split to a line
    # start: the build notes inside the comment mention the outline heading by
    # name, and splitting on the bare string cut the document at that mention,
    # leaving 847 characters and an unterminated comment.
    doc = re.sub(r"<!--.*?-->", "", raw, flags=re.DOTALL)
    doc = re.split(r"^# Presentation outline\s*$", doc, maxsplit=1, flags=re.MULTILINE)[0]

    meta = {}
    for line in doc.splitlines():
        m = re.match(r">\s*(\w+):\s*(.+)", line)
        if m:
            meta[m.group(1)] = m.group(2).strip()
    doc = "\n".join(l for l in doc.splitlines() if not re.match(r">\s*\w+:", l))
    doc = doc.split("\n", 1)[1]  # drop the H1; the header block renders it
    doc = context.substitute(doc, ctx)
    doc = doc.rstrip().removesuffix("---").rstrip()

    body = markdown.markdown(doc, extensions=["tables", "fenced_code", "sane_lists"])
    # First paragraph is the standfirst.
    body = body.replace("<p>", '<p class="lead">', 1)

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>A Year of SDMX MCP</title><style>{CSS}</style></head><body>
<h1>A Year of SDMX MCP</h1>
<p class="sub">{meta.get('subtitle','')}</p>
<p class="by"><b>{meta.get('author','')}</b> &nbsp;·&nbsp; with {meta.get('with','')}
&nbsp;·&nbsp; {date.today().isoformat()}<br>
Code: <code>{meta.get('repo','')}</code> &nbsp;·&nbsp;
Endpoint: <code>{meta.get('endpoint','')}</code></p>
{body}
<footer>Measured October 2026 against UNICEF's SDMX warehouse. All figures graded against ground
truth re-read from the live registry; cost is the gateway's own per-request billing.
{ctx['cases_graded']} graded cases across {ctx['pooled_models']} open-weight models at n=500 each,
plus four proprietary runs. Generated from <code>master.md</code>.</footer>
</body></html>"""

    html_path = DIR / "SDMX-MCP-Report.html"
    pdf_path = DIR / "SDMX-MCP-Report.pdf"
    html_path.write_text(html, encoding="utf-8")

    if not Path(CHROME).exists():
        raise SystemExit(f"Chrome not found at {CHROME}; cannot render the PDF.")
    pdf_path.unlink(missing_ok=True)
    subprocess.run([CHROME, "--headless", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={pdf_path}", "--virtual-time-budget=12000",
                    html_path.as_uri()], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not pdf_path.exists():
        raise SystemExit("Chrome reported success but wrote no PDF.")
    print(f"wrote {pdf_path} ({pdf_path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
