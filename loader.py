"""
Shared loader: takes zip bytes (or direct file bytes), runs Arelle, returns a DataFrame of facts.
Also provides extract_text_sections() for narrative text extraction from the same iXBRL source.
skipDTS=True avoids network calls to ESMA taxonomy servers.
"""
import os
import re
import tempfile
import zipfile
import pandas as pd

# ─── Statement classification ──────────────────────────────────────────────────

STATEMENT_MAP = {
    "Revenue": "Income Statement", "GrossProfit": "Income Statement",
    "OperatingIncomeLoss": "Income Statement", "ProfitLoss": "Income Statement",
    "ProfitLossFromOperatingActivities": "Income Statement", "ProfitLossBeforeTax": "Income Statement",
    "IncomeTaxExpenseContinuingOperations": "Income Statement", "ProfitLossFromContinuingOperations": "Income Statement",
    "ProfitLossAttributableToOwnersOfParent": "Income Statement", "BasicEarningsLossPerShare": "Income Statement",
    "DilutedEarningsLossPerShare": "Income Statement", "FinanceCosts": "Income Statement",
    "FinanceIncome": "Income Statement", "DistributionCosts": "Income Statement",
    "AdministrativeExpense": "Income Statement", "CostOfSales": "Income Statement",
    "Assets": "Balance Sheet", "NoncurrentAssets": "Balance Sheet", "CurrentAssets": "Balance Sheet",
    "Liabilities": "Balance Sheet", "NoncurrentLiabilities": "Balance Sheet", "CurrentLiabilities": "Balance Sheet",
    "Equity": "Balance Sheet", "EquityAttributableToOwnersOfParent": "Balance Sheet",
    "CashAndCashEquivalents": "Balance Sheet", "TradeAndOtherCurrentReceivables": "Balance Sheet",
    "Inventories": "Balance Sheet", "PropertyPlantAndEquipment": "Balance Sheet",
    "IntangibleAssetsOtherThanGoodwill": "Balance Sheet", "Goodwill": "Balance Sheet",
    "TradeAndOtherCurrentPayables": "Balance Sheet", "IssuedCapital": "Balance Sheet",
    "RetainedEarnings": "Balance Sheet",
    "CashFlowsFromUsedInOperatingActivities": "Cash Flow", "CashFlowsFromUsedInInvestingActivities": "Cash Flow",
    "CashFlowsFromUsedInFinancingActivities": "Cash Flow", "IncreaseDecreaseInCashAndCashEquivalents": "Cash Flow",
    "AdjustmentsForDepreciationAndAmortisationExpense": "Cash Flow",
    "OtherComprehensiveIncome": "Other Comprehensive Income", "ComprehensiveIncome": "Other Comprehensive Income",
}

def classify_statement(name):
    if name in STATEMENT_MAP:
        return STATEMENT_MAP[name]
    n = name.lower()
    if any(k in n for k in ["revenue","profit","loss","income","expense","earnings","ebit","sales","cost"]):
        return "Income Statement"
    if any(k in n for k in ["asset","liabilit","equity","cash","inventor","payable","receivable","goodwill","capital","reserve","borrowing"]):
        return "Balance Sheet"
    if any(k in n for k in ["cashflow","cash flow","operating activit","investing activit","financing activit"]):
        return "Cash Flow"
    return "Other / Extension"


# ─── Annual report section classification ─────────────────────────────────────


def _clean(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()


# ─── Text section extraction ───────────────────────────────────────────────────

# Extended section patterns for section-inheritance classification
_SECTION_PATTERNS_CLASSIFY = [
    ("Strategic Report",             r"strategic\s+report"),
    ("Chair's Statement",            r"chair(?:man|woman|person)?['\u2019]?s?\s+statement|letter\s+from\s+the\s+chair"),
    ("CEO Statement",                r"(?:group\s+)?chief\s+executi\w+['\u2019]?s?\s+(?:statement|review|letter)|ceo\s+(?:statement|review)"),
    ("CFO Review",                   r"chief\s+financial\s+officer|cfo\s+(?:statement|review)|finance\s+(?:director|review)"),
    ("Chief Investment Officer",     r"chief\s+investment\s+officer"),
    ("Business Overview",            r"business\s+(?:overview|model|description|at\s+a\s+glance)"),
    ("Market Review",                r"market\s+(?:review|overview|context|environment)"),
    ("Strategy",                     r"(?:our\s+)?strateg(?:y|ic\s+priorities|ic\s+objectives)"),
    ("Key Performance Indicators",   r"key\s+performance\s+indicators|kpis?"),
    ("Principal Risks",              r"principal\s+risks?|risk\s+management|risks?\s+and\s+uncertainties"),
    ("Viability Statement",          r"viability\s+statement"),
    ("Sustainability / ESG",         r"sustainabilit|esg|environmental|social\s+and\s+governance|climate.related|net\s+zero|carbon"),
    ("Section 172",                  r"section\s+172|stakeholder\s+engagement"),
    ("Directors' Report",            r"directors['\u2019]?\s+report"),
    ("Corporate Governance",         r"corporate\s+governance|^governance$"),
    ("Board of Directors",           r"board\s+of\s+directors|our\s+board"),
    ("Audit Committee",              r"audit\s+and\s+risk\s+committee|audit\s+committee"),
    ("Remuneration Report",          r"remuneration"),
    ("Nomination Committee",         r"nomination\s+(?:and\s+governance\s+)?committee"),
    ("Independent Auditor's Report", r"independent\s+auditor"),
    ("Financial Statements",         r"^financial\s+statements?$|^consolidated\s+financial\s+statements$"),
    ("Income Statement",             r"(?:consolidated\s+)?(?:income\s+statement|profit\s+(?:and|&)\s+loss|statement\s+of\s+(?:comprehensive\s+)?income)"),
    ("Balance Sheet",                r"(?:consolidated\s+)?(?:balance\s+sheet|statement\s+of\s+financial\s+position)"),
    ("Cash Flow Statement",          r"(?:consolidated\s+)?(?:cash\s+flow|statement\s+of\s+cash\s+flows)"),
    ("Statement of Changes in Equity", r"statement\s+of\s+(?:changes\s+in\s+)?equity"),
    ("Accounting Policies",          r"accounting\s+policies|basis\s+of\s+(?:preparation|consolidation)"),
    ("Notes to Accounts",            r"notes?\s+to\s+(?:the\s+)?(?:financial\s+)?(?:statements?|accounts?)"),
    ("Five Year Summary",            r"five.year|financial\s+(?:summary|highlights)|historical\s+(?:data|summary)"),
    ("Shareholder Information",      r"shareholder\s+(?:information|and\s+sustainability)|investor\s+(?:relations|information)"),
    ("Glossary",                     r"^glossar"),
]
_CLASSIFY_COMPILED = [(n, re.compile(p, re.IGNORECASE)) for n, p in _SECTION_PATTERNS_CLASSIFY]


def _classify_section(text: str):
    """Return section name if heading matches a known section, else None (inherit current)."""
    for name, pat in _CLASSIFY_COMPILED:
        if pat.search(text):
            return name
    return None


def _is_pdf2htmlex(html_str: str) -> bool:
    """Detect if the HTML was produced by pdf2htmlEX (PDF-to-HTML conversion)."""
    return "pdf2htmlEX" in html_str[:2000] or "pdf2htmlex" in html_str[:2000].lower()


def _extract_pdf2htmlex(html_str: str) -> list:
    """
    Extract text sections from a pdf2htmlEX-generated iXBRL file.
    These files have no h1-h4 tags; text sizing comes from CSS font-size classes.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        import subprocess, sys
        subprocess.run([sys.executable, "-m", "pip", "install", "beautifulsoup4",
                        "--break-system-packages", "-q"], check=False)
        from bs4 import BeautifulSoup

    import collections
    soup = BeautifulSoup(html_str, "html.parser")

    # Extract font-size and y-position maps from the CSS blocks
    all_css = "\n".join(s.string or "" for s in soup.find_all("style") if s.string)
    fs_sizes = {k: float(v) for k, v in re.findall(
        r'\.fs(\w+)\s*\{[^}]*font-size\s*:\s*([\d.]+)', all_css)}
    y_positions = {k: float(v) for k, v in re.findall(
        r'\.y(\w+)\s*\{bottom:([\d.]+)px\}', all_css)}

    # Find modal (body text) font size
    fs_usage = collections.Counter()
    for d in soup.find_all("div", class_=True):
        for c in d.get("class", []):
            if c.startswith("fs"):
                fs_usage[c] += 1
    if not fs_usage:
        return []
    modal_class = fs_usage.most_common(1)[0][0]
    body_size = fs_sizes.get(modal_class[2:], 32.0)

    pages = soup.find_all("div", class_="pf")
    if not pages:
        pages = [soup]  # fallback: treat whole doc as one page

    rows = []
    current_section = "Other"
    current_heading = None
    current_lines = []
    seq = 0

    def flush():
        nonlocal seq
        text = re.sub(r"\s+", " ", " ".join(current_lines)).strip()
        if len(text) > 40:
            rows.append({
                "section":    current_section,
                "heading":    current_heading or "",
                "seq":        seq,
                "char_count": len(text),
                "text":       text,
            })
            seq += 1
        current_lines.clear()

    for page in pages:
        text_divs = []
        for d in page.find_all("div", class_=True):
            classes = d.get("class", [])
            if "t" not in classes:
                continue
            fs_cls = next((c for c in classes if c.startswith("fs")), None)
            size = fs_sizes.get(fs_cls[2:], 0) if fs_cls else 0
            y_cls = next((c for c in classes if c.startswith("y")), None)
            y = y_positions.get(y_cls[1:], 0) if y_cls else 0
            text = re.sub(r"\s+", " ", d.get_text()).strip()
            if text and len(text) > 1:
                text_divs.append((y, size, text))

        # Sort top-of-page first (largest bottom offset = highest on page)
        text_divs.sort(key=lambda x: -x[0])

        for y, size, text in text_divs:
            is_heading = size > body_size and len(text) < 150
            # Skip chart/table noise: pure numbers, symbols, short labels
            is_noise = bool(re.match(r"^[\d.,£%°\s\-–€$()n/a]+$", text, re.IGNORECASE)) and len(text) < 30
            if is_noise:
                continue
            if is_heading:
                flush()
                classified = _classify_section(text)
                if classified:
                    current_section = classified  # known section → update
                # always update heading label
                current_heading = text
            else:
                current_lines.append(text)

    flush()
    return rows


def _looks_like_heading(element, has_semantic_headings: bool) -> bool:
    """
    Decide whether an element acts as a heading.
    In files with real h1-h4 tags, only those count.
    In files without (common in ESEF), also check class names and inline styles.
    """
    name = element.name or ""
    # Always treat semantic heading tags as headings
    if name in {"h1", "h2", "h3", "h4", "h5"}:
        return True
    if has_semantic_headings:
        return False  # semantic file → only h tags

    # --- ESEF / CSS-structured files ---
    text = _clean(element.get_text())
    if not text or len(text) > 200:
        return False  # too long to be a heading

    # Check class names for common heading indicators
    classes = " ".join(element.get("class", [])).lower()
    heading_class_hints = {
        "head", "title", "caption", "label", "h1", "h2", "h3", "h4",
        "section", "chapter", "rubric", "subhead", "subtitle",
    }
    if any(h in classes for h in heading_class_hints):
        return True

    # Check inline style for large or bold font
    style = element.get("style", "").lower()
    if "font-weight" in style and any(w in style for w in ("bold", "700", "800", "900")):
        if len(text) < 120:
            return True
    if "font-size" in style:
        sizes = re.findall(r"font-size\s*:\s*([\d.]+)(pt|px|em|rem)", style)
        for val, unit in sizes:
            v = float(val)
            if (unit == "pt" and v >= 11) or (unit == "px" and v >= 15) or \
               (unit in ("em", "rem") and v >= 1.1):
                if len(text) < 120:
                    return True

    # Standalone <b> or <strong> that is the entire element content
    if name in {"b", "strong"} and len(text) < 120:
        parent = element.parent
        if parent and _clean(parent.get_text()) == text:
            return True

    return False


def _extract_semantic_html(html_str: str) -> list:
    """
    Extract narrative text sections from a semantic HTML or ESEF iXBRL file.

    Key behaviours:
    - Removes ALL <table> elements before processing — financial statement
      tables (with hundreds of row-label <td> cells) are noise for narrative
      extraction.
    - Detects headings from <h1>-<h4> when present; falls back to CSS class /
      inline-style analysis for ESEF reports that use styled <div>/<p> headings.
    - Only collects body text from <p> and <li> elements (not <td>/<th>).
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        import subprocess, sys
        subprocess.run([sys.executable, "-m", "pip", "install", "beautifulsoup4",
                        "--break-system-packages", "-q"], check=False)
        from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_str, "html.parser")

    # ── 1. Remove boilerplate and noise ──────────────────────────────────────
    for tag in soup(["script", "style", "nav", "noscript", "iframe", "footer"]):
        tag.decompose()

    # Remove ALL tables — financial statement tables are noise for narrative
    for table in soup.find_all("table"):
        table.decompose()

    # Unwrap iXBRL inline namespace tags (ix:nonfraction, ix:nonnumeric, etc.)
    # so their text content is preserved but the wrappers disappear
    for ix_tag in soup.find_all(re.compile(r"^ix", re.I)):
        try:
            ix_tag.unwrap()
        except Exception:
            pass

    # ── 2. Detect whether this file uses semantic heading tags ────────────────
    body = soup.body if soup.body else soup
    has_semantic_headings = bool(body.find(["h1", "h2", "h3", "h4"]))

    # ── 3. Walk the document, splitting on headings ───────────────────────────
    # Tags we treat as potential content or headings
    heading_tags = {"h1", "h2", "h3", "h4", "h5"}
    # Tags whose text we collect as body content (leaf nodes only)
    collect_tags = {"p", "li", "div", "span", "td", "dd", "blockquote"}
    # Tags that count as "structural" children — if an element has one of these
    # as a direct child, it is a container not a leaf, so skip it
    structural = {"p", "li", "div", "h1", "h2", "h3", "h4", "h5",
                  "section", "article", "aside", "main", "ul", "ol", "dl"}

    rows = []
    current_heading = None
    current_section = "Other"
    current_chunks = []
    seen_texts: set = set()   # deduplicate identical adjacent fragments
    seq = 0

    def flush():
        nonlocal seq
        text = _clean(" ".join(current_chunks))
        if len(text) > 60:
            rows.append({
                "section":    current_section,
                "heading":    current_heading or "",
                "seq":        seq,
                "char_count": len(text),
                "text":       text,
            })
            seq += 1

    def is_leaf(el) -> bool:
        """True if element has no structural block children."""
        return not any(
            c.name in structural
            for c in el.children
            if hasattr(c, "name") and c.name
        )

    def is_noise(text: str) -> bool:
        """True if text is purely numeric/symbolic — leftover from tables."""
        return bool(
            re.match(r"^[\d.,£€$%\s\-–—()\[\]/naN]+$", text, re.I)
            and len(text) < 80
        )

    for element in body.descendants:
        if not hasattr(element, "name") or element.name is None:
            continue

        tag = element.name

        # ── Heading elements ──────────────────────────────────────────────────
        if tag in heading_tags or _looks_like_heading(element, has_semantic_headings):
            if not is_leaf(element):
                continue
            text = _clean(element.get_text())
            if not text:
                continue
            flush()
            current_chunks = []
            seen_texts.clear()
            current_heading = text
            classified = _classify_section(text)
            if classified:
                current_section = classified
            # Unclassified headings keep the current section — don't reset to Other

        # ── Body text elements ────────────────────────────────────────────────
        elif tag in collect_tags:
            if not is_leaf(element):
                continue
            text = _clean(element.get_text())
            if not text or len(text) < 30:
                continue
            if is_noise(text):
                continue
            if text in seen_texts:
                continue
            seen_texts.add(text)
            current_chunks.append(text)

    flush()
    return rows


def extract_text_sections(html_bytes: bytes) -> pd.DataFrame:
    """
    Given raw iXBRL/HTML bytes, extract narrative text classified into
    annual report sections.

    Auto-detects whether the file is a pdf2htmlEX conversion (uses CSS-based
    font-size classes for structure) or a standard semantic HTML file (uses
    h1-h4 tags). Returns a DataFrame with columns:
        section, heading, seq, char_count, text
    """
    COLS = ["section", "heading", "seq", "char_count", "text"]
    html_str = html_bytes.decode("utf-8", errors="replace")

    if _is_pdf2htmlex(html_str):
        rows = _extract_pdf2htmlex(html_str)
    else:
        rows = _extract_semantic_html(html_str)

    if not rows:
        return pd.DataFrame(columns=COLS)
    return pd.DataFrame(rows, columns=COLS)


# ─── ESEF zip utilities ────────────────────────────────────────────────────────

def find_entry_point(extract_dir):
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(('.xhtml','.htm','.html')) and 'report' in root.lower():
                return os.path.join(root, f)
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith(('.xhtml','.htm','.html')):
                return os.path.join(root, f)
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith('.xml') and not f.startswith('.'):
                return os.path.join(root, f)
    return None


def _parse_arelle(entry_path: str, logs: list, meta: dict) -> pd.DataFrame:
    """Shared Arelle parsing logic given a resolved file path."""
    from arelle import Cntlr, ModelManager

    cntlr = Cntlr.Cntlr(logFileName="logToStdErr")
    model_manager = ModelManager.initialize(cntlr)
    model_manager.skipDTS = True
    model_xbrl = model_manager.load(entry_path)

    if model_xbrl is None:
        raise RuntimeError("Arelle failed to load the report.")

    rows = []
    for fact in model_xbrl.facts:
        try:
            concept = fact.concept
            if concept is not None:
                local_name = concept.qname.localName
                ns = concept.qname.namespaceURI or ""
                try:
                    label = concept.label(lang="en") or local_name
                except Exception:
                    label = local_name
            else:
                local_name = fact.qname.localName
                ns = fact.qname.namespaceURI or ""
                label = local_name
        except Exception:
            try:
                local_name = fact.qname.localName
                ns = ""
                label = local_name
            except Exception:
                continue

        try:
            ctx = fact.context
            if ctx is None:
                period_type = period_start = period_end = ""
            elif ctx.isInstantPeriod:
                period_type = "instant"
                period_start = ""
                period_end = str(ctx.instantDatetime.date()) if ctx.instantDatetime else ""
            elif ctx.isStartEndPeriod:
                period_type = "duration"
                period_start = str(ctx.startDatetime.date()) if ctx.startDatetime else ""
                period_end = str(ctx.endDatetime.date()) if ctx.endDatetime else ""
            else:
                period_type = "forever"
                period_start = period_end = ""
        except Exception:
            ctx = None
            period_type = period_start = period_end = ""

        unit_str = ""
        try:
            unit = fact.unit
            if unit is not None:
                unit_str = str(unit.value)
        except Exception:
            pass

        dims = {}
        try:
            if ctx is not None:
                for dq, dv in ctx.qnameDims.items():
                    try:
                        dims[dq.localName] = dv.memberQname.localName if dv.isExplicit else str(dv.typedMember)
                    except Exception:
                        dims[dq.localName] = str(dv)
        except Exception:
            pass

        value = ""
        try:
            value = fact.value
        except Exception:
            try:
                value = fact.text or ""
            except Exception:
                pass

        entity = ""
        try:
            if ctx and ctx.entityIdentifier:
                entity = ctx.entityIdentifier[1]
        except Exception:
            pass

        rows.append({
            "Concept": local_name, "Label": label, "Namespace": ns,
            "Statement": classify_statement(local_name),
            "Period Type": period_type, "Period Start": period_start, "Period End": period_end,
            "Value": value, "Unit": unit_str,
            "Decimals": getattr(fact, "decimals", "") or "",
            "Entity": entity,
            "Dimensions": "; ".join(f"{k}={v}" for k, v in dims.items()) if dims else "",
        })

    try:
        for ctx in model_xbrl.contexts.values():
            if ctx.entityIdentifier:
                meta["entity_id"] = ctx.entityIdentifier[1]
                break
    except Exception:
        pass

    logs.append(f"Facts found: {len(rows)}")
    model_manager.close()

    COLUMNS = ["Concept","Label","Namespace","Statement","Period Type",
               "Period Start","Period End","Value","Unit","Decimals","Entity","Dimensions"]

    if not rows:
        df = pd.DataFrame(columns=COLUMNS)
        df["_numeric"] = pd.Series(dtype=float)
        logs.append("Warning: no facts could be extracted.")
        return df

    df = pd.DataFrame(rows, columns=COLUMNS)
    df["Value"] = pd.to_numeric(df["Value"], errors="ignore")
    df["_numeric"] = pd.to_numeric(df["Value"], errors="coerce")
    return df


# ─── Public API ────────────────────────────────────────────────────────────────

def load_facts(zip_bytes: bytes):
    """Load XBRL facts from a ZIP package. Returns (df, logs, meta)."""
    logs = []
    meta = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "report.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)
        extract_dir = os.path.join(tmpdir, "extracted")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_dir)

        entry = find_entry_point(extract_dir)
        if not entry:
            raise FileNotFoundError("Could not find an XBRL/iXBRL entry point in the zip.")
        logs.append(f"Entry point: {os.path.relpath(entry, extract_dir)}")

        df = _parse_arelle(entry, logs, meta)

    return df, logs, meta


def load_facts_from_file(file_bytes: bytes, file_ext: str, filename: str):
    """
    Handles zip packages and direct xhtml/html/xml files.
    Returns (df, logs, meta).
    """
    if file_ext == "zip":
        return load_facts(file_bytes)

    logs = [f"Direct file load: {filename}"]
    meta = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        entry = os.path.join(tmpdir, filename)
        with open(entry, "wb") as f:
            f.write(file_bytes)
        df = _parse_arelle(entry, logs, meta)

    return df, logs, meta


def load_text_sections(file_bytes: bytes, file_ext: str, filename: str) -> pd.DataFrame:
    """
    Extract narrative text sections from an iXBRL/HTML file or ZIP package.
    Returns a DataFrame with columns: section, heading, seq, char_count, text.
    """
    if file_ext == "zip":
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "report.zip")
            with open(zip_path, "wb") as f:
                f.write(file_bytes)
            extract_dir = os.path.join(tmpdir, "extracted")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(extract_dir)
            entry = find_entry_point(extract_dir)
            if not entry:
                raise FileNotFoundError("Could not find an HTML/iXBRL entry point in the zip.")
            with open(entry, "rb") as f:
                html_bytes = f.read()
    else:
        html_bytes = file_bytes

    return extract_text_sections(html_bytes)
