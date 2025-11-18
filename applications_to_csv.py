#!/usr/bin/env python3
"""
Fetch submissions from the Find A Grant Open Data API using the local `curl` binary (throttled),
flatten into a single CSV with one row per application, insert blank per‑section
separator columns (“Section: <name>” in the correct JSON order), and (by default)
drop columns that are constant across all rows.


REQUIRES
- `curl` binary available on PATH (or specify via --curl-path).

USAGE (examples)
    # Minimal (1 req/s, drop constants, add blank section separators):
    ./applications_to_csv.py applications.csv \
      --api-base 'https://api.example.gov.uk' \
      --grant-ref 'XX-XXXX-YYYY' \
      --api-key 'YOUR_API_KEY'

    # Slower throttle and TSV:
    ./applications_to_csv.py applications.tsv \
      --api-base 'https://api.example.gov.uk' \
      --grant-ref 'XX-XXXX-YYYY' \
      --api-key 'YOUR_API_KEY' \
      --sleep-seconds 1.5 \
      --delimiter '\t'

    # Keep constant columns and include section-title prefixes + question IDs:
    ./applications_to_csv.py applications.csv \
      --api-base 'https://api.example.gov.uk' \
      --grant-ref 'XX-XXXX-YYYY' \
      --api-key 'YOUR_API_KEY' \
      --keep-constants \
      --prefix-section-title \
      --include-question-id
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

# ------------------------- helpers -------------------------

def sanitize_col(name: str) -> str:
    import re as _re
    name = _re.sub(r"\s+", " ", str(name)).strip()
    name = _re.sub(r"[^A-Za-z0-9]+", "_", name)
    return name.strip("_") or "unnamed"

def try_json_cell(obj: Any) -> Any:
    if obj is None:
        return ""
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, (list, dict)):
        return json.dumps(obj, ensure_ascii=False)
    return str(obj)

def flatten(prefix: str, value: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(value, dict):
        for k, v in value.items():
            col = sanitize_col(f"{prefix}_{k}")
            if isinstance(v, (dict, list)):
                out.update(flatten(col, v))
            else:
                out[col] = v
    elif isinstance(value, list):
        if all(isinstance(x, (str, int, float, type(None))) for x in value):
            out[sanitize_col(prefix)] = " | ".join("" if x is None else str(x) for x in value)
        else:
            out[sanitize_col(prefix)] = json.dumps(value, ensure_ascii=False)
    else:
        out[sanitize_col(prefix)] = value
    return out

def build_header_name(
    question_title: str,
    question_id: str | None,
    section_title: str | None,
    include_qid: bool,
    prefix_section: bool,
) -> str:
    """
    Create a stable header for a question response (sanitized).
    Section separator headers are human-readable (“Section: …”) and are handled elsewhere.
    """
    base = sanitize_col(question_title or question_id or "question")
    if prefix_section and section_title:
        base = f"{sanitize_col(section_title)}__{base}"
    if include_qid and question_id:
        base = f"{base}__{sanitize_col(question_id)}"
    return base

def section_separator_header(section_title: str) -> str:
    """Human-readable separator header placed where the section begins in JSON."""
    title = (section_title or "Untitled Section").strip()
    return f"Section: {title}"

def add_page_param(base_url: str, param: str, page: int) -> str:
    parts = urlsplit(base_url)
    q = parse_qsl(parts.query, keep_blank_values=True)
    q = [(k, v) for (k, v) in q if k.lower() != param.lower()]
    q.append((param, str(page)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))

# ------------------------- curl runner -------------------------

def run_curl_json(
    curl_path: str,
    url: str,
    api_key: str,
    *,
    user_agent: str = "curl/8.6.0",
    timeout: int = 60,
    max_retries: int = 3,
    backoff_base: float = 0.4,
    backoff_cap: float = 4.0,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Invoke curl to GET JSON with x-api-key header (key is provided via --api-key).
    Retries curl failures with exponential backoff + jitter.
    """
    api_key = api_key.strip()
    attempt = 0
    last_err: str | None = None

    while True:
        attempt += 1
        cmd = [
            curl_path,
            "-sS",
            "--fail-with-body",     # requires curl >= 7.76; remove if not available
            "--max-time", str(timeout),
            "-H", f"x-api-key: {api_key}",
            "-H", "Accept: application/json",
            "-A", user_agent,
            url,
        ]
        if verbose:
            print(f"[curl attempt {attempt}] {url}", file=sys.stderr)

        proc = subprocess.run(cmd, capture_output=True, text=True)

        if proc.returncode == 0:
            text = (proc.stdout or "").strip() or (proc.stderr or "").strip()
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                snippet = (text[:300] + "…") if len(text) > 300 else text
                raise RuntimeError(f"curl returned non-JSON response (len={len(text)}). Snippet: {snippet}")
            # API Gateway-style error guard
            if isinstance(data, dict) and data.get("Message", "").lower().find("not authorized") >= 0:
                raise RuntimeError(f"API responded with error: {data}")
            return data

        last_err = proc.stderr.strip() or f"curl exit {proc.returncode}"
        if verbose:
            print(f"[curl error] rc={proc.returncode} stderr:\n{last_err}\n", file=sys.stderr)

        if attempt >= max_retries:
            raise RuntimeError(f"curl failed after {max_retries} attempts: {last_err}")

        delay = min(backoff_base * (2 ** (attempt - 1)), backoff_cap) + random.uniform(0, backoff_base / 2)
        time.sleep(delay)

# ------------------------- shape coercion & pagination -------------------------

def coerce_to_pairs(data: Any) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    if isinstance(data, dict) and isinstance(data.get("applications"), list):
        for app in data["applications"]:
            root_meta = {k: v for k, v in app.items() if k != "submissions"}
            subs = app.get("submissions", []) or []
            for sub in subs:
                pairs.append((root_meta, sub))
    elif isinstance(data, dict) and isinstance(data.get("applications"), dict):
        app = data["applications"]
        root_meta = {k: v for k, v in app.items() if k != "submissions"}
        subs = app.get("submissions", []) or []
        for sub in subs:
            pairs.append((root_meta, sub))
    elif isinstance(data, dict) and isinstance(data.get("submissions"), list):
        root_meta = {k: v for k, v in data.items() if k != "submissions"}
        for sub in data["submissions"]:
            pairs.append((root_meta, sub))
    elif isinstance(data, list):
        for sub in data:
            pairs.append(({}, sub))
    elif isinstance(data, dict) and any(k in data for k in ("submissionId", "sections")):
        pairs.append(({}, data))
    else:
        raise ValueError("Unrecognised JSON shape for submissions.")
    return pairs

def find_total_pages(doc: Dict[str, Any]) -> int:
    apps = doc.get("applications")
    if isinstance(apps, list) and apps:
        t = apps[0].get("totalSubmissionPages")
        if isinstance(t, int): return t
        if isinstance(t, str) and t.isdigit(): return int(t)
    if isinstance(apps, dict):
        t = apps.get("totalSubmissionPages")
        if isinstance(t, int): return t
        if isinstance(t, str) and t.isdigit(): return int(t)
    t = doc.get("totalSubmissionPages")
    if isinstance(t, int): return t
    if isinstance(t, str) and t.isdigit(): return int(t)
    return 1

def aggregate_all_pages_curl(
    curl_path: str,
    base_url: str,
    api_key: str,
    *,
    sleep_seconds: float,
    user_agent: str,
    timeout: int,
    curl_retries: int,
    backoff_base: float,
    backoff_cap: float,
    verbose: bool,
) -> Dict[str, Any]:
    first = run_curl_json(
        curl_path, base_url, api_key,
        user_agent=user_agent, timeout=timeout, max_retries=curl_retries,
        backoff_base=backoff_base, backoff_cap=backoff_cap, verbose=verbose,
    )
    total_pages = max(find_total_pages(first), 1)
    if total_pages <= 1:
        return first

    merged = json.loads(json.dumps(first))  # shallow copy

    def extend_app_list(target: Dict[str, Any], page_data: Dict[str, Any]) -> None:
        at, ap = target.get("applications"), page_data.get("applications")
        if not isinstance(at, list) or not isinstance(ap, list):
            return
        for i in range(min(len(at), len(ap))):
            at[i].setdefault("submissions", []).extend(ap[i].get("submissions", []) or [])

    def extend_app_obj(target: Dict[str, Any], page_data: Dict[str, Any]) -> None:
        at, ap = target.get("applications"), page_data.get("applications")
        if not isinstance(at, dict) or not isinstance(ap, dict):
            return
        at.setdefault("submissions", []).extend(ap.get("submissions", []) or [])

    def extend_top_subs(target: Dict[str, Any], page_data: Dict[str, Any]) -> None:
        target.setdefault("submissions", []).extend(page_data.get("submissions", []) or [])

    param = "pageNumber"  # change to "page" if your environment uses ?page=
    for p in range(2, total_pages + 1):
        url_p = add_page_param(base_url, param, p)
        time.sleep(max(0.0, sleep_seconds))  # throttle
        page_data = run_curl_json(
            curl_path, url_p, api_key,
            user_agent=user_agent, timeout=timeout, max_retries=curl_retries,
            backoff_base=backoff_base, backoff_cap=backoff_cap, verbose=verbose,
        )
        extend_app_list(merged, page_data)
        extend_app_obj(merged, page_data)
        extend_top_subs(merged, page_data)

    return merged

# ------------------------- extraction & CSV -------------------------

SUBMISSION_META_FIELDS = [
    "submissionId",
    "grantApplicantEmailAddress",
    "submittedTimeStamp",
    "gapId",
]
ROOT_META_CANDIDATES = [
    "applicationFormName",
    "applicationFormVersion",
    "applicationId",
    "ggisReferenceNumber",
    "grantAdminEmailAddress",
]

def extract_row(
    root_meta: Dict[str, Any],
    sub: Dict[str, Any],
    *,
    include_qid: bool,
    prefix_section: bool,
    add_section_separators: bool,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Tuple[str, List[str]]]]:
    """
    Return (meta, dynamic, ordered_blocks).
    - meta: metadata columns (sanitized).
    - dynamic: all question columns + a blank cell for each section separator.
    - ordered_blocks: [(Section: <name>, [question_cols_in_order]), ...] in the JSON order.
    """
    meta: Dict[str, Any] = {}
    for k in ROOT_META_CANDIDATES:
        if k in root_meta:
            meta[sanitize_col(k)] = root_meta[k]
    for k in SUBMISSION_META_FIELDS:
        if k in sub:
            meta[sanitize_col(k)] = sub.get(k, "")
    for k in ROOT_META_CANDIDATES:
        if k in sub:
            meta[sanitize_col(k)] = sub[k]

    dynamic: Dict[str, Any] = {}
    blocks: List[Tuple[str, List[str]]] = []

    sections = sub.get("sections", []) or []
    for sec in sections:
        sec_title = sec.get("sectionTitle") or sec.get("sectionId") or ""
        sep_header = section_separator_header(sec_title) if add_section_separators else None
        if sep_header:
            dynamic.setdefault(sep_header, "")  # blank on purpose
        section_cols: List[str] = []

        for q in (sec.get("questions", []) or []):
            q_title = (q.get("questionTitle") or "").strip()
            q_id    = q.get("questionId")
            col     = build_header_name(q_title, q_id, sec_title, include_qid, prefix_section)
            resp    = q.get("questionResponse")

            if isinstance(resp, (dict, list)):
                flat = flatten(col, resp)
                for fk, fv in flat.items():
                    dynamic[fk] = fv
                    section_cols.append(fk)
            else:
                if col in dynamic and not include_qid:
                    col = f"{col}__{sanitize_col(q_id) if q_id else 'dup'}"
                dynamic[col] = resp
                section_cols.append(col)

        blocks.append((sep_header or "", section_cols))

    # to CSV-safe primitives
    meta = {k: try_json_cell(v) for k, v in meta.items()}
    dynamic = {k: try_json_cell(v) for k, v in dynamic.items()}
    return meta, dynamic, blocks

def drop_constant_columns(
    rows: List[Dict[str, Any]],
    *,
    ignore_empty: bool,
    keep_patterns: List[re.Pattern],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not rows:
        return rows, []
    cols = list(rows[0].keys())
    remove: List[str] = []

    def force_keep(c: str) -> bool:
        return any(p.search(c) for p in keep_patterns)

    for c in cols:
        if force_keep(c):
            continue
        vals = [r.get(c, "") for r in rows]
        s = set(v for v in vals if str(v).strip() != "") if ignore_empty else set(vals)
        if len(s) <= 1:
            remove.append(c)
    if remove:
        rows = [{k: v for k, v in r.items() if k not in remove} for r in rows]
    return rows, remove

# ------------------------- CLI -------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Fetch submissions via curl (throttled), flatten to CSV, add 'Section: <name>' separators in JSON order, drop constant columns by default.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("output_csv", type=Path, nargs="?", help="Output CSV (default: applications.csv)")
    ap.add_argument("--api-base", required=True, help="Base URL (no trailing slash), e.g., 'https://api.example.gov.uk'")
    ap.add_argument("--submissions-path", default="/api/open-data/submissions/{grantRef}",
                    help="Path template for submissions endpoint, must include {grantRef}")
    ap.add_argument("--grant-ref", required=True, help="Grant reference to insert into {grantRef} path template")
    ap.add_argument("--api-key", required=True, help="API key for the 'x-api-key' header (REQUIRED)")
    ap.add_argument("--curl-path", default="curl", help="Path to curl binary")
    ap.add_argument("--sleep-seconds", type=float, default=1.0, help="Seconds to sleep between page requests")
    ap.add_argument("--user-agent", default="curl/8.6.0", help="User-Agent to send via curl (-A)")
    ap.add_argument("--timeout-seconds", type=int, default=60, help="Per-call curl max time")
    ap.add_argument("--curl-retries", type=int, default=3, help="Retries for curl non-zero exits")
    ap.add_argument("--backoff-base", type=float, default=0.4, help="Base backoff (curl retry) seconds")
    ap.add_argument("--backoff-cap", type=float, default=4.0, help="Max backoff cap (curl retry) seconds")
    ap.add_argument("--verbose", action="store_true", help="Print progress to stderr")
    ap.add_argument("--delimiter", default=",", help="CSV delimiter (',' or '\\t')")

    # Column naming tweaks for question columns
    ap.add_argument("--prefix-section-title", action="store_true", help="Prefix question headers with sectionTitle__")
    ap.add_argument("--include-question-id", action="store_true", help="Append __<questionId> to question headers")

    # Section separator columns
    gsep = ap.add_mutually_exclusive_group()
    gsep.add_argument("--section-separators", dest="section_separators", action="store_true", help="Insert 'Section: <name>' separator columns")
    gsep.add_argument("--no-section-separators", dest="section_separators", action="store_false", help="Do not insert section separators")
    ap.set_defaults(section_separators=True)

    # Constant-column handling
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--drop-constant-columns", dest="drop_constants", action="store_true", help="Drop constant-value columns")
    g.add_argument("--keep-constants", dest="drop_constants", action="store_false", help="Keep constant-value columns")
    ap.set_defaults(drop_constants=True)

    ap.add_argument("--constant-ignore-empty", action="store_true", help="Ignore empties when testing constancy")
    ap.add_argument("--keep-col", action="append", default=[], help="Regex (repeatable) of columns to force-keep")

    return ap.parse_args()

# ------------------------- Main -------------------------

def main() -> None:
    args = parse_args()

    out_path: Path = args.output_csv or Path("applications.csv")

    # Construct base submissions URL from arguments
    if "{grantRef}" not in args.submissions_path:
        print("ERROR: --submissions-path must include '{grantRef}' placeholder.", file=sys.stderr)
        sys.exit(2)
    path = args.submissions_path.replace("{grantRef}", args.grant_ref)
    api_base = args.api_base.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    base_url = f"{api_base}{path}"

    # 1) Aggregate all pages via curl (throttled)
    merged = aggregate_all_pages_curl(
        args.curl_path,
        base_url,
        args.api_key,
        sleep_seconds=args.sleep_seconds,
        user_agent=args.user_agent,
        timeout=args.timeout_seconds,
        curl_retries=args.curl_retries,
        backoff_base=args.backoff_base,
        backoff_cap=args.backoff_cap,
        verbose=args.verbose,
    )

    # 2) Extract rows (meta + questions + per-row ordered blocks)
    pairs = coerce_to_pairs(merged)
    # Build ordered headers: meta first, then for each row: Section + its question columns
    meta_order: List[str] = [sanitize_col(k) for k in (
        "applicationFormName","applicationFormVersion","applicationId","ggisReferenceNumber","grantAdminEmailAddress"
    )] + [sanitize_col(k) for k in ("submissionId","grantApplicantEmailAddress","submittedTimeStamp","gapId")]
    seen_headers = set(meta_order)
    final_headers: List[str] = list(dict.fromkeys(meta_order))

    cache: List[Tuple[Dict[str, Any], Dict[str, Any], List[Tuple[str, List[str]]]]] = []
    all_cols_seen: set[str] = set(final_headers)

    for root_meta, sub in pairs:
        meta, dyn, blocks = extract_row(
            root_meta, sub,
            include_qid=args.include_question_id,
            prefix_section=args.prefix_section_title,
            add_section_separators=args.section_separators,
        )
        cache.append((meta, dyn, blocks))
        all_cols_seen.update(meta.keys())
        all_cols_seen.update(dyn.keys())

        for section_header, block_cols in blocks:
            if section_header and section_header not in seen_headers:
                final_headers.append(section_header); seen_headers.add(section_header)
            for col in block_cols:
                if col not in seen_headers:
                    final_headers.append(col); seen_headers.add(col)

    for c in all_cols_seen:
        if c not in seen_headers:
            final_headers.append(c); seen_headers.add(c)

    # 3) Materialize CSV rows
    rows: List[Dict[str, Any]] = []
    for meta, dyn, blocks in cache:
        row = {h: "" for h in final_headers}
        row.update(meta)
        row.update(dyn)
        rows.append(row)

    # 4) Drop constant columns unless disabled (NEVER drop “Section: …”)
    if args.drop_constants and rows:
        keep_patterns = [re.compile(p) for p in (args.keep_col or [])]
        keep_patterns.append(re.compile(r"^Section:\s"))  # keep all section separators
        rows, _ = drop_constant_columns(
            rows,
            ignore_empty=args.constant_ignore_empty,
            keep_patterns=keep_patterns,
        )
        if rows:
            final_headers = list(rows[0].keys())

    # 5) Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=final_headers, extrasaction="ignore", delimiter=args.delimiter)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

if __name__ == "__main__":
    main()