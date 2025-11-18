from pathlib import Path
import re
from .http_client import aggregate_all_pages_http
from .csv_utils import (
    sanitize_col,
    drop_constant_columns,
    coerce_to_pairs,
    extract_row,
    ROOT_META_CANDIDATES,
    SUBMISSION_META_FIELDS,
)
import time
import asyncio


async def run_pipeline(
    api_base: str,
    ggis_reference_number: str,
    api_key: str,
    output_csv: Path | None = None,
    max_concurrent_requests: int = 20,
) -> tuple[Path, int, float]:
    """Core async pipeline used by the CLI.

    Returns (out_path, num_apps, elapsed_seconds).
    """
    start_time = time.time()

    def to_snake_case(s: str) -> str:
        s = re.sub(r"[^A-Za-z0-9]+", "_", s)
        s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
        return s.strip("_").lower()

    submissions_path = "/api/open-data/submissions/{ggisReferenceNumber}"
    path = submissions_path.replace("{ggisReferenceNumber}", ggis_reference_number)
    api_base = api_base.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    base_url = f"{api_base}{path}"

    # Decide output path (may require a preview request)
    if output_csv is None:
        merged_preview = aggregate_all_pages_http(
            base_url,
            api_key,
            max_concurrent_requests=max_concurrent_requests,
        )
        app_name = None
        if "applications" in merged_preview:
            if (
                isinstance(merged_preview["applications"], list)
                and merged_preview["applications"]
            ):
                app_name = merged_preview["applications"][0].get("applicationFormName")
            elif isinstance(merged_preview["applications"], dict):
                app_name = merged_preview["applications"].get("applicationFormName")
        if not app_name:
            app_name = "applications"
        today = __import__("datetime").date.today()
        out_path = Path(
            f"{to_snake_case(app_name)}-{today.year}-{today.month:02d}-{today.day:02d}.csv"
        )
    else:
        out_path = output_csv

    # Fetch all pages
    merged = aggregate_all_pages_http(
        base_url,
        api_key,
        max_concurrent_requests=max_concurrent_requests,
    )

    # Flatten to rows
    pairs = coerce_to_pairs(merged)
    meta_order = [sanitize_col(k) for k in ROOT_META_CANDIDATES] + [
        sanitize_col(k) for k in SUBMISSION_META_FIELDS
    ]
    seen_headers = set(meta_order)
    final_headers = list(dict.fromkeys(meta_order))
    cache = []
    all_cols_seen = set(final_headers)
    for root_meta, sub in pairs:
        meta, dyn, blocks = extract_row(
            root_meta,
            sub,
            include_qid=False,
            prefix_section=False,
            add_section_separators=True,
        )
        cache.append((meta, dyn, blocks))
        all_cols_seen.update(meta.keys())
        all_cols_seen.update(dyn.keys())
        for section_header, block_cols in blocks:
            if section_header and section_header not in seen_headers:
                final_headers.append(section_header)
                seen_headers.add(section_header)
            for col in block_cols:
                if col not in seen_headers:
                    final_headers.append(col)
                    seen_headers.add(col)
    for c in all_cols_seen:
        if c not in seen_headers:
            final_headers.append(c)
            seen_headers.add(c)
    rows: list[dict[str, object]] = []
    for meta, dyn, blocks in cache:
        row: dict[str, object] = {h: "" for h in final_headers}
        row.update(meta)
        row.update(dyn)
        rows.append(row)

    # Drop constant columns (except section separators)
    if rows:
        import re as _re

        keep_patterns = [_re.compile(r"^Section:\s")]  # keep all section separators
        rows, _ = drop_constant_columns(
            rows,
            ignore_empty=False,
            keep_patterns=keep_patterns,
        )
        if rows:
            final_headers = list(rows[0].keys())

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    import csv

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=final_headers, extrasaction="ignore", delimiter=","
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    elapsed = time.time() - start_time
    num_apps = len(rows)
    return out_path, num_apps, elapsed


def run_pipeline_sync(
    api_base: str,
    ggis_reference_number: str,
    api_key: str,
    output_csv: Path | None = None,
    max_concurrent_requests: int = 20,
) -> tuple[Path, int, float]:
    """Synchronous wrapper around run_pipeline for non-async callers (like click)."""
    return asyncio.run(
        run_pipeline(
            api_base=api_base,
            ggis_reference_number=ggis_reference_number,
            api_key=api_key,
            output_csv=output_csv,
            max_concurrent_requests=max_concurrent_requests,
        )
    )
