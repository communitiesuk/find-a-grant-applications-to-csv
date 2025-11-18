from typing import Any, Dict, List, Tuple
import json
import re

ROOT_META_CANDIDATES = [
    "applicationFormName",
    "applicationFormVersion",
    "applicationId",
    "ggisReferenceNumber",
    "grantAdminEmailAddress",
]
SUBMISSION_META_FIELDS = [
    "submissionId",
    "grantApplicantEmailAddress",
    "submittedTimeStamp",
    "gapId",
]


def extract_row(
    root_meta: Dict[str, Any],
    sub: Dict[str, Any],
    *,
    include_qid: bool,
    prefix_section: bool,
    add_section_separators: bool,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Tuple[str, List[str]]]]:
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
        sep_header = (
            section_separator_header(sec_title) if add_section_separators else None
        )
        if sep_header:
            dynamic.setdefault(sep_header, "")  # blank on purpose
        section_cols: List[str] = []

        for q in sec.get("questions", []) or []:
            q_title = (q.get("questionTitle") or "").strip()
            q_id = q.get("questionId")
            col = build_header_name(
                q_title, q_id, sec_title, include_qid, prefix_section
            )
            resp = q.get("questionResponse")

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


def sanitize_col(name: str) -> str:
    name = re.sub(r"\s+", " ", str(name)).strip()
    name = re.sub(r"[^A-Za-z0-9]+", "_", name)
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
            out[sanitize_col(prefix)] = " | ".join(
                "" if x is None else str(x) for x in value
            )
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
    base = sanitize_col(question_title or question_id or "question")
    if prefix_section and section_title:
        base = f"{sanitize_col(section_title)}__{base}"
    if include_qid and question_id:
        base = f"{base}__{sanitize_col(question_id)}"
    return base


def section_separator_header(section_title: str) -> str:
    title = (section_title or "Untitled Section").strip()
    return f"Section: {title}"


def drop_constant_columns(
    rows: List[Dict[str, Any]],
    *,
    ignore_empty: bool,
    keep_patterns: List[re.Pattern[str]],
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
    elif isinstance(data, dict) and any(
        k in data for k in ("submissionId", "sections")
    ):
        pairs.append(({}, data))
    else:
        raise ValueError("Unrecognised JSON shape for submissions.")
    return pairs
