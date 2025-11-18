import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import requests


def find_total_pages(doc: Dict[str, Any]) -> int:
    apps = doc.get("applications")
    if isinstance(apps, list) and apps:
        t = apps[0].get("totalSubmissionPages")
        if isinstance(t, int):
            return t
        if isinstance(t, str) and t.isdigit():
            return int(t)
    if isinstance(apps, dict):
        t = apps.get("totalSubmissionPages")
        if isinstance(t, int):
            return t
        if isinstance(t, str) and t.isdigit():
            return int(t)
    t = doc.get("totalSubmissionPages")
    if isinstance(t, int):
        return t
    if isinstance(t, str) and t.isdigit():
        return int(t)
    return 1


def run_http_json(
    url: str,
    api_key: str,
    *,
    timeout: int = 60,
    max_retries: int = 3,
    backoff_base: float = 0.4,
    backoff_cap: float = 4.0,
    verbose: bool = False,
) -> Dict[str, Any]:
    api_key = api_key.strip()
    attempt = 0
    last_err: Optional[str] = None
    while True:
        if attempt > 0:
            delay = min(
                backoff_base * (2 ** (attempt - 1)), backoff_cap
            ) + random.uniform(0, backoff_base / 2)
            time.sleep(delay)
        else:
            time.sleep(0.5)
        attempt += 1
        headers = {"x-api-key": api_key, "Accept": "application/json"}
        try:
            if verbose:
                print(f"[http attempt {attempt}] {url}", file=sys.stderr)
            resp: requests.Response = requests.get(
                url, headers=headers, timeout=timeout
            )
            text: str = resp.text
            if resp.status_code == 403:
                last_err = "HTTP 403: Forbidden"
                if verbose:
                    print(f"[http error] HTTPError: {last_err}", file=sys.stderr)
                raise Exception(last_err)
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception:
                snippet = (text[:300] + "…") if len(text) > 300 else text
                raise RuntimeError(
                    f"HTTP returned non-JSON response (len={len(text)}). Snippet: {snippet}"
                )

            # Ensure we always return a dict[str, Any] for callers.
            if not isinstance(data, dict):
                snippet = (text[:300] + "…") if len(text) > 300 else text
                raise RuntimeError(
                    f"HTTP returned JSON that is not an object (len={len(text)}). Snippet: {snippet}"
                )

            if data.get("Message", "").lower().find("not authorized") >= 0:
                raise RuntimeError(f"API responded with error: {data}")
            return data
        except Exception as e:
            last_err = str(e)
            if verbose:
                print(f"[http error] Exception: {last_err}", file=sys.stderr)
        if attempt >= max_retries:
            raise RuntimeError(f"HTTP failed after {max_retries} attempts: {last_err}")


def aggregate_all_pages_http(
    base_url: str,
    api_key: str,
    max_concurrent_requests: int = 10,
) -> Dict[str, Any]:
    first = run_http_json(base_url, api_key)
    total_pages = max(find_total_pages(first), 1)
    if total_pages <= 1:
        return first
    merged = first.copy() if isinstance(first, dict) else first

    def extend_app_list(target: Dict[str, Any], page_data: Dict[str, Any]) -> None:
        at, ap = target.get("applications"), page_data.get("applications")
        if not isinstance(at, list) or not isinstance(ap, list):
            return
        for i in range(min(len(at), len(ap))):
            at[i].setdefault("submissions", []).extend(
                ap[i].get("submissions", []) or []
            )

    def extend_app_obj(target: Dict[str, Any], page_data: Dict[str, Any]) -> None:
        at, ap = target.get("applications"), page_data.get("applications")
        if not isinstance(at, dict) or not isinstance(ap, dict):
            return
        at.setdefault("submissions", []).extend(ap.get("submissions", []) or [])

    def extend_top_subs(target: Dict[str, Any], page_data: Dict[str, Any]) -> None:
        target.setdefault("submissions", []).extend(
            page_data.get("submissions", []) or []
        )

    page_urls = [
        f"{base_url}{'&' if '?' in base_url else '?'}pageNumber={p}"
        for p in range(2, total_pages + 1)
    ]
    page_results: List[Optional[Dict[str, Any]]] = [None] * (total_pages - 1)

    def fetch(url: str) -> Dict[str, Any]:
        return run_http_json(url, api_key)

    with ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
        futures = [executor.submit(fetch, url) for url in page_urls]
        for idx, fut in enumerate(futures):
            try:
                page_results[idx] = fut.result()
            except Exception as e:
                raise RuntimeError(f"Failed to fetch page {idx + 2}: {e}")
    for page_data in page_results:
        if page_data is not None:
            extend_app_list(merged, page_data)
            extend_app_obj(merged, page_data)
            extend_top_subs(merged, page_data)
    return merged
