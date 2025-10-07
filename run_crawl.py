# run_crawl.py
import asyncio
import hashlib
import inspect
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Set, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse

import yaml
from pydantic import BaseModel
from aiohttp import ClientSession, ClientTimeout

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode

# ---------- Paths ----------
OUTPUT_HTML = Path("output_raw/html")
OUTPUT_FILES = Path("output_raw/files")
OUTPUT_META = Path("output_raw/meta")
LOG_PATH = Path("logs/crawl.log")
SEEN_PATH = Path("logs/seen.json")

# ---------- HTTP headers ----------
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.google.com/",
}

class _PseudoPage:
    """Gói kết quả HTTP GET thành 'page' giống Crawl4AI để dùng chung pipeline."""
    def __init__(self, url, status, headers, text):
        self.url = url
        self.status = status
        self.content_type = headers.get("Content-Type") if headers else None
        self.html = text
        self.domain = url.split("/")[2]


# ---------- Helpers ----------
def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class PageMeta(BaseModel):
    url: str
    fetched_at: str
    status: Optional[int] = None
    content_type: Optional[str] = None
    file_path: Optional[str] = None
    html_path: Optional[str] = None
    out_links: list[str] = []
    domain: Optional[str] = None
    notes: Optional[str] = None


def safe_filename(url: str, ext: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return f"{h}{ext}"


def pick_ext_by_ct(content_type: Optional[str]) -> str:
    if not content_type:
        return ".bin"
    ct = content_type.lower()
    if "pdf" in ct:
        return ".pdf"
    if "msword" in ct or "officedocument.wordprocessingml" in ct:
        return ".docx"
    if "html" in ct:
        return ".html"
    if "spreadsheet" in ct or "excel" in ct:
        return ".xlsx"
    if "presentation" in ct or "powerpoint" in ct:
        return ".pptx"
    return ".bin"


def is_allowed(url: str, include: List[str], exclude: List[str]) -> bool:
    ok = any(re.search(p, url) for p in include) if include else True
    bad = any(re.search(p, url) for p in exclude) if exclude else False
    return ok and not bad


def allowed_by_domain(url: str, allowed_domains: List[str]) -> bool:
    host = urlparse(url).netloc.lower()
    return any(host == d or host.endswith("." + d) for d in (allowed_domains or []))


def looks_like_file(url: str, extensions: List[str]) -> bool:
    lower = url.lower()
    return any(lower.endswith(ext) for ext in extensions)


async def download_file(session: ClientSession, url: str, timeout_ms: int) -> Tuple[bytes, Optional[str], Optional[int]]:
    try:
        async with session.get(url, timeout=ClientTimeout(total=timeout_ms / 1000), headers=DEFAULT_HEADERS) as resp:
            content = await resp.read()
            ctype = resp.headers.get("Content-Type")
            return content, ctype, resp.status
    except Exception:
        return b"", None, None


async def fetch_html_fallback(session: ClientSession, url: str, timeout_ms: int) -> _PseudoPage | None:
    """Khi Playwright bị chặn (403/5xx), dùng HTTP GET thẳng để lấy HTML."""
    try:
        async with session.get(
            url,
            timeout=ClientTimeout(total=timeout_ms / 1000),
            headers=DEFAULT_HEADERS,
            allow_redirects=True,
        ) as resp:
            txt = await resp.text(errors="ignore")
            return _PseudoPage(url, resp.status, resp.headers, txt)
    except Exception:
        return None


def make_run_config(crawler_cfg: dict, allowed_domains_list: List[str]) -> CrawlerRunConfig:
    """Build CrawlerRunConfig."""
    sig = inspect.signature(CrawlerRunConfig)
    params = set(sig.parameters.keys())

    max_pages_val = crawler_cfg.get("max_pages", 150)
    depth_val = crawler_cfg.get("max_depth", 3)
    conc_val = crawler_cfg.get("concurrency", 3)
    to_val = crawler_cfg.get("timeout_ms", 30000)

    kv = {}
    # limits
    if "max_pages" in params: kv["max_pages"] = max_pages_val
    elif "max_visits" in params: kv["max_visits"] = max_pages_val
    elif "max_requests" in params: kv["max_requests"] = max_pages_val
    # depth
    if "max_depth" in params: kv["max_depth"] = depth_val
    elif "depth" in params: kv["depth"] = depth_val
    # concurrency
    if "concurrency" in params: kv["concurrency"] = conc_val
    elif "max_concurrency" in params: kv["max_concurrency"] = conc_val
    elif "concurrent_tasks" in params: kv["concurrent_tasks"] = conc_val
    # timeout
    if "timeout" in params: kv["timeout"] = to_val
    elif "timeout_ms" in params: kv["timeout_ms"] = to_val
    elif "request_timeout" in params: kv["request_timeout"] = to_val
    # toggles
    if "render_js" in params: kv["render_js"] = crawler_cfg.get("render_js", True)
    if "obey_robots_txt" in params: kv["obey_robots_txt"] = crawler_cfg.get("obey_robots_txt", True)
    if "follow_sitemaps" in params: kv["follow_sitemaps"] = crawler_cfg.get("follow_sitemaps", True)
    if "respect_crawl_delay" in params: kv["respect_crawl_delay"] = crawler_cfg.get("respect_crawl_delay", True)
    if "cache_mode" in params: kv["cache_mode"] = CacheMode.BYPASS
    if "user_agent" in params: kv["user_agent"] = crawler_cfg.get("user_agent", DEFAULT_HEADERS["User-Agent"])
    if "allowed_domains" in params: kv["allowed_domains"] = allowed_domains_list or None
    if "extract_links" in params: kv["extract_links"] = crawler_cfg.get("extract_links", True)
    if "save_html" in params: kv["save_html"] = crawler_cfg.get("save_html", True)

    print("[DEBUG] Using run_cfg params:", kv)
    return CrawlerRunConfig(**kv)


def normalize_pages(res: Any) -> List[Any]:
    if res is None:
        return []
    if isinstance(res, (list, tuple)):
        return list(res)
    if hasattr(res, "pages"):
        return list(getattr(res, "pages"))
    if hasattr(res, "results"):
        return list(getattr(res, "results"))
    return [res]


# ----- Cross-version page helpers -----
def page_content_type(page):
    return getattr(page, "content_type", None) or getattr(page, "mime_type", None)


def page_html_text(page) -> Optional[str]:
    for attr in ["html", "rendered_html", "content_html", "cleaned_html", "content", "text", "markdown", "markdown_v2"]:
        val = getattr(page, attr, None)
        if isinstance(val, bytes):
            try:
                val = val.decode("utf-8", "ignore")
            except Exception:
                continue
        if isinstance(val, str) and val.strip():
            if attr.startswith("markdown"):
                return f"<pre>\n{val}\n</pre>"
            return val
    return None


def page_is_html(page) -> bool:
    if page_html_text(page):
        return True
    ct = page_content_type(page)
    return bool(ct and "html" in ct.lower())


def page_links(page) -> List[str]:
    candidates = []
    for attr in ["out_links", "outgoing_urls", "links", "hrefs", "urls"]:
        v = getattr(page, attr, None)
        if v:
            candidates.extend(list(v))
    html = page_html_text(page)
    if html:
        candidates.extend(re.findall(r'(?:href|src)=["\']([^"\']+)["\']', html))
    out = []
    seen = set()
    for u in candidates:
        if not isinstance(u, str):
            continue
        u = u.strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


async def expand_seeds_via_sitemap(seeds: List[str], allowed_domains: List[str]) -> List[str]:
    """Best-effort: lấy thêm URL từ sitemap.* của mỗi seed root."""
    try_paths = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml", "/sitemap"]

    def ok(u: str) -> bool:
        host = urlparse(u).netloc.lower()
        return any(host == d or host.endswith("." + d) for d in (allowed_domains or []))

    extra: List[str] = []
    async with ClientSession(headers=DEFAULT_HEADERS) as s:
        for root in list(seeds):
            for path in try_paths:
                url = root.rstrip("/") + path
                try:
                    async with s.get(url, timeout=ClientTimeout(total=10)) as resp:
                        if resp.status == 200 and "xml" in (resp.headers.get("Content-Type", "").lower()):
                            xml = await resp.text()
                            extra += re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml)
                except Exception:
                    pass
    extra = [u.strip() for u in extra if ok(u)]
    merged = list(dict.fromkeys(seeds + extra))
    print(f"[DEBUG] seeds after sitemap expansion: {len(merged)} (added {len(merged)-len(seeds)})")
    return merged


# ---------- Main ----------
async def main():
    # 1) Load config
    domains_cfg = load_yaml("crawl_config/domains.yaml")
    crawler_cfg = load_yaml("crawl_config/crawler.yaml")

    seeds: List[str] = domains_cfg["seeds"]
    include_patterns: List[str] = domains_cfg.get("include_patterns", [])
    exclude_patterns: List[str] = domains_cfg.get("exclude_patterns", [])
    allowed_domains: List[str] = domains_cfg.get("allowed_domains", [])
    file_exts: List[str] = domains_cfg.get("file_extensions", [".pdf", ".doc", ".docx"])

    # 2) Ensure folders
    for p in [OUTPUT_HTML, OUTPUT_FILES, OUTPUT_META, LOG_PATH.parent]:
        p.mkdir(parents=True, exist_ok=True)

    # 3) Run config
    run_cfg = make_run_config(crawler_cfg, allowed_domains)

    # 3.1) Optional: expand seeds via sitemap if enabled
    if crawler_cfg.get("follow_sitemaps", True):
        seeds = await expand_seeds_via_sitemap(seeds, allowed_domains)

    # 4) Counters & seen
    fetched_count = 0
    file_count = 0
    html_count = 0
    seen: Set[str] = set()
    if SEEN_PATH.exists():
        try:
            seen = set(json.loads(SEEN_PATH.read_text(encoding="utf-8")))
            print(f"[DEBUG] loaded seen: {len(seen)} urls")
        except Exception:
            pass

    start_ts = datetime.utcnow().isoformat()

    # 5) Crawl
    async with AsyncWebCrawler() as crawler, ClientSession(headers=DEFAULT_HEADERS) as session:
        for seed in seeds:
            pages = []
            try:
                res = await crawler.arun(seed, config=run_cfg)  # Playwright
                pages = normalize_pages(res)
            except Exception as e:
                error_msg = str(e)
                print(f"[FALLBACK] Playwright error at {seed}: {error_msg}")
                # Log the error for debugging
                with open(LOG_PATH, "a", encoding="utf-8") as logf:
                    logf.write(f"[{datetime.utcnow().isoformat()}] PlaywrightError {seed} -> {error_msg}\n")

            # Fallback nếu rỗng hoặc Playwright thất bại
            if not pages:
                print(f"[FALLBACK] Attempting HTTP fallback for {seed}")
                fp = await fetch_html_fallback(session, seed, crawler_cfg.get("timeout_ms", 30000))
                if fp:
                    pages = [fp]
                    print(f"[FALLBACK] HTTP fallback succeeded for {seed} (status={fp.status})")
                else:
                    print(f"[FALLBACK] HTTP fallback also failed for {seed}")

            print(f"[DEBUG] Seed {seed} -> {len(pages)} page(s) returned (after fallback)")

            for page in pages:
                url = getattr(page, "url", None)
                if not url:
                    print("[SKIP] missing url on page object")
                    continue
                if url in seen:
                    print(f"[SKIP] seen {url}")
                    continue

                allowed = is_allowed(url, include_patterns, exclude_patterns)
                if not allowed and allowed_by_domain(url, allowed_domains):
                    allowed = True  # nới theo danh sách domain trắng

                if not allowed:
                    print(f"[SKIP] filtered by patterns: {url}")
                    continue

                seen.add(url)
                fetched_count += 1

                # --------- HTML ----------
                if page_is_html(page):
                    html_text = page_html_text(page) or ""
                    html_path = None
                    if html_text:
                        html_name = safe_filename(url, ".html")
                        html_path = OUTPUT_HTML / html_name
                        html_path.write_text(html_text, encoding="utf-8")
                        html_count += 1

                    out_links = page_links(page)

                    meta = PageMeta(
                        url=url,
                        fetched_at=datetime.utcnow().isoformat(),
                        status=getattr(page, "status", None),
                        content_type=page_content_type(page),
                        html_path=str(html_path) if html_path else None,
                        out_links=out_links,
                        domain=getattr(page, "domain", None),
                    )
                    (OUTPUT_META / (safe_filename(url, ".json"))).write_text(
                        meta.model_dump_json(indent=2), encoding="utf-8"
                    )

                    # download files (absolute-ize relative URLs)
                    to_download = [l for l in out_links if looks_like_file(l, file_exts)]
                    for raw_url in to_download:
                        f_url = urljoin(url, raw_url)
                        content, ctype, status = await download_file(
                            session, f_url, crawler_cfg.get("timeout_ms", 30000)
                        )
                        if not content:
                            with open(LOG_PATH, "a", encoding="utf-8") as logf:
                                logf.write(f"[{datetime.utcnow().isoformat()}] FileError {f_url} -> empty/failed\n")
                            continue
                        ext = pick_ext_by_ct(ctype)
                        f_name = safe_filename(f_url, ext)
                        f_path = OUTPUT_FILES / f_name
                        f_path.write_bytes(content)

                        f_meta = PageMeta(
                            url=f_url,
                            fetched_at=datetime.utcnow().isoformat(),
                            status=status,
                            content_type=ctype,
                            file_path=str(f_path),
                            domain=getattr(page, "domain", None),
                            notes=f"discovered from {url}",
                        )
                        (OUTPUT_META / f"{f_name}.json").write_text(
                            f_meta.model_dump_json(indent=2), encoding="utf-8"
                        )
                        file_count += 1

                # --------- Direct file ----------
                else:
                    content = getattr(page, "content", None) or b""
                    ctype = page_content_type(page)
                    status = getattr(page, "status", None)
                    if not content:
                        content, ctype, status = await download_file(
                            session, url, crawler_cfg.get("timeout_ms", 30000)
                        )
                        if not content:
                            with open(LOG_PATH, "a", encoding="utf-8") as logf:
                                logf.write(f"[{datetime.utcnow().isoformat()}] DirectFileError {url}\n")
                            # vẫn ghi meta để trace
                            meta = PageMeta(
                                url=url,
                                fetched_at=datetime.utcnow().isoformat(),
                                status=status,
                                content_type=ctype,
                                file_path=None,
                                domain=getattr(page, "domain", None),
                                notes="empty binary",
                            )
                            (OUTPUT_META / f"{safe_filename(url, '.json')}").write_text(
                                meta.model_dump_json(indent=2), encoding="utf-8"
                            )
                            continue

                    ext = pick_ext_by_ct(ctype)
                    f_name = safe_filename(url, ext)
                    f_path = OUTPUT_FILES / f_name
                    f_path.write_bytes(content)

                    meta = PageMeta(
                        url=url,
                        fetched_at=datetime.utcnow().isoformat(),
                        status=status,
                        content_type=ctype,
                        file_path=str(f_path),
                        domain=getattr(page, "domain", None),
                    )
                    (OUTPUT_META / f"{f_name}.json").write_text(
                        meta.model_dump_json(indent=2), encoding="utf-8"
                    )
                    file_count += 1

                # Log & persist seen
                with open(LOG_PATH, "a", encoding="utf-8") as logf:
                    logf.write(f"[{datetime.utcnow().isoformat()}] {url} status={getattr(page,'status',None)}\n")
                if fetched_count % 100 == 0:
                    try:
                        SEEN_PATH.write_text(json.dumps(list(seen)), encoding="utf-8")
                    except Exception:
                        pass

    # persist seen at end
    try:
        SEEN_PATH.write_text(json.dumps(list(seen)), encoding="utf-8")
    except Exception:
        pass

    end_ts = datetime.utcnow().isoformat()
    print(f"Start: {start_ts}")
    print(f"End  : {end_ts}")
    print(f"Fetched pages: {fetched_count}, HTML saved: {html_count}, Files saved: {file_count}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
        sys.exit(1)
