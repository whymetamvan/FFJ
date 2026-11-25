"""
Scraper FF Wiki (freefirejornal.com)
- curl_cffi (impersonate Chrome) + optional WARP proxy (socks5h) untuk bypass Cloudflare
- Ambil semua item dari /api/ffwiki/search (pageSize di-clamp server ke 12)
- Paralel + token cache + resume
- Output: all_skins.json
"""

import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests

# =========================================================
# KONFIGURASI
# =========================================================
BASE_URL = "https://freefirejornal.com"
REFERER = "https://freefirejornal.com/en/add-any-skin-to-your-wishlist-in-free-fire/"

COOKIES = {
    "FreeFireJornal.FFSearch": os.getenv(
        "FFJ_SEARCH",
        "eu4FKe4BIUhPNKvC6SJVKHo_alTnbG9puhlrg0mB3KY",
    ),
    "_ga_5D7HM8TKB8": os.getenv(
        "FFJ_GA1",
        "GS2.1.s1795578616$o1$g0$t1795578616$j60$l0$h0",
    ),
    "_ga": os.getenv("FFJ_GA", "GA1.1.1099034784.1795578617"),
    "ffj_wishlist_owner": os.getenv(
        "FFJ_WISHLIST",
        "T-NBcIbzMmB66I4Z-RzSFplgx8a7SMO6OImLKdbTFh8",
    ),
    "cf_clearance": os.getenv(
        "CF_CLEARANCE",
        "Cg77y3DFXeMuqrNdJylS0CvjUCefsv05ja639evyMEPrROPe4_LlEeamZs_8dPd2uiKvkYDbshGWo9HY7l9Qc90q4eC68bNlwbo-Qd9JKCymOTKInEy7b0z6Bw3qqeXjSOZe9fUpvcBeX8Q7dqW_pXIYgvsd0O1WQAugx_aZaysh9uUew-5tAsy4nOuWkBxdT3y1mDb89ssjoSLVTeNY7Uy_85aFXvSqVi73CnjldYnZ8ftA_melpcXeba3IgstbEG5DtY7JGBBMp-R3xFJRuTZEmNDc9qETF81bs6MMYiP-22uzUkw",
    ),
}

COMMON_HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "origin": BASE_URL,
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": REFERER,
    "sec-ch-ua": '"Chromium";v="158", "Not.A/Brand";v="24", "Google Chrome";v="158"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36",
    "x-ffj-ff-search": "browser",
    "dnt": "1",
}

PAGE_PROOF = os.getenv(
    "PAGE_PROOF",
    "CfDJ8PQDJmHf4yBElLYqnYa4OoWvvjuQ8vfczbFeAnEs59lavhIPsB_zLZYnJSfMOLC4sHAyxc_ZVRhb4x1oIuEdDhWAqZwosxVfaMn5U_QVFJ1fqH3DA0Eg_BvsTFZanxrMdJ_8fehHmdfNy0PFwTnldU77SqtrKtBeleA9Nx2FuU3g0beRc_YpQX4isIQ70bzIShkqjUPtMUGMYl8VA7TWNI0dilX4w_V2eJYYidQzN5HpxHQpeLCb2eks3qBSp1nWrq_k6td9k8hfQgTODLtyBVKhdILGwNmUCnDTZDbXjgufd6wbQM8fzjyuDz2T7i4lhaUyjiJHgcZl0euSPvViSFYwrBXXbHjw5wznkISz7pts"
)

PAGE_SIZE      = 12
MAX_WORKERS    = int(os.getenv("MAX_WORKERS", "12"))
TOKEN_LIFETIME = float(os.getenv("TOKEN_LIFETIME", "12"))
TOKEN_BATCH    = int(os.getenv("TOKEN_BATCH", "80"))
OUTPUT_FILE    = os.getenv("OUTPUT_FILE", "all_skins.json")
RESUME_FILE    = os.getenv("RESUME_FILE", "progress.json")
SAVE_EVERY     = 50
MAX_RETRY      = 5
IMPERSONATE    = os.getenv("IMPERSONATE", "chrome124")

# WARP proxy (opsional). Format: socks5h://127.0.0.1:40000
PROXY_URL = os.getenv("PROXY_URL", "").strip()


# =========================================================
# SESSION
# =========================================================
def make_session() -> requests.Session:
    kwargs = {"impersonate": IMPERSONATE}
    if PROXY_URL:
        kwargs["proxies"] = {"http": PROXY_URL, "https": PROXY_URL}
        print(f"[SESSION] curl_cffi impersonate={IMPERSONATE} proxy={PROXY_URL}")
    else:
        print(f"[SESSION] curl_cffi impersonate={IMPERSONATE} (tanpa proxy)")
    s = requests.Session(**kwargs)
    return s


# =========================================================
# TOKEN MANAGER
# =========================================================
class TokenManager:
    def __init__(self, session: requests.Session):
        self.session = session
        self._token = None
        self._ts = 0.0
        self._uses = 0
        self._lock = threading.Lock()

    def get(self, force: bool = False) -> str:
        with self._lock:
            now = time.time()
            need = (
                force
                or self._token is None
                or (now - self._ts) > TOKEN_LIFETIME
                or self._uses >= TOKEN_BATCH
            )
            if need:
                url = f"{BASE_URL}/api/ffwiki/search/token"
                headers = {**COMMON_HEADERS, "content-type": "application/json"}
                r = self.session.post(
                    url, headers=headers, cookies=COOKIES,
                    json={"pageProof": PAGE_PROOF}, timeout=30,
                )
                r.raise_for_status()
                d = r.json()
                if not d.get("ok") or not d.get("token"):
                    raise RuntimeError(f"token fail: {d}")
                self._token = d["token"]
                self._ts = now
                self._uses = 0
            self._uses += 1
            return self._token


# =========================================================
# FETCH 1 HALAMAN
# =========================================================
def fetch_page(session: requests.Session, tm: TokenManager,
               page: int, page_size: int = PAGE_SIZE) -> dict:
    url = f"{BASE_URL}/api/ffwiki/search"
    params = {
        "q": "", "type": "", "subtype": "", "version": "",
        "order": "popular", "page": page,
        "pageSize": page_size, "lang": "en",
    }
    last_err = None
    for attempt in range(MAX_RETRY):
        token = tm.get(force=(attempt > 0))
        headers = {**COMMON_HEADERS, "x-ffj-search-token": token}
        try:
            r = session.get(
                url, headers=headers, cookies=COOKIES,
                params=params, timeout=30,
            )
            if r.status_code == 429:
                last_err = "429"
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code in (401, 403):
                last_err = str(r.status_code)
                time.sleep(1.0 * (attempt + 1))
                continue
            r.raise_for_status()
            d = r.json()
            if not d.get("ok"):
                last_err = "ok=False"
                continue
            return d
        except Exception as e:
            last_err = str(e)
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"page {page} gagal: {last_err}")


# =========================================================
# FETCH SEMUA (paralel + resume)
# =========================================================
def fetch_all(session: requests.Session, tm: TokenManager) -> dict:
    done_pages = {}
    meta = None
    if os.path.exists(RESUME_FILE):
        try:
            with open(RESUME_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            done_pages = saved.get("pages", {})
            meta = saved.get("meta")
            print(f"[RESUME] {len(done_pages)} halaman tersimpan, lanjut...")
        except Exception as e:
            print(f"[RESUME] gagal baca progress, mulai baru: {e}")
            done_pages = {}

    if not meta or "1" not in done_pages:
        first = fetch_page(session, tm, 1)
        d = first.get("data", {})
        meta = {
            "page": d.get("page"), "pages": d.get("pages"),
            "total": d.get("total"), "facets": d.get("facets"),
        }
        done_pages["1"] = d.get("items", [])
        print(f"[INFO] total={meta['total']}  pages={meta['pages']}  pageSize={PAGE_SIZE}")

    total_pages = meta["pages"] or 1

    all_items = []
    seen_ids = set()

    def push(items):
        for it in items:
            iid = it.get("itemId")
            if iid is not None and iid not in seen_ids:
                seen_ids.add(iid)
                all_items.append(it)

    for p_str in sorted(done_pages.keys(), key=lambda x: int(x)):
        push(done_pages[p_str])

    todo = [p for p in range(1, total_pages + 1) if str(p) not in done_pages]
    print(f"[INFO] sisa {len(todo)} halaman (worker={MAX_WORKERS})")

    if not todo:
        return {"ok": True, "meta": meta, "count": len(all_items), "items": all_items}

    lock = threading.Lock()
    done_count = len(done_pages)
    t0 = time.time()

    def worker(p: int):
        try:
            data = fetch_page(session, tm, p)
            return p, data["data"].get("items", [])
        except Exception as e:
            return p, f"ERR: {e}"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(worker, p): p for p in todo}
        for fut in as_completed(futures):
            p, items = fut.result()
            with lock:
                if isinstance(items, list):
                    done_pages[str(p)] = items
                    push(items)
                    done_count += 1
                else:
                    print(f"  [SKIP] page {p}: {items}")

                if done_count % 10 == 0 or done_count == total_pages:
                    elapsed = time.time() - t0
                    speed = done_count / elapsed if elapsed > 0 else 0
                    eta = (total_pages - done_count) / speed if speed > 0 else 0
                    print(
                        f"[{done_count}/{total_pages}] unik={len(all_items)} "
                        f"| {speed:.1f} hal/s | ETA {eta:.0f}s"
                    )

                if done_count % SAVE_EVERY == 0:
                    with open(RESUME_FILE, "w", encoding="utf-8") as f:
                        json.dump(
                            {"pages": done_pages, "meta": meta},
                            f, ensure_ascii=False,
                        )

    with open(RESUME_FILE, "w", encoding="utf-8") as f:
        json.dump({"pages": done_pages, "meta": meta}, f, ensure_ascii=False)

    return {"ok": True, "meta": meta, "count": len(all_items), "items": all_items}


# =========================================================
# MAIN
# =========================================================
def main():
    session = make_session()
    tm = TokenManager(session)
    t0 = time.time()
    try:
        result = fetch_all(session, tm)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
        print(
            f"\n✓ Selesai dalam {time.time()-t0:.1f}s — "
            f"{result['count']} item unik → {OUTPUT_FILE}"
        )
    except KeyboardInterrupt:
        print("\n⏸  Dihentikan. Jalankan lagi untuk resume.")
        raise
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
