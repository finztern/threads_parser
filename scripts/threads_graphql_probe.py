"""Can the replies of a post be paged through Threads GraphQL WITHOUT an account?

Research helper (nothing here is used by the scraper). Based on a HAR of the web client:
  query  BarcelonaPostPageDirectRepliesRefetchQuery  (doc_id 29347154008211656)
  needs  postID (decoded from the shortcode), the reply cursor from the post page HTML,
         the `lsd` token from the same HTML, x-fb-lsd / x-ig-app-id headers.

Usage (inside the running container, one proxy, a handful of requests):
  docker compose exec parser python3 scripts/threads_graphql_probe.py https://www.threads.com/@USER/post/CODE
Pick a post with many replies (50+). Env: PROBE_PROXY, otherwise the first line of proxies.txt.

Prints, for every experiment, HTTP status, replies per request, has_next_page and the age range
of the replies. Raw answers are saved to debug/ (git-ignored). No cookies or tokens are printed.
"""
from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from curl_cffi import requests as cffi  # noqa: E402

from scraper.constants import BASE_DIR  # noqa: E402
from scraper.threads_parse import THREADS_BASE_URL, _extract_ssr_reply_posts  # noqa: E402

GRAPHQL_URL = f"{THREADS_BASE_URL}/api/graphql"
QUERY_NAME = "BarcelonaPostPageDirectRepliesRefetchQuery"
DOC_ID = "29347154008211656"
APP_ID = "238260118697367"  # public id of the Threads web app (x-ig-app-id)
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

# Feature flags the web client sends with this query (copied from the HAR; not secret).
RELAY_FLAGS = {
    "__relay_internal__pv__BarcelonaCanSeeSponsoredContentrelayprovider": False,
    "__relay_internal__pv__BarcelonaGenAIRepliesEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasBestOfThreadsrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunitiesrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityChampionCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityEmojiUpdateCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityEntityCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityIntroCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunityTopContributorsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCustomLikesConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasDearAlgoConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasDearAlgoWebProductionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasEventBadgerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasGameScoreSharerelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGhostPostEmojiActivationrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGhostPostProductionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasKeyboardShortcutsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMWRLocationConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMembersOnlyReplyConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasMessagingrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasMetaAiContentAttachmentsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasNewsletterLinkPreviewrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasNewspaperLinkStylerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasPermalinkIndentationrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasPodcastTranscriptConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPodcastV2Consumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPrivateRepliesDeprecationrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasProfileUpdatePostrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPublicViewCountCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPublicViewCountShareCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasScorecardCommunityrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasSportTeamAllegianceCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasStitchedAttachmentrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasTopLevelSaveButtonrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasUnseenRepliesrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasViewerRepliedrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasWebFaviconsrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsCrawlerrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsInternalUserrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider": False,  # anonymous: we have no account
    "__relay_internal__pv__BarcelonaIsLoggedOutShareTestEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsSearchDiscoveryEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaMessagesHasLiveChatMessagingrelayprovider": False,
    "__relay_internal__pv__BarcelonaMessagingHasMetaAIBotrelayprovider": False,
    "__relay_internal__pv__BarcelonaOptionalCookiesEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaPostMediaReuseEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldFetchPostAuthorFullNamerelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldFulfillLightboxQueryrelayprovider": True,
    "__relay_internal__pv__BarcelonaShouldShowFediverseM075Featuresrelayprovider": True,
}


# ---- pure helpers (checked offline against the HAR) --------------------------------------------
def shortcode_to_pk(code: str) -> int:
    """Post shortcode (.../post/CODE) -> numeric post id, base64-like with the Instagram alphabet."""
    value = 0
    for ch in code:
        value = value * 64 + _ALPHABET.index(ch)
    return value


def find_lsd(html: str) -> str | None:
    m = re.search(r'\["LSD",\[\],\{"token":"([^"]+)"', html)
    return m.group(1) if m else None


def find_reply_cursor(html: str) -> str | None:
    """The `end_cursor` of the direct-replies connection (it decodes to {"t":"downward_other_replies",..})."""
    for cursor in re.findall(r'"end_cursor":"([^"]+)"', html):
        try:
            data = json.loads(base64.b64decode(cursor + "=" * (-len(cursor) % 4)))
        except (ValueError, UnicodeDecodeError):
            continue
        if isinstance(data, dict) and data.get("t") == "downward_other_replies":
            return cursor
    return None


def _count_posts(obj, times: list[int]) -> int:
    n = 0
    if isinstance(obj, dict):
        if "pk" in obj and "code" in obj and "text_post_app_info" in obj:
            n += 1
            if isinstance(obj.get("taken_at"), int):
                times.append(obj["taken_at"])
        for value in obj.values():
            n += _count_posts(value, times)
    elif isinstance(obj, list):
        for value in obj:
            n += _count_posts(value, times)
    return n


def summarize(text: str) -> dict:
    """Short description of a GraphQL answer (no content of the replies)."""
    try:
        data = json.loads(re.sub(r"^for \(;;\);", "", text))
    except ValueError:
        return {"ok": False, "why": "not JSON: " + text[:120].replace("\n", " ")}
    body = data.get("data") if isinstance(data, dict) else None
    conn = None
    if isinstance(body, dict):
        conn = ((body.get("media") or {}).get("text_post_app_info") or {}).get("direct_replies")
    if not conn:
        why = data.get("errors") or data.get("error") or data.get("errorSummary") or list(data)[:6]
        return {"ok": False, "why": str(why)[:200]}
    times: list[int] = []
    n_posts = _count_posts(conn.get("edges", []), times)
    page_info = conn.get("page_info") or {}
    return {
        "ok": True,
        "threads": len(conn.get("edges", [])),
        "comments": n_posts,
        "has_next": page_info.get("has_next_page"),
        "next_cursor": page_info.get("end_cursor"),
        "newest": max(times) if times else None,
        "oldest": min(times) if times else None,
    }


def _age(ts: int | None) -> str:
    if not ts:
        return "-"
    return f"{(time.time() - ts) / 3600:.1f}h ago"


# ---- the experiments ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) < 2 or not re.search(r"/@[^/]+/post/[^/?#]+", sys.argv[1]):
        print(__doc__)
        sys.exit(1)
    page_url = sys.argv[1].split("?")[0]
    code = re.search(r"/post/([^/?#]+)", page_url).group(1)

    proxy = os.environ.get("PROBE_PROXY")
    if not proxy:
        path = BASE_DIR / "proxies.txt"
        lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines()] if path.is_file() else []
        proxy = next((ln for ln in lines if ln and not ln.startswith("#")), None)
    if not proxy:
        print("no proxy: set PROBE_PROXY or fill proxies.txt")
        sys.exit(1)
    if "://" not in proxy:
        proxy = "http://" + proxy

    out = BASE_DIR / "debug"
    out.mkdir(parents=True, exist_ok=True)
    session = cffi.Session(impersonate="chrome124", proxies={"http": proxy, "https": proxy}, timeout=30)
    session.headers.update({"Accept-Language": "en-US,en;q=0.9"})

    t0 = time.time()
    page = session.get(page_url)
    html = page.text
    lsd, cursor, pk = find_lsd(html), find_reply_cursor(html), shortcode_to_pk(code)
    ssr = _extract_ssr_reply_posts(html, str(pk))
    print(f"post page: http={page.status_code} bytes={len(html)} time={time.time() - t0:.1f}s")
    print(f"  postID={pk} lsd={'found' if lsd else 'MISSING'} reply_cursor={'found' if cursor else 'MISSING'} "
          f"ssr_replies={len(ssr)} (what the scraper gets today)")
    if not lsd:
        print("no lsd token in the page: cannot continue (login wall or a changed page)")
        sys.exit(2)

    def call(label: str, variables: dict, with_jazoest: bool = False) -> dict:
        body = {
            "lsd": lsd,
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": QUERY_NAME,
            "variables": json.dumps(variables, separators=(",", ":")),
            "server_timestamps": "true",
            "doc_id": DOC_ID,
            "__a": "1",
            "__user": "0",
            "__comet_req": "29",
            "__crn": "comet.threads.BarcelonaPostColumnRoute",
        }
        if with_jazoest:
            body["jazoest"] = "2" + str(sum(map(ord, lsd)))
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": THREADS_BASE_URL,
            "Referer": page_url,
            "X-FB-LSD": lsd,
            "X-FB-Friendly-Name": QUERY_NAME,
            "X-IG-App-ID": APP_ID,
            "X-ASBD-ID": "359341",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        csrf = session.cookies.get("csrftoken")
        if csrf:
            headers["X-CSRFToken"] = csrf
        started = time.time()
        r = session.post(GRAPHQL_URL, data=urlencode(body), headers=headers)
        info = summarize(r.text)
        (out / f"threads_graphql_{label.replace(' ', '_').replace(',', '').replace('=', '')}.json").write_text(
            r.text, encoding="utf-8"
        )
        if info["ok"]:
            print(f"  [{label}] http={r.status_code} OK comments={info['comments']} in {info['threads']} threads "
                  f"has_next={info['has_next']} newest={_age(info['newest'])} oldest={_age(info['oldest'])} "
                  f"bytes={len(r.text)} {time.time() - started:.1f}s")
        else:
            print(f"  [{label}] http={r.status_code} FAILED: {info['why']}")
        time.sleep(1.0)
        return info

    def variables(after, first=4, sort="TOP") -> dict:
        return {"after": after, "filterType": None, "first": first, "postID": str(pk), "sortOrder": sort, **RELAY_FLAGS}

    print("\nexperiments (anonymous, one proxy):")
    base = None
    if cursor:
        base = call("A cursor first=4 TOP", variables(cursor))
        if not base["ok"]:
            base = call("A2 same with jazoest", variables(cursor), with_jazoest=True)
    else:
        print("  (no reply cursor in the page: the post has too few replies for a second page)")
    if base and base["ok"]:
        call("B cursor first=20 TOP", variables(cursor, first=20))
        if base["next_cursor"]:
            call("C next page first=4 TOP", variables(base["next_cursor"]))
    call("D no cursor first=10 TOP", variables(None, first=10))
    call("E no cursor first=10 RECENT", variables(None, first=10, sort="RECENT"))
    print("\nraw answers are in debug/threads_graphql_*.json (replies are public content, no tokens inside).")


if __name__ == "__main__":
    main()
