"""Anonymous GraphQL access to the replies of a post (BarcelonaPostPageDirectRepliesRefetchQuery).

Measured (scripts/threads_graphql_probe*.py, no account): with sortOrder=RECENT the newest replies of a post come
back in ~12-16 KB per comment (the post page gives ~10-14 ranked replies for ~1 MB and misses the newest ones of
big threads). The request needs the `lsd` token of a Threads page loaded in the SAME session (cookies included).
Everything here is a pure function (picklable, usable from the parse worker); no secrets, the flags are public.
"""
from __future__ import annotations

import base64
import json
import re
from typing import Any
from urllib.parse import urlencode

from scraper.pipeline import RawThreadsPost
from scraper.threads_parse import THREADS_BASE_URL, _extract_reply_posts, _loads, _search_posts_from_data, _user_replies_from_data

GRAPHQL_URL = f"{THREADS_BASE_URL}/api/graphql"
BOOT_URL = f"{THREADS_BASE_URL}/t/news"  # ~280 KB page that carries an lsd token (the post page is ~1 MB)
QUERY_NAME = "BarcelonaPostPageDirectRepliesRefetchQuery"
DOC_ID = "29347154008211656"
APP_ID = "238260118697367"  # public id of the Threads web app (x-ig-app-id)
_LSD_RE = re.compile(r'\["LSD",\[\],\{"token":"([^"]+)"')

# Feature flags the web client sends with this query (copied from a HAR of the web client; not secret).
RELAY_FLAGS: dict[str, bool] = {
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


def find_lsd(html: str) -> str | None:
    m = _LSD_RE.search(html)
    return m.group(1) if m else None


def build_request(lsd: str, page_url: str, post_pk: str, first: int, after: str | None, sort: str = "RECENT") -> tuple[str, dict[str, str]]:
    """(form body, headers) of one replies query."""
    variables = {"after": after, "filterType": None, "first": int(first), "postID": str(post_pk), "sortOrder": sort, **RELAY_FLAGS}
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
    return urlencode(body), headers


def parse_graphql_replies(text: str, post_pk: str) -> tuple[bool, list[RawThreadsPost], str | None, bool]:
    """(ok, replies, next_cursor, has_next). ok=False: not a replies answer (error JSON, expired lsd, deleted post...)."""
    if text.startswith("for (;;);"):
        text = text[9:]
    try:
        data: Any = _loads(text)
        info = data["data"]["media"]["text_post_app_info"]["direct_replies"]["page_info"]
    except (ValueError, KeyError, TypeError):
        return False, [], None, False
    posts = _extract_reply_posts(data, str(post_pk))
    return True, posts, info.get("end_cursor"), bool(info.get("has_next_page"))


# --- "Replies" tab of a profile: BarcelonaProfileRepliesTabRefetchableDirectQuery (logged-out web client) ---
TAB_URL = f"{THREADS_BASE_URL}/graphql/query"
TAB_QUERY_NAME = "BarcelonaProfileRepliesTabRefetchableDirectQuery"
TAB_DOC_ID = "29222313014046534"
TAB_RELAY_FLAGS: dict[str, bool] = {
    "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldFetchPostAuthorFullNamerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasLocalTrendLabelInFeedrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasDearAlgoConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasMetaAiContentAttachmentsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMWRConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaMessagesHasLiveChatMessagingrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasEventBadgerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasAMAConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaMessagingHasMetaAIBotrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMembersOnlyReplyConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaGenAIRepliesEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaQuotedPostParentCaptionEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsSearchDiscoveryEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunitiesrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGameScoreSharerelayprovider": True,
    "__relay_internal__pv__BarcelonaHasInsightsMetricShareCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMiddlemistConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasPublicViewCountCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityEmojiUpdateCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityEntityCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunityIntroCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunityChampionCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityFounderCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasScorecardCommunityrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasSportTeamAllegianceCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasStitchedAttachmentrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasNewspaperLinkStylerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMessagingrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasProfileUpdatePostrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasLiveChatHostlessUIrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasThreadsFriendsApplicationrelayprovider": True,
    "__relay_internal__pv__BarcelonaOptionalCookiesEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaShouldFulfillLightboxQueryrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasUnseenRepliesrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasWebFaviconsrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsCrawlerrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasDearAlgoWebProductionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunityTopContributorsrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsDesktoprelayprovider": True,
    "__relay_internal__pv__BarcelonaHasTopLevelSaveButtonrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasUFINotInterestedrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCustomLikesConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasViewerRepliedrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPrivateRepliesDeprecationrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasGhostPostEmojiActivationrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasGhostPostProductionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasKeyboardShortcutsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasSaveCountrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasNewsletterLinkPreviewrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsLoggedOutShareTestEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasAdditionalLikeCountrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPodcastTranscriptConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPodcastV2Consumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPublicViewCountShareCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldShowFediverseM075Featuresrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsInternalUserrelayprovider": False,
}


def build_tab_request(lsd: str, page_url: str, user_id: str, first: int) -> tuple[str, dict[str, str]]:
    """(form body, headers) of one Replies-tab query. Logged out, only the first page exists (no pagination)."""
    variables = {
        "after": None,
        "allow_page_info_for_lox_user": False,
        "before": None,
        "first": int(first),
        "last": None,
        "userID": str(user_id),
        **TAB_RELAY_FLAGS,
    }
    body = {
        "av": "0",
        "__user": "0",
        "__a": "1",
        "__req": "a",
        "__comet_req": "122",
        "lsd": lsd,
        "__crn": "comet.barcelonawebloggedout.BarcelonaProfileRepliesColumnRoute",
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": TAB_QUERY_NAME,
        "variables": json.dumps(variables, separators=(",", ":")),
        "server_timestamps": "true",
        "doc_id": TAB_DOC_ID,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": THREADS_BASE_URL,
        "Referer": page_url,
        "X-FB-LSD": lsd,
        "X-FB-Friendly-Name": TAB_QUERY_NAME,
        "X-IG-App-ID": APP_ID,
        "X-ASBD-ID": "359341",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    return urlencode(body), headers


def parse_graphql_tab(text: str, username: str) -> tuple[bool, list[RawThreadsPost]]:
    """(ok, replies written by @username). ok=False: not a tab answer (error JSON, expired lsd...)."""
    if text.startswith("for (;;);"):
        text = text[9:]
    try:
        data: Any = _loads(text)
        edges = data["data"]["mediaData"]["edges"]
    except (ValueError, KeyError, TypeError):
        return False, []
    if not isinstance(edges, list):
        return False, []
    return True, _user_replies_from_data(data, username)


# --- Search, newest first: BarcelonaSearchResultsQuery with recent=1 (works logged out, one page per word) ---
SEARCH_QUERY_NAME = "BarcelonaSearchResultsQuery"
SEARCH_DOC_ID = "29794646893463541"
SEARCH_SESSION = "7q6xvs:7omza1:xoscma"
# Opaque bootstrap blobs of the same web-client build (public, copied from a HAR; the query is answered
# "execution error" without them).
SEARCH_BUILD_FIELDS: dict[str, str] = {
    "__dyn": "7xe6E5q5U5ObwKBAg5S1Dxu13w8CewSwMwNw9G2S0lW4o0B-q1ew6ywaq0yE462mcw5Mx62G3i0Bo7O2l0Fwqo31w9O1lwlE-U2zxe2GewbS361qw8Xxm16wa-0raazo7u0zE2ZwrUdUbGw4mwr86C2q6oe84J6wWxibwzwHxW5Ec8yu4Q2-q1pxW1VwkUy1xwu81P82jwRwaSi3u5o",
    "__csr": "gB7YQgBvL8iyfOrnqAztpLO9qaJWhHUBryyf-zhWnAWjzF4-A4Fp6jJdbKqbRht121uwRnm6A1Uw-G78_wyw07dhN03GjCAtwRl0ce581moK0jgM4d7hQ10hVFokt5e269wC81tAhtzk0Bo6aeG6BCt022za6A7e1kyZ0KwTzQ335G6A9KfOwNzEcogmp9y9ayEgxTyLm0sa0gR2yyAE188FeQGxG2pwLJ6X811psEB3EG2e083z982mw2dK0HOaAHaWwSguqz6q0ia5Eyew9e9g1Xvc7oun4mdx0SHCmbzUN3G50YyZwGx5Axe2nei0lubADBpUvDadwei5-4qwoFF9AuqRxZwT4a9x11-0wE6gE9h2xF1Qt60Yy-0ny1kwCDB5xSdDc3q26362V0Wwiykmbh8DsU3wDyECezGKbyqkd05Uw1Ut0yEF877DpmC4960IVo19oV2u8UeA3y79p99o2fg88vw6y80SU07t20UU0m0g6-7Qt7gS1Jc2x1SoPzo1Zz0hFo0vXw0xlw",
    "__hsdp": "h4feg7xc0_WcEVzb2o90p5qHEJ4TRp2qWDUIgb80CO15A7xgE9c2o48NaEMMqoz6d3bxcEgwcN2kC7yk1Fz43y1uyEZ2A699Z0nUuXHc5oV7ofUf8gyoGEkxiaCx66o-8xl0Sz8jwKxzwGwfudwbi3V110lA4U10d2UG3-cg8oaE42EqwVwuE5e1TCF1m48iwkEco423i6E985mqp2r82qaxEOHS8iK9y5dhE4O78PwsA7oCrAwUw_x10r8Sqrxa4OwhE4u1dwIg88",
    "__hblp": "014a33x63y488EeE11EKJ1-E4K7E8E8UbU5q3S0E9E2Yxi0g906cw9e3-3C1Zw_wEwZwiE6y3m2613yEqxmm2eUoyoc821xq0gu49oWu9UsCwzw4HwgE4i3m9w8q1-xS6o5y16wuA",
    "__sjsp": "h4feg7xM3_aCEVzb2o2rUIg0Gy15A51AEa09gtiG",
}


def build_search_request(lsd: str, word: str, session_id: str) -> tuple[str, dict[str, str]]:
    """(form body, headers) of one newest-first search for `word`."""
    variables = {
        "meta_place_id": None,
        "power_search_info": None,
        "query": word,
        "recent": 1,
        "requested_subtab": None,
        "search_surface": None,
        "serp_session_id": session_id,
        "tagID": None,
        "trend_fbid": None,
    }
    body = {
        "av": "0",
        "__user": "0",
        "__a": "1",
        "__req": "1",
        # Build fields of the web client (copied from a HAR, public): the minimal body is answered "execution error".
        "__hs": "20782.HCSV2:barcelona_logged_out_pkg.2.1...0",
        "dpr": "1",
        "__ccg": "GOOD",
        "__rev": "1056222758",
        "__s": SEARCH_SESSION,
        "__hsi": "7712110477617572567",
        "__comet_req": "122",
        "lsd": lsd,
        "jazoest": "2" + str(sum(map(ord, lsd))),
        "__spin_r": "1056222758",
        "__spin_b": "trunk",
        "__spin_t": "1795615646",
        **SEARCH_BUILD_FIELDS,
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": SEARCH_QUERY_NAME,
        "variables": json.dumps(variables, separators=(",", ":")),
        "server_timestamps": "true",
        "doc_id": SEARCH_DOC_ID,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": THREADS_BASE_URL,
        "Referer": f"{THREADS_BASE_URL}/search?q={word}&serp_type=default",
        "X-FB-LSD": lsd,
        "X-FB-Friendly-Name": SEARCH_QUERY_NAME,
        "X-IG-App-ID": APP_ID,
        "X-ASBD-ID": "359341",
        "X-Logged-Out-Threads-Migrated-Request": "true",
        "X-Root-Field-Name": "xdt_api__v1__text_feed__search_results__connection_v2",
        "X-Bloks-Version-ID": "c879256a176052393d623a59d49c533cfe09564f34a3415ca7ad096d6067a378",
        "X-Web-Session-ID": SEARCH_SESSION,
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    return urlencode(body), headers


def parse_graphql_search(text: str, word: str) -> tuple[bool, list[RawThreadsPost]]:
    """(ok, posts). ok=False: not a search answer (error JSON, rate limit...)."""
    if text.startswith("for (;;);"):
        text = text[9:]
    try:
        data: Any = _loads(text)
        edges = data["data"]["searchResults"]["edges"]
    except (ValueError, KeyError, TypeError):
        return False, []
    if not isinstance(edges, list):
        return False, []
    return True, _search_posts_from_data(data)
