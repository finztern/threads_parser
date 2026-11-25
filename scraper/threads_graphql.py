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
from scraper.threads_parse import THREADS_BASE_URL, _extract_reply_posts, _loads, _user_replies_from_data

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
