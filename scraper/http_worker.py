"""HTTP mode runner: no browser, no accounts. Every cycle fetches all
targets through the rotating proxy pool with bounded concurrency and hands
the posts/replies to the shared dispatch (max_age filter, SeenCache dedup,
TokenBucket, batch POST to store_items).
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

from scraper.config import ConfigStore
from scraper.constants import BASE_DIR, LOGGER
from scraper.discovery import TargetRegistry
from scraper.budget import AutoBudget
from scraper.hotposts import HotPosts
from scraper.http_client import ReplyTracker, ThreadsHttpClient, make_url_check_poster
from scraper.proxies import ProxyPool
from scraper.shard import make_owner
from scraper.ipv6_pool import Ipv6Pool
from scraper.tor_pool import TorPool
from scraper.vpn import VpnPool, load_nodes, make_vpn_probe, make_vpn_url_check_poster
from scraper.state import SeenCache, TokenBucket
from scraper.pipeline import configure_url_check, set_url_check_poster
from scraper.worker import AGE_LABELS, dispatch_posts


def _resolve(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else BASE_DIR / path


def load_targets(cfg: dict) -> list[str]:
    """Usernames from config.yaml `targets` (type: profile) plus the optional
    `targets_file` (one username per line, '#' comments). Re-read every cycle.
    """
    names: list[str] = []
    for target in cfg.get("targets", []) or []:
        if target.get("type") == "profile" and target.get("value"):
            names.append(str(target["value"]).lstrip("@"))

    targets_file = cfg.get("targets_file")
    if targets_file:
        path = _resolve(targets_file)
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    names.append(line.lstrip("@"))

    return list(dict.fromkeys(names))  # dedupe, keep order


async def http_runner(
    config_store: ConfigStore,
    token_bucket: TokenBucket,
    seen_cache: SeenCache,
    stop_event: asyncio.Event,
) -> None:
    cfg = config_store.get()
    tor_instances = int(os.getenv("TOR_INSTANCES", "0") or 0)
    ipv6_prefixes = os.getenv("IPV6_PREFIX", "").strip()
    try:
        if ipv6_prefixes:
            # IPv6 mode: no proxies, a random address of your own /64 per request (see ipv6_pool.py).
            pool = Ipv6Pool(ipv6_prefixes)
            tor_instances = 0
        elif tor_instances > 0:
            # Tor mode: proxies.txt is not used; every request gets its own circuit (see tor_pool.py).
            pool = TorPool(os.getenv("TOR_HOST", "tor"), int(os.getenv("TOR_BASE_PORT", "9050")), tor_instances,
                           circuit_reuse=int(cfg.get("tor_circuit_reuse", 1)))
        else:
            pool = ProxyPool.from_file(
                _resolve(cfg.get("proxies_file", "proxies.txt")),
                cooldown_seconds=cfg.get("proxy_cooldown_seconds", 120),
            )
    except (FileNotFoundError, ValueError) as exc:
        LOGGER.error("cannot start http mode: %s", exc)
        return

    if isinstance(pool, Ipv6Pool):
        http_timeout = cfg.get("http_timeout_seconds", 15)
        http_attempts = cfg.get("http_attempts", 3)
    elif tor_instances > 0:
        LOGGER.info("http mode: Tor, %d instances (new circuit per request)", len(pool))
        # Tor is slower than a proxy and many exits are blocked: longer timeout, more retries.
        http_timeout = max(float(cfg.get("http_timeout_seconds", 15)), float(cfg.get("tor_http_timeout_seconds", 30)))
        http_attempts = max(int(cfg.get("http_attempts", 3)), int(cfg.get("tor_http_attempts", 5)))
    else:
        LOGGER.info("http mode: %d proxies in pool", len(pool))
        http_timeout = cfg.get("http_timeout_seconds", 15)
        http_attempts = cfg.get("http_attempts", 3)
    client = ThreadsHttpClient(
        pool,
        timeout=http_timeout,
        attempts=http_attempts,
        parse_workers=cfg.get("parse_workers"),
        error_cooldown=cfg.get("proxy_error_cooldown_seconds", 600),
        concurrency=int(cfg.get("http_concurrency", 30)),
    )
    tracker = ReplyTracker()

    owner = make_owner(os.getenv("PARSER_SHARD", ""))
    if owner is not None:
        LOGGER.info("shard %s: this instance owns only its part of the usernames", os.getenv("PARSER_SHARD"))
    disc = cfg.get("discovery", {}) or {}
    discovery_on = bool(disc.get("enabled", True))
    registry = TargetRegistry(
        _resolve(disc.get("state_file", "data/targets_state.json")) if discovery_on else None,
        min_interval=disc.get("min_interval_seconds", 300),
        max_interval=disc.get("max_interval_seconds", 21600),
        seed_interval=cfg.get("poll_interval_seconds", 30),
        max_known=disc.get("max_known_targets", 200_000),
        owns=owner,
    )
    if discovery_on:
        registry.load()

    # Hot posts: post pages are opened by expected yield, not via the profile that lists them.
    hot: HotPosts | None = None
    if cfg.get("hot_posts", True):
        hot = HotPosts(
            _resolve(cfg.get("hot_state_file", "data/hot_posts.json")),
            max_age=float(cfg.get("reply_post_max_age_seconds", 7 * 86400)),
            min_growth=int(cfg.get("reply_min_growth", 4)),
            base_interval=float(cfg.get("hot_base_interval_seconds", 300)),
            min_interval=float(cfg.get("hot_min_interval_seconds", 90)),
            max_misses=int(cfg.get("hot_max_misses", 5)),
            max_posts=int(cfg.get("hot_max_posts", 100_000)),
            owns=owner,
            fresh_min_interval=float(cfg.get("hot_fresh_min_interval_seconds", 45)),
        )
        hot.load()

    # Auto budget: tunes profile pages vs hot post pages and the repeat interval from measured yield.
    budget: AutoBudget | None = None
    if hot is not None and cfg.get("auto_budget", True):
        budget = AutoBudget(
            _resolve(cfg.get("budget_state_file", "data/budget.json")),
            profiles=int((cfg.get("discovery", {}) or {}).get("max_targets_per_cycle", 1500)),
            hot=int(cfg.get("hot_posts_per_cycle", 2000)),
            base_interval=float(cfg.get("hot_base_interval_seconds", 300)),
        )
        budget.load()

    # One client for url-check and store_items. Defaults (20 keepalive connections, 5 s expiry, no
    # retries) force a new TLS handshake for most of ~100 parallel dispatches and fail under bursts.
    # retries=2 only repeats failed CONNECTs, so nothing is ever sent twice.
    store_transport = httpx.AsyncHTTPTransport(
        retries=2,
        limits=httpx.Limits(max_connections=128, max_keepalive_connections=128, keepalive_expiry=10.0),
    )
    # url-check: VPN nodes from vpn_nodes.txt (fastest healthy one, automatic replacement) when the file
    # has nodes; otherwise through the proxy pool (fresh proxy + connection per request).
    vpn = await _start_vpn(cfg)
    if vpn is not None:
        set_url_check_poster(make_vpn_url_check_poster(vpn))
    else:
        # The url-check API may be IPv4-only: in IPv6 mode it is always called directly.
        via_pool = cfg.get("url_check_via_proxy", True) and not isinstance(pool, Ipv6Pool)
        set_url_check_poster(make_url_check_poster(pool) if via_pool else None)
    loop_lag = {"max": 0.0}
    tune: dict = {"conc": None, "cfg_conc": None, "prev_conc": None, "prev_rps": 0.0, "hold": 0}
    lag_task = asyncio.create_task(_lag_monitor(loop_lag))
    # Continuous flow: a cycle does not wait for its last slow requests. Counters are shared by all
    # cycles (cleared at every cycle start), so late jobs still count into the cycle that is running.
    totals: Counter[str] = Counter()
    timing: Counter[str] = Counter()
    live: set[asyncio.Task] = set()  # all jobs still running, from this and earlier cycles
    inflight_targets: set[str] = set()  # profiles being fetched right now (never started twice)
    inflight_hot: set[str] = set()  # post pks being fetched right now

    def spawn(coro, bag: set[str], key: str) -> None:
        async def guarded() -> None:
            try:
                await coro
            finally:
                bag.discard(key)

        bag.add(key)
        task = asyncio.create_task(guarded())
        live.add(task)

        def done(t: asyncio.Task) -> None:
            live.discard(t)
            if not t.cancelled():
                t.exception()  # mark retrieved; jobs log their own errors

        task.add_done_callback(done)

    async with httpx.AsyncClient(transport=store_transport) as store_client:
        try:
            while not stop_event.is_set():
                cycle_start = time.monotonic()
                cfg = config_store.get()
                disc = cfg.get("discovery", {}) or {}
                max_per_cycle = budget.profiles if budget else int(disc.get("max_targets_per_cycle", 400))
                if budget is not None and hot is not None:
                    hot.set_barren_interval(budget.base_interval)
                max_age = cfg.get("max_age_seconds", 86400)

                seeds = load_targets(cfg)
                if not seeds and not registry.stats()["known"]:
                    LOGGER.error("no targets (config.yaml `targets` / targets_file) - waiting")
                    await _sleep(stop_event, 30)
                    continue
                registry.sync_seeds(seeds)

                # Skip what earlier cycles are still fetching.
                batch = [u for u in registry.due(max_per_cycle, float(disc.get("explore_ratio", 0.3))) if u not in inflight_targets]
                hot_batch = (
                    [h for h in hot.due(budget.hot if budget else int(cfg.get("hot_posts_per_cycle", 2000))) if h[2] not in inflight_hot]
                    if hot is not None
                    else []
                )
                if not batch and not hot_batch:
                    if live:
                        await asyncio.wait(live, timeout=2.0)
                    else:
                        await _sleep(stop_event, min(cfg.get("poll_interval_seconds", 30), registry.seconds_until_next_due() + 0.5))
                    continue

                # http_concurrency limits HTTP requests (inside the client); targets in flight
                # get a little headroom so request slots stay busy between a target's two fetch rounds.
                # No requests are in flight between cycles, so the limit can be swapped safely.
                cfg_conc = max(1, int(cfg.get("http_concurrency", 30)))
                autotune = bool(cfg.get("autotune", True))
                if tune["conc"] is None or tune["cfg_conc"] != cfg_conc:  # first cycle, or edited in config.yaml
                    tune.update(conc=cfg_conc, cfg_conc=cfg_conc, prev_conc=None, hold=0)
                concurrency = tune["conc"] if autotune else cfg_conc
                client.set_concurrency(concurrency)
                target_slots = max(concurrency, int(concurrency * float(cfg.get("target_concurrency_factor", 1.5))))
                sem = asyncio.Semaphore(target_slots)
                # Many tiny concurrent POSTs (url-check + store_items) starve the httpx pool and the
                # event loop, so dispatch runs through its own small limit.
                dispatch_sem = asyncio.Semaphore(max(1, int(cfg.get("dispatch_concurrency", 64))))
                configure_url_check(
                    cfg.get("url_check_batch_size", 300),
                    cfg.get("url_check_min_batch", 30),
                    cfg.get("url_check_max_wait_seconds", 2.0),
                )
                fetch_replies = cfg.get("fetch_replies", True)
                reply_max_age = cfg.get("reply_post_max_age_seconds", 7 * 86400)
                reply_min_growth = max(1, int(cfg.get("reply_min_growth", 1)))
                totals.clear()
                stats_before = Counter(client.stats)
                client.error_kinds.clear()
                # Per-cycle timing (seconds summed over targets) to see where the time goes.
                timing.clear()

                async def job(username: str) -> None:
                    posts: list = []
                    # The target slot covers only the network part. Dispatch (url-check + POST) runs
                    # after the slot is released, so slow sends never starve fetching.
                    async with sem:
                        if stop_event.is_set():
                            return
                        try:
                            t_fetch = time.monotonic()
                            # With hot posts on, reply pages are fetched by hot_job, not here.
                            label, posts, reply_pages = await client.fetch_target(
                                username, tracker, reply_max_age, fetch_replies and hot is None, reply_min_growth
                            )
                            if hot is not None and posts:
                                hot.observe(posts)
                            timing["fetch"] += time.monotonic() - t_fetch
                            timing["targets"] += 1
                            totals[f"profile_{label}"] += 1
                            totals["reply_pages"] += reply_pages

                            now = datetime.now(timezone.utc)
                            own_fresh = sum(
                                1
                                for p in posts
                                if p.username.lower() == username.lower()
                                and (now - p.created_at.astimezone(timezone.utc)).total_seconds() <= max_age
                            )
                            registry.report(username, label, own_fresh)
                            if discovery_on and posts:
                                totals["discovered"] += registry.observe({p.username for p in posts})
                        except Exception:
                            totals["profile_error"] += 1
                            registry.report(username, "failed", 0)
                            LOGGER.exception("target @%s failed", username)
                            return

                    if not posts:
                        return
                    try:
                        t_wait = time.monotonic()
                        async with dispatch_sem:
                            timing["dispatch_wait"] += time.monotonic() - t_wait
                            t_run = time.monotonic()
                            result = await dispatch_posts(posts, cfg, store_client, token_bucket, seen_cache)
                            totals.update(result)
                            # Items new to Exorde (rate-limited ones were new too) = this target's yield.
                            registry.report_yield(username, result["to_send"] + result["rate_limited"])
                            totals["profile_new"] += result["to_send"] + result["rate_limited"]
                            timing["dispatch_run"] += time.monotonic() - t_run
                            timing["dispatches"] += 1
                    except Exception:
                        totals["dispatch_error"] += 1
                        LOGGER.exception("dispatch for @%s failed", username)

                async def hot_job(username: str, code: str, pk: str) -> None:
                    async with sem:
                        if stop_event.is_set():
                            return
                        try:
                            label, replies = await client.fetch_post_replies(username, code, pk)
                        except Exception:
                            totals["hot_error"] += 1
                            hot.retry_later(pk)
                            LOGGER.exception("hot post %s failed", code)
                            return
                    totals["reply_pages"] += 1
                    totals["hot_pages"] += 1
                    first = hot.is_first_fetch(pk)
                    if label == "not_found":
                        hot.drop(pk)
                        return
                    if label == "failed":
                        totals["hot_failed"] += 1
                        hot.retry_later(pk)
                        return
                    new = 0
                    if replies:
                        if discovery_on:
                            # Reply authors are the main source of new profiles (hot mode never saw them).
                            totals["discovered"] += registry.observe({r.username for r in replies})
                        try:
                            async with dispatch_sem:
                                result = await dispatch_posts(replies, cfg, store_client, token_bucket, seen_cache)
                            totals.update(result)
                            new = result["to_send"] + result["rate_limited"]
                            registry.report_yield(username, new)
                        except Exception:
                            totals["dispatch_error"] += 1
                            hot.retry_later(pk)
                            LOGGER.exception("dispatch for hot post %s failed", code)
                            return
                    totals["hot_new"] += new
                    kind = "first" if first else "again"
                    totals[f"hot_{kind}_pages"] += 1
                    totals[f"hot_{kind}_new"] += new
                    hot.report(pk, new)

                # Hot posts go first: the request semaphore serves them before the profile fetches.
                for u, c, k in hot_batch:
                    spawn(hot_job(u, c, k), inflight_hot, k)
                for u in batch:
                    spawn(job(u), inflight_targets, u)
                # Next cycle starts as soon as the queue is drained and only a slow tail (at most
                # cycle_overlap_ratio of the slots) is still in flight; 0 = wait for everything.
                tail = int(target_slots * max(0.0, float(cfg.get("cycle_overlap_ratio", 0.5))))
                while len(live) > tail and not stop_event.is_set():
                    await asyncio.wait(set(live), return_when=asyncio.FIRST_COMPLETED)
                if hot is not None:
                    hot.expire()
                    hot.save()
                if discovery_on:
                    registry.save()

                delta = {k: client.stats[k] - stats_before[k] for k in client.stats}
                rs = registry.stats()
                LOGGER.info(
                    "http cycle: scraped=%d ok=%d empty=%d not_found=%d failed=%d | requests=%d "
                    "blocked=%d transport_err=%d(timeout=%d) proxies_ok=%d/%d reply_pages=%d | known=%d (+%d new) "
                    "active=%d dead=%d due_now=%d | fetched=%d fresh=%d to_send=%d sent=%d "
                    "duplicates=%d remote_dup=%d unconfirmed=%d rate_limited=%d | %.0fs (%.1f sent/s, %.1f req/s)",
                    len(batch),
                    totals["profile_ok"],
                    totals["profile_empty"],
                    totals["profile_not_found"],
                    totals["profile_failed"] + totals["profile_error"],
                    delta.get("requests", 0),
                    delta.get("blocked", 0),
                    delta.get("transport_errors", 0),
                    delta.get("timeouts", 0),
                    pool.available(),
                    len(pool),
                    totals["reply_pages"],
                    rs["known"],
                    totals["discovered"],
                    rs["active"],
                    rs["dead"],
                    rs["due"],
                    totals["fetched"],
                    totals["fresh"],
                    totals["to_send"],
                    totals["sent"],
                    totals["duplicates"],
                    totals["remote_duplicates"],
                    totals["unconfirmed"],
                    totals["rate_limited"],
                    time.monotonic() - cycle_start,
                    totals["sent"] / max(time.monotonic() - cycle_start, 0.001),
                    delta.get("requests", 0) / max(time.monotonic() - cycle_start, 0.001),
                )
                sent_top = totals["sent"] - totals["sent_replies"]
                fresh_top = totals["fresh"] - totals["fresh_replies"]
                LOGGER.info(
                    "yield: top-level fresh=%d new=%d (%.0f%%) from ~%d profile pages = %.2f new/page | "
                    "replies fresh=%d new=%d (%.0f%%) from %d reply pages = %.2f new/page | url_unverified=%d too_short=%d non_text=%d",
                    fresh_top,
                    sent_top,
                    100.0 * sent_top / max(fresh_top, 1),
                    len(batch),
                    sent_top / max(len(batch), 1),
                    totals["fresh_replies"],
                    totals["sent_replies"],
                    100.0 * totals["sent_replies"] / max(totals["fresh_replies"], 1),
                    totals["reply_pages"],
                    totals["sent_replies"] / max(totals["reply_pages"], 1),
                    totals["url_unverified"],
                    totals["too_short"],
                    totals["non_text"],
                )
                age_parts = []
                for i, label in enumerate(AGE_LABELS):
                    new_n, known_n = totals[f"age_new_{i}"], totals[f"age_known_{i}"]
                    if new_n + known_n:
                        age_parts.append(f"{label}: {new_n}/{known_n} ({100.0 * new_n / (new_n + known_n):.0f}% new)")
                if age_parts:
                    LOGGER.info("item age (new/known to Exorde): %s", " | ".join(age_parts))
                if hot is not None:
                    LOGGER.info(
                        "hot posts: pages=%d new=%d (%.2f new/page) failed=%d | first opens=%d -> %.2f new/page, "
                        "repeat opens=%d -> %.2f new/page | tracked=%d due_now=%d",
                        totals["hot_pages"],
                        totals["hot_new"],
                        totals["hot_new"] / max(totals["hot_pages"], 1),
                        totals["hot_failed"],
                        totals["hot_first_pages"],
                        totals["hot_first_new"] / max(totals["hot_first_pages"], 1),
                        totals["hot_again_pages"],
                        totals["hot_again_new"] / max(totals["hot_again_pages"], 1),
                        len(hot),
                        hot.count_due(),
                    )
                LOGGER.info("event loop lag: max=%.2fs this cycle", loop_lag["max"])
                loop_lag["max"] = 0.0
                LOGGER.info(
                    "timing per target: fetch=%.1fs dispatch_wait=%.1fs dispatch_run=%.1fs (dispatch_concurrency=%d)",
                    timing["fetch"] / max(timing["targets"], 1),
                    timing["dispatch_wait"] / max(timing["dispatches"], 1),
                    timing["dispatch_run"] / max(timing["dispatches"], 1),
                    int(cfg.get("dispatch_concurrency", 64)),
                )
                if budget is not None:
                    message = budget.update(
                        len(batch),
                        totals["profile_new"],
                        totals["hot_first_new"],
                        totals["hot_again_pages"],
                        totals["hot_again_new"],
                    )
                    if message:
                        LOGGER.info("auto budget: %s", message)
                if autotune:
                    cycle_seconds = max(time.monotonic() - cycle_start, 0.001)
                    message = _autotune(
                        tune,
                        lo=int(cfg.get("http_concurrency_min", 30)),
                        hi=int(cfg.get("http_concurrency_max", 110)),
                        rps=delta.get("requests", 0) / cycle_seconds,
                        block_ratio=0.0 if pool.rotating else delta.get("blocked", 0) / max(delta.get("requests", 0), 1),
                        ok_ratio=pool.available() / max(len(pool), 1),
                    )
                    if message:
                        LOGGER.info("autotune: %s", message)
                if client.error_kinds:
                    LOGGER.info(
                        "transport errors by kind: %s",
                        " | ".join(f"{n}x {kind}" for kind, n in client.error_kinds.most_common(4)),
                    )

                # More targets already due -> go on right away; otherwise wait a bit.
                if registry.count_due() == 0:
                    base = cfg.get("poll_interval_seconds", 30)
                    jitter = base * cfg.get("poll_jitter_ratio", 0.25) * (2 * random.random() - 1)
                    await _sleep(stop_event, max(0.0, base + jitter - (time.monotonic() - cycle_start)))
        finally:
            for task in list(live):
                task.cancel()
            if live:
                await asyncio.gather(*live, return_exceptions=True)
            lag_task.cancel()
            set_url_check_poster(None)
            if vpn is not None:
                await vpn.close()
            client.close()
            if discovery_on:
                registry.save(force=True)
            if hot is not None:
                hot.save(force=True)


async def _start_vpn(cfg: dict) -> VpnPool | None:
    """Start Xray for the nodes in vpn_nodes.txt. None = no nodes / not usable (caller falls back)."""
    nodes = load_nodes(_resolve(cfg.get("vpn_nodes_file", "vpn/nodes.txt")))
    endpoint = cfg.get("url_check_endpoint")
    if not nodes or not endpoint:
        return None
    vpn = VpnPool(
        nodes,
        base_port=int(cfg.get("vpn_base_port", 10800)),
        probe=make_vpn_probe(endpoint, float(cfg.get("vpn_probe_timeout_seconds", 8))),
        rebench_seconds=float(cfg.get("vpn_rebench_seconds", 600)),
    )
    try:
        await vpn.start()
        vpn.acquire()  # at least one node must answer the url-check
    except Exception as exc:
        LOGGER.error("vpn for url-check unavailable (%s), falling back to the proxy pool", exc)
        await vpn.close()
        return None
    return vpn


def _autotune(tune: dict, lo: int, hi: int, rps: float, block_ratio: float, ok_ratio: float) -> str | None:
    """Hill-climb the request concurrency once per cycle. Mutates `tune`, returns a log message or None.

    - proxies getting banned (many blocks / few healthy proxies) -> back off hard
    - a raise that did not add >=5% req/s -> we hit a ceiling (CPU, link, provider): go back, hold
    - healthy and not at the ceiling -> try +15%
    """
    conc = tune["conc"]
    if block_ratio > 0.03 or ok_ratio < 0.80:
        tune.update(conc=max(lo, int(conc * 0.7)), prev_conc=None, hold=6)
        return f"backing off ({conc} -> {tune['conc']}): blocked={block_ratio:.1%} proxies_ok={ok_ratio:.0%}"
    if tune["prev_conc"] is not None:
        prev_conc, prev_rps = tune["prev_conc"], tune["prev_rps"]
        tune["prev_conc"] = None
        if rps < prev_rps * 1.05:
            tune.update(conc=prev_conc, hold=5)
            return f"ceiling reached: {conc} slots gave {rps:.0f} req/s vs {prev_rps:.0f} at {prev_conc}; back to {prev_conc}"
    if tune["hold"] > 0:
        tune["hold"] -= 1
        return None
    if block_ratio < 0.01 and ok_ratio > 0.92 and conc < hi:
        tune.update(prev_conc=conc, prev_rps=rps, conc=min(hi, int(conc * 1.15) + 1))
        return f"raising {conc} -> {tune['conc']} (blocked={block_ratio:.1%} proxies_ok={ok_ratio:.0%}, {rps:.0f} req/s)"
    return None


async def _lag_monitor(lag: dict[str, float]) -> None:
    """Measures how late a 0.5 s sleep wakes up: a blocked event loop shows as lag (and as connect timeouts)."""
    while True:
        started = time.monotonic()
        await asyncio.sleep(0.5)
        lag["max"] = max(lag["max"], time.monotonic() - started - 0.5)


async def _sleep(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
