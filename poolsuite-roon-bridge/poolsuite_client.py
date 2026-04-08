from __future__ import annotations

"""Client for the Poolsuite (formerly Poolside FM) API.

Fetches curated playlists and track metadata from the Poolsuite
Cloudflare Workers API, which wraps SoundCloud.
"""

import asyncio
import logging
import random

import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://api.poolsidefm.workers.dev"
PLAYLISTS_ENDPOINT = f"{API_BASE}/v1/get_tracks_by_playlist"
STREAM_ENDPOINT = f"{API_BASE}/v2/get_sc_mp3_stream"


async def fetch_playlists() -> list[dict]:
    """Fetch all Poolsuite playlists and their tracks.

    Returns a list of playlist dicts, each containing a list of tracks
    with SoundCloud metadata (track_id, title, artist, artwork, etc.).
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(PLAYLISTS_ENDPOINT)
        resp.raise_for_status()
        data = resp.json()

    # The API wraps playlists in a response envelope:
    # { "status_code": 200, "summary_message": "...", "payload": [...] }
    if isinstance(data, dict):
        # Try known wrapper keys in order of likelihood
        for key in ("payload", "playlists", "data", "results"):
            if key in data and isinstance(data[key], list):
                playlists = data[key]
                break
        else:
            playlists = [data]
    elif isinstance(data, list):
        playlists = data
    else:
        playlists = []

    logger.debug("Raw API response keys: %s", list(data.keys()) if isinstance(data, dict) else type(data))

    logger.info("Fetched %d playlists from Poolsuite API", len(playlists))
    return playlists


def extract_tracks(playlists: list[dict], playlist_filter: str | None = None) -> list[dict]:
    """Flatten playlists into a single track list.

    Args:
        playlists: Raw playlist data from the API.
        playlist_filter: Optional substring to match playlist names.
            If None, all playlists are included.

    Returns:
        List of track dicts with at minimum a 'track_id' field.
    """
    tracks = []
    for playlist in playlists:
        name = playlist.get("name") or playlist.get("title") or ""
        # Apply filter if specified
        if playlist_filter and playlist_filter.lower() not in name.lower():
            continue

        # The API uses "tracks_in_order" for the track list
        playlist_tracks = (
            playlist.get("tracks_in_order")
            or playlist.get("tracks")
            or playlist.get("songs")
            or []
        )
        for track in playlist_tracks:
            # The API uses "soundcloud_id" as the track identifier
            tid = (
                track.get("soundcloud_id")
                or track.get("track_id")
                or track.get("id")
                or track.get("sc_id")
            )
            if tid:
                track["track_id"] = str(tid)
                track["_playlist"] = name
                tracks.append(track)

        logger.info("Playlist %r: %d tracks", name, len(playlist_tracks))

    logger.info("Total tracks extracted: %d", len(tracks))
    return tracks


def build_queue(tracks: list[dict], shuffle: bool = True) -> list[dict]:
    """Build a playback queue from extracted tracks.

    Args:
        tracks: Flat list of track dicts.
        shuffle: Whether to randomize the order.

    Returns:
        A new list (copy) of tracks in playback order.
    """
    queue = list(tracks)
    if shuffle:
        random.shuffle(queue)
    return queue


async def get_stream_url_from_api(track_id: str, retries: int = 3) -> str | None:
    """Try to get a direct MP3 stream URL from Poolsuite's own API.

    This calls the /v2/get_sc_mp3_stream endpoint which may return
    a direct audio URL or redirect to one. Retries on rate limiting.
    """
    url = f"{STREAM_ENDPOINT}?track_id={track_id}"
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for attempt in range(retries):
            try:
                resp = await client.get(url)

                # Handle rate limiting with backoff
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning("Rate limited on track %s, waiting %ds...", track_id, wait)
                    await asyncio.sleep(wait)
                    continue

                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")
                    # If the response is audio, the URL itself is the stream
                    if "audio" in content_type or "mpeg" in content_type:
                        return str(resp.url)
                    # If JSON, extract the URL
                    try:
                        data = resp.json()
                        return data.get("url") or data.get("stream_url")
                    except Exception:
                        # Might be a direct redirect to audio
                        return str(resp.url)
            except Exception as e:
                logger.warning("Poolsuite stream API failed for track %s: %s", track_id, e)
    return None
