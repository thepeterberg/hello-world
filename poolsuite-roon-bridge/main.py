#!/usr/bin/env python3
"""Poolsuite → Roon Bridge

Streams curated Poolsuite FM tracks as a local internet radio station
that Roon can consume via its Live Radio feature.

Usage:
    python main.py [--config config.json] [--port 8489] [--no-shuffle]
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

from audio_pipeline import (
    check_dependencies,
    generate_silence,
    resolve_stream_url,
    transcode_to_mp3_stream,
)
from poolsuite_client import (
    build_queue,
    extract_tracks,
    fetch_playlists,
    get_stream_url_from_api,
)
from stream_server import RadioServer

logger = logging.getLogger("poolsuite-roon")

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8489,
    "bitrate": "192k",
    "format": "mp3",
    "crossfade_seconds": 2,
    "shuffle": True,
    "poolsuite_api": "https://api.poolsidefm.workers.dev",
    "playlist_filter": None,
}


def load_config(path: str | None) -> dict:
    config = dict(DEFAULT_CONFIG)
    if path and Path(path).exists():
        with open(path) as f:
            config.update(json.load(f))
        logger.info("Loaded config from %s", path)
    return config


async def stream_track(server: RadioServer, audio_url: str, bitrate: str) -> bool:
    """Stream a single track through ffmpeg to all connected listeners.

    Returns True if the track was streamed successfully.
    """
    proc = await transcode_to_mp3_stream(audio_url, bitrate=bitrate)

    try:
        while True:
            chunk = await proc.stdout.read(8192)
            if not chunk:
                break
            await server.push_audio(chunk)
    except Exception as e:
        logger.warning("Error streaming track: %s", e)
        proc.kill()
        return False

    await proc.wait()
    return proc.returncode == 0


async def playback_loop(server: RadioServer, config: dict) -> None:
    """Main playback loop: fetch playlists, resolve tracks, stream continuously."""
    bitrate = config["bitrate"]
    shuffle = config["shuffle"]
    playlist_filter = config.get("playlist_filter")
    silence = await generate_silence(config["crossfade_seconds"], bitrate)

    while True:
        # Fetch fresh playlist data each cycle
        logger.info("Fetching Poolsuite playlists...")
        try:
            playlists = await fetch_playlists()
        except Exception as e:
            logger.error("Failed to fetch playlists: %s", e)
            logger.info("Retrying in 30 seconds...")
            await asyncio.sleep(30)
            continue

        tracks = extract_tracks(playlists, playlist_filter)
        if not tracks:
            logger.error("No tracks found. Retrying in 30 seconds...")
            await asyncio.sleep(30)
            continue

        queue = build_queue(tracks, shuffle=shuffle)
        logger.info("Starting playback of %d tracks", len(queue))

        for track in queue:
            track_id = track["track_id"]
            title = track.get("title") or track.get("name") or f"Track {track_id}"
            artist = track.get("artist") or track.get("user", {}).get("username") or "Unknown"
            display = f"{artist} - {title}"
            sc_url = track.get("permalink_url") or track.get("soundcloud_url")

            # Try Poolsuite's own stream API first, fall back to yt-dlp
            audio_url = await get_stream_url_from_api(track_id)
            if not audio_url:
                audio_url = await resolve_stream_url(track_id, sc_url)

            if not audio_url:
                logger.warning("Skipping unresolvable track: %s (%s)", display, track_id)
                continue

            server.set_now_playing(display)

            success = await stream_track(server, audio_url, bitrate)
            if success and silence:
                await server.push_audio(silence)

            if not success:
                logger.warning("Track failed to stream: %s", display)

        logger.info("Playlist complete, reshuffling...")


async def main(config: dict) -> None:
    server = RadioServer(host=config["host"], port=config["port"])
    runner = await server.start()

    # Print connection info
    local_ip = config["host"]
    if local_ip == "0.0.0.0":
        local_ip = "YOUR_LOCAL_IP"
    port = config["port"]

    print()
    print("=" * 60)
    print("  Poolsuite -> Roon Bridge")
    print("=" * 60)
    print()
    print(f"  Stream URL:  http://{local_ip}:{port}/stream")
    print(f"  Status:      http://{local_ip}:{port}/status")
    print(f"  Web UI:      http://{local_ip}:{port}/")
    print()
    print("  Add the stream URL as a Live Radio station in Roon:")
    print("    Roon > My Live Radio > + > paste the stream URL")
    print()
    print("=" * 60)
    print()

    # Handle graceful shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down...", sig)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    # Run playback loop until stopped
    playback_task = asyncio.create_task(playback_loop(server, config))

    await stop_event.wait()

    playback_task.cancel()
    try:
        await playback_task
    except asyncio.CancelledError:
        pass
    await server.stop(runner)
    print("\nGoodbye!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poolsuite -> Roon Bridge")
    parser.add_argument("--config", "-c", help="Path to config JSON file")
    parser.add_argument("--port", "-p", type=int, help="HTTP server port")
    parser.add_argument("--host", help="HTTP server bind address")
    parser.add_argument("--no-shuffle", action="store_true", help="Play tracks in order")
    parser.add_argument(
        "--playlist", help="Filter to a specific Poolsuite playlist by name"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Check dependencies before doing anything
    try:
        check_dependencies()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    config = load_config(args.config)
    if args.port:
        config["port"] = args.port
    if args.host:
        config["host"] = args.host
    if args.no_shuffle:
        config["shuffle"] = False
    if args.playlist:
        config["playlist_filter"] = args.playlist

    asyncio.run(main(config))
