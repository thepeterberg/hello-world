#!/usr/bin/env python3
from __future__ import annotations

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


async def stream_track(
    server: RadioServer,
    audio_url: str,
    bitrate: str,
    stop_silence: asyncio.Event | None = None,
) -> bool:
    """Stream a single track through ffmpeg to all connected listeners.

    Args:
        server: The radio server to push audio to.
        audio_url: Direct URL to the audio source.
        bitrate: Target MP3 bitrate.
        stop_silence: If provided, this event is set once the first audio
            chunk arrives, signaling the caller to stop any gap-filling silence.

    Returns True if the track completed naturally, False on error.
    Raises asyncio.CancelledError if skipped.
    """
    proc = await transcode_to_mp3_stream(audio_url, bitrate=bitrate)
    first_chunk = True

    try:
        while True:
            # Check for skip request
            if server.skip_event.is_set():
                server.skip_event.clear()
                logger.info("Track skipped")
                proc.kill()
                await proc.wait()
                raise asyncio.CancelledError("skipped")

            # Read with a short timeout so we can check skip_event periodically
            try:
                chunk = await asyncio.wait_for(proc.stdout.read(8192), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not chunk:
                break

            # Signal that real audio is flowing — stop gap-filling silence
            if first_chunk and stop_silence is not None:
                stop_silence.set()
                first_chunk = False

            await server.push_audio(chunk)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning("Error streaming track: %s", e)
        proc.kill()
        return False

    await proc.wait()
    return proc.returncode == 0


async def resolve_track(track: dict) -> tuple[str, str | None]:
    """Resolve a track dict to a (display_name, audio_url) tuple."""
    track_id = track["track_id"]
    title = track.get("title") or track.get("name") or f"Track {track_id}"
    artist = track.get("artist") or track.get("user", {}).get("username") or "Unknown"
    display = f"{artist} - {title}"
    sc_url = track.get("permalink_url") or track.get("soundcloud_url")

    audio_url = await get_stream_url_from_api(track_id)
    if not audio_url:
        audio_url = await resolve_stream_url(track_id, sc_url)

    # Small delay to avoid SoundCloud rate limiting
    await asyncio.sleep(1)
    return display, audio_url


async def keep_alive_silence(
    server: RadioServer, stop: asyncio.Event, silence_chunk: bytes
) -> None:
    """Push pre-generated silence to keep the stream alive during transitions."""
    while not stop.is_set():
        await server.push_audio(silence_chunk)
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            pass


async def playback_loop(server: RadioServer, config: dict) -> None:
    """Main playback loop: fetch playlists, resolve tracks, stream continuously."""
    bitrate = config["bitrate"]
    shuffle = config["shuffle"]
    playlist_filter = config.get("playlist_filter")
    # Pre-generate silence buffers once at startup — no ffmpeg delay during transitions
    silence = await generate_silence(config["crossfade_seconds"], bitrate)
    keepalive_chunk = await generate_silence(0.5, bitrate)

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

        # Pre-resolve the first track
        next_resolved: tuple[str, str | None] | None = None
        next_resolve_task: asyncio.Task | None = None
        # Silence pump keeps the stream alive during track transitions.
        # It's started after each track ends and stopped when the next
        # track's ffmpeg produces its first audio chunk.
        silence_stop: asyncio.Event | None = None
        silence_task: asyncio.Task | None = None

        for i, track in enumerate(queue):
            # Use pre-resolved result if available, otherwise resolve now
            if next_resolved is not None:
                display, audio_url = next_resolved
                next_resolved = None
            else:
                display, audio_url = await resolve_track(track)

            if not audio_url:
                logger.warning("Skipping unresolvable track: %s", display)
                continue

            # Start pre-resolving the NEXT track in the background
            if i + 1 < len(queue):
                next_track = queue[i + 1]

                async def _resolve_next(t=next_track):
                    return await resolve_track(t)

                next_resolve_task = asyncio.create_task(_resolve_next())

            server.set_now_playing(display)

            # Pass the current silence_stop event to stream_track.
            # When ffmpeg produces its first chunk, it sets this event,
            # which stops the silence pump from the PREVIOUS transition.
            try:
                success = await stream_track(
                    server, audio_url, bitrate, stop_silence=silence_stop
                )
                if not success:
                    logger.warning("Track failed to stream: %s", display)
            except asyncio.CancelledError:
                # Track was skipped
                pass

            # Track ended — immediately start pumping silence so the
            # stream never goes dead. This pump runs until the NEXT
            # track's ffmpeg starts producing audio.
            silence_stop = asyncio.Event()
            if silence_task and not silence_task.done():
                silence_task.cancel()
            silence_task = asyncio.create_task(
                keep_alive_silence(server, silence_stop, keepalive_chunk)
            )

            # Wait for next track resolution if it's still in progress
            if next_resolve_task is not None and not next_resolve_task.done():
                next_resolved = await next_resolve_task
                next_resolve_task = None
            elif next_resolve_task is not None:
                next_resolved = next_resolve_task.result()
                next_resolve_task = None

        # Clean up silence pump at end of playlist
        if silence_task and not silence_task.done():
            silence_stop.set()
            silence_task.cancel()

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
