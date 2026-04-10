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
    decode_to_pcm,
    resolve_stream_url,
    start_master_encoder,
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


async def feed_track_to_encoder(
    server: RadioServer,
    encoder: asyncio.subprocess.Process,
    audio_url: str,
    silence_stop: asyncio.Event | None = None,
) -> bool:
    """Decode a track to PCM and feed it into the master encoder's stdin.

    Args:
        silence_stop: If set, signals the silence feeder to stop once
            the first audio chunk from this track arrives.

    Returns True if the track completed naturally.
    Raises asyncio.CancelledError if skipped.
    """
    decoder = await decode_to_pcm(audio_url)
    first_chunk = True

    try:
        while True:
            if server.skip_event.is_set():
                server.skip_event.clear()
                logger.info("Track skipped")
                decoder.kill()
                raise asyncio.CancelledError("skipped")

            try:
                chunk = await asyncio.wait_for(decoder.stdout.read(8192), timeout=0.25)
            except asyncio.TimeoutError:
                continue
            if not chunk:
                break

            # Stop the silence feeder once real audio arrives
            if first_chunk and silence_stop is not None:
                silence_stop.set()
                first_chunk = False

            # Feed raw PCM into the master encoder
            encoder.stdin.write(chunk)
            await encoder.stdin.drain()
    except asyncio.CancelledError:
        decoder.kill()
        raise
    except Exception as e:
        logger.warning("Error feeding track: %s", e)
        decoder.kill()
        return False

    return True


async def feed_silence_loop(
    encoder: asyncio.subprocess.Process,
    stop: asyncio.Event,
    sample_rate: int = 44100,
) -> None:
    """Continuously feed realtime-paced silence PCM to the encoder.

    This keeps the MP3 stream alive when no track is being decoded.
    Feeds 0.5s of silence at a time, paced at roughly realtime.
    """
    # 0.5 seconds of silence at a time
    chunk_duration = 0.5
    silence = b"\x00" * int(2 * 2 * sample_rate * chunk_duration)
    while not stop.is_set():
        try:
            encoder.stdin.write(silence)
            await encoder.stdin.drain()
        except Exception:
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=chunk_duration)
        except asyncio.TimeoutError:
            pass


async def resolve_track(track: dict) -> tuple[str, str | None, str | None]:
    """Resolve a track dict to a (display_name, audio_url, soundcloud_url) tuple."""
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
    return display, audio_url, sc_url


async def encoder_output_loop(
    server: RadioServer, encoder: asyncio.subprocess.Process
) -> None:
    """Read MP3 data from the master encoder and push to all listeners.

    This runs as a background task for the lifetime of the encoder.
    """
    try:
        while True:
            chunk = await encoder.stdout.read(8192)
            if not chunk:
                break
            await server.push_audio(chunk)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Encoder output loop error: %s", e)


async def playback_loop(server: RadioServer, config: dict) -> None:
    """Main playback loop: fetch playlists, resolve tracks, stream continuously."""
    bitrate = config["bitrate"]
    shuffle = config["shuffle"]
    playlist_filter = config.get("playlist_filter")

    # Start the master MP3 encoder — one continuous stream, no EOF markers
    encoder = await start_master_encoder(bitrate=bitrate)
    output_task = asyncio.create_task(encoder_output_loop(server, encoder))
    logger.info("Master encoder running — continuous MP3 stream active")

    try:
      while True:
        # Check if a channel change was requested via the web UI
        if server.channel_change_event.is_set():
            server.channel_change_event.clear()
            playlist_filter = server.pending_channel
            channel_name = playlist_filter or "All"
            server.set_current_channel(channel_name)
            logger.info("Switched to channel: %s", channel_name)

        # Fetch fresh playlist data each cycle
        logger.info("Fetching Poolsuite playlists...")
        try:
            playlists = await fetch_playlists()
        except Exception as e:
            logger.error("Failed to fetch playlists: %s", e)
            logger.info("Retrying in 30 seconds...")
            # Feed silence while waiting (realtime paced)
            wait_stop = asyncio.Event()
            wait_task = asyncio.create_task(feed_silence_loop(encoder, wait_stop))
            await asyncio.sleep(30)
            wait_stop.set()
            wait_task.cancel()
            continue

        # Populate available channel names for the web UI
        channel_names = []
        for pl in playlists:
            name = pl.get("name") or pl.get("title") or ""
            if name:
                channel_names.append(name)
        server.set_available_channels(channel_names)
        server.set_current_channel(playlist_filter or "All")

        tracks = extract_tracks(playlists, playlist_filter)
        if not tracks:
            logger.error("No tracks found. Retrying in 30 seconds...")
            wait_stop = asyncio.Event()
            wait_task = asyncio.create_task(feed_silence_loop(encoder, wait_stop))
            await asyncio.sleep(30)
            wait_stop.set()
            wait_task.cancel()
            continue

        queue = build_queue(tracks, shuffle=shuffle)
        logger.info("Starting playback of %d tracks", len(queue))

        # Pre-resolve the first track
        next_resolved: tuple[str, str | None] | None = None
        next_resolve_task: asyncio.Task | None = None
        # Track history for "previous" support
        history: list[dict] = []
        # Silence feeder keeps the encoder fed between tracks
        silence_feeder_stop: asyncio.Event | None = None
        silence_feeder_task: asyncio.Task | None = None

        i = 0
        while i < len(queue):
            track = queue[i]

            # Check for channel change — break out to re-fetch with new filter
            if server.channel_change_event.is_set():
                logger.info("Channel change — reloading playlist")
                break

            # Check for "previous" request
            if server.prev_event.is_set():
                server.prev_event.clear()
                if len(history) >= 2:
                    history.pop()
                    prev_track = history.pop()
                    queue.insert(i, prev_track)
                    next_resolved = None
                    logger.info("Going back to previous track")
                    continue
                else:
                    logger.info("No previous track available")

            # Use pre-resolved result if available, otherwise resolve now
            if next_resolved is not None:
                display, audio_url, sc_url = next_resolved
                next_resolved = None
            else:
                display, audio_url, sc_url = await resolve_track(track)

            if not audio_url:
                logger.warning("Skipping unresolvable track: %s", display)
                i += 1
                continue

            # Start pre-resolving the NEXT track in the background
            if i + 1 < len(queue):
                next_track = queue[i + 1]

                async def _resolve_next(t=next_track):
                    return await resolve_track(t)

                next_resolve_task = asyncio.create_task(_resolve_next())

            server.set_now_playing(display, soundcloud_url=sc_url)
            history.append(track)

            # Feed the track to the encoder. Pass silence_feeder_stop so
            # the silence feeder is stopped once real audio arrives.
            try:
                success = await feed_track_to_encoder(
                    server, encoder, audio_url,
                    silence_stop=silence_feeder_stop,
                )
                if not success:
                    logger.warning("Track failed to stream: %s", display)
            except asyncio.CancelledError:
                pass

            # Track ended — start feeding realtime-paced silence to keep
            # the encoder producing output while we resolve the next track
            silence_feeder_stop = asyncio.Event()
            if silence_feeder_task and not silence_feeder_task.done():
                silence_feeder_task.cancel()
            silence_feeder_task = asyncio.create_task(
                feed_silence_loop(encoder, silence_feeder_stop)
            )

            # Wait for next track resolution if it's still in progress
            if next_resolve_task is not None and not next_resolve_task.done():
                next_resolved = await next_resolve_task
                next_resolve_task = None
            elif next_resolve_task is not None:
                next_resolved = next_resolve_task.result()
                next_resolve_task = None

            i += 1

        # Clean up silence feeder at end of playlist
        if silence_feeder_task and not silence_feeder_task.done():
            silence_feeder_stop.set()
            silence_feeder_task.cancel()

        logger.info("Playlist complete, reshuffling...")
    finally:
        output_task.cancel()
        encoder.kill()


async def main(config: dict) -> None:
    server = RadioServer(host=config["host"], port=config["port"])
    runner = await server.start()

    # Print connection info
    local_ip = server.local_ip
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
