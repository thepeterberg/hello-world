from __future__ import annotations

"""Audio pipeline: resolve SoundCloud tracks and transcode to a continuous MP3 stream.

Uses yt-dlp to resolve SoundCloud URLs to direct audio streams,
and ffmpeg to transcode into a constant-bitrate MP3 stream suitable
for internet radio consumption by Roon.
"""

import asyncio
import logging
import shutil

logger = logging.getLogger(__name__)

SOUNDCLOUD_TRACK_URL = "https://soundcloud.com/track/{track_id}"
# yt-dlp can resolve SoundCloud tracks by API URL too
SOUNDCLOUD_API_V2_URL = "https://api-v2.soundcloud.com/tracks/{track_id}"


def check_dependencies() -> None:
    """Verify that yt-dlp and ffmpeg are installed."""
    for cmd in ("yt-dlp", "ffmpeg"):
        if not shutil.which(cmd):
            raise RuntimeError(
                f"'{cmd}' not found in PATH. Install it first:\n"
                f"  yt-dlp: pip install yt-dlp\n"
                f"  ffmpeg: apt install ffmpeg / brew install ffmpeg"
            )


async def resolve_stream_url(track_id: str, soundcloud_url: str | None = None) -> str | None:
    """Use yt-dlp to resolve a SoundCloud track to a direct audio URL.

    Args:
        track_id: The SoundCloud track ID.
        soundcloud_url: Optional direct SoundCloud URL (e.g. https://soundcloud.com/artist/track).
            If not provided, we construct one from the track_id.

    Returns:
        Direct audio stream URL, or None if resolution fails.
    """
    # Prefer a full SoundCloud URL if we have one; otherwise use the API URL
    # which yt-dlp may also support
    target = soundcloud_url or f"https://api.soundcloud.com/tracks/{track_id}"

    cmd = [
        "yt-dlp",
        "--no-download",
        "--print", "urls",
        "-f", "bestaudio",
        "--no-playlist",
        "--no-warnings",
        target,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)

        if proc.returncode == 0 and stdout:
            url = stdout.decode().strip().splitlines()[0]
            if url.startswith("http"):
                logger.debug("Resolved track %s -> %s", track_id, url[:80])
                return url

        logger.warning(
            "yt-dlp failed for track %s (exit=%s): %s",
            track_id, proc.returncode, stderr.decode().strip()[:200],
        )
    except asyncio.TimeoutError:
        logger.warning("yt-dlp timed out for track %s", track_id)
    except Exception as e:
        logger.warning("yt-dlp error for track %s: %s", track_id, e)

    return None


async def transcode_to_mp3_stream(
    audio_url: str,
    bitrate: str = "192k",
    sample_rate: int = 44100,
) -> asyncio.subprocess.Process:
    """Launch an ffmpeg process that reads from audio_url and outputs MP3 to stdout.

    Args:
        audio_url: Direct URL to the source audio.
        bitrate: Target MP3 bitrate (e.g. "192k").
        sample_rate: Output sample rate in Hz.

    Returns:
        The ffmpeg subprocess (read from proc.stdout for MP3 bytes).
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-re",              # Read at realtime speed (important for streaming)
        "-i", audio_url,    # Input from URL
        "-vn",              # No video
        "-codec:a", "libmp3lame",
        "-b:a", bitrate,
        "-ar", str(sample_rate),
        "-ac", "2",         # Stereo
        "-f", "mp3",        # Output format
        "pipe:1",           # Output to stdout
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return proc


async def generate_silence(duration_seconds: float = 2.0, bitrate: str = "192k") -> bytes:
    """Generate a short silence MP3 buffer for gaps between tracks."""
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-f", "lavfi",
        "-i", f"anullsrc=r=44100:cl=stereo",
        "-t", str(duration_seconds),
        "-codec:a", "libmp3lame",
        "-b:a", bitrate,
        "-f", "mp3",
        "pipe:1",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout
