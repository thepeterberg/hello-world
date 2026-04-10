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


async def start_master_encoder(
    bitrate: str = "192k",
    sample_rate: int = 44100,
) -> asyncio.subprocess.Process:
    """Start a long-lived ffmpeg process that reads raw PCM from stdin
    and outputs a continuous MP3 stream to stdout.

    This is the key to keeping Roon connected: one never-ending MP3
    stream with no end-of-stream markers between tracks.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-f", "s16le",          # Raw PCM input
        "-ar", str(sample_rate),
        "-ac", "2",             # Stereo
        "-i", "pipe:0",         # Read from stdin
        "-codec:a", "libmp3lame",
        "-b:a", bitrate,
        "-ar", str(sample_rate),
        "-ac", "2",
        "-f", "mp3",            # Output format
        "pipe:1",               # Write to stdout
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    logger.info("Master MP3 encoder started (pid=%d)", proc.pid)
    return proc


async def decode_to_pcm(
    audio_url: str,
    sample_rate: int = 44100,
) -> asyncio.subprocess.Process:
    """Decode an audio URL to raw PCM (s16le, stereo) via ffmpeg.

    The output should be piped into the master encoder's stdin.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-re",                  # Realtime speed
        "-i", audio_url,
        "-vn",
        "-f", "s16le",         # Raw PCM output
        "-acodec", "pcm_s16le",
        "-ar", str(sample_rate),
        "-ac", "2",
        "pipe:1",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    return proc


def generate_silence_pcm(duration_seconds: float = 1.0, sample_rate: int = 44100) -> bytes:
    """Generate raw PCM silence (s16le, stereo).

    No ffmpeg needed — it's just zero bytes.
    """
    # s16le stereo: 2 bytes per sample * 2 channels * sample_rate * duration
    num_bytes = int(2 * 2 * sample_rate * duration_seconds)
    return b"\x00" * num_bytes


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
