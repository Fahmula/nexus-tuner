


from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, NewType, TypedDict

# --- Constants ---
NEXUS_STREAM_VERSION = (Path(__file__).parent.parent / "VERSION").read_text().strip()
NEXUS_STREAM_USER_AGENT = f"NexusStream/{NEXUS_STREAM_VERSION}"
CREATE_STREAM_DEADLINE = 25         # The maximum time that clients will wait for a stream to be created
NEW_DEADLINE_NON_BEST = 1           # The number of seconds after a stream is healthy before giving up waiting on others, the best remaining source deadline is immediate
CREATE_STREAM_POLL_INTERVAL = 0.01  # Polling interval for stream creation
MPEGTS_PACKET_SIZE = 188            # Size of a single MPEG-TS packet in bytes
DEFAULT_PRIORITY = 5                # Default priority for sources
FFMPEG_TERMINATE_TIMEOUT = 5        # Timeout for terminating FFmpeg processes

ProviderAlias = NewType("ProviderAlias", str)
LogicalChannelId = NewType("LogicalChannelId", str)
LogicalChannelName = NewType("LogicalChannelName", str)
SourceServiceId = NewType("SourceServiceId", str)
VideoKey = NewType("VideoKey", str)
VideoName = NewType("VideoName", str)
DateTimeISO = NewType("DateTimeISO", str)


class VideoType(StrEnum):
    HLS = "hls"
    MPEGTS = "mpegts"


class JobName(StrEnum):
    BACKUP = "backup"
    CLEANUP = "cleanup"
    DISCOVER = "discover"
    QUALITY = "quality"


class Label(StrEnum):
    CONFIG = "config"
    HANDLER = "handler"
    SCHEDULER = "scheduler"
    SERVER = "server"
    SESSION = "session"
    STARTUP = "startup"
    STREAM = "stream"
    QUALITY = "quality"


class ProviderInfo(TypedDict):
    url: str
    max_concurrent_streams: int
    updated_at: DateTimeISO
ProvidersData = NewType("ProvidersData", dict[ProviderAlias, ProviderInfo])


def relative_time(dt: datetime, reference_time: datetime | None = None) -> str:
    """Formats a datetime as a relative time string (e.g. '5m ago' or 'in 2h')."""
    reference_time = reference_time or datetime.now()
    delta = dt - reference_time if dt > reference_time else reference_time - dt
    
    seconds = delta.total_seconds()
    if seconds < 60:
        unit = "s"
        value = int(seconds)
    elif seconds < 3600:
        unit = "m"
        value = int(seconds / 60)
    elif seconds < 86400:
        unit = "h"
        value = int(seconds / 3600)
    else:
        unit = "d"
        value = int(seconds / 86400)
    if dt < reference_time:
        return f"{value}{unit} ago"
    else:
        return f"in {value}{unit}"


def sort_sources(sources: list[dict[str, Any]], quality_scores: dict[str, dict[str, float]], *, reverse: bool) -> dict[str, int]:
    """Sorts sources based on priority and quality. (Sync - CPU-bound pure function)"""
    prev_score = None
    curr_priority = -1
    source_scores = sorted(((source["priority"],
                             -quality_scores.get(source["source_service_id"], {}).get("total_score", 0),
                             -quality_scores.get(source["source_service_id"], {}).get("uptime", 0),
                             source["source_service_id"]),
                    source["source_service_id"]) for source in sources)
    source_priorities: dict[str, int] = {}
    for score, source_service_id in source_scores:
        if score != prev_score:
            prev_score = score
            curr_priority += 1
        source_priorities[source_service_id] = curr_priority
    sources.sort(key=lambda x: source_priorities[x["source_service_id"]], reverse=reverse)
    return source_priorities
