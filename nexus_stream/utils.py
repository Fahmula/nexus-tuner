


from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, NewType, ReadOnly, TypedDict

# --- Constants ---
NEXUS_STREAM_VERSION = (Path(__file__).parent.parent / "VERSION").read_text().strip()
NEXUS_STREAM_USER_AGENT = f"NexusStream/{NEXUS_STREAM_VERSION}"
CREATE_STREAM_DEADLINE = 25         # The maximum time that clients will wait for a stream to be created
NEW_DEADLINE_NON_BEST = 1           # The number of seconds after a stream is healthy before giving up waiting on others, the best remaining source deadline is immediate
CREATE_STREAM_POLL_INTERVAL = 0.01  # Polling interval for stream creation
MPEGTS_PACKET_SIZE = 188            # Size of a single MPEG-TS packet in bytes
DEFAULT_PRIORITY = 5                # Default priority for sources
FFMPEG_TERMINATE_TIMEOUT = 5        # Timeout for terminating FFmpeg processes

DateTimeISO = NewType("DateTimeISO", str)

ProviderAlias = NewType("ProviderAlias", str)
M3UURL = NewType("M3UURL", str)
MaxStreams = NewType("MaxStreams", int)
MainM3UPlaylist = NewType("MainM3UPlaylist", str)

LogicalChannelId = NewType("LogicalChannelId", str)
LogicalChannelName = NewType("LogicalChannelName", str)
SourceServiceId = NewType("SourceServiceId", str)
StreamURL = NewType("StreamURL", str)
StreamKey = NewType("StreamKey", str)
VideoKey = NewType("VideoKey", str)
VideoName = NewType("VideoName", str)
Priority = NewType("Priority", int)

ChannelNum = NewType("ChannelNum", str)
ChannelTitle = NewType("ChannelTitle", str)
ChannelAliases = NewType("ChannelAliases", str)

Width = NewType("Width", int)
Height = NewType("Height", int)
BitRate = NewType("BitRate", float)
FrameRate = NewType("FrameRate", float)

TVGName = NewType("TVGName", str)
TVGDisplayName = NewType("TVGDisplayName", str)
GroupTitle = NewType("GroupTitle", str)
TVGId = NewType("TVGId", str)
TVGLogo = NewType("TVGLogo", str)


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
    url: ReadOnly[M3UURL]
    max_concurrent_streams: ReadOnly[MaxStreams]
    updated_at: ReadOnly[DateTimeISO | None]
ProvidersData = NewType("ProvidersData", dict[ProviderAlias, ProviderInfo])
ProvidersSourceData = NewType("ProvidersSourceData", dict[Literal["source_m3u_providers"], ProvidersData])

class SourceInfo(TypedDict):
    id: ReadOnly[SourceServiceId]
    provider_alias: ReadOnly[ProviderAlias]
    original_tvg_name: ReadOnly[TVGName]
    original_display_name_extinf: ReadOnly[TVGDisplayName]
    original_group_title: ReadOnly[GroupTitle]
    original_tvg_id: ReadOnly[TVGId]
    original_tvg_logo: ReadOnly[TVGLogo]
    actual_stream_url: ReadOnly[StreamURL]
DiscoveredSourcesData = NewType("DiscoveredSourcesData", dict[SourceServiceId, SourceInfo])

class LogicalChannelInfo(TypedDict):
    display_name: ReadOnly[LogicalChannelName]
    channel_num: ReadOnly[ChannelNum]
    group_title: ReadOnly[GroupTitle]
    tvg_id: ReadOnly[TVGId]
    tvg_logo: ReadOnly[TVGLogo]
    logical_channel_id: ReadOnly[LogicalChannelId]
LogicalChannelsData = NewType("LogicalChannelsData", list[LogicalChannelInfo])

class SourcePriority(TypedDict):
    source_service_id: ReadOnly[SourceServiceId]
    priority: ReadOnly[Priority]
ChannelMappingsData = NewType("ChannelMappingsData", dict[LogicalChannelId, list[SourcePriority]])

class QualityInfo(TypedDict):
    statuses: ReadOnly[list[Literal["online", "offline"]]]
    widths: ReadOnly[list[Width]]
    heights: ReadOnly[list[Height]]
    bitrates: ReadOnly[list[BitRate]]
    framerates: ReadOnly[list[FrameRate]]
    updated_at: ReadOnly[DateTimeISO]
ServiceQualityCacheData = NewType("ServiceQualityCacheData", dict[SourceServiceId, QualityInfo])

class ChannelInfo(TypedDict):
    num: ReadOnly[ChannelNum]
    title: ReadOnly[ChannelTitle]
    names: ReadOnly[list[ChannelAliases]]

class JobInfo(TypedDict):
    last_run: ReadOnly[DateTimeISO]
class JobsData(TypedDict):
    backup: ReadOnly[JobInfo]
    cleanup: ReadOnly[JobInfo]
    discover: ReadOnly[JobInfo]
    quality: ReadOnly[JobInfo]

def create_stream_key(video_type: VideoType, logical_channel_id: LogicalChannelId) -> StreamKey:
    """Generates a unique key for the stream."""
    return StreamKey(f"{video_type}_{logical_channel_id}")


def create_video_key(stream_key: StreamKey, source_service_id: SourceServiceId) -> VideoKey:
    """Generates a unique key for the stream."""
    return VideoKey(f"{stream_key}_{source_service_id}")


def create_video_name(logical_channel_name: LogicalChannelName, source_service_id: SourceServiceId) -> VideoName:
    """Generates a unique name for the stream."""
    return VideoName(f"{logical_channel_name} - {source_service_id}")


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
