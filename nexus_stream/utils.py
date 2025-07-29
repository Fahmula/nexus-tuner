


import asyncio
from datetime import datetime
from enum import StrEnum
import os
from pathlib import Path
from typing import Final, Literal, Mapping, NewType, ReadOnly, TypedDict

import aiofiles

# --- Constants ---
NEXUS_STREAM_VERSION: Final[str] = (Path(__file__).parent.parent / "VERSION").read_text().strip()
NEXUS_STREAM_USER_AGENT: Final[str] = f"NexusStream/{NEXUS_STREAM_VERSION}"
NEXUS_STREAM_PORT: Final[int] = int(os.getenv("NEXUS_PORT", 4040))
CREATE_STREAM_DEADLINE: Final[int] = 25           # The maximum time that clients will wait for a stream to be created
NEW_DEADLINE_NON_BEST: Final[int] = 1             # The number of seconds after a stream is healthy before giving up waiting on others, the best remaining source deadline is immediate
CREATE_STREAM_POLL_INTERVAL: Final[float] = 0.01  # Polling interval for stream creation
MPEGTS_PACKET_SIZE: Final[int] = 188              # Size of a single MPEG-TS packet in bytes
DEFAULT_PRIORITY: Final[int] = 5                  # Default priority for sources
FFMPEG_TERMINATE_TIMEOUT: Final[int] = 5          # Timeout for terminating FFmpeg processes

DateTimeISO = NewType("DateTimeISO", str)
Percent = NewType("Percent", float)

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
ResolutionScore = NewType("ResolutionScore", float)
BitrateScore = NewType("BitrateScore", float)
FramerateScore = NewType("FramerateScore", float)
UptimeScore = NewType("UptimeScore", float)
TotalScore = NewType("TotalScore", float)

TVGName = NewType("TVGName", str)
TVGDisplayName = NewType("TVGDisplayName", str)
GroupTitle = NewType("GroupTitle", str)
TVGId = NewType("TVGId", str)
TVGLogo = NewType("TVGLogo", str)

ReaderId = NewType("ReaderId", int)
SegmentNum = NewType("SegmentNum", int)


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
ProvidersData = NewType("ProvidersData", Mapping[ProviderAlias, ProviderInfo])
ProvidersDataMutable = NewType("ProvidersDataMutable", dict[ProviderAlias, ProviderInfo])
class ProviderInfoMutable(TypedDict):
    url: M3UURL
    max_concurrent_streams: MaxStreams
    updated_at: DateTimeISO | None
ProvidersSourceData = NewType("ProvidersSourceData", Mapping[Literal["source_m3u_providers"], ProvidersData])

class DiscoveredSource(TypedDict):
    id: ReadOnly[SourceServiceId]
    provider_alias: ReadOnly[ProviderAlias]
    original_tvg_name: ReadOnly[TVGName]
    original_display_name_extinf: ReadOnly[TVGDisplayName]
    original_group_title: ReadOnly[GroupTitle]
    original_tvg_id: ReadOnly[TVGId]
    original_tvg_logo: ReadOnly[TVGLogo]
    actual_stream_url: ReadOnly[StreamURL]
DiscoveredSourcesData = NewType("DiscoveredSourcesData", Mapping[SourceServiceId, DiscoveredSource])

class LogicalChannelInfo(TypedDict):
    display_name: ReadOnly[LogicalChannelName]
    channel_num: ReadOnly[ChannelNum]
    group_title: ReadOnly[GroupTitle]
    tvg_id: ReadOnly[TVGId]
    tvg_logo: ReadOnly[TVGLogo]
    logical_channel_id: ReadOnly[LogicalChannelId]
LogicalChannelsData = NewType("LogicalChannelsData", tuple[LogicalChannelInfo, ...])

class SourcePriority(TypedDict):
    source_service_id: ReadOnly[SourceServiceId]
    priority: ReadOnly[Priority]
ChannelMappingsData = NewType("ChannelMappingsData", Mapping[LogicalChannelId, tuple[SourcePriority, ...]])

class SourceInfo(TypedDict):
    source_service_id: ReadOnly[SourceServiceId]
    priority: ReadOnly[Priority]
    provider_alias: ReadOnly[ProviderAlias]
    actual_stream_url: ReadOnly[StreamURL]

class QualityInfo(TypedDict):
    statuses: ReadOnly[tuple[Literal["online", "offline"], ...]]
    widths: ReadOnly[tuple[Width, ...]]
    heights: ReadOnly[tuple[Height, ...]]
    bitrates: ReadOnly[tuple[BitRate, ...]]
    framerates: ReadOnly[tuple[FrameRate, ...]]
    updated_at: ReadOnly[DateTimeISO]
ServiceQualityCacheData = NewType("ServiceQualityCacheData", Mapping[SourceServiceId, QualityInfo])

class QualityScore(TypedDict):
    width: ReadOnly[Width]
    height: ReadOnly[Height]
    bitrate: ReadOnly[BitRate]
    framerate: ReadOnly[FrameRate]
    uptime: ReadOnly[Percent]
    resolution_score: ReadOnly[ResolutionScore]
    bitrate_score: ReadOnly[BitrateScore]
    framerate_score: ReadOnly[FramerateScore]
    uptime_score: ReadOnly[UptimeScore]
    total_score: ReadOnly[TotalScore]
QualityScores = NewType("QualityScores", Mapping[SourceServiceId, QualityScore])
QualityScoresMutable = NewType("QualityScoresMutable", dict[SourceServiceId, QualityScore])

class ChannelInfo(TypedDict):
    num: ReadOnly[ChannelNum]
    title: ReadOnly[ChannelTitle]
    names: ReadOnly[tuple[ChannelAliases, ...]]

class JobInfo(TypedDict):
    last_run: ReadOnly[DateTimeISO]
class JobsData(TypedDict):
    backup: ReadOnly[JobInfo]
    cleanup: ReadOnly[JobInfo]
    discover: ReadOnly[JobInfo]
    quality: ReadOnly[JobInfo]

class MPEGTSProcessInfo(TypedDict):
    process: ReadOnly[asyncio.subprocess.Process]
    is_long_term: ReadOnly[bool]
    is_preview: ReadOnly[bool]
    video_type: ReadOnly[Literal[VideoType.MPEGTS]]
    provider_alias: ReadOnly[ProviderAlias]
    logical_channel_id: ReadOnly[LogicalChannelId]
    source_service_id: ReadOnly[SourceServiceId]
    logical_channel_name: ReadOnly[LogicalChannelName]
    channel_hls_dir: ReadOnly[None]
    last_access: ReadOnly[datetime]
    is_mpegts_active: ReadOnly[bool]
    stderr_log_file_obj: ReadOnly[aiofiles.threadpool.text.AsyncTextIOWrapper]
class HLSProcessInfo(TypedDict):
    process: ReadOnly[asyncio.subprocess.Process]
    is_long_term: ReadOnly[bool]
    is_preview: ReadOnly[bool]
    video_type: ReadOnly[Literal[VideoType.HLS]]
    provider_alias: ReadOnly[ProviderAlias]
    logical_channel_id: ReadOnly[LogicalChannelId]
    source_service_id: ReadOnly[SourceServiceId]
    logical_channel_name: ReadOnly[LogicalChannelName]
    channel_hls_dir: ReadOnly[Path]
    last_access: ReadOnly[datetime]
    is_mpegts_active: ReadOnly[None]
    stderr_log_file_obj: ReadOnly[aiofiles.threadpool.text.AsyncTextIOWrapper]
type FFmpegProcessInfo = MPEGTSProcessInfo | HLSProcessInfo
FFmpegProcessInfos = NewType("FFmpegProcessInfos", Mapping[VideoKey, FFmpegProcessInfo])
class FFmpegProcessInfoMutable(TypedDict):
    is_long_term: bool
    last_access: datetime
    is_mpegts_active: bool
FFmpegProcessInfosMutable = NewType("FFmpegProcessInfosMutable", dict[VideoKey, FFmpegProcessInfo])

def create_stream_key(video_type: VideoType, logical_channel_id: LogicalChannelId) -> StreamKey:
    """Generates a unique key for the stream."""
    return StreamKey(f"{video_type}_{logical_channel_id}")


def create_video_key(stream_key: StreamKey, source_service_id: SourceServiceId) -> VideoKey:
    """Generates a unique key for the stream."""
    return VideoKey(f"{stream_key}_{source_service_id}")


def create_video_name(logical_channel_name: LogicalChannelName, source_service_id: SourceServiceId) -> VideoName:
    """Generates a unique name for the stream."""
    return VideoName(f"{logical_channel_name} - {source_service_id}")


def get_segment_format() -> str:
    """Returns the format string for HLS segment files."""
    return "segment_%05d.ts"


def get_segment_number(segment_filename: str) -> SegmentNum:
    """Extracts the segment number from the segment filename."""
    return SegmentNum(int(segment_filename.split('_')[1].split('.')[0]))


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


def sort_sources(sources: list[SourceInfo], quality_scores: QualityScores, *, reverse: bool) -> dict[SourceServiceId, Priority]:
    """Sorts sources based on priority and quality."""
    prev_score: tuple[Priority, float, float, SourceServiceId] | None = None
    curr_priority: Priority = Priority(-1)
    source_scores: list[tuple[Priority, float, float, SourceServiceId]] = sorted((source["priority"],
                             -quality_scores.get(source["source_service_id"], {}).get("total_score", 0),
                             -quality_scores.get(source["source_service_id"], {}).get("uptime", 0),
                             source["source_service_id"]) for source in sources)
    source_priorities: dict[SourceServiceId, Priority] = {}
    for score in source_scores:
        if score != prev_score:
            prev_score = score
            curr_priority = Priority(curr_priority + 1)
        source_priorities[score[3]] = curr_priority
    sources.sort(key=lambda x: source_priorities[x["source_service_id"]], reverse=reverse)
    return source_priorities
