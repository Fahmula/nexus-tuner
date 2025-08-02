"""The goal is for every long running value to be uniquely typed so that they cannot be used incorrectly.
For example, LogicalChannelId cannot be used instead of SourceServiceId as it will fail type checking.
Futher more these types are then used in TypedDicts to model the data structures used in the application.
All the TypedDicts have all fields marked as ReadOnly and uses Mapping and Tuple instead of dict and list
to signal immutability, the underlying data structures are likely still dict or lists. This ensures that 99%
of the application sees these data as immutable as they should, only in the select few areas were we perform
CRUD operations do we use the mutable versions of these types by contructing a new object or copying the previous.
This design allows the most dangerous operations to be clearly marked and the rest of the code to be type safe and const safe.
"""


import asyncio
from datetime import datetime
from enum import StrEnum
import os
from pathlib import Path
import re
from typing import Any, Coroutine, Final, Literal, Mapping, NewType, ReadOnly, TypedDict

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
URL_REGEX: Final[re.Pattern[str]] = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
    r'localhost|' #localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)  # Django: https://github.com/django/django/blob/6726d750979a7c29e0dd866b4ea367eef7c8a420/django/core/validators.py#L45


# --- Types ---


DateTimeISO = NewType("DateTimeISO", str)
Percent = NewType("Percent", float)
PercentDisplay = NewType("PercentDisplay", float)

ProviderAlias = NewType("ProviderAlias", str)
M3UURL = NewType("M3UURL", str)
MaxStreams = NewType("MaxStreams", int)
ActiveStreams = NewType("ActiveStreams", int)
AvailableStreams = NewType("AvailableStreams", int)
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

Width = NewType("Width", float)
Height = NewType("Height", float)
Bitrate = NewType("Bitrate", float)
Framerate = NewType("Framerate", float)
ResolutionScore = NewType("ResolutionScore", float)
BitrateScore = NewType("BitrateScore", float)
FramerateScore = NewType("FramerateScore", float)
UptimeScore = NewType("UptimeScore", float)
TotalScore = NewType("TotalScore", float)

TVGName = NewType("TVGName", str)
TVGDisplayName = NewType("TVGDisplayName", str)
TVGGroupTitle = NewType("TVGGroupTitle", str)
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


class ProviderStatus(TypedDict):
    alias: ReadOnly[ProviderAlias]
    m3u_url: ReadOnly[M3UURL]
    active_streams: ReadOnly[ActiveStreams]
    max_streams: ReadOnly[MaxStreams]
ProviderStatuses = NewType("ProviderStatuses", Mapping[ProviderAlias, ProviderStatus])

class ProviderInfo(TypedDict):
    m3u_url: ReadOnly[M3UURL]
    max_streams: ReadOnly[MaxStreams]
    updated_at: ReadOnly[DateTimeISO | None]
ProvidersDataImpl = NewType("ProvidersDataImpl", dict[ProviderAlias, ProviderInfo])
_ProvidersDataReadOnly = NewType("_ProvidersDataReadOnly", Mapping[ProviderAlias, ProviderInfo])
type ProvidersData = ProvidersDataImpl | _ProvidersDataReadOnly

class M3USource(TypedDict):
    tvg_name: ReadOnly[TVGName]
    display_title: ReadOnly[TVGDisplayName]
    group_title: ReadOnly[TVGGroupTitle]
    tvg_id: ReadOnly[TVGId]
    tvg_log: ReadOnly[TVGLogo]
    stream_url: ReadOnly[StreamURL]

class DiscoveredSource(M3USource):
    provider_alias: ReadOnly[ProviderAlias]
DiscoveredSourcesDataImpl = NewType("DiscoveredSourcesDataImpl", dict[SourceServiceId, DiscoveredSource])
_DiscoveredSourcesDataReadOnly = NewType("_DiscoveredSourcesDataReadOnly", Mapping[SourceServiceId, DiscoveredSource])
type DiscoveredSourcesData = DiscoveredSourcesDataImpl | _DiscoveredSourcesDataReadOnly
class DiscoveredSourceWithId(DiscoveredSource):
    source_id: ReadOnly[SourceServiceId]

class LogicalChannelInfo(TypedDict):
    logical_channel_id: ReadOnly[LogicalChannelId]
    logical_channel_name: ReadOnly[LogicalChannelName]
    channel_num: ReadOnly[ChannelNum]
    group_title: ReadOnly[TVGGroupTitle]
    tvg_id: ReadOnly[TVGId]
    tvg_logo: ReadOnly[TVGLogo]
LogicalChannelsDataImpl = NewType("LogicalChannelsDataImpl", list[LogicalChannelInfo])
_LogicalChannelsDataReadOnly = NewType("_LogicalChannelsDataReadOnly", tuple[LogicalChannelInfo, ...])
type LogicalChannelsData = LogicalChannelsDataImpl | _LogicalChannelsDataReadOnly

class LogicalChannelMetrics(TypedDict):
    health_score: ReadOnly[PercentDisplay | None]
    lowest_uptime: ReadOnly[PercentDisplay | None]
    enabled_mappings: ReadOnly[int]
    discovered_mappings: ReadOnly[int]

class SourcePriority(TypedDict):
    source_service_id: ReadOnly[SourceServiceId]
    priority: ReadOnly[Priority]
ChannelMappingsDataImpl = NewType("ChannelMappingsDataImpl", dict[LogicalChannelId, list[SourcePriority]])
ChannelMappingsDataReadOnly = NewType("ChannelMappingsDataReadOnly", Mapping[LogicalChannelId, tuple[SourcePriority, ...]])
type ChannelMappingsData = ChannelMappingsDataImpl | ChannelMappingsDataReadOnly

class SourceMetrics(TypedDict):
    priority: ReadOnly[Priority]
    uptime: ReadOnly[PercentDisplay | None]

class SourceInfo(TypedDict):
    source_service_id: ReadOnly[SourceServiceId]
    priority: ReadOnly[Priority]
    provider_alias: ReadOnly[ProviderAlias]
    stream_url: ReadOnly[StreamURL]

class ChannelInfo(LogicalChannelInfo):
    sources: ReadOnly[list[SourceInfo]]
ChannelInfosImpl = NewType("ChannelInfosImpl", dict[LogicalChannelId, ChannelInfo])
_ChannelInfosReadOnly = NewType("_ChannelInfosReadOnly", Mapping[LogicalChannelId, ChannelInfo])
type ChannelInfos = ChannelInfosImpl | _ChannelInfosReadOnly

class QualityInfo(TypedDict):
    statuses: ReadOnly[tuple[Literal["online", "offline"], ...]]
    widths: ReadOnly[tuple[Width, ...]]
    heights: ReadOnly[tuple[Height, ...]]
    bitrates: ReadOnly[tuple[Bitrate, ...]]
    framerates: ReadOnly[tuple[Framerate, ...]]
    updated_at: ReadOnly[DateTimeISO]
class QualityInfoImpl(TypedDict):
    statuses: list[Literal["online", "offline"]]
    widths: list[Width]
    heights: list[Height]
    bitrates: list[Bitrate]
    framerates: list[Framerate]
    updated_at: DateTimeISO
ServiceQualityCacheDataImpl = NewType("ServiceQualityCacheDataImpl", dict[SourceServiceId, QualityInfoImpl])
_ServiceQualityCacheDataReadOnly = NewType("_ServiceQualityCacheDataReadOnly", Mapping[SourceServiceId, QualityInfo])
type ServiceQualityCacheData = ServiceQualityCacheDataImpl | _ServiceQualityCacheDataReadOnly

class QualityScore(TypedDict):
    width: ReadOnly[Width]
    height: ReadOnly[Height]
    bitrate: ReadOnly[Bitrate]
    framerate: ReadOnly[Framerate]
    uptime: ReadOnly[Percent]
    resolution_score: ReadOnly[ResolutionScore]
    bitrate_score: ReadOnly[BitrateScore]
    framerate_score: ReadOnly[FramerateScore]
    uptime_score: ReadOnly[UptimeScore]
    total_score: ReadOnly[TotalScore]
QualityScoresImpl = NewType("QualityScoresImpl", dict[SourceServiceId, QualityScore])
_QualityScoresReadOnly = NewType("_QualityScoresReadOnly", Mapping[SourceServiceId, QualityScore])
type QualityScores = QualityScoresImpl | _QualityScoresReadOnly

class ChannelListInfo(TypedDict):
    num: ReadOnly[ChannelNum]
    title: ReadOnly[ChannelTitle]
    names: ReadOnly[tuple[ChannelAliases, ...]]
ChannelListDataImpl = NewType("ChannelListDataImpl", dict[TVGGroupTitle, list[ChannelListInfo]])
_ChannelListDataReadOnly = NewType("_ChannelListDataReadOnly", Mapping[TVGGroupTitle, tuple[ChannelListInfo, ...]])
type ChannelListData = ChannelListDataImpl | _ChannelListDataReadOnly

class ChannelListGroup(ChannelListInfo):
    group: ReadOnly[TVGGroupTitle]

class JobInfo(TypedDict):
    last_run: ReadOnly[DateTimeISO | None]
class JobInfoImpl(TypedDict):
    last_run: DateTimeISO | None
JobsDataImpl = NewType("JobsDataImpl", dict[JobName, JobInfoImpl])
_JobsDataReadOnly = NewType("_JobsDataReadOnly", Mapping[JobName, JobInfo])
type JobsData = JobsDataImpl | _JobsDataReadOnly

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

class ProbeSuccess(TypedDict):
    status: ReadOnly[Literal["online"]]
    width: ReadOnly[Width]
    height: ReadOnly[Height]
    bitrate: ReadOnly[Bitrate]
    framerate: ReadOnly[Framerate]
class ProbeFailure(TypedDict):
    status: ReadOnly[Literal["offline"]]
    reason: ReadOnly[str]
type ProbeInfo = ProbeSuccess | ProbeFailure


# --- Functions ---
background_tasks: set[asyncio.Task[Any]] = set()
def run_bg(coro: Coroutine[Any, Any, Any]) -> None:
    """Adds a background task to the global set and discard on completion as asyncio.create_task() requires a reference to it."""
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

def create_stream_key(video_type: VideoType, logical_channel_id: LogicalChannelId) -> StreamKey:
    """Generates a unique key for the stream."""
    return StreamKey(f"{video_type}_{logical_channel_id}")


def create_video_key(stream_key: StreamKey, source_service_id: SourceServiceId) -> VideoKey:
    """Generates a unique key for the stream."""
    return VideoKey(f"{stream_key}_{source_service_id}")


def create_video_name(logical_channel_name: LogicalChannelName, source_name: TVGDisplayName | TVGName, source_service_id: SourceServiceId) -> VideoName:
    """Generates a unique name for the stream."""
    return VideoName(f"{logical_channel_name} - {source_name} ({source_service_id})")


def get_segment_format() -> str:
    """Returns the format string for HLS segment files."""
    return "segment_%05d.ts"


def get_segment_number(segment_filename: str) -> SegmentNum:
    """Extracts the segment number from the segment filename."""
    return SegmentNum(int(segment_filename.split('_')[1].split('.')[0]))


def is_valid_url(url: str) -> bool:
    """Check if the given URL is valid."""
    return bool(URL_REGEX.match(url))


def relative_time(dt: datetime, reference_time: datetime | None = None) -> str:
    """Formats a datetime as a relative time string (e.g. '5m ago' or 'in 2h')."""
    reference_time = reference_time or datetime.now()
    delta = dt - reference_time if dt > reference_time else reference_time - dt
    
    seconds = delta.total_seconds()
    if seconds < 60:
        unit = "s"
        value = round(seconds)
    elif seconds < 3600:
        unit = "m"
        value = round(seconds / 60)
    elif seconds < 86400:
        unit = "h"
        value = round(seconds / 3600)
    else:
        unit = "d"
        value = round(seconds / 86400)
    if dt < reference_time:
        return f"{value}{unit} ago"
    else:
        return f"in {value}{unit}"


def sort_sources(sources: list[SourceInfo] | list[SourcePriority], quality_scores: QualityScores, *, reverse: bool) -> dict[SourceServiceId, Priority]:
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
