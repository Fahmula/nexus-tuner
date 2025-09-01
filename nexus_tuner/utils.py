"""The goal is for every long running value to be uniquely typed so that they cannot be used incorrectly.
For example, LogicalChannelId cannot be used instead of SourceId as it will fail type checking.
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
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path
import re
from typing import Any, Coroutine, Final, Literal, Mapping, NewType, ReadOnly, TypedDict

import aiofiles
from dotenv import load_dotenv

# --- Environment ---

load_dotenv()
config_dir_str: str | None = os.getenv("NEXUS_CONFIG_DIR")
if not config_dir_str:
    raise ValueError("NEXUS_CONFIG_DIR environment variable is not set on docker container or system.")
CONFIG_DIR: Final[Path] = Path(config_dir_str)
env_file: Path = CONFIG_DIR / ".env"
if env_file.exists():
    load_dotenv(env_file)
del config_dir_str, env_file

# --- Constants ---

NEXUS_TUNER_VERSION: Final[str] = (Path(__file__).parent.parent / "VERSION").read_text().strip()
NEXUS_TUNER_USER_AGENT: Final[str] = f"NexusTuner/{NEXUS_TUNER_VERSION}"
NEXUS_TUNER_PORT: Final[int] = int(os.getenv("NEXUS_PORT", 4040))
if NEXUS_TUNER_PORT < 1 or NEXUS_TUNER_PORT > 65535:
    raise ValueError("NEXUS_PORT must be a valid port number between 1 and 65535.")
CREATE_STREAM_DEADLINE: Final[int] = 25                   # The maximum time that clients will wait for a stream to be created
NEW_DEADLINE_NON_BEST: Final[int] = 1                     # The number of seconds after a stream is healthy before giving up waiting on others, the best remaining source deadline is immediate
CREATE_STREAM_POLL_INTERVAL: Final[float] = 0.01          # Polling interval for stream creation
MPEGTS_PACKET_SIZE: Final[int] = 188                      # Size of a single MPEGTS packet in bytes
MPEGTS_CHUNK_SIZE: Final[int] = MPEGTS_PACKET_SIZE * 21   # Size of each chunk we send to clients. Update with MPEGTS_CHUNK_READ_TIMEOUT.
MPEGTS_CHUNK_READ_TIMEOUT: Final[int] = 10                # Timeout for reading MPEGTS chunks. Update with MPEGTS_CHUNK_SIZE.
DEFAULT_PRIORITY: Final[int] = 5                          # Default priority for sources
PROCESS_TERMINATE_TIMEOUT: Final[int] = 5                 # Timeout for terminating processes
PROCESS_TERMINATE_INTERVAL: Final[float] = 0.01           # Polling interval for checking process termination
URL_REGEX: Final[re.Pattern[str]] = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
    r'localhost|' #localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)  # Django: https://github.com/django/django/blob/6726d750979a7c29e0dd866b4ea367eef7c8a420/django/core/validators.py#L45


# --- Types ---


DateTimeISO = NewType("DateTimeISO", str)
DurationStr = NewType("DurationStr", str)
RelativeTimeStr = NewType("RelativeTimeStr", str)
Percent = NewType("Percent", float)
PercentDisplay = NewType("PercentDisplay", float)

ProviderAlias = NewType("ProviderAlias", str)
M3UURL = NewType("M3UURL", str)
MaxStreams = NewType("MaxStreams", int)
ActiveStreams = NewType("ActiveStreams", int)
AvailableStreams = NewType("AvailableStreams", int)
MainM3UPlaylist = NewType("MainM3UPlaylist", str)

LogicalChannelId = NewType("LogicalChannelId", str)
LogicalChannelTitle = NewType("LogicalChannelTitle", str)
SourceId = NewType("SourceId", str)
PreviewId = NewType("PreviewId", str)
StreamURL = NewType("StreamURL", str)
StreamKey = NewType("StreamKey", str)
VideoKey = NewType("VideoKey", str)
StreamName = NewType("StreamName", str)
VideoName = NewType("VideoName", str)
Priority = NewType("Priority", int)

ChannelNum = NewType("ChannelNum", str)
ChannelTitle = NewType("ChannelTitle", str)
ChannelAliases = NewType("ChannelAliases", str)

Width = NewType("Width", float)
Height = NewType("Height", float)
Bitrate = NewType("Bitrate", float)
Framerate = NewType("Framerate", float)
Runtime = NewType("Runtime", float)
ResolutionScore = NewType("ResolutionScore", float)
BitrateScore = NewType("BitrateScore", float)
FramerateScore = NewType("FramerateScore", float)
UptimeScore = NewType("UptimeScore", float)
RuntimeScore = NewType("RuntimeScore", float)
TotalScore = NewType("TotalScore", float)

TVGName = NewType("TVGName", str)
TVGDisplayTitle = NewType("TVGDisplayTitle", str)
TVGGroupTitle = NewType("TVGGroupTitle", str)
TVGId = NewType("TVGId", str)
TVGLogo = NewType("TVGLogo", str)

ReaderId = NewType("ReaderId", int)
SegmentNum = NewType("SegmentNum", int)


class StreamEngine(StrEnum):
    FFMPEG = "ffmpeg"
    VLC = "vlc"


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

class StopReason(StrEnum):
    DECLINED = "declined"
    MANUAL = "manual"
    SHUTDOWN = "shutdown"
    TIMEOUT = "timeout"
    PRUNE = "prune"
    PROVIDER = "provider"
    ERROR = "error"
    STALLED = "stalled"
FAILED_STOP_REASONS = (StopReason.ERROR, StopReason.STALLED)


class ProviderStatus(TypedDict):
    alias: ReadOnly[ProviderAlias]
    m3u_url: ReadOnly[M3UURL]
    active_streams: ReadOnly[ActiveStreams]
    max_streams: ReadOnly[MaxStreams]
ProviderStatuses = NewType("ProviderStatuses", Mapping[ProviderAlias, ProviderStatus])

class ProviderInfo(TypedDict):
    m3u_url: ReadOnly[M3UURL]
    max_streams: ReadOnly[MaxStreams]
    updated_at: ReadOnly[DateTimeISO]
ProvidersDataImpl = NewType("ProvidersDataImpl", dict[ProviderAlias, ProviderInfo])
_ProvidersDataReadOnly = NewType("_ProvidersDataReadOnly", Mapping[ProviderAlias, ProviderInfo])
type ProvidersData = ProvidersDataImpl | _ProvidersDataReadOnly

class M3USource(TypedDict):
    tvg_name: ReadOnly[TVGName]
    display_title: ReadOnly[TVGDisplayTitle]
    group_title: ReadOnly[TVGGroupTitle]
    tvg_id: ReadOnly[TVGId]
    tvg_logo: ReadOnly[TVGLogo]
    stream_url: ReadOnly[StreamURL]

class DiscoveredSource(M3USource):
    provider_alias: ReadOnly[ProviderAlias]
DiscoveredSourcesDataImpl = NewType("DiscoveredSourcesDataImpl", dict[SourceId, DiscoveredSource])
_DiscoveredSourcesDataReadOnly = NewType("_DiscoveredSourcesDataReadOnly", Mapping[SourceId, DiscoveredSource])
type DiscoveredSourcesData = DiscoveredSourcesDataImpl | _DiscoveredSourcesDataReadOnly
class DiscoveredSourceWithId(DiscoveredSource):
    source_id: ReadOnly[SourceId]

class LogicalChannelInfo(TypedDict):
    logical_channel_title: ReadOnly[LogicalChannelTitle]
    channel_num: ReadOnly[ChannelNum]
    group_title: ReadOnly[TVGGroupTitle]
    tvg_id: ReadOnly[TVGId]
    tvg_logo: ReadOnly[TVGLogo]
LogicalChannelsDataImpl = NewType("LogicalChannelsDataImpl", dict[LogicalChannelId, LogicalChannelInfo])
_LogicalChannelsDataReadOnly = NewType("_LogicalChannelsDataReadOnly", Mapping[LogicalChannelId, LogicalChannelInfo])
type LogicalChannelsData = LogicalChannelsDataImpl | _LogicalChannelsDataReadOnly
class LogicalChannelInfoWithId(LogicalChannelInfo):
    logical_channel_id: ReadOnly[LogicalChannelId]

class LogicalChannelMetrics(TypedDict):
    health_score: ReadOnly[PercentDisplay | None]
    lowest_uptime: ReadOnly[PercentDisplay | None]
    lowest_runtime: ReadOnly[Runtime | None]
    enabled_mappings: ReadOnly[int]
    discovered_mappings: ReadOnly[int]

class SourceMappingInfo(TypedDict):
    priority: ReadOnly[Priority]
ChannelMappingsImpl = NewType("ChannelMappingsImpl", dict[SourceId, SourceMappingInfo])
_ChannelMappingsReadOnly = NewType("_ChannelMappingsReadOnly", Mapping[SourceId, SourceMappingInfo])
type ChannelMappings = ChannelMappingsImpl | _ChannelMappingsReadOnly
ChannelMappingsDataImpl = NewType("ChannelMappingsDataImpl", dict[LogicalChannelId, ChannelMappingsImpl])
_ChannelMappingsDataReadOnly = NewType("_ChannelMappingsDataReadOnly", Mapping[LogicalChannelId, _ChannelMappingsReadOnly])
type ChannelMappingsData = ChannelMappingsDataImpl | _ChannelMappingsDataReadOnly
class SourceMappingInfoWithId(SourceMappingInfo):
    source_id: ReadOnly[SourceId]

class SourceMetrics(TypedDict):
    priority: ReadOnly[Priority]
    uptime: ReadOnly[PercentDisplay | None]
    runtime: ReadOnly[Runtime | None]

class SourceInfo(TypedDict):
    source_id: ReadOnly[SourceId]
    priority: ReadOnly[Priority]
    provider_alias: ReadOnly[ProviderAlias]
    stream_url: ReadOnly[StreamURL]

class ClientChannelInfo(LogicalChannelInfo):
    sources: ReadOnly[list[SourceInfo]]
ClientChannelInfosImpl = NewType("ClientChannelInfosImpl", dict[LogicalChannelId, ClientChannelInfo])
_ClientChannelInfosReadOnly = NewType("_ClientChannelInfosReadOnly", Mapping[LogicalChannelId, ClientChannelInfo])
type ClientChannelInfos = ClientChannelInfosImpl | _ClientChannelInfosReadOnly

class RuntimeInfo(TypedDict):
    stop_reason: ReadOnly[StopReason]
    runtime: ReadOnly[Runtime]
class QualityInfo(TypedDict):
    statuses: ReadOnly[tuple[Literal["online", "offline"], ...]]
    widths: ReadOnly[tuple[Width, ...]]
    heights: ReadOnly[tuple[Height, ...]]
    bitrates: ReadOnly[tuple[Bitrate, ...]]
    framerates: ReadOnly[tuple[Framerate, ...]]
    runtimes: ReadOnly[tuple[RuntimeInfo, ...]]
    updated_at: ReadOnly[DateTimeISO]
class QualityInfoImpl(TypedDict):
    statuses: list[Literal["online", "offline"]]
    widths: list[Width]
    heights: list[Height]
    bitrates: list[Bitrate]
    framerates: list[Framerate]
    runtimes: list[RuntimeInfo]
    updated_at: DateTimeISO
QualityCacheDataImpl = NewType("QualityCacheDataImpl", dict[SourceId, QualityInfoImpl])
_QualityCacheDataReadOnly = NewType("_QualityCacheDataReadOnly", Mapping[SourceId, QualityInfo])
type QualityCacheData = QualityCacheDataImpl | _QualityCacheDataReadOnly

class QualityScore(TypedDict):
    width: ReadOnly[Width]
    height: ReadOnly[Height]
    bitrate: ReadOnly[Bitrate]
    framerate: ReadOnly[Framerate]
    uptime: ReadOnly[Percent]
    runtime: ReadOnly[Runtime | None]
    resolution_score: ReadOnly[ResolutionScore]
    bitrate_score: ReadOnly[BitrateScore]
    framerate_score: ReadOnly[FramerateScore]
    uptime_score: ReadOnly[UptimeScore]
    runtime_score: ReadOnly[RuntimeScore]
    total_score: ReadOnly[TotalScore]
QualityScoresImpl = NewType("QualityScoresImpl", dict[SourceId, QualityScore])
_QualityScoresReadOnly = NewType("_QualityScoresReadOnly", Mapping[SourceId, QualityScore])
type QualityScores = QualityScoresImpl | _QualityScoresReadOnly

class ChannelListInfo(TypedDict):
    num: ReadOnly[ChannelNum]
    title: ReadOnly[ChannelTitle]
    aliases: ReadOnly[tuple[ChannelAliases, ...]]
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

class MPEGTSHealth(TypedDict):
    is_healthy: ReadOnly[bool | None]
    buffer: ReadOnly[tuple[bytes, ...]]
    stop_read: ReadOnly[bool]
    stopped: ReadOnly[asyncio.Event]
    started_at: ReadOnly[float]
class MPEGTSHealthImpl(TypedDict):
    is_healthy: bool | None
    buffer: list[bytes]
    stop_read: bool
    stopped: asyncio.Event
    started_at: float
class MPEGTSProcessInfo(TypedDict):
    process: ReadOnly[asyncio.subprocess.Process]
    provider_alias: ReadOnly[ProviderAlias]
    logical_channel_id: ReadOnly[LogicalChannelId | PreviewId]
    source_id: ReadOnly[SourceId]
    stream_engine: ReadOnly[StreamEngine]
    video_type: ReadOnly[Literal[VideoType.MPEGTS]]
    video_name: ReadOnly[VideoName]
    is_long_term: ReadOnly[bool]
    is_preview: ReadOnly[bool]
    started_at: ReadOnly[datetime]
    stopped_at: ReadOnly[datetime | None]
    stop_reason: ReadOnly[StopReason | None]
    last_access: ReadOnly[datetime]
    is_mpegts_active: ReadOnly[bool]
    mpegts_health: ReadOnly[MPEGTSHealth | None]
    channel_hls_dir: ReadOnly[None]
    stderr_log_file_obj: ReadOnly[aiofiles.threadpool.text.AsyncTextIOWrapper]
class HLSProcessInfo(TypedDict):
    process: ReadOnly[asyncio.subprocess.Process]
    provider_alias: ReadOnly[ProviderAlias]
    logical_channel_id: ReadOnly[LogicalChannelId | PreviewId]
    source_id: ReadOnly[SourceId]
    stream_engine: ReadOnly[StreamEngine]
    video_type: ReadOnly[Literal[VideoType.HLS]]
    video_name: ReadOnly[VideoName]
    is_long_term: ReadOnly[bool]
    is_preview: ReadOnly[bool]
    started_at: ReadOnly[datetime]
    stopped_at: ReadOnly[datetime | None]
    stop_reason: ReadOnly[StopReason | None]
    last_access: ReadOnly[datetime]
    is_mpegts_active: ReadOnly[None]
    mpegts_health: ReadOnly[None]
    channel_hls_dir: ReadOnly[Path]
    stderr_log_file_obj: ReadOnly[aiofiles.threadpool.text.AsyncTextIOWrapper]
type ProcessInfo = MPEGTSProcessInfo | HLSProcessInfo
ProcessInfos = NewType("ProcessInfos", Mapping[VideoKey, ProcessInfo])
class ProcessInfoMutable(TypedDict):
    is_long_term: bool
    stopped_at: datetime | None
    stop_reason: StopReason | None
    last_access: datetime
    is_mpegts_active: bool
    mpegts_health: MPEGTSHealthImpl | None
ProcessInfosMutable = NewType("ProcessInfosMutable", dict[VideoKey, ProcessInfo])

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


class LogicalChannelFormDetails(TypedDict):
    channel_metrics: ReadOnly[LogicalChannelMetrics]
    all_source_metrics: ReadOnly[Mapping[SourceId, SourceMetrics]]
    mapped_sources: ReadOnly[list[DiscoveredSource]]
    unmapped_sources_for_page: ReadOnly[list[DiscoveredSource]]
    total_unmapped_sources: ReadOnly[int]
    total_pages: ReadOnly[int]
    current_page: ReadOnly[int]


# --- Logging ---


class Log:
    """Logging utilities namespace."""

    LOG_FILE_NAME: Final[str] = "app.log"
    LOG_FILE_NAME_VERBOSE: Final[str] = "verbose.app.log"
    _logger: logging.Logger
    initialized: bool = False

    @classmethod
    def initialize_logger(cls, logs_dir: Path, log_backup_count: int) -> None:
        """Initializes the logger"""     
        if cls.initialized:
            return   
        logger = logging.getLogger(cls.LOG_FILE_NAME)
        logger.propagate = False
        logger.setLevel(logging.DEBUG)  # Each handler will filter its own level
        cls._logger = logger
        format_str = "%(asctime)s.%(msecs)03d %(levelname)s: %(message)s"
        date_fmt = "%Y-%m-%d %H:%M:%S"

        # app.log
        file_handler = TimedRotatingFileHandler(logs_dir / cls.LOG_FILE_NAME, when='midnight', backupCount=log_backup_count)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(format_str, datefmt=date_fmt))
        logger.addHandler(file_handler)

        # verbose.app.log
        file_handler_verbose = TimedRotatingFileHandler(logs_dir / cls.LOG_FILE_NAME_VERBOSE, when='midnight', backupCount=log_backup_count)
        file_handler_verbose.setLevel(logging.DEBUG)
        file_handler_verbose.setFormatter(logging.Formatter(format_str, datefmt=date_fmt))
        logger.addHandler(file_handler_verbose)

        # Console logger
        class ColoredFormatter(logging.Formatter):
            __slots__ = ('formats',)
            
            def __init__(self, fmt: str = format_str, datefmt: str = date_fmt) -> None:
                super().__init__(fmt, datefmt)
            
                GREY_ANSI = "\x1b[38;20m"
                GREEN_ANSI = "\x1b[32;20m"
                YELLOW_ANSI = "\x1b[33;20m"
                RED_ANSI = "\x1b[31;20m"
                BOLD_RED_ANSI = "\x1b[31;1m"
                RESET_ANSI = "\x1b[0m"

                self.formats = {
                    logging.DEBUG: format_str.replace("%(levelname)s", f"{GREY_ANSI}%(levelname)s{RESET_ANSI}"),
                    logging.INFO: format_str.replace("%(levelname)s", f"{GREEN_ANSI}%(levelname)s{RESET_ANSI}"),
                    logging.WARNING: format_str.replace("%(levelname)s", f"{YELLOW_ANSI}%(levelname)s{RESET_ANSI}"),
                    logging.ERROR: format_str.replace("%(levelname)s", f"{RED_ANSI}%(levelname)s{RESET_ANSI}"),
                    logging.CRITICAL: format_str.replace("%(levelname)s", f"{BOLD_RED_ANSI}%(levelname)s{RESET_ANSI}"),
                }

            def format(self, record: logging.LogRecord) -> str:
                log_fmt = self.formats.get(record.levelno, format_str)
                formatter = logging.Formatter(log_fmt, datefmt=date_fmt)
                return formatter.format(record)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(ColoredFormatter())
        logger.addHandler(console_handler)
        cls.initialized = True

    @classmethod
    def debug(cls, label: Label, msg: str, options: tuple[VideoType, StreamEngine | None] | None = None) -> None:
        """Logs a debug message with the specified label."""
        if options:
            if options[1]:
                cls._logger.debug(f"[{label}/{options[0]}/{options[1]}] {msg}")
            else:
                cls._logger.debug(f"[{label}/{options[0]}] {msg}")
        else:
            cls._logger.debug(f"[{label}] {msg}")

    @classmethod
    def info(cls, label: Label, msg: str, options: tuple[VideoType, StreamEngine | None] | None = None) -> None:
        """Logs an info message with the specified label."""
        if options:
            if options[1]:
                cls._logger.info(f"[{label}/{options[0]}/{options[1]}] {msg}")
            else:
                cls._logger.info(f"[{label}/{options[0]}] {msg}")
        else:
            cls._logger.info(f"[{label}] {msg}")

    @classmethod
    def warn(cls, label: Label, msg: str, options: tuple[VideoType, StreamEngine | None] | None = None) -> None:
        """Logs a warning message with the specified label."""
        if options:
            if options[1]:
                cls._logger.warning(f"[{label}/{options[0]}/{options[1]}] {msg}")
            else:
                cls._logger.warning(f"[{label}/{options[0]}] {msg}")
        else:
            cls._logger.warning(f"[{label}] {msg}")

    @classmethod
    def error(cls, label: Label, msg: str, options: tuple[VideoType, StreamEngine | None] | None = None) -> None:
        """Logs an error message with the specified label."""
        if options:
            if options[1]:
                cls._logger.error(f"[{label}/{options[0]}/{options[1]}] {msg}")
            else:
                cls._logger.error(f"[{label}/{options[0]}] {msg}")
        else:
            cls._logger.error(f"[{label}] {msg}")

    @classmethod
    def critical(cls, label: Label, msg: str, options: tuple[VideoType, StreamEngine | None] | None = None) -> None:
        """Logs a critical message with the specified label."""
        if options:
            if options[1]:
                cls._logger.critical(f"[{label}/{options[0]}/{options[1]}] {msg}")
            else:
                cls._logger.critical(f"[{label}/{options[0]}] {msg}")
        else:
            cls._logger.critical(f"[{label}] {msg}")


# --- Functions ---

background_tasks: set[asyncio.Task[Any]] = set()
def run_bg(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    """Adds a background task to the global set and discard on completion as asyncio.create_task() requires a reference to it."""
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task

def create_stream_key(stream_engine: StreamEngine, video_type: VideoType, logical_channel_id: LogicalChannelId | PreviewId) -> StreamKey:
    """Generates a unique key for the stream."""
    return StreamKey(f"{stream_engine}_{video_type}_{logical_channel_id}")


def create_video_key(stream_key: StreamKey, source_id: SourceId) -> VideoKey:
    """Generates a unique key for the video stream."""
    if source_id in stream_key:
        return VideoKey(stream_key)  # Preview streams use source_id in logical_channel_id, see create_preview_id()
    return VideoKey(f"{stream_key}_{source_id}")


def create_stream_name(logical_channel_title: LogicalChannelTitle, channel_num: ChannelNum | None) -> StreamName:
    """Generates a unique name for the stream."""
    if channel_num:
        return StreamName(f"{channel_num}|{logical_channel_title}")
    return StreamName(logical_channel_title)


def create_video_name(stream_name: StreamName, source_name: TVGDisplayTitle | TVGName, source_id: SourceId) -> VideoName:
    """Generates a unique name for the video stream."""
    return VideoName(f"{stream_name} - {source_name} ({source_id})")


def create_preview_id(source_id: SourceId) -> PreviewId:
    """Generates a unique ID for the preview stream."""
    return PreviewId(f"preview_{source_id}")  # Need to update create_video_key() if changing


def get_source_id_from_preview(preview_id: PreviewId) -> SourceId:
    """Extracts the source ID from the preview stream ID."""
    return SourceId(preview_id.replace("preview_", ""))


def is_preview_id(input_id: PreviewId | LogicalChannelId) -> bool:
    """Checks if the given ID is a preview ID."""
    return input_id.startswith("preview_")


def get_playlist_path(channel_hls_dir: Path) -> Path:
    """Generates the playlist path for a given channel and stream engine."""
    return channel_hls_dir / "playlist.m3u8"


def get_segment_path(channel_hls_dir: Path, seg_format: str) -> Path:
    """Returns the segment path for HLS segment files."""
    return channel_hls_dir / seg_format


def get_segment_number(segment_filename: str) -> SegmentNum:
    """Extracts the segment number from the segment filename."""
    return SegmentNum(int(segment_filename.split('_')[1].split('.')[0]))


def is_valid_url(url: str) -> bool:
    """Check if the given URL is valid."""
    return bool(URL_REGEX.match(url))


def relative_time(dt: datetime, reference_time: datetime | None = None) -> RelativeTimeStr:
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
        return RelativeTimeStr(f"{value}{unit} ago")
    else:
        return RelativeTimeStr(f"in {value}{unit}")


def duration_to_str(duration: float) -> DurationStr:
    """Converts a duration in seconds to a string representation."""
    cur = round(duration)
    if cur <= 0:
        return DurationStr("0s")
    if cur >= 3600:
        hours, cur = divmod(cur, 3600)
    else:
        hours = 0
    if cur >= 60:
        minutes, cur = divmod(cur, 60)
    else:
        minutes = 0
    hours_str = f"{hours}h" if hours else ""
    minutes_str = f"{minutes}m" if minutes else ""
    seconds_str = f"{cur}s" if cur else ""
    return DurationStr(f"{hours_str}{minutes_str}{seconds_str}")


def sort_sources(sources: list[SourceInfo] | list[SourceMappingInfoWithId], quality_scores: QualityScores, *, reverse: bool) -> dict[SourceId, Priority]:
    """Sorts sources based on priority and quality."""
    prev_score: tuple[Priority, float, float, float, SourceId] | None = None
    curr_priority: Priority = Priority(-1)
    source_scores: list[tuple[Priority, float, float, float, SourceId]] = sorted((source["priority"],
                             -quality_scores.get(source["source_id"], {}).get("total_score", 0),
                             -(quality_scores.get(source["source_id"], {}).get("runtime", float("inf")) or float("inf")),
                             -quality_scores.get(source["source_id"], {}).get("uptime", 0),
                             source["source_id"]) for source in sources)
    source_priorities: dict[SourceId, Priority] = {}
    for score in source_scores:
        if score != prev_score:
            prev_score = score
            curr_priority = Priority(curr_priority + 1)
        source_priorities[score[4]] = curr_priority
    sources.sort(key=lambda x: source_priorities[x["source_id"]], reverse=reverse)
    return source_priorities
