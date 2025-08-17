from datetime import datetime
import os
import json
import re
import time
from pathlib import Path
from typing import Any, Final, Mapping, Self
from urllib.parse import urlparse

import asyncio
import aiofiles.os
import aioshutil

from nexus_tuner.utils import (CONFIG_DIR, NEXUS_TUNER_PORT, NEXUS_TUNER_VERSION, ChannelListDataImpl, ChannelMappingsData, ChannelMappingsDataImpl, DiscoveredSourcesData, DiscoveredSourcesDataImpl, JobName, JobsData, JobsDataImpl,
                               Label, Log, LogicalChannelsData, LogicalChannelsDataImpl, ProvidersData, ProvidersDataImpl, QualityCacheData, QualityCacheDataImpl, VideoKey, is_valid_url)


NOT_ALPHANUM_REGEX: Final[re.Pattern[str]] = re.compile(r'[^a-zA-Z0-9_-]')


class Config:
    """
    Manages all application configuration, file paths, and logging.
    
    Loads settings from environment variables and provides async methods for
    accessing and persisting data to JSON files in a non-blocking, safe manner.
    """
    __slots__ = (
        'nexus_url', 'logs_dir', 'log_backup_count', 'ffmpeg_logs_retention_seconds',
        'providers_name', 'providers_path',
        'discovered_sources_name', 'discovered_sources_path',
        'logical_channels_name', 'logical_channels_path',
        'channel_mappings_name', 'channel_mappings_path',
        'channel_list_name', 'channel_list_path', 'channel_list_default_path',
        'quality_cache_name', 'quality_cache_path',
        'jobs_name', 'jobs_path',
        'backups_base_path', 'backups_scheduled_path', 'backups_manual_path', 'backup_count',
        'hls_base_segment_dir', 'hls_segment_duration', 'segment_prune_timeout', 'latest_segment_timeout', 'hls_playlist_length',
        'ffmpeg_start_timeout', 'ffmpeg_inactivity_timeout', 'ffmpeg_path', 'ffprobe_path', 'ffmpeg_logs_dir',
        'emby_url', 'emby_api_key', 'jellyfin_url', 'jellyfin_api_key', 'ghost_check_interval',
        'file_lock', '_cleaning_up_ffmpeg_logs',
    )
    
    def __init__(self) -> None:
        """
        Initializes the configuration object.
        
        NOTE: This constructor is now lightweight and performs no I/O.
        All file system operations are deferred to the async `_initialize` method,
        which is called by the `create` factory method.
        """        
        nexus_url: str | None = os.getenv("NEXUS_URL")
        if not nexus_url:
            raise ValueError("NEXUS_URL environment variable is not set.")
        nexus_url = nexus_url.strip().rstrip('/')
        url_port = urlparse(nexus_url).port
        if url_port is not None:
            raise ValueError("NEXUS_URL should not contain a port, set the NEXUS_PORT environment variable instead.")
        self.nexus_url: Final[str] = f"{nexus_url}:{NEXUS_TUNER_PORT}"

        ffmpeg_path: str | None = os.getenv("NEXUS_FFMPEG_PATH")
        if not ffmpeg_path:
            raise ValueError("NEXUS_FFMPEG_PATH environment variable is not set.")
        self.ffmpeg_path: Final[Path] = Path(ffmpeg_path.strip())
        if not os.path.isfile(self.ffmpeg_path):
            raise ValueError(f"NEXUS_FFMPEG_PATH '{self.ffmpeg_path}' does not point to a valid file.")
        if not os.access(self.ffmpeg_path, os.X_OK):
            raise ValueError(f"NEXUS_FFMPEG_PATH '{self.ffmpeg_path}' is not executable. Please check permissions.")
        self.ffprobe_path: Final[Path] = self.ffmpeg_path.with_name(self.ffmpeg_path.name.replace("ffmpeg", "ffprobe"))
        if not os.path.isfile(self.ffprobe_path):
            raise ValueError(f"NEXUS_FFMPEG_PATH '{self.ffprobe_path}' does not point to a valid ffprobe file.")
        if not os.access(self.ffprobe_path, os.X_OK):
            raise ValueError(f"NEXUS_FFMPEG_PATH '{self.ffprobe_path}' is not executable. Please check permissions.")

        # --- Logging ---
        self.logs_dir: Final[Path] = CONFIG_DIR / "logs"
        self.log_backup_count: Final[int] = int(os.getenv("NEXUS_LOG_BACKUP_COUNT", 7))
        if self.log_backup_count < 0:
            raise ValueError("NEXUS_LOG_BACKUP_COUNT must be a non-negative integer.")
        self.ffmpeg_logs_retention_seconds: Final[int] = int(os.getenv("NEXUS_FFMPEG_LOGS_RETENTION_SECONDS", 86400))
        if self.ffmpeg_logs_retention_seconds < 0:
            raise ValueError("NEXUS_FFMPEG_LOGS_RETENTION_SECONDS must be a non-negative integer.")

        # --- JSON Data Paths ---
        self.providers_name: Final[str] = "providers.json"
        self.providers_path: Final[Path] = CONFIG_DIR / self.providers_name
        
        self.discovered_sources_name: Final[str] = "discovered_sources.json"
        self.discovered_sources_path: Final[Path] = CONFIG_DIR / self.discovered_sources_name
        
        self.logical_channels_name: Final[str] = "logical_channels.json"
        self.logical_channels_path: Final[Path] = CONFIG_DIR / self.logical_channels_name
        
        self.channel_mappings_name: Final[str] = "channel_mappings.json"
        self.channel_mappings_path: Final[Path] = CONFIG_DIR / self.channel_mappings_name
        
        self.channel_list_name: Final[str] = "channel_list.json"
        self.channel_list_path: Final[Path] = CONFIG_DIR / self.channel_list_name
        self.channel_list_default_path: Final[Path] = Path(__file__).parent / "channel_list.json.default"
        
        self.quality_cache_name: Final[str] = "quality_cache.json"
        self.quality_cache_path: Final[Path] = CONFIG_DIR / self.quality_cache_name

        self.jobs_name: Final[str] = "jobs.json"
        self.jobs_path: Final[Path] = CONFIG_DIR / self.jobs_name

        # --- Backup Config ---
        self.backups_base_path: Final[Path] = CONFIG_DIR / "backups"
        self.backups_scheduled_path: Final[Path] = self.backups_base_path / "scheduled"
        self.backups_manual_path: Final[Path] = self.backups_base_path / "manual"
        self.backup_count: Final[int] = int(os.getenv("NEXUS_BACKUP_COUNT", 30))
        if self.backup_count < 0:
            raise ValueError("NEXUS_BACKUP_COUNT must be a non-negative integer.")

        # --- HLS Segment Directory ---
        self.hls_base_segment_dir: Final[Path] = CONFIG_DIR / "hls_segments"
        
        # --- FFmpeg & HLS Configs ---
        self.hls_segment_duration: Final[int] = 1
        self.segment_prune_timeout: Final[float] = 3
        self.latest_segment_timeout: Final[float] = 60
        self.hls_playlist_length: Final[int] = 30
        self.ffmpeg_start_timeout: Final[float] = 5
        self.ffmpeg_inactivity_timeout: Final[int] = int(os.getenv("NEXUS_FFMPEG_INACTIVITY_TIMEOUT", 900))
        if self.ffmpeg_inactivity_timeout < 0:
            raise ValueError("NEXUS_FFMPEG_INACTIVITY_TIMEOUT must be a non-negative integer.")
        self.ffmpeg_logs_dir: Final[Path] = self.logs_dir / "ffmpeg_logs"

        # --- Media Server Monitoring Configs ---
        self.emby_url: Final[str | None] = os.getenv("NEXUS_EMBY_URL")
        self.emby_api_key: Final[str | None] = os.getenv("NEXUS_EMBY_API_KEY")
        self.jellyfin_url: Final[str | None] = os.getenv("NEXUS_JELLYFIN_URL")
        self.jellyfin_api_key: Final[str | None] = os.getenv("NEXUS_JELLYFIN_API_KEY")
        self.ghost_check_interval: Final[int] = int(os.getenv("NEXUS_GHOST_SESSION_CHECK_INTERVAL", 60))
        if self.ghost_check_interval < 1:
            raise ValueError("NEXUS_GHOST_SESSION_CHECK_INTERVAL must be a positive integer.")

        self.file_lock: Final[asyncio.Lock] = asyncio.Lock()
        self._cleaning_up_ffmpeg_logs: bool = False

    @classmethod
    async def create(cls) -> Self:
        """
        Asynchronous factory for creating and initializing a Config instance.
        This pattern is used because __init__ cannot be async.
        Usage: `config = await Config.create()`
        """
        instance = cls()
        await instance._initialize()
        return instance

    async def _initialize(self) -> None:
        """
        Performs all asynchronous I/O operations required for initialization.
        """
        await aiofiles.os.makedirs(CONFIG_DIR, exist_ok=True)
        await aiofiles.os.makedirs(self.logs_dir, exist_ok=True)
        await aiofiles.os.makedirs(self.backups_scheduled_path, exist_ok=True)
        await aiofiles.os.makedirs(self.backups_manual_path, exist_ok=True)

        Log.initialize_logger(self.logs_dir, self.log_backup_count)
        Log.info(Label.STARTUP, f"NexusTuner v{NEXUS_TUNER_VERSION}")

        # If all the JSON config files do not exist, create them for first time setup
        if all([not await aiofiles.os.path.exists(path) for path in (
            self.providers_path,
            self.discovered_sources_path,
            self.logical_channels_path,
            self.channel_mappings_path,
            self.channel_list_path,
            self.quality_cache_path,
            self.jobs_path
        )]):  # Create all except jobs, it's handled by scheduler
            Log.info(Label.STARTUP, "No configuration files found, creating defaults...")
            await self.save_providers_config(ProvidersDataImpl({}))
            await self.save_discovered_sources_config(DiscoveredSourcesDataImpl({}))
            await self.save_logical_channels_config(LogicalChannelsDataImpl({}))
            await self.save_channel_mappings_config(ChannelMappingsDataImpl({}))
            await self.save_quality_cache(QualityCacheDataImpl({}))
            await aioshutil.copy(self.channel_list_default_path, self.channel_list_path)
        elif not await aiofiles.os.path.exists(self.channel_list_path):
            Log.debug(Label.STARTUP, f"Creating default channel list at {self.channel_list_path}")
            await aioshutil.copy(self.channel_list_default_path, self.channel_list_path)

        Log.debug(Label.STARTUP, f"Cleaning HLS directory: {self.hls_base_segment_dir}")
        await aioshutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)
        await aiofiles.os.makedirs(self.hls_base_segment_dir, exist_ok=True)
        await aiofiles.os.makedirs(self.ffmpeg_logs_dir, exist_ok=True)

    async def clean_up_hls_segments(self) -> None:
        """Cleans up old HLS segment files in the configured directory asynchronously."""
        Log.info(Label.STREAM, f"Cleaning up HLS segments in {self.hls_base_segment_dir}")
        await aioshutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)

    def get_fs_safe_alphanum(self, name: str) -> str:
        """
        Converts a string to a filesystem-safe alphanumeric format.
        This is a pure function with no I/O, so it remains synchronous.
        """
        return re.sub(NOT_ALPHANUM_REGEX, '_', name)

    def get_ffmpeg_log_path(self, video_key: VideoKey) -> Path:
        """
        Generates a safe file path for an FFmpeg log file.
        This is a pure function with no I/O, so it remains synchronous.
        """
        log_filename = f"ffmpeg_{self.get_fs_safe_alphanum(f'{video_key}_{time.time()}')}.log" 
        return self.ffmpeg_logs_dir / log_filename
    
    async def cleanup_ffmpeg_logs_by_age(self) -> None:
        """
        Deletes FFmpeg log files older than a configured number of seconds asynchronously.
        """
        if self._cleaning_up_ffmpeg_logs:
            return
        try:
            self._cleaning_up_ffmpeg_logs = True
            if not await aiofiles.os.path.exists(self.ffmpeg_logs_dir):
                Log.debug(Label.STREAM, f"No FFmpeg logs directory at {self.ffmpeg_logs_dir}. Skipping cleanup.")
                return

            cutoff_time = time.time() - self.ffmpeg_logs_retention_seconds
            files_cleaned_up = False
            
            try:
                log_files = await aiofiles.os.listdir(self.ffmpeg_logs_dir)
            except OSError as e:
                Log.error(Label.STREAM, f"Error listing files in {self.ffmpeg_logs_dir}: {e}")
                return

            for filename in log_files:
                if not filename.startswith("ffmpeg_") or not filename.endswith(".log"):
                    continue
                
                log_file = self.ffmpeg_logs_dir / filename
                try:
                    stat_result = await aiofiles.os.stat(log_file)
                    if stat_result.st_mtime < cutoff_time:
                        await aiofiles.os.remove(log_file)
                        files_cleaned_up = True
                except OSError as e:
                    Log.error(Label.STREAM, f"Error deleting old log file {log_file}: {e}")
            
            if files_cleaned_up:
                Log.debug(Label.STREAM, f"Cleaned up FFmpeg log files older than {self.ffmpeg_logs_retention_seconds} seconds.")
        finally:
            self._cleaning_up_ffmpeg_logs = False

    async def _load_json_file(self, file_path: Path) -> Any:
        """Loads data from a JSON file."""
        async with self.file_lock:
            async with aiofiles.open(file_path, "r") as f:
                return json.loads(await f.read())

    async def _save_json_file[K: str, V](self, file_path: Path, data: Mapping[K, V] | list[V] | tuple[V, ...]) -> bool:
        """
        Saves data to a JSON file atomically and asynchronously.
        """
        temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
        try:
            async with self.file_lock:
                async with aiofiles.open(temp_file_path, "w") as f:
                    await f.write(json.dumps(data, indent=2))
                await aiofiles.os.replace(temp_file_path, file_path)
                return True
        except BaseException as e:
            Log.critical(Label.CONFIG, f"Could not write to {file_path}: {e}")
            if await aiofiles.os.path.exists(temp_file_path):
                try:
                    await aiofiles.os.remove(temp_file_path)
                except Exception as remove_error:
                    Log.critical(Label.CONFIG, f"Error removing temporary file {temp_file_path}: {remove_error}")
            if isinstance(e, Exception):
                return False
            raise

    async def get_providers_config(self, *, label: Label | None = None) -> ProvidersDataImpl | None:
        """Loads the providers configuration from providers.json asynchronously."""
        try:
            data: ProvidersDataImpl = await self._load_json_file(self.providers_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid providers configuration format in {self.providers_path}")
            for key1, val1 in data.items():
                if type(key1) is not str:
                    raise ValueError(f'"{key1}": expected str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2 in ("m3u_url", "max_streams", "updated_at"):
                    if key2 not in val1:
                        raise ValueError(f'"{key1}" - missing required key "{key2}"')
                    val2 = val1[key2]
                    if key2 == "m3u_url":
                        if type(val2) is not str or not is_valid_url(val2):
                            raise ValueError(f'"{key1}" - "m3u_url": expected valid URL, got {val2}')
                    elif key2 == "max_streams":
                        if type(val2) is not int or val2 < 0:
                            raise ValueError(f'"{key1}" - "max_streams": expected non-negative int, got {val2}')
                    elif key2 == "updated_at":
                        try:
                            if type(val2) is not str:
                                raise ValueError()
                            datetime.fromisoformat(val2)
                        except Exception:
                            raise ValueError(f'"{key1}" - "updated_at": expected ISO 8601 date string, got {val2}')
                    else:
                        raise ValueError(f'"{key1}" - unexpected key "{key2}"')
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No providers configuration found at {self.providers_path}, using default...")
                return ProvidersDataImpl({})
            Log.critical(label or Label.CONFIG, f"Failed to load providers configuration from {self.providers_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_providers_config(self, data: ProvidersData) -> bool:
        """Saves the providers configuration to providers.json asynchronously."""
        return await self._save_json_file(self.providers_path, data)

    async def get_discovered_sources_config(self, *, label: Label | None = None) -> DiscoveredSourcesDataImpl | None:
        """Loads the discovered sources from discovered_sources.json asynchronously."""
        try:
            data: DiscoveredSourcesDataImpl = await self._load_json_file(self.discovered_sources_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid discovered sources configuration format in {self.discovered_sources_path}")
            for key1, val1 in data.items():
                if type(key1) is not str:
                    raise ValueError(f'"{key1}": expected str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2 in ("provider_alias", "tvg_name", "display_title", "group_title", "tvg_id", "tvg_logo", "stream_url"):
                    if key2 not in val1:
                        raise ValueError(f'"{key1}" - missing required key "{key2}"')
                    val2 = val1[key2]
                    if type(val2) is not str:
                        raise ValueError(f'"{key1}" - "{key2}": expected str, got {type(val2)}')
                    if key2 == "stream_url" and not is_valid_url(val2):
                        raise ValueError(f'"{key1}" - "stream_url": expected valid URL, got {val2}')
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No discovered sources configuration found at {self.discovered_sources_path}, using default...")
                return DiscoveredSourcesDataImpl({})
            Log.critical(label or Label.CONFIG, f"Failed to load discovered sources configuration from {self.discovered_sources_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_discovered_sources_config(self, data: DiscoveredSourcesData) -> bool:
        """Saves the discovered sources to discovered_sources.json asynchronously."""
        return await self._save_json_file(self.discovered_sources_path, data)

    async def get_logical_channels_config(self, *, label: Label | None = None) -> LogicalChannelsDataImpl | None:
        """Loads the logical channels configuration from logical_channels.json asynchronously."""
        try:
            data: LogicalChannelsDataImpl = await self._load_json_file(self.logical_channels_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid logical channels configuration format in {self.logical_channels_path}")
            for key1, val1 in data.items():
                if type(key1) is not str or not key1.isdigit():
                    raise ValueError(f'"{key1}": expected int str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2 in ("logical_channel_title", "channel_num", "group_title", "tvg_id", "tvg_logo"):
                    if key2 not in val1:
                        raise ValueError(f'"{key1}" - missing required key "{key2}"')
                    val2 = val1[key2]
                    if type(val2) is not str:
                        raise ValueError(f'"{key1}" - "{key2}": expected str, got {type(val2)}')
                    if key2 == "channel_num" and not val2.isdigit():
                        raise ValueError(f'"{key1}" - "channel_num": expected int string, got {val2}')
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No logical channels configuration found at {self.logical_channels_path}, using default...")
                return LogicalChannelsDataImpl({})
            Log.critical(label or Label.CONFIG, f"Failed to load logical channels configuration from {self.logical_channels_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_logical_channels_config(self, data: LogicalChannelsData) -> bool:
        """Saves the logical channels configuration to logical_channels.json asynchronously."""
        return await self._save_json_file(self.logical_channels_path, data)

    async def get_channel_mappings_config(self, *, label: Label | None = None) -> ChannelMappingsDataImpl | None:
        """Loads the channel mappings from channel_mappings.json asynchronously."""
        try:
            data: ChannelMappingsDataImpl = await self._load_json_file(self.channel_mappings_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid channel mappings configuration format in {self.channel_mappings_path}")
            for key1, val1 in data.items():
                if type(key1) is not str or not key1.isdigit():
                    raise ValueError(f'"{key1}": expected int str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2, val2 in val1.items():
                    if type(key2) is not str:
                        raise ValueError(f'"{key1}" - "{key2}": expected str, got {type(key2)}')
                    if type(val2) is not dict:
                        raise ValueError(f'"{key1}" - "{key2}": expected dict, got {type(val2)}')
                    for key3, val3 in val2.items():
                        if key3 not in ("priority",):
                            raise ValueError(f'"{key1}" - "{key2}" - unexpected key "{key3}"')
                        if type(val3) is not int or val3 < 0 or val3 > 10:
                            raise ValueError(f'"{key1}" - "{key2}" - "{key3}": expected int between 0 and 10, got {val3}')
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No channel mappings configuration found at {self.channel_mappings_path}, using default...")
                return ChannelMappingsDataImpl({})
            Log.critical(label or Label.CONFIG, f"Failed to load channel mappings configuration from {self.channel_mappings_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_channel_mappings_config(self, data: ChannelMappingsData) -> bool:
        """Saves the channel mappings to channel_mappings.json asynchronously."""
        return await self._save_json_file(self.channel_mappings_path, data)
    
    async def get_channel_list_config(self, *, use_default: bool = False, label: Label | None = None) -> ChannelListDataImpl | None:
        """Loads the predefined channel list from channel_list.json asynchronously."""
        try:
            if use_default:
                data: ChannelListDataImpl = await self._load_json_file(self.channel_list_default_path)
            else:
                data: ChannelListDataImpl = await self._load_json_file(self.channel_list_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid channel list configuration format in {self.channel_list_path}")
            for key1, val1 in data.items():
                if type(key1) is not str:
                    raise ValueError(f'"{key1}": expected str, got {type(key1)}')
                if type(val1) is not list:
                    raise ValueError(f'"{key1}" val: expected list, got {type(val1)}')
                for index, item in enumerate(val1):
                    if type(item) is not dict:
                        raise ValueError(f'"{key1}" - item {index}: expected dict, got {type(item)}')
                    for key2 in ("num", "title", "aliases"):
                        if key2 not in item:
                            raise ValueError(f'"{key1}" - item {index} - missing required key "{key2}"')
                        val2 = item[key2]
                        if not val2:
                            raise ValueError(f'"{key1}" - item {index} - "{key2}": cannot be empty')
                        if key2 == "num":
                            if type(val2) is not str:
                                raise ValueError(f'"{key1}" - item {index} - "num": expected str, got {val2}')
                        elif key2 == "title":
                            if type(val2) is not str:
                                raise ValueError(f'"{key1}" - item {index} - "title": expected str, got {type(val2)}')
                        elif key2 == "aliases":
                            if type(val2) is not list:
                                raise ValueError(f'"{key1}" - item {index} - "aliases": expected list, got {type(val2)}')
                            for alias in val2:
                                if type(alias) is not str:
                                    raise ValueError(f'"{key1}" - item {index} - "aliases" item: expected str, got {type(alias)}')
                        else:
                            raise ValueError(f'"{key1}" - item {index} - unexpected key "{key2}"')
            return data
        except BaseException as e:
            if not use_default and isinstance(e, FileNotFoundError):
                Log.info(label or Label.CONFIG, f"No channel list configuration found at {self.channel_list_path}, using default...")
                return await self.get_channel_list_config(use_default=True, label=label)
            Log.critical(label or Label.CONFIG, f"Failed to load channel list configuration from {self.channel_list_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def get_quality_cache(self, *, label: Label | None = None) -> QualityCacheDataImpl | None:
        """Loads the quality cache from quality_cache.json asynchronously."""
        try:
            data: QualityCacheDataImpl = await self._load_json_file(self.quality_cache_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid quality cache format in {self.quality_cache_path}")
            for key1, val1 in data.items():
                if type(key1) is not str:
                    raise ValueError(f'"{key1}": expected str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2 in ("statuses", "widths", "heights", "bitrates", "framerates", "updated_at"):
                    if key2 not in val1:
                        raise ValueError(f'"{key1}" - missing required key "{key2}"')
                    val2 = val1[key2]
                    if key2 == "statuses":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "statuses": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) is not str or item not in ("online", "offline"):
                                raise ValueError(f'"{key1}" - "statuses" item {index}: expected "online" or "offline", got {item}')
                    elif key2 == "widths":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "widths": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) is not int or item < 0:
                                raise ValueError(f'"{key1}" - "widths" item {index}: expected non-negative int, got {item}')
                    elif key2 == "heights":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "heights": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) is not int or item < 0:
                                raise ValueError(f'"{key1}" - "heights" item {index}: expected non-negative int, got {item}')
                    elif key2 == "bitrates":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "bitrates": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) is not float or item < 0:
                                raise ValueError(f'"{key1}" - "bitrates" item {index}: expected non-negative float, got {item}')
                    elif key2 == "framerates":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "framerates": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) is not float or item < 0:
                                raise ValueError(f'"{key1}" - "framerates" item {index}: expected non-negative float, got {item}')
                    elif key2 == "updated_at":
                        try:
                            if type(val2) is not str:
                                raise ValueError()
                            datetime.fromisoformat(val2)
                        except Exception:
                            raise ValueError(f'"{key1}" - "updated_at": expected ISO 8601 date string, got {val2}')
                    else:
                        raise ValueError(f'"{key1}" - unexpected key "{key2}"')
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No quality cache found at {self.quality_cache_path}, using default...")
                return QualityCacheDataImpl({})
            Log.critical(label or Label.CONFIG, f"Failed to load quality cache from {self.quality_cache_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_quality_cache(self, data: QualityCacheData) -> bool:
        """Saves the quality cache to quality_cache.json asynchronously."""
        return await self._save_json_file(self.quality_cache_path, data)

    async def get_jobs_config(self, *, label: Label | None = None) -> JobsDataImpl | None:
        """Loads the jobs configuration from jobs.json asynchronously."""
        try:
            data: JobsDataImpl = await self._load_json_file(self.jobs_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid jobs configuration format in {self.jobs_path}")
            for key1 in JobName:
                if key1 not in data:
                    raise ValueError(f'Missing required job type "{key1}" in jobs configuration')
                val1 = data[key1]
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2, val2 in val1.items():
                    if key2 != "last_run":
                        raise ValueError(f'"{key1}" - unexpected key "{key2}"')
                    if type(val2) is not str:
                        raise ValueError(f'"{key1}" - "last_run": expected str, got {type(val2)}')
                    try:
                        datetime.fromisoformat(val2)
                    except ValueError:
                        raise ValueError(f'"{key1}" - "last_run": expected ISO 8601 date string, got {val2}')
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No jobs configuration found at {self.jobs_path}, using default...")
                return JobsDataImpl({})
            Log.critical(label or Label.CONFIG, f"Failed to load jobs configuration from {self.jobs_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_jobs_config(self, data: JobsData) -> bool:
        """Saves the jobs configuration to jobs.json asynchronously."""
        return await self._save_json_file(self.jobs_path, data)

    async def backup_config(self, *, scheduled: bool) -> Path | None:
        """Creates a zip backup of the current configuration files."""
        if scheduled and not self.backup_count:
            Log.debug(Label.CONFIG, "Scheduled backups are disabled (NEXUS_BACKUP_COUNT is 0). Skipping backup.")
            return
        try:
            base_path = self.backups_scheduled_path if scheduled else self.backups_manual_path
            backup_folder = base_path / f"nexus_tuner_backup_{datetime.now().isoformat(timespec='seconds').replace(':', '-')}"
            await aiofiles.os.makedirs(backup_folder, exist_ok=True)
            backup_path = backup_folder.with_name(f"{backup_folder.name}.zip")
            Log.info(Label.CONFIG, f"Creating backup at {backup_path}")
            async with self.file_lock:
                await aioshutil.copy2(self.providers_path, backup_folder / self.providers_name)
                await aioshutil.copy2(self.discovered_sources_path, backup_folder / self.discovered_sources_name)
                await aioshutil.copy2(self.logical_channels_path, backup_folder / self.logical_channels_name)
                await aioshutil.copy2(self.channel_mappings_path, backup_folder / self.channel_mappings_name)
                await aioshutil.copy2(self.channel_list_path, backup_folder / self.channel_list_name)
                await aioshutil.copy2(self.quality_cache_path, backup_folder / self.quality_cache_name)
                await aioshutil.copy2(self.jobs_path, backup_folder / self.jobs_name)
                await aioshutil.make_archive(str(backup_folder), 'zip', backup_folder)
                await aioshutil.rmtree(backup_folder, ignore_errors=True)
            return backup_path
        except Exception as e:
            Log.error(Label.CONFIG, f"Failed to create backup: {e}")
            return

    async def cleanup_backups(self) -> None:
        """Cleans up old scheduled backups, keeping only the most recent N backups."""
        try:
            backup_names = sorted(await aiofiles.os.listdir(self.backups_scheduled_path), reverse=True)
        except Exception as e:
            Log.error(Label.CONFIG, f"Failed to get backups in {self.backups_scheduled_path}: {e}")
            return
        for backup_name in backup_names[self.backup_count:]:
            backup_path = self.backups_scheduled_path / backup_name
            try:
                await aiofiles.os.remove(backup_path)
                Log.debug(Label.CONFIG, f"Removed old backup: {backup_path}")
            except Exception as e:
                Log.error(Label.CONFIG, f"Failed to remove old backup {backup_path}: {e}")
