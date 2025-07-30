from datetime import datetime
import os
import json
import re
import logging
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from typing import Callable, Final, Mapping, Self

import asyncio
import aiofiles.os
import aioshutil

from nexus_stream.utils import (NEXUS_STREAM_PORT, NEXUS_STREAM_VERSION, ChannelListData, ChannelMappingsData, DiscoveredSourcesData, JobsData,
                                Label, LogicalChannelsData, ProvidersData, ProvidersSourceData, ServiceQualityCacheData, VideoKey, VideoType)


NOT_ALPHANUM_REGEX: Final[re.Pattern[str]] = re.compile(r'[^a-zA-Z0-9_-]')


class Config:
    """
    Manages all application configuration, file paths, and logging.
    
    Loads settings from environment variables and provides async methods for
    accessing and persisting data to JSON files in a non-blocking, safe manner.
    """
    __slots__ = (
        'config_dir', 'nexus_url', 'nexus_port',
        'logs_dir', '_logger', 'log_level', 'log_backup_count', 'ffmpeg_logs_retention_seconds',
        'providers_name', 'providers_path',
        'discovered_source_services_name', 'discovered_source_services_path',
        'logical_channels_name', 'logical_channels_path',
        'channel_mappings_name', 'channel_mappings_path',
        'channel_list_name', 'channel_list_path',
        'service_quality_cache_name', 'service_quality_cache_path',
        'jobs_name', 'jobs_path',
        'backups_base_path', 'backups_scheduled_path', 'backups_manual_path', 'backup_count',
        'hls_base_segment_dir', 'hls_segment_duration', 'segment_prune_timeout', 'latest_segment_timeout', 'hls_playlist_length',
        'ffmpeg_start_timeout', 'ffmpeg_inactivity_timeout', 'ffmpeg_path', 'ffmpeg_logs_dir',
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
        load_dotenv()
        config_dir_str: str | None = os.getenv("NEXUS_CONFIG_DIR")
        if not config_dir_str:
            raise ValueError("NEXUS_CONFIG_DIR environment variable is not set on docker container or system.")
        self.config_dir: Path = Path(config_dir_str)
        env_file: Path = self.config_dir / ".env"
        if env_file.exists():
            load_dotenv(env_file)
        
        nexus_url: str | None = os.getenv("NEXUS_URL")
        if not nexus_url:
            raise ValueError("NEXUS_URL environment variable is not set.")
        self.nexus_url: str = nexus_url
        self.nexus_port: int = NEXUS_STREAM_PORT
        
        # --- Logging ---
        self.logs_dir: Path = self.config_dir / "logs"
        self._logger: logging.Logger
        self.log_level: str = os.getenv("NEXUS_LOG_LEVEL", "INFO").upper()
        self.log_backup_count: int = int(os.getenv("NEXUS_LOG_BACKUP_COUNT", 7))
        self.ffmpeg_logs_retention_seconds: int = int(os.getenv("NEXUS_FFMPEG_LOGS_RETENTION_SECONDS", 86400))

        # --- JSON Data Paths ---
        self.providers_name: str = "providers.json"
        self.providers_path: Path = self.config_dir / self.providers_name
        
        self.discovered_source_services_name: str = "discovered_source_services.json"
        self.discovered_source_services_path: Path = self.config_dir / self.discovered_source_services_name
        
        self.logical_channels_name: str = "logical_channels.json"
        self.logical_channels_path: Path = self.config_dir / self.logical_channels_name
        
        self.channel_mappings_name: str = "channel_mappings.json"
        self.channel_mappings_path: Path = self.config_dir / self.channel_mappings_name
        
        self.channel_list_name: str = "channel_list.json"
        self.channel_list_path: Path = self.config_dir / self.channel_list_name
        
        self.service_quality_cache_name: str = "service_quality_cache.json"
        self.service_quality_cache_path: Path = self.config_dir / self.service_quality_cache_name

        self.jobs_name: str = "jobs.json"
        self.jobs_path: Path = self.config_dir / self.jobs_name

        # --- Backup Config ---
        self.backups_base_path: Path = self.config_dir / "backups"
        self.backups_scheduled_path: Path = self.backups_base_path / "scheduled"
        self.backups_manual_path: Path = self.backups_base_path / "manual"
        self.backup_count: int = int(os.getenv("NEXUS_BACKUP_COUNT", 7))

        # --- HLS Segment Directory ---
        self.hls_base_segment_dir: Path = self.config_dir / "hls_segments"
        
        # --- FFmpeg & HLS Configs ---
        self.hls_segment_duration: int = 1
        self.segment_prune_timeout: float = 3
        self.latest_segment_timeout: float = 60
        self.hls_playlist_length: int = 30
        self.ffmpeg_start_timeout: float = 5
        self.ffmpeg_inactivity_timeout: int = int(os.getenv("NEXUS_FFMPEG_INACTIVITY_TIMEOUT", 900))
        self.ffmpeg_path: str = os.getenv("FFMPEG_PATH", "/usr/bin/ffmpeg")
        self.ffmpeg_logs_dir: Path = self.logs_dir / "ffmpeg_logs"

        # --- Media Server Monitoring Configs ---
        self.emby_url: str | None = os.getenv("NEXUS_EMBY_URL")
        self.emby_api_key: str | None = os.getenv("NEXUS_EMBY_API_KEY")
        self.jellyfin_url: str | None = os.getenv("NEXUS_JELLYFIN_URL")
        self.jellyfin_api_key: str | None = os.getenv("NEXUS_JELLYFIN_API_KEY")
        self.ghost_check_interval: int = int(os.getenv("NEXUS_GHOST_SESSION_CHECK_INTERVAL", 60))

        self.file_lock: asyncio.Lock = asyncio.Lock()
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
        self._initialize_logger()
        self.info(Label.STARTUP, f"NexusStream v{NEXUS_STREAM_VERSION}")
        await aiofiles.os.makedirs(self.logs_dir, exist_ok=True)
        await aiofiles.os.makedirs(self.config_dir, exist_ok=True)
        await aiofiles.os.makedirs(self.backups_scheduled_path, exist_ok=True)
        await aiofiles.os.makedirs(self.backups_manual_path, exist_ok=True)

        if not await aiofiles.os.path.exists(self.channel_list_path):
            self.debug(Label.STARTUP, f"Creating default channel list at {self.channel_list_path}")
            default_list_path = Path(__file__).parent / "channel_list.json.default"
            await aioshutil.copy(default_list_path, self.channel_list_path)
        
        self.debug(Label.STARTUP, f"Cleaning HLS directory: {self.hls_base_segment_dir}")
        await aioshutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)
        await aiofiles.os.makedirs(self.hls_base_segment_dir, exist_ok=True)
        
        await aiofiles.os.makedirs(self.ffmpeg_logs_dir, exist_ok=True)

    def _initialize_logger(self) -> None:
        """Initializes the logger"""
        log_filename = "app.log"
        logger = logging.getLogger(log_filename)
        logger.setLevel(self.log_level)
        log_file_path = self.logs_dir / log_filename
        file_handler = TimedRotatingFileHandler(
            log_file_path, when='midnight', backupCount=self.log_backup_count
        )
        format_str = "%(asctime)s.%(msecs)03d %(levelname)s: %(message)s"
        date_fmt = "%Y-%m-%d %H:%M:%S"

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

        file_handler.setFormatter(logging.Formatter(format_str, datefmt=date_fmt))
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ColoredFormatter())
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False
        self._logger = logger

    def debug(self, label: Label | VideoType, msg: str) -> None:
        """Logs a debug message with the specified label."""
        self._logger.debug(f"[{label}] {msg}")

    def info(self, label: Label | VideoType, msg: str) -> None:
        """Logs an info message with the specified label."""
        self._logger.info(f"[{label}] {msg}")

    def warn(self, label: Label | VideoType, msg: str) -> None:
        """Logs a warning message with the specified label."""
        self._logger.warning(f"[{label}] {msg}")

    def error(self, label: Label | VideoType, msg: str) -> None:
        """Logs an error message with the specified label."""
        self._logger.error(f"[{label}] {msg}")

    def critical(self, label: Label | VideoType, msg: str) -> None:
        """Logs a critical message with the specified label."""
        self._logger.critical(f"[{label}] {msg}")

    async def clean_up_hls_segments(self) -> None:
        """Cleans up old HLS segment files in the configured directory asynchronously."""
        self.info(Label.STREAM, f"Cleaning up HLS segments in {self.hls_base_segment_dir}")
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
                self.debug(Label.STREAM, f"No FFmpeg logs directory at {self.ffmpeg_logs_dir}. Skipping cleanup.")
                return

            cutoff_time = time.time() - self.ffmpeg_logs_retention_seconds
            files_cleaned_up = False
            
            try:
                log_files = await aiofiles.os.listdir(self.ffmpeg_logs_dir)
            except OSError as e:
                self.error(Label.STREAM, f"Error listing files in {self.ffmpeg_logs_dir}: {e}")
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
                    self.error(Label.STREAM, f"Error deleting old log file {log_file}: {e}")
            
            if files_cleaned_up:
                self.debug(Label.STREAM, f"Cleaned up FFmpeg log files older than {self.ffmpeg_logs_retention_seconds} seconds.")
        finally:
            self._cleaning_up_ffmpeg_logs = False

    async def _load_json_file[JsonType](self, file_path: Path, default_content_factory: Callable[[], JsonType]) -> JsonType:
        """
        Loads data from a JSON file asynchronously and in a coroutine-safe manner.
        """
        async with self.file_lock:
            try:
                if not await aiofiles.os.path.exists(file_path):
                    self.debug(Label.CONFIG, f"{file_path} not found. Creating with default content.")
                    default_content = default_content_factory()
                    async with aiofiles.open(file_path, "w") as f:
                        await f.write(json.dumps(default_content, indent=2))
                    return default_content
                
                async with aiofiles.open(file_path, "r") as f:
                    content = await f.read()
                    if not content.strip():
                         self.debug(Label.CONFIG, f"{file_path} is empty. Initializing with default content.")
                         default_content = default_content_factory()
                         async with aiofiles.open(file_path, "w") as wf:
                            await wf.write(json.dumps(default_content, indent=2))
                         return default_content
                    return json.loads(content)
            except (json.JSONDecodeError, OSError) as e:
                self.error(Label.CONFIG, f"Could not load or parse {file_path}: {e}. Returning default.")
                return default_content_factory()

    async def _save_json_file[K: str, V](self, file_path: Path, data: Mapping[K, V] | list[V] | tuple[V, ...]) -> bool:
        """
        Saves data to a JSON file atomically and asynchronously.
        """
        async with self.file_lock:
            temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
            try:
                async with aiofiles.open(temp_file_path, "w") as f:
                    await f.write(json.dumps(data, indent=2))
                await aiofiles.os.replace(temp_file_path, file_path)
                return True
            except Exception as e:
                self.error(Label.CONFIG, f"Could not write to {file_path}: {e}")
                if await aiofiles.os.path.exists(temp_file_path):
                    try:
                        await aiofiles.os.remove(temp_file_path)
                    except Exception as remove_error:
                        self.error(Label.CONFIG, f"Error removing temporary file {temp_file_path}: {remove_error}")
                return False

    async def get_providers_config(self) -> ProvidersSourceData:
        """Loads the providers configuration from providers.json asynchronously."""
        return await self._load_json_file(self.providers_path, lambda: ProvidersSourceData({"source_m3u_providers": ProvidersData({})}))

    async def save_providers_config(self, data: ProvidersSourceData) -> bool:
        """Saves the providers configuration to providers.json asynchronously."""
        return await self._save_json_file(self.providers_path, data)

    async def get_discovered_source_services_config(self) -> DiscoveredSourcesData:
        """Loads the discovered source services from discovered_source_services.json asynchronously."""
        return await self._load_json_file(self.discovered_source_services_path, lambda: DiscoveredSourcesData({}))

    async def save_discovered_source_services_config(self, data: DiscoveredSourcesData) -> bool:
        """Saves the discovered source services to discovered_source_services.json asynchronously."""
        return await self._save_json_file(self.discovered_source_services_path, data)

    async def get_logical_channels_config(self) -> LogicalChannelsData:
        """Loads the logical channels configuration from logical_channels.json asynchronously."""
        return await self._load_json_file(self.logical_channels_path, lambda: LogicalChannelsData(()))

    async def save_logical_channels_config(self, data: LogicalChannelsData) -> bool:
        """Saves the logical channels configuration to logical_channels.json asynchronously."""
        return await self._save_json_file(self.logical_channels_path, data)

    async def get_channel_mappings_config(self) -> ChannelMappingsData:
        """Loads the channel mappings from channel_mappings.json asynchronously."""
        return await self._load_json_file(self.channel_mappings_path, lambda: ChannelMappingsData({}))

    async def save_channel_mappings_config(self, data: ChannelMappingsData) -> bool:
        """Saves the channel mappings to channel_mappings.json asynchronously."""
        return await self._save_json_file(self.channel_mappings_path, data)
    
    async def get_channel_list_config(self) -> ChannelListData:
        """Loads the predefined channel list from channel_list.json asynchronously."""
        return await self._load_json_file(self.channel_list_path, lambda: ChannelListData({}))

    async def get_service_quality_cache(self) -> ServiceQualityCacheData:
        """Loads the service quality cache from service_quality_cache.json asynchronously."""
        return await self._load_json_file(self.service_quality_cache_path, lambda: ServiceQualityCacheData({}))

    async def save_service_quality_cache(self, data: ServiceQualityCacheData) -> bool:
        """Saves the service quality cache to service_quality_cache.json asynchronously."""
        return await self._save_json_file(self.service_quality_cache_path, data)

    async def get_jobs_config(self) -> JobsData:
        """Loads the jobs configuration from jobs.json asynchronously."""
        return await self._load_json_file(self.jobs_path, lambda: JobsData({}))

    async def save_jobs_config(self, data: JobsData) -> bool:
        """Saves the jobs configuration to jobs.json asynchronously."""
        return await self._save_json_file(self.jobs_path, data)

    async def backup_config(self, *, scheduled: bool) -> Path | None:
        """Creates a zip backup of the current configuration files."""
        try:
            base_path = self.backups_scheduled_path if scheduled else self.backups_manual_path
            backup_folder = base_path / f"nexus_stream_backup_{datetime.now().isoformat(timespec='seconds').replace(':', '-')}"
            await aiofiles.os.makedirs(backup_folder, exist_ok=True)
            backup_path = backup_folder.with_name(f"{backup_folder.name}.zip")
            self.info(Label.CONFIG, f"Creating backup at {backup_path}")
            async with self.file_lock:
                await aioshutil.copy2(self.providers_path, backup_folder / self.providers_name)
                await aioshutil.copy2(self.discovered_source_services_path, backup_folder / self.discovered_source_services_name)
                await aioshutil.copy2(self.logical_channels_path, backup_folder / self.logical_channels_name)
                await aioshutil.copy2(self.channel_mappings_path, backup_folder / self.channel_mappings_name)
                await aioshutil.copy2(self.channel_list_path, backup_folder / self.channel_list_name)
                await aioshutil.copy2(self.service_quality_cache_path, backup_folder / self.service_quality_cache_name)
                await aioshutil.copy2(self.jobs_path, backup_folder / self.jobs_name)
                await aioshutil.make_archive(str(backup_folder), 'zip', backup_folder)
                await aioshutil.rmtree(backup_folder, ignore_errors=True)
            return backup_path
        except Exception as e:
            self.error(Label.CONFIG, f"Failed to create backup: {e}")
            return

    async def cleanup_backups(self) -> None:
        """Cleans up old scheduled backups, keeping only the most recent N backups."""
        try:
            backup_names = sorted(await aiofiles.os.listdir(self.backups_scheduled_path), reverse=True)
        except Exception as e:
            self.error(Label.CONFIG, f"Failed to get backups in {self.backups_scheduled_path}: {e}")
            return
        for backup_name in backup_names[self.backup_count:]:
            backup_path = self.backups_scheduled_path / backup_name
            try:
                await aiofiles.os.remove(backup_path)
                self.debug(Label.CONFIG, f"Removed old backup: {backup_path}")
            except Exception as e:
                self.error(Label.CONFIG, f"Failed to remove old backup {backup_path}: {e}")
