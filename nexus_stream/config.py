from datetime import datetime
import os
import json
import re
import logging
import time
from enum import StrEnum
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Callable, NewType, Self

import asyncio
import aiofiles
import aiofiles.os
import aioshutil

VideoKey = NewType("VideoKey", str)
VideoName = NewType("VideoName", str)
NEXUS_STREAM_VERSION = (Path(__file__).parent.parent / "VERSION").read_text().strip()
NEXUS_STREAM_USER_AGENT = f"NexusStream/{NEXUS_STREAM_VERSION}"

class VideoType(StrEnum):
    HLS = "hls"
    MPEGTS = "mpegts"

class Label(StrEnum):
    STARTUP = "startup"
    SERVER = "server"
    CONFIG = "config"
    HANDLER = "handler"
    STREAM = "stream"
    QUALITY = "quality"
    SESSION = "session"

# --- Constants ---
NOT_ALPHANUM_REGEX = re.compile(r'[^a-zA-Z0-9_-]')
CREATE_STREAM_DEADLINE = 25  # The maximum time that clients will wait for a stream to be created
NEW_DEADLINE_NON_BEST = 1  # The number of seconds after a stream is healthy before giving up waiting on others, the best remaining source deadline is immediate


class Config:
    """
    Manages all application configuration, file paths, and logging.
    
    Loads settings from environment variables and provides async methods for
    accessing and persisting data to JSON files in a non-blocking, safe manner.
    """
    def __init__(self) -> None:
        """
        Initializes the configuration object.
        
        NOTE: This constructor is now lightweight and performs no I/O.
        All file system operations are deferred to the async `_initialize` method,
        which is called by the `create` factory method.
        """
        load_dotenv()
        config_dir_str = os.getenv("NEXUS_CONFIG_DIR")
        if not config_dir_str:
            raise ValueError("NEXUS_CONFIG_DIR environment variable is not set on docker container or system.")
        self.config_dir = Path(config_dir_str)
        env_file = self.config_dir / ".env"
        if env_file.exists():
            load_dotenv(env_file)
        
        nexus_url = os.getenv("NEXUS_URL")
        if not nexus_url:
            raise ValueError("NEXUS_URL environment variable is not set.")
        self.nexus_url: str = nexus_url
        self.nexus_port = int(os.getenv("NEXUS_PORT", 4040))
        
        # --- Logging ---
        self.logs_dir: Path = self.config_dir / "logs"
        self._logger: logging.Logger
        self.log_level: str = os.getenv("NEXUS_LOG_LEVEL", "INFO").upper()
        self.log_backup_count = int(os.getenv("NEXUS_LOG_BACKUP_COUNT", 7))
        self.ffmpeg_logs_retention_seconds = int(os.getenv("NEXUS_FFMPEG_LOGS_RETENTION_SECONDS", 86400))

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

        # --- Backup Config ---
        self.backups_path: Path = self.config_dir / "backups"

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

        self.file_lock = asyncio.Lock()
        self._cleaning_up_ffmpeg_logs = False

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
        await aiofiles.os.makedirs(self.backups_path, exist_ok=True)

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
        datefmt_str = "%Y-%m-%d %H:%M:%S"

        class ColoredFormatter(logging.Formatter):
            grey = "\x1b[38;20m"
            green = "\x1b[32;20m"
            yellow = "\x1b[33;20m"
            red = "\x1b[31;20m"
            bold_red = "\x1b[31;1m"
            reset = "\x1b[0m"

            FORMATS = {
                logging.DEBUG: format_str.replace("%(levelname)s", f"{grey}%(levelname)s{reset}"),
                logging.INFO: format_str.replace("%(levelname)s", f"{green}%(levelname)s{reset}"),
                logging.WARNING: format_str.replace("%(levelname)s", f"{yellow}%(levelname)s{reset}"),
                logging.ERROR: format_str.replace("%(levelname)s", f"{red}%(levelname)s{reset}"),
                logging.CRITICAL: format_str.replace("%(levelname)s", f"{bold_red}%(levelname)s{reset}"),
            }

            def format(self, record: logging.LogRecord) -> str:
                log_fmt = self.FORMATS.get(record.levelno, format_str)
                formatter = logging.Formatter(log_fmt, datefmt=datefmt_str)
                return formatter.format(record)

        file_handler.setFormatter(logging.Formatter(format_str, datefmt=datefmt_str))
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

    def get_segment_format(self) -> str:
        """Returns the format string for HLS segment files."""
        return "segment_%05d.ts"

    def get_segment_number(self, segment_filename: str) -> int:
        """Extracts the segment number from the segment filename."""
        return int(segment_filename.split('_')[1].split('.')[0])

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

    async def _load_json_file(self, file_path: Path, default_content_factory: Callable[[], Any]) -> Any:
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

    async def _save_json_file(self, file_path: Path, data: Any) -> bool:
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

    async def get_providers_config(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Loads the providers configuration from providers.json asynchronously."""
        return await self._load_json_file(self.providers_path, lambda: {"source_m3u_providers": {}})

    async def save_providers_config(self, data: dict[str, dict[str, dict[str, Any]]]) -> bool:
        """Saves the providers configuration to providers.json asynchronously."""
        return await self._save_json_file(self.providers_path, data)

    async def get_discovered_source_services_config(self) -> dict[str, dict[str, Any]]:
        """Loads the discovered source services from discovered_source_services.json asynchronously."""
        return await self._load_json_file(self.discovered_source_services_path, dict)

    async def save_discovered_source_services_config(self, data: dict[str, dict[str, Any]]) -> bool:
        """Saves the discovered source services to discovered_source_services.json asynchronously."""
        return await self._save_json_file(self.discovered_source_services_path, data)

    async def get_logical_channels_config(self) -> list[dict[str, Any]]:
        """Loads the logical channels configuration from logical_channels.json asynchronously."""
        return await self._load_json_file(self.logical_channels_path, list)

    async def save_logical_channels_config(self, data: list[dict[str, Any]]) -> bool:
        """Saves the logical channels configuration to logical_channels.json asynchronously."""
        for channel in data:
            channel.pop("lowest_uptime", None)
            channel.pop("health_score", None)
            channel.pop("enabled_mappings", None)
            channel.pop("discovered_mappings", None)
        return await self._save_json_file(self.logical_channels_path, data)

    async def get_channel_mappings_config(self) -> dict[str, Any]:
        """Loads the channel mappings from channel_mappings.json asynchronously."""
        return await self._load_json_file(self.channel_mappings_path, dict)
    
    async def get_channel_list_config(self) -> dict[str, Any]:
        """Loads the predefined channel list from channel_list.json asynchronously."""
        return await self._load_json_file(self.channel_list_path, dict)

    async def save_channel_mappings_config(self, data: dict[str, Any]) -> bool:
        """Saves the channel mappings to channel_mappings.json asynchronously."""
        return await self._save_json_file(self.channel_mappings_path, data)

    async def get_service_quality_cache(self) -> dict[str, Any]:
        """Loads the service quality cache from service_quality_cache.json asynchronously."""
        return await self._load_json_file(self.service_quality_cache_path, dict)

    async def save_service_quality_cache(self, data: dict[str, Any]) -> bool:
        """Saves the service quality cache to service_quality_cache.json asynchronously."""
        return await self._save_json_file(self.service_quality_cache_path, data)

    async def backup_config(self, scheduled: bool) -> Path | None:
        """Creates a zip backup of the current configuration files."""
        try:
            sub_folder = "scheduled" if scheduled else "manual"
            backup_folder = self.backups_path / sub_folder / f"nexus_stream_backup_{datetime.now().isoformat(timespec='seconds').replace(':', '-')}"
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
                await aioshutil.make_archive(str(backup_folder), 'zip', backup_folder)
                await aioshutil.rmtree(backup_folder, ignore_errors=True)
            return backup_path
        except Exception as e:
            self.error(Label.CONFIG, f"Failed to create backup: {e}")
            return
