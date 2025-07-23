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
        self._loggers: dict[str, logging.Logger] = {}
        self.log_level: str = os.getenv("NEXUS_LOG_LEVEL", "INFO").upper()
        self.log_backup_count = int(os.getenv("NEXUS_LOG_BACKUP_COUNT", 7))
        self.ffmpeg_logs_retention_seconds = int(os.getenv("NEXUS_FFMPEG_LOGS_RETENTION_SECONDS", 86400))

        # --- JSON Data Paths ---
        self.providers_path: Path = self.config_dir / "providers.json"
        self.logical_channels_path: Path = self.config_dir / "logical_channels.json"
        self.channel_mappings_path: Path = self.config_dir / "channel_mappings.json"
        self.service_quality_cache_path: Path = self.config_dir / "service_quality_cache.json"
        self.channel_list_path: Path = self.config_dir / "channel_list.json"
        
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
        self.log_message(f"NexusStream v{NEXUS_STREAM_VERSION}", level="INFO")
        await aiofiles.os.makedirs(self.logs_dir, exist_ok=True)
        await aiofiles.os.makedirs(self.config_dir, exist_ok=True)
        
        if not await aiofiles.os.path.exists(self.channel_list_path):
            self.log_message(f"Creating default channel list at {self.channel_list_path}", level="DEBUG")
            default_list_path = Path(__file__).parent / "channel_list.json.default"
            await aioshutil.copy(default_list_path, self.channel_list_path)
        
        self.log_message(f"Cleaning HLS directory at startup: {self.hls_base_segment_dir}", level="DEBUG")
        await aioshutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)
        await aiofiles.os.makedirs(self.hls_base_segment_dir, exist_ok=True)
        
        await aiofiles.os.makedirs(self.ffmpeg_logs_dir, exist_ok=True)

    def get_segment_format(self) -> str:
        """Returns the format string for HLS segment files."""
        return "segment_%05d.ts"

    def get_segment_number(self, segment_filename: str) -> int:
        """Extracts the segment number from the segment filename."""
        return int(segment_filename.split('_')[1].split('.')[0])

    async def clean_up_hls_segments(self) -> None:
        """Cleans up old HLS segment files in the configured directory asynchronously."""
        self.log_message(f"Cleaning up HLS segments in {self.hls_base_segment_dir}", level="INFO")
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
        if not await aiofiles.os.path.exists(self.ffmpeg_logs_dir):
            self.log_message(f"No FFmpeg logs directory at {self.ffmpeg_logs_dir}. Skipping cleanup.", level="DEBUG")
            return

        cutoff_time = time.time() - self.ffmpeg_logs_retention_seconds
        files_cleaned_up = False
        
        try:
            log_files = await aiofiles.os.listdir(self.ffmpeg_logs_dir)
        except OSError as e:
            self.log_message(f"Error listing files in {self.ffmpeg_logs_dir}: {e}", level="ERROR")
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
                self.log_message(f"Error deleting old log file {log_file}: {e}", level="ERROR")
        
        if files_cleaned_up:
            self.log_message(f"Cleaned up FFmpeg log files older than {self.ffmpeg_logs_retention_seconds} seconds.", level="DEBUG")

    async def _load_json_file(self, file_path: Path, default_content_factory: Callable[[], Any]) -> Any:
        """
        Loads data from a JSON file asynchronously and in a coroutine-safe manner.
        """
        async with self.file_lock:
            try:
                if not await aiofiles.os.path.exists(file_path):
                    self.log_message(f"{file_path} not found. Creating with default content.", level="DEBUG")
                    default_content = default_content_factory()
                    async with aiofiles.open(file_path, "w") as f:
                        await f.write(json.dumps(default_content, indent=2))
                    return default_content
                
                async with aiofiles.open(file_path, "r") as f:
                    content = await f.read()
                    if not content.strip():
                         self.log_message(f"{file_path} is empty. Initializing with default content.", level="DEBUG")
                         default_content = default_content_factory()
                         async with aiofiles.open(file_path, "w") as wf:
                            await wf.write(json.dumps(default_content, indent=2))
                         return default_content
                    return json.loads(content)
            except (json.JSONDecodeError, OSError) as e:
                self.log_message(f"Could not load or parse {file_path}: {e}. Returning default.", level="ERROR")
                return default_content_factory()

    async def _save_json_file(self, file_path: Path, data: Any) -> bool:
        """
        Saves data to a JSON file atomically and asynchronously.
        """
        async with self.file_lock:
            try:
                temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
                async with aiofiles.open(temp_file_path, "w") as f:
                    await f.write(json.dumps(data, indent=2))
                await aiofiles.os.replace(temp_file_path, file_path)
                return True
            except (IOError, OSError) as e:
                self.log_message(f"Could not write to {file_path}: {e}", level="ERROR")
                return False
            
    def log_message(self, message: str, log_filename: str = "app.log", level: str = "INFO") -> None:
        """Logs a message to a specified file and the console."""
        log_level_map = {
            "DEBUG": logging.DEBUG, "INFO": logging.INFO,
            "WARN": logging.WARNING, "WARNING": logging.WARNING,
            "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL
        }
        log_level_const = log_level_map.get(level.upper(), logging.INFO)

        if log_filename not in self._loggers:
            logger = logging.getLogger(log_filename)
            logger.setLevel(self.log_level)
            log_file_path = self.logs_dir / log_filename
            file_handler = TimedRotatingFileHandler(
                log_file_path, when='midnight', backupCount=self.log_backup_count
            )
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            file_handler.setFormatter(formatter)
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.propagate = False
            self._loggers[log_filename] = logger

        self._loggers[log_filename].log(log_level_const, message)

    async def get_providers_config(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Loads the providers configuration from providers.json asynchronously."""
        return await self._load_json_file(self.providers_path, lambda: {"source_m3u_providers": {}})

    async def save_providers_config(self, data: dict[str, dict[str, dict[str, Any]]]) -> bool:
        """Saves the providers configuration to providers.json asynchronously."""
        return await self._save_json_file(self.providers_path, data)

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

    async def reload_all_configs(self) -> None:
        """Logs a message indicating that configs should be reloaded by handlers."""
        self.log_message("Signalling reload of all JSON configurations.", level="INFO")