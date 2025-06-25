import os
import json
import re
import threading
import logging
import time
import shutil
from enum import Enum
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Callable, NewType

VideoKey = NewType("VideoKey", str)
VideoName = NewType("VideoName", str)


class VideoType(Enum):
    HLS = "hls"
    MPEGTS = "mpegts"


# --- Constants ---
NOT_ALPHANUM_REGEX = re.compile(r'[^a-zA-Z0-9_-]')


class Config:
    """
    Manages all application configuration, file paths, and logging.
    
    Loads settings from environment variables and provides methods for
    accessing and persisting data to JSON files in a thread-safe manner.
    """
    def __init__(self) -> None:
        """Initializes the configuration object."""
        load_dotenv()
        config_dir_str = os.getenv("NEXUS_CONFIG_DIR")
        if not config_dir_str:
            raise ValueError("NEXUS_CONFIG_DIR environment variable is not set on docker container or system.")
        config_dir = Path(config_dir_str)
        env_file = config_dir / ".env"
        if env_file.exists():
            load_dotenv(env_file)
        
        nexus_url = os.getenv("NEXUS_URL")
        if not nexus_url:
            raise ValueError("NEXUS_URL environment variable is not set.")
        self.nexus_url: str = nexus_url
        self.nexus_port = int(os.getenv("NEXUS_PORT", 4040))
        

        # --- Logging ---
        self.logs_dir: Path = config_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self._loggers: dict[str, logging.Logger] = {}
        self.log_level: str = os.getenv("NEXUS_LOG_LEVEL", "INFO").upper()
        self.log_backup_count = int(os.getenv("NEXUS_LOG_BACKUP_COUNT", 7))
        self.ffmpeg_logs_retention_seconds = int(os.getenv("NEXUS_FFMPEG_LOGS_RETENTION_SECONDS", 86400))

        # --- JSON Data Paths ---
        config_dir.mkdir(parents=True, exist_ok=True)
        self.providers_path: Path = config_dir / "providers.json"
        self.logical_channels_path: Path = config_dir / "logical_channels.json"
        self.channel_mappings_path: Path = config_dir / "channel_mappings.json"
        self.service_quality_cache_path: Path = config_dir / "service_quality_cache.json"
        self.channel_list_path: Path = config_dir / "channel_list.json"
        if not self.channel_list_path.exists():
            self.log_message(f"Creating default channel list at {self.channel_list_path}", level="DEBUG")
            shutil.copy(Path(__file__).parent / "channel_list.json.default", self.channel_list_path)
        
        # --- HLS Segment Directory ---
        self.hls_base_segment_dir: Path = config_dir / "hls_segments"
        self.log_message(f"Cleaning HLS directory at startup: {self.hls_base_segment_dir}", level="DEBUG")
        shutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)
        self.hls_base_segment_dir.mkdir(parents=True, exist_ok=True)
        
        # --- FFmpeg & HLS Configs ---
        self.hls_segment_duration: int = 1  # This allows for a faster time to prune inactive streams
        self.segment_prune_timeout: int = 3  # This should be greater than hls_segment_duration by at least a few seconds
        self.hls_playlist_length: int = 30
        self.ffmpeg_start_timeout: int = 5  # Balance between getting the best source and being able to test more sources
        self.ffmpeg_inactivity_timeout: int = int(os.getenv("NEXUS_FFMPEG_INACTIVITY_TIMEOUT", 900))
        self.ffmpeg_path: str = os.getenv("FFMPEG_PATH", "/usr/bin/ffmpeg")
        self.ffmpeg_logs_dir: Path = self.logs_dir / "ffmpeg_logs"
        self.ffmpeg_logs_dir.mkdir(parents=True, exist_ok=True)

        # --- Media Server Monitoring Configs ---
        self.emby_url: str | None = os.getenv("NEXUS_EMBY_URL")
        self.emby_api_key: str | None = os.getenv("NEXUS_EMBY_API_KEY")
        self.jellyfin_url: str | None = os.getenv("NEXUS_JELLYFIN_URL")
        self.jellyfin_api_key: str | None = os.getenv("NEXUS_JELLYFIN_API_KEY")
        self.ghost_check_interval: int = int(os.getenv("NEXUS_GHOST_SESSION_CHECK_INTERVAL", 60))

        self.file_lock = threading.Lock()

    def clean_up_hls_segments(self) -> None:
        """Cleans up old HLS segment files in the configured directory."""
        self.log_message(f"Cleaning up HLS segments in {self.hls_base_segment_dir}", level="INFO")
        shutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)

    def get_fs_safe_alphanum(self, name: str) -> str:
        """
        Converts a string to a filesystem-safe alphanumeric format.

        Replaces non-alphanumeric characters with underscores.

        Args:
            name: The input string to sanitize.

        Returns:
            A sanitized string suitable for filesystem use.
        """
        return re.sub(NOT_ALPHANUM_REGEX, '_', name)

    def get_ffmpeg_log_path(self, logical_channel_id: str) -> Path:
        """
        Generates a safe file path for an FFmpeg log file.

        Args:
            logical_channel_id: The ID of the logical channel.
            provider_alias: The alias of the provider being used.

        Returns:
            A Path object for the log file.
        """
        log_filename = f"ffmpeg_{self.get_fs_safe_alphanum(logical_channel_id)}.log" 
        return self.ffmpeg_logs_dir / log_filename
    
    def cleanup_ffmpeg_logs_by_age(self) -> None:
        """
        Deletes FFmpeg log files older than a configured number of days.
        """
        if not self.ffmpeg_logs_dir.exists():
            self.log_message(f"No FFmpeg logs directory at {self.ffmpeg_logs_dir}. Skipping cleanup.", level="DEBUG")
            return

        cutoff_time = time.time() - self.ffmpeg_logs_retention_seconds
        files_cleaned_up = False
        for log_file in self.ffmpeg_logs_dir.glob("ffmpeg_*.log"):
            try:
                if log_file.stat().st_mtime < cutoff_time:
                    log_file.unlink()
                    files_cleaned_up = True
            except OSError as e:
                self.log_message(f"Error deleting old log file {log_file}: {e}", level="ERROR")
        if files_cleaned_up:
            self.log_message(f"Cleaned up FFmpeg log files older than 24 hours.", level="DEBUG")

    def _load_json_file(self, file_path: Path, default_content_factory: Callable[[], Any]) -> Any:
        """
        Loads data from a JSON file in a thread-safe manner.

        If the file doesn't exist or is empty/invalid, it creates it with
        default content.

        Args:
            file_path: The path to the JSON file.
            default_content_factory: A function (e.g., `dict` or `list`) to generate default content.

        Returns:
            The parsed JSON data.
        """
        with self.file_lock:
            try:
                if not file_path.exists():
                    self.log_message(f"{file_path} not found. Creating with default content.", level="DEBUG")
                    default_content = default_content_factory()
                    with file_path.open("w") as f:
                        json.dump(default_content, f, indent=2)
                    return default_content
                
                with file_path.open("r") as f:
                    content = f.read()
                    if not content.strip():
                         self.log_message(f"{file_path} is empty. Initializing with default content.", level="DEBUG")
                         default_content = default_content_factory()
                         with file_path.open("w") as wf:
                            json.dump(default_content, wf, indent=2)
                         return default_content
                    return json.loads(content)
            except (json.JSONDecodeError, OSError) as e:
                self.log_message(f"Could not load or parse {file_path}: {e}. Returning default.", level="ERROR")
                return default_content_factory()

    def _save_json_file(self, file_path: Path, data: Any) -> bool:
        """
        Saves data to a JSON file atomically and in a thread-safe manner.

        Uses a temporary file and an atomic `os.replace` to prevent data corruption.

        Args:
            file_path: The path to the JSON file to save.
            data: The Python object (dict, list, etc.) to serialize.

        Returns:
            True if the save was successful, False otherwise.
        """
        with self.file_lock:
            try:
                temp_file_path = file_path.with_suffix(file_path.suffix + '.tmp')
                with temp_file_path.open("w") as f:
                    json.dump(data, f, indent=2)
                os.replace(temp_file_path, file_path)
                return True
            except (IOError, OSError) as e:
                self.log_message(f"Could not write to {file_path}: {e}", level="ERROR")
                return False
            
    def log_message(self, message: str, log_filename: str = "app.log", level: str = "INFO") -> None:
        """
        Logs a message to a specified file and the console.

        Manages logger instances to avoid duplicating handlers.

        Args:
            message: The message to log.
            log_filename: The file to log to (within the logs directory).
            level: The logging level (e.g., "INFO", "ERROR", "DEBUG").
        """
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

    def get_providers_config(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Loads the providers configuration from providers.json."""
        return self._load_json_file(self.providers_path, lambda: {"source_m3u_providers": {}})

    def save_providers_config(self, data: dict[str, dict[str, dict[str, Any]]]) -> bool:
        """Saves the providers configuration to providers.json."""
        return self._save_json_file(self.providers_path, data)

    def get_logical_channels_config(self) -> list[dict[str, Any]]:
        """Loads the logical channels configuration from logical_channels.json."""
        return self._load_json_file(self.logical_channels_path, list)

    def save_logical_channels_config(self, data: list[dict[str, Any]]) -> bool:
        """Saves the logical channels configuration to logical_channels.json."""
        for channel in data:  # These should values don't make sense to save
            channel.pop("lowest_uptime", None)
            channel.pop("health_score", None)
        return self._save_json_file(self.logical_channels_path, data)

    def get_channel_mappings_config(self) -> dict[str, Any]:
        """Loads the channel mappings from channel_mappings.json."""
        return self._load_json_file(self.channel_mappings_path, dict)
    
    def get_channel_list_config(self) -> dict[str, Any]:
        """Loads the predefined channel list from channel_list.json."""
        return self._load_json_file(self.channel_list_path, dict)

    def save_channel_mappings_config(self, data: dict[str, Any]) -> bool:
        """Saves the channel mappings to channel_mappings.json."""
        return self._save_json_file(self.channel_mappings_path, data)

    def get_service_quality_cache(self) -> dict[str, Any]:
        """Loads the service quality cache from service_quality_cache.json."""
        return self._load_json_file(self.service_quality_cache_path, dict)

    def save_service_quality_cache(self, data: dict[str, Any]) -> bool:
        """Saves the service quality cache to service_quality_cache.json."""
        return self._save_json_file(self.service_quality_cache_path, data)

    def reload_all_configs(self) -> None:
        """Logs a message indicating that configs should be reloaded by handlers."""
        self.log_message("Signalling reload of all JSON configurations.", level="INFO")