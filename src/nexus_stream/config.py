import os
import json
import re
import threading
import logging
import time
import shutil
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Callable

# --- Constants ---
DATA_DIR_NAME = "data"
HLS_SEGMENT_ROOT_DIR_NAME = "hls_segments"
LOG_BACKUP_COUNT = 7
FFMPEG_LOGS_RETENTION_SECONDS = 86400

class Config:
    """
    Manages all application configuration, file paths, and logging.
    
    Loads settings from environment variables and provides methods for
    accessing and persisting data to JSON files in a thread-safe manner.
    """
    def __init__(self) -> None:
        """Initializes the configuration object."""
        load_dotenv()
        
        base_url = os.environ.get("BASE_URL")
        if not base_url:
            raise ValueError("BASE_URL environment variable is not set.")
        self.base_url: str = base_url

        # --- Logging ---
        project_root = Path.cwd()
        self.logs_dir: Path = project_root / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self._loggers: dict[str, logging.Logger] = {}
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

        # --- JSON Data Paths ---
        data_dir = project_root / DATA_DIR_NAME
        data_dir.mkdir(parents=True, exist_ok=True)
        self.providers_path: Path = data_dir / "providers.json"
        self.logical_channels_path: Path = data_dir / "logical_channels.json"
        self.channel_mappings_path: Path = data_dir / "channel_mappings.json"
        self.channel_list_path: Path = data_dir / "channel_list.json"
        
        # --- HLS Segment Directory ---
        self.hls_base_segment_dir: Path = project_root / HLS_SEGMENT_ROOT_DIR_NAME
        self.log_message(f"Cleaning HLS directory at startup: {self.hls_base_segment_dir}", level="DEBUG")
        shutil.rmtree(self.hls_base_segment_dir, ignore_errors=True)
        self.hls_base_segment_dir.mkdir(parents=True, exist_ok=True)
        
        # --- FFmpeg & HLS Configs ---
        self.hls_segment_duration: int = int(os.getenv("NEXUS_HLS_SEGMENT_DURATION", "3"))
        self.hls_playlist_length: int = int(os.getenv("NEXUS_HLS_PLAYLIST_LENGTH", "10"))
        self.ffmpeg_hls_inactivity_timeout: int = int(os.getenv("NEXUS_FFMPEG_HLS_INACTIVITY_TIMEOUT", "60"))
        self.ffmpeg_start_timeout: int = int(os.getenv("NEXUS_FFMPEG_START_TIMEOUT", "10"))
        self.ffmpeg_path: str = os.getenv("FFMPEG_PATH", "/usr/bin/ffmpeg")
        self.ffmpeg_logs_dir: Path = self.logs_dir / "ffmpeg_logs"
        self.ffmpeg_logs_dir.mkdir(parents=True, exist_ok=True)

        # --- Media Server Monitoring Configs ---
        self.emby_base_url: str | None = os.environ.get("EMBY_BASE_URL")
        self.emby_api_key: str | None = os.environ.get("EMBY_API_KEY")
        self.jellyfin_base_url: str | None = os.environ.get("JELLYFIN_BASE_URL")
        self.jellyfin_api_key: str | None = os.environ.get("JELLYFIN_API_KEY")
        self.ghost_check_interval: int = int(os.getenv("GHOST_SESSION_CHECK_INTERVAL", "60"))

        self.file_lock = threading.Lock()

    def get_ffmpeg_log_path(self, logical_channel_id: str = "unknown_channel", provider_alias: str = "unknown_provider") -> Path:
        """
        Generates a safe file path for an FFmpeg log file.

        Args:
            logical_channel_id: The ID of the logical channel.
            provider_alias: The alias of the provider being used.

        Returns:
            A Path object for the log file.
        """
        safe_lc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', logical_channel_id)
        safe_prov_id = re.sub(r'[^a-zA-Z0-9_-]', '_', provider_alias)
        log_filename = f"ffmpeg_{safe_lc_id}_{safe_prov_id}.log" 
        return self.ffmpeg_logs_dir / log_filename
    
    def cleanup_ffmpeg_logs_by_age(self) -> None:
        """
        Deletes FFmpeg log files older than a configured number of days.
        """
        if not self.ffmpeg_logs_dir.exists():
            self.log_message(f"No FFmpeg logs directory at {self.ffmpeg_logs_dir}. Skipping cleanup.", level="DEBUG")
            return

        cutoff_time = time.time() - FFMPEG_LOGS_RETENTION_SECONDS
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
                log_file_path, when='midnight', backupCount=LOG_BACKUP_COUNT
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

    def get_providers_config(self) -> dict[str, Any]:
        """Loads the providers configuration from providers.json."""
        return self._load_json_file(self.providers_path, lambda: {"source_m3u_providers": {}})

    def save_providers_config(self, data: dict[str, Any]) -> bool:
        """Saves the providers configuration to providers.json."""
        return self._save_json_file(self.providers_path, data)

    def get_logical_channels_config(self) -> list[dict[str, Any]]:
        """Loads the logical channels configuration from logical_channels.json."""
        return self._load_json_file(self.logical_channels_path, list)

    def save_logical_channels_config(self, data: list[dict[str, Any]]) -> bool:
        """Saves the logical channels configuration to logical_channels.json."""
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

    def reload_all_configs(self) -> None:
        """Logs a message indicating that configs should be reloaded by handlers."""
        self.log_message("Signalling reload of all JSON configurations.", level="INFO")