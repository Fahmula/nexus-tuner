from datetime import datetime
import os
import json
import time
from pathlib import Path
from typing import Any, Final, Mapping, Self, cast
from urllib.parse import urlparse

import asyncio
import aiofiles.os
import aioshutil

from nexus_tuner.utils import (CONFIG_DIR, NEXUS_TUNER_PORT, NEXUS_TUNER_VERSION, ChannelListDataImpl, ChannelMappingsData, ChannelMappingsDataImpl, DiscoveredSourcesData, DiscoveredSourcesDataImpl, JobName, JobsData, JobsDataImpl,
                               Label, Log, LCGroupMode, LogicalChannelsData, LogicalChannelsDataImpl, MAIN_PLAYLIST_ID, MEDIA_SERVER_ID_REGEX, PLAYLIST_ID_REGEX, MediaServerId, MediaServerInfo, MediaServerType, PlaylistsData, PlaylistsDataImpl,
                               ProvidersData, ProvidersDataImpl, QualityCacheData, QualityCacheDataImpl, SettingsInfo, StopReason, StreamEngine, VideoKey, create_media_server_id, default_playlist_info, default_settings,
                               get_fs_safe_alphanum, is_valid_url, normalize_group_key)


# Whole number settings with their display name and minimum value
SETTINGS_INT_FIELDS: Final[dict[str, tuple[str, int]]] = {
    "process_inactivity_timeout": ("Inactivity timeout", 0),
    "process_logs_retention_seconds": ("Process log retention", 0),
    "log_backup_count": ("Log files kept", 0),
    "backup_count": ("Scheduled backups kept", 0),
    "ghost_check_interval": ("Ghost session check interval", 1),
}

# Environment variables used before settings.json existed, they are only read once to create it
LEGACY_ENV_VARS: Final[dict[str, str]] = {
    "NEXUS_URL": "nexus_url",
    "NEXUS_FFMPEG_PATH": "ffmpeg_path",
    "NEXUS_VLC_PATH": "vlc_path",
    "NEXUS_STREAM_ENGINE": "stream_engine",  # After NEXUS_VLC_PATH as the VLC engine requires a VLC path
    "NEXUS_REMOVE_CAPTIONS_AND_SUBTITLES": "remove_captions_and_subtitles",
    "NEXUS_PROCESS_INACTIVITY_TIMEOUT": "process_inactivity_timeout",
    "NEXUS_PROCESS_LOGS_RETENTION_SECONDS": "process_logs_retention_seconds",
    "NEXUS_LOG_BACKUP_COUNT": "log_backup_count",
    "NEXUS_BACKUP_COUNT": "backup_count",
    "NEXUS_GHOST_SESSION_CHECK_INTERVAL": "ghost_check_interval",
}
LEGACY_MEDIA_SERVER_ENV_VARS: Final[tuple[tuple[MediaServerType, str, str, str], ...]] = (
    (MediaServerType.EMBY, "Emby", "NEXUS_EMBY_URL", "NEXUS_EMBY_API_KEY"),
    (MediaServerType.JELLYFIN, "Jellyfin", "NEXUS_JELLYFIN_URL", "NEXUS_JELLYFIN_API_KEY"),
)


def validate_media_server(server: Any) -> MediaServerInfo:
    """Validates and normalizes a media server. Raises ValueError if it is invalid."""
    if type(server) is not dict:
        raise ValueError(f"Media server: expected dict, got {server}")
    server = cast(dict[str, Any], server)
    server_id = server.get("id")
    if type(server_id) is not str or not MEDIA_SERVER_ID_REGEX.match(server_id):
        raise ValueError(f"Media server: invalid ID {server_id}")
    name = server.get("name")
    if type(name) is not str or not name.strip():
        raise ValueError("Media server: a name is required.")
    name = name.strip()
    server_type = server.get("type")
    if server_type not in tuple(MediaServerType):
        raise ValueError(f'Media server "{name}": type must be one of {", ".join(MediaServerType)}, got {server_type}')
    url = server.get("url")
    if type(url) is not str or not is_valid_url(url.strip()):
        raise ValueError(f'Media server "{name}": expected a URL like http://192.168.1.10:8096, got "{url}"')
    api_key = server.get("api_key", "")
    if type(api_key) is not str:
        raise ValueError(f'Media server "{name}": API key must be text.')
    enabled = server.get("enabled", True)
    if type(enabled) is not bool:
        raise ValueError(f'Media server "{name}": enabled must be true or false, got {enabled}')
    if enabled and not api_key.strip():
        raise ValueError(f'Media server "{name}": an API key is required while it is enabled.')
    return {
        "id": MediaServerId(server_id),
        "name": name,
        "type": MediaServerType(server_type),
        "url": url.strip().rstrip("/"),
        "api_key": api_key.strip(),
        "enabled": enabled,
    }


def validate_settings(data: Any) -> SettingsInfo:
    """Validates and normalizes settings, missing keys use their defaults and unknown keys are dropped.
    Raises ValueError with a message that can be shown to the user if the settings are invalid.
    Only checks the format of paths, see Config.get_path_problems() for whether they exist.
    """
    if type(data) is not dict:
        raise ValueError(f"Settings: expected dict, got {type(data)}")
    settings: dict[str, Any] = {key: data.get(key, default) for key, default in default_settings().items()}  # type: ignore[reportUnknownMemberType]

    nexus_url = settings["nexus_url"]
    if type(nexus_url) is not str:
        raise ValueError(f"Fallback URL: expected text, got {nexus_url}")
    nexus_url = nexus_url.strip().rstrip("/")
    if nexus_url and not is_valid_url(nexus_url):
        raise ValueError(f'Fallback URL: expected empty or a URL like http://192.168.1.10, got "{nexus_url}"')
    if nexus_url and urlparse(nexus_url).port is not None:
        raise ValueError(f"Fallback URL: should not contain a port, the port is set by NEXUS_PORT ({NEXUS_TUNER_PORT}).")
    settings["nexus_url"] = nexus_url

    for key, display_name, executable in (("ffmpeg_path", "FFmpeg path", "ffmpeg"), ("vlc_path", "VLC path", "vlc")):
        path = settings[key]
        if type(path) is not str:
            raise ValueError(f"{display_name}: expected text, got {path}")
        path = path.strip()
        if key == "ffmpeg_path" and not path:
            raise ValueError("FFmpeg path: required.")
        if path and executable not in Path(path).name.lower():
            raise ValueError(f'{display_name}: must point to the {executable} executable, got "{path}"')
        settings[key] = path

    if settings["stream_engine"] not in tuple(StreamEngine):
        raise ValueError(f'Stream engine: expected one of {", ".join(StreamEngine)}, got {settings["stream_engine"]}')
    settings["stream_engine"] = StreamEngine(settings["stream_engine"])
    if settings["stream_engine"] == StreamEngine.VLC and not settings["vlc_path"]:
        raise ValueError("Stream engine: VLC requires a VLC path.")

    for key, display_name in (("tls_verify", "Verify HTTPS certificates"), ("remove_captions_and_subtitles", "Remove captions and subtitles")):
        if type(settings[key]) is not bool:
            raise ValueError(f'{display_name}: expected true or false, got {settings[key]}')

    for key, (display_name, minimum) in SETTINGS_INT_FIELDS.items():
        if type(settings[key]) is not int or settings[key] < minimum:
            raise ValueError(f"{display_name}: expected a whole number of at least {minimum}, got {settings[key]}")

    if type(settings["media_servers"]) not in (list, tuple):
        raise ValueError(f'Media servers: expected list, got {settings["media_servers"]}')
    servers = tuple(validate_media_server(server) for server in settings["media_servers"])
    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    for server in servers:
        if server["id"] in seen_ids:
            raise ValueError(f'Media server "{server["name"]}": ID {server["id"]} is used more than once.')
        if server["name"].casefold() in seen_names:
            raise ValueError(f'Media server "{server["name"]}": another server already has this name.')
        seen_ids.add(server["id"])
        seen_names.add(server["name"].casefold())
    settings["media_servers"] = servers
    return cast(SettingsInfo, settings)


def seed_settings_from_env() -> tuple[SettingsInfo, list[str]]:
    """Creates settings from the environment variables used before settings.json existed.
    Invalid values are skipped with a warning so that the application always starts.
    """
    settings = default_settings()
    warnings: list[str] = []
    for env_var, key in LEGACY_ENV_VARS.items():
        raw = os.getenv(env_var, "").strip()
        if not raw:
            continue
        default = cast(Mapping[str, Any], settings)[key]
        try:
            value: Any = raw
            if type(default) is bool:
                if raw.lower() not in ("true", "false"):
                    raise ValueError("expected 'true' or 'false'")
                value = raw.lower() == "true"
            elif type(default) is int:
                value = int(raw)
            settings = validate_settings({**settings, key: value})
        except ValueError as e:
            warnings.append(f"Ignored {env_var}={raw}: {e}")

    for server_type, name, url_env_var, api_key_env_var in LEGACY_MEDIA_SERVER_ENV_VARS:
        url = os.getenv(url_env_var, "").strip()
        if not url:
            continue
        api_key = os.getenv(api_key_env_var, "").strip()
        server: MediaServerInfo = {"id": create_media_server_id(), "name": name, "type": server_type, "url": url, "api_key": api_key, "enabled": bool(api_key)}
        try:
            settings = validate_settings({**settings, "media_servers": (*settings["media_servers"], server)})
        except ValueError as e:
            warnings.append(f"Ignored {url_env_var}={url}: {e}")
            continue
        if not api_key:
            warnings.append(f"{name} media server was added disabled because {api_key_env_var} is not set, add its API key in Settings.")
    return settings, warnings


class Config:
    """
    Manages all application configuration, file paths, and logging.
    
    Settings are managed in the web UI and stored in settings.json, they are applied without a restart.
    Provides async methods for accessing and persisting data to JSON files in a non-blocking, safe manner.
    """
    __slots__ = (
        'logs_dir',
        'settings_name', 'settings_path',
        'providers_name', 'providers_path',
        'discovered_sources_name', 'discovered_sources_path',
        'logical_channels_name', 'logical_channels_path',
        'channel_mappings_name', 'channel_mappings_path',
        'channel_list_name', 'channel_list_path', 'channel_list_default_path',
        'playlists_name', 'playlists_path',
        'quality_cache_name', 'quality_cache_path',
        'jobs_name', 'jobs_path',
        'backups_base_path', 'backups_scheduled_path', 'backups_manual_path',
        'tmp_dir', 'hls_segment_duration', 'segment_prune_timeout', 'latest_segment_timeout', 'hls_playlist_length',
        'process_start_timeout', 'process_logs_dir',
        'file_lock', '_cleaning_up_process_logs', '_settings', '_settings_lock'
    )
    
    def __init__(self) -> None:
        """
        Initializes the configuration object.
        
        NOTE: This constructor is now lightweight and performs no I/O.
        All file system operations are deferred to the async `_initialize` method,
        which is called by the `create` factory method.
        """

        # --- Settings ---

        self._settings: SettingsInfo = default_settings()  # Replaced with settings.json in _initialize()
        self._settings_lock: Final[asyncio.Lock] = asyncio.Lock()
        self.settings_name: Final[str] = "settings.json"
        self.settings_path: Final[Path] = CONFIG_DIR / self.settings_name

        # --- Logging ---

        self.logs_dir: Final[Path] = CONFIG_DIR / "logs"

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

        self.playlists_name: Final[str] = "playlists.json"
        self.playlists_path: Final[Path] = CONFIG_DIR / self.playlists_name
        
        self.quality_cache_name: Final[str] = "quality_cache.json"
        self.quality_cache_path: Final[Path] = CONFIG_DIR / self.quality_cache_name

        self.jobs_name: Final[str] = "jobs.json"
        self.jobs_path: Final[Path] = CONFIG_DIR / self.jobs_name

        # --- Backup Config ---

        self.backups_base_path: Final[Path] = CONFIG_DIR / "backups"
        self.backups_scheduled_path: Final[Path] = self.backups_base_path / "scheduled"
        self.backups_manual_path: Final[Path] = self.backups_base_path / "manual"

        # --- Temp Directory ---

        self.tmp_dir: Final[Path] = CONFIG_DIR / "tmp"
        
        # --- Process & HLS Configs ---

        self.hls_segment_duration: Final[int] = 1
        self.segment_prune_timeout: Final[float] = 3
        self.latest_segment_timeout: Final[float] = 60
        self.hls_playlist_length: Final[int] = 30
        self.process_start_timeout: Final[float] = 5
        self.process_logs_dir: Final[Path] = self.logs_dir / "process_logs"

        self.file_lock: Final[asyncio.Lock] = asyncio.Lock()
        self._cleaning_up_process_logs: bool = False

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

    # --- Settings ---

    @property
    def settings(self) -> SettingsInfo:
        return self._settings

    @property
    def nexus_url(self) -> str:
        """Fallback URL used when the address a client connected with is unknown."""
        return f"{self._settings['nexus_url'] or 'http://localhost'}:{NEXUS_TUNER_PORT}"

    @property
    def ffmpeg_path(self) -> Path:
        return Path(self._settings["ffmpeg_path"])

    @property
    def ffprobe_path(self) -> Path:
        ffmpeg_path = self.ffmpeg_path
        return ffmpeg_path.with_name(ffmpeg_path.name.replace("ffmpeg", "ffprobe"))

    @property
    def vlc_path(self) -> Path | None:
        return Path(self._settings["vlc_path"]) if self._settings["vlc_path"] else None

    @property
    def stream_engine(self) -> StreamEngine:
        return self._settings["stream_engine"]

    @property
    def tls_verify(self) -> bool:
        return self._settings["tls_verify"]

    @property
    def remove_captions_and_subtitles(self) -> bool:
        return self._settings["remove_captions_and_subtitles"]

    @property
    def process_inactivity_timeout(self) -> int:
        return self._settings["process_inactivity_timeout"]

    @property
    def process_logs_retention_seconds(self) -> int:
        return self._settings["process_logs_retention_seconds"]

    @property
    def log_backup_count(self) -> int:
        return self._settings["log_backup_count"]

    @property
    def backup_count(self) -> int:
        return self._settings["backup_count"]

    @property
    def ghost_check_interval(self) -> int:
        return self._settings["ghost_check_interval"]

    @property
    def media_servers(self) -> tuple[MediaServerInfo, ...]:
        return self._settings["media_servers"]

    def get_media_server(self, server_id: MediaServerId) -> MediaServerInfo | None:
        return next((server for server in self.media_servers if server["id"] == server_id), None)

    def get_engine_problem(self, stream_engine: StreamEngine) -> str | None:
        """Returns why streams cannot be created with the stream engine, or None if its executables are usable."""
        if stream_engine == StreamEngine.FFMPEG:
            executables = (("FFmpeg", self.ffmpeg_path), ("FFprobe", self.ffprobe_path))
        elif self.vlc_path:
            executables = (("VLC", self.vlc_path),)
        else:
            return "VLC path is not set."
        for display_name, path in executables:
            if not os.path.isfile(path):
                return f"{display_name} was not found at '{path}'."
            if not os.access(path, os.X_OK):
                return f"{display_name} at '{path}' is not executable, check its permissions."
        return None

    def get_path_problems(self) -> list[str]:
        """Returns problems with the configured executables, FFmpeg is always needed for quality analysis."""
        problems: list[str] = []
        if (problem := self.get_engine_problem(StreamEngine.FFMPEG)):
            problems.append(problem)
        if (self.vlc_path or self.stream_engine == StreamEngine.VLC) and (problem := self.get_engine_problem(StreamEngine.VLC)):
            problems.append(problem)
        return problems

    async def update_settings(self, changes: Mapping[str, Any]) -> set[str]:
        """Validates, saves and applies the changed settings, returning the keys whose value changed.
        Raises ValueError if the settings are invalid and RuntimeError if they could not be saved.
        """
        async with self._settings_lock:
            return await self._save_and_apply_settings({**self._settings, **changes})

    async def add_media_server(self, server: MediaServerInfo) -> None:
        """Adds a media server, raises like update_settings()."""
        async with self._settings_lock:
            await self._save_and_apply_settings({**self._settings, "media_servers": (*self._settings["media_servers"], server)})

    async def update_media_server(self, server: MediaServerInfo) -> None:
        """Replaces the media server with the same ID, raises like update_settings()."""
        async with self._settings_lock:
            if not any(existing["id"] == server["id"] for existing in self._settings["media_servers"]):
                raise ValueError(f"Media server '{server['id']}' not found.")
            servers = tuple(server if existing["id"] == server["id"] else existing for existing in self._settings["media_servers"])
            await self._save_and_apply_settings({**self._settings, "media_servers": servers})

    async def delete_media_server(self, server_id: MediaServerId) -> None:
        """Deletes a media server, raises like update_settings()."""
        async with self._settings_lock:
            servers = tuple(server for server in self._settings["media_servers"] if server["id"] != server_id)
            if len(servers) == len(self._settings["media_servers"]):
                raise ValueError(f"Media server '{server_id}' not found.")
            await self._save_and_apply_settings({**self._settings, "media_servers": servers})

    async def reload_settings(self) -> set[str] | None:
        """Reloads settings.json, e.g. after it was edited by hand. Returns the changed keys, or None if it could not be loaded."""
        async with self._settings_lock:
            settings = await self.get_settings_config()
            if settings is None:
                return None
            return self._apply_settings(settings)

    async def _save_and_apply_settings(self, data: Mapping[str, Any]) -> set[str]:
        """Must be called with _settings_lock held."""
        settings = validate_settings(dict(data))
        if not await self.save_settings_config(settings):
            raise RuntimeError(f"Could not save {self.settings_name}, see the logs for details.")
        return self._apply_settings(settings)

    def _apply_settings(self, settings: SettingsInfo) -> set[str]:
        """Swaps in the new settings in a single assignment so readers always see a consistent snapshot."""
        old_settings = cast(Mapping[str, Any], self._settings)
        new_settings = cast(Mapping[str, Any], settings)
        self._settings = settings
        changed = {key for key in new_settings if new_settings[key] != old_settings[key]}
        if "log_backup_count" in changed:
            Log.set_backup_count(settings["log_backup_count"])
        for key in sorted(changed):
            if key == "media_servers":
                servers = ", ".join(f"{server['name']} ({server['type']}{'' if server['enabled'] else ', disabled'})" for server in settings["media_servers"])
                Log.info(Label.CONFIG, f"Setting media_servers changed to: {servers or 'none'}")
            else:
                Log.info(Label.CONFIG, f"Setting {key} changed from {json.dumps(old_settings[key])} to {json.dumps(new_settings[key])}")
        return changed

    async def _load_or_seed_settings(self) -> None:
        """Loads settings.json, creating it from the legacy environment variables on the first start."""
        legacy_env_vars = [*LEGACY_ENV_VARS, *(env_var for _, _, *env_vars in LEGACY_MEDIA_SERVER_ENV_VARS for env_var in env_vars)]
        set_legacy_env_vars = [env_var for env_var in legacy_env_vars if os.getenv(env_var)]
        if not await aiofiles.os.path.exists(self.settings_path):
            settings, warnings = seed_settings_from_env()
            if not await self.save_settings_config(settings):
                raise RuntimeError(f"Could not create {self.settings_path}")
            source = "environment variables" if set_legacy_env_vars else "default settings"
            Log.info(Label.STARTUP, f"Created {self.settings_path} from {source}, settings are now managed in the web UI.")
            for warning in warnings:
                Log.warn(Label.STARTUP, warning)
        else:
            loaded_settings = await self.get_settings_config(label=Label.STARTUP)
            if loaded_settings is None:
                raise RuntimeError(f"Could not load {self.settings_path}, fix or delete the file to recreate it.")
            settings = loaded_settings
            if (ignored := set_legacy_env_vars):
                Log.info(Label.STARTUP, f"Ignoring environment variables {', '.join(ignored)}, settings are managed in the web UI ({self.settings_name}).")
        self._settings = settings
        Log.set_backup_count(settings["log_backup_count"])
        for problem in self.get_path_problems():
            Log.error(Label.STARTUP, f"{problem} Streams cannot be created until this is fixed in Settings.")

    async def get_settings_config(self, *, label: Label | None = None) -> SettingsInfo | None:
        """Loads the settings from settings.json."""
        try:
            return validate_settings(await self._load_json_file(self.settings_path))
        except BaseException as e:
            Log.critical(label or Label.CONFIG, f"Failed to load settings from {self.settings_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_settings_config(self, data: SettingsInfo) -> bool:
        """Saves the settings to settings.json."""
        return await self._save_json_file(self.settings_path, data)

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

        await self._load_or_seed_settings()

        # If all the JSON config files do not exist, create them for first time setup
        if all([not await aiofiles.os.path.exists(path) for path in (
            self.providers_path,
            self.discovered_sources_path,
            self.logical_channels_path,
            self.channel_mappings_path,
            self.channel_list_path,
            self.playlists_path,
            self.quality_cache_path,
            self.jobs_path
        )]):  # Create all except jobs, it's handled by scheduler
            Log.info(Label.STARTUP, "No configuration files found, creating defaults...")
            await self.save_providers_config(ProvidersDataImpl({}))
            await self.save_discovered_sources_config(DiscoveredSourcesDataImpl({}))
            await self.save_logical_channels_config(LogicalChannelsDataImpl({}))
            await self.save_channel_mappings_config(ChannelMappingsDataImpl({}))
            await self.save_quality_cache(QualityCacheDataImpl({}))
            await self.save_playlists_config(PlaylistsDataImpl({MAIN_PLAYLIST_ID: default_playlist_info()}))
            await aioshutil.copy(self.channel_list_default_path, self.channel_list_path)
        else:
            if not await aiofiles.os.path.exists(self.channel_list_path):
                Log.debug(Label.STARTUP, f"Creating default channel list at {self.channel_list_path}")
                await aioshutil.copy(self.channel_list_default_path, self.channel_list_path)
            if not await aiofiles.os.path.exists(self.playlists_path):
                Log.info(Label.STARTUP, f"Creating default playlists at {self.playlists_path}")
                await self.save_playlists_config(PlaylistsDataImpl({MAIN_PLAYLIST_ID: default_playlist_info()}))

        await aioshutil.rmtree(self.tmp_dir, ignore_errors=True)
        await aiofiles.os.makedirs(self.tmp_dir, exist_ok=True)
        await aiofiles.os.makedirs(self.process_logs_dir, exist_ok=True)

    async def cleanup_tmp_dir(self) -> None:
        """Cleans up old temporary files."""
        Log.debug(Label.STREAM, f"Cleaning up temporary files in {self.tmp_dir}...")
        await aioshutil.rmtree(self.tmp_dir, ignore_errors=True)

    def get_hls_dir(self, video_key: VideoKey) -> Path:
        """Generates a safe directory path for HLS segments and playlists."""
        return self.tmp_dir / get_fs_safe_alphanum(f"{video_key}_{time.time()}")

    def get_offset_path(self, video_key: VideoKey) -> Path:
        """Generates a safe file path for storing offset information."""
        return self.tmp_dir / f"{get_fs_safe_alphanum(f'{video_key}_{time.time()}')}.wav"

    def get_process_log_path(self, video_key: VideoKey) -> Path:
        """Generates a safe file path for a process log file."""
        log_filename = f"{get_fs_safe_alphanum(f'{video_key}_{time.time()}')}.log" 
        return self.process_logs_dir / log_filename
    
    async def cleanup_process_logs_by_age(self) -> None:
        """
        Deletes process log files older than a configured number of seconds.
        """
        if self._cleaning_up_process_logs:
            return
        try:
            self._cleaning_up_process_logs = True
            if not await aiofiles.os.path.exists(self.process_logs_dir):
                Log.debug(Label.STREAM, f"No process logs directory at {self.process_logs_dir}. Skipping cleanup.")
                return

            cutoff_time = time.time() - self.process_logs_retention_seconds
            files_cleaned_up = False
            
            try:
                log_files = await aiofiles.os.listdir(self.process_logs_dir)
            except OSError as e:
                Log.error(Label.STREAM, f"Error listing files in {self.process_logs_dir}: {e}")
                return

            for filename in log_files:
                if not filename.endswith(".log"):
                    continue
                
                log_file = self.process_logs_dir / filename
                try:
                    stat_result = await aiofiles.os.stat(log_file)
                    if stat_result.st_mtime < cutoff_time:
                        await aiofiles.os.remove(log_file)
                        files_cleaned_up = True
                except OSError as e:
                    Log.error(Label.STREAM, f"Error deleting old log file {log_file}: {e}")
            
            if files_cleaned_up:
                Log.debug(Label.STREAM, f"Cleaned up process log files older than {self.process_logs_retention_seconds} seconds.")
        finally:
            self._cleaning_up_process_logs = False

    async def _load_json_file(self, file_path: Path) -> Any:
        """Loads data from a JSON file."""
        async with self.file_lock:
            async with aiofiles.open(file_path, "r") as f:
                return json.loads(await f.read())

    async def _save_json_file[K: str, V](self, file_path: Path, data: Mapping[K, V] | list[V] | tuple[V, ...]) -> bool:
        """
        Saves data to a JSON file atomically.
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
        """Loads the providers configuration from providers.json."""
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
        """Saves the providers configuration to providers.json."""
        return await self._save_json_file(self.providers_path, data)

    async def get_discovered_sources_config(self, *, label: Label | None = None) -> DiscoveredSourcesDataImpl | None:
        """Loads the discovered sources from discovered_sources.json."""
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
        """Saves the discovered sources to discovered_sources.json."""
        return await self._save_json_file(self.discovered_sources_path, data)

    async def get_logical_channels_config(self, *, label: Label | None = None) -> LogicalChannelsDataImpl | None:
        """Loads the logical channels configuration from logical_channels.json."""
        try:
            data: LogicalChannelsDataImpl = await self._load_json_file(self.logical_channels_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid logical channels configuration format in {self.logical_channels_path}")
            for key1, val1 in data.items():  # type: ignore[reportUnknownVariableType]
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
        """Saves the logical channels configuration to logical_channels.json."""
        return await self._save_json_file(self.logical_channels_path, data)

    async def get_channel_mappings_config(self, *, label: Label | None = None) -> ChannelMappingsDataImpl | None:
        """Loads the channel mappings from channel_mappings.json."""
        try:
            data: ChannelMappingsDataImpl = await self._load_json_file(self.channel_mappings_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid channel mappings configuration format in {self.channel_mappings_path}")
            for key1, val1 in data.items():  # type: ignore[reportUnknownVariableType]
                if type(key1) is not str or not key1.isdigit():
                    raise ValueError(f'"{key1}": expected int str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                for key2, val2 in val1.items():
                    if type(key2) is not str:
                        raise ValueError(f'"{key1}" - "{key2}": expected str, got {type(key2)}')
                    if type(val2) is not dict:
                        raise ValueError(f'"{key1}" - "{key2}": expected dict, got {type(val2)}')
                    val2.setdefault("offset", None)
                    for key3 in ("priority", "offset"):
                        if key3 not in val2:
                            raise ValueError(f'"{key1}" - "{key2}" - missing required key "{key3}"')
                        val3 = val2[key3]
                        if key3 == "priority":
                            if type(val3) is not int or val3 < 0 or val3 > 10:
                                raise ValueError(f'"{key1}" - "{key2}" - "{key3}": expected int between 0 and 10, got {val3}')
                        elif key3 == "offset":
                            if val3 is not None and (type(val3) not in (float, int) or val3 < 0):
                                raise ValueError(f'"{key1}" - "{key2}" - "{key3}": expected non-negative float or null, got {val3}')
                        else:
                            raise ValueError(f'"{key1}" - "{key2}" - unexpected key "{key3}"')
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
        """Saves the channel mappings to channel_mappings.json."""
        return await self._save_json_file(self.channel_mappings_path, data)

    async def get_playlists_config(self, *, label: Label | None = None) -> PlaylistsDataImpl | None:
        """Loads the playlists configuration from playlists.json."""
        try:
            data: PlaylistsDataImpl = await self._load_json_file(self.playlists_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid playlists configuration format in {self.playlists_path}")
            for key1, val1 in data.items():  # type: ignore[reportUnknownVariableType]
                if type(key1) is not str or not PLAYLIST_ID_REGEX.match(key1):
                    raise ValueError(f'"{key1}": expected lowercase alphanumeric str with dashes, got {key1}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                if "mode" in val1 or "groups" in val1:  # Format from an earlier playlists implementation
                    legacy_mode = val1.pop("mode", "exclude")  # type: ignore[reportUnknownMemberType]
                    legacy_groups = val1.pop("groups", [])  # type: ignore[reportUnknownMemberType]
                    val1.setdefault("lc_group_mode", {"exclude": LCGroupMode.INCLUDE_ALL_EXCEPT, "include": LCGroupMode.ONLY_INCLUDE}.get(legacy_mode, legacy_mode))  # type: ignore[reportUnknownArgumentType]
                    val1.setdefault("lc_groups", legacy_groups)
                    Log.info(label or Label.CONFIG, f'"{key1}": Converted playlist from earlier format (mode={legacy_mode}, groups={legacy_groups})')
                    if (legacy_sources := val1.pop("sources", [])):  # type: ignore[reportUnknownMemberType]
                        Log.warn(label or Label.CONFIG, f'"{key1}": Individually selected sources are not supported, they will be removed when the playlist is saved: {legacy_sources}')
                val1.setdefault("lc_group_mode", LCGroupMode.INCLUDE_ALL_EXCEPT)
                val1.setdefault("lc_groups", [])
                val1.setdefault("source_groups", [])
                val1.setdefault("base_url", "")
                for key2 in ("name", "lc_group_mode", "lc_groups", "source_groups", "base_url"):
                    if key2 not in val1:
                        raise ValueError(f'"{key1}" - missing required key "{key2}"')
                    val2 = val1[key2]
                    if key2 == "name":
                        if type(val2) is not str or not val2.strip():
                            raise ValueError(f'"{key1}" - "name": expected non-empty str, got {val2}')
                    elif key2 == "lc_group_mode":
                        if val2 not in tuple(LCGroupMode):
                            raise ValueError(f'"{key1}" - "lc_group_mode": expected one of {", ".join(LCGroupMode)}, got {val2}')
                    elif key2 in ("lc_groups", "source_groups"):
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "{key2}": expected list of str, got {val2}')
                        groups: list[str] = []
                        for group in val2:  # type: ignore[reportUnknownVariableType]
                            if type(group) is dict and type(group.get("group")) is str:  # type: ignore[reportUnknownMemberType]
                                group = group["group"]  # Format from an earlier playlists implementation: [{"group": "Name"}]
                            if type(group) is not str:
                                raise ValueError(f'"{key1}" - "{key2}": expected list of str, got {val2}')
                            groups.append(normalize_group_key(group))
                        if groups != val2:
                            Log.info(label or Label.CONFIG, f'"{key1}" - "{key2}": Normalized group names to {groups}')
                        val1[key2] = list(dict.fromkeys(groups))
                    elif key2 == "base_url":
                        if type(val2) is not str or (val2 and not is_valid_url(val2)):
                            raise ValueError(f'"{key1}" - "base_url": expected empty str or valid URL, got {val2}')
                    else:
                        raise ValueError(f'"{key1}" - unexpected key "{key2}"')
            data.setdefault(MAIN_PLAYLIST_ID, default_playlist_info())
            return data
        except BaseException as e:
            if label == Label.STARTUP and isinstance(e, FileNotFoundError):
                Log.info(label, f"No playlists configuration found at {self.playlists_path}, using default...")
                return PlaylistsDataImpl({MAIN_PLAYLIST_ID: default_playlist_info()})
            Log.critical(label or Label.CONFIG, f"Failed to load playlists configuration from {self.playlists_path}: {e}")
            if isinstance(e, Exception):
                return
            raise

    async def save_playlists_config(self, data: PlaylistsData) -> bool:
        """Saves the playlists configuration to playlists.json."""
        return await self._save_json_file(self.playlists_path, data)
    
    async def get_channel_list_config(self, *, use_default: bool = False, label: Label | None = None) -> ChannelListDataImpl | None:
        """Loads the predefined channel list from channel_list.json."""
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
        """Loads the quality cache from quality_cache.json."""
        try:
            data: QualityCacheDataImpl = await self._load_json_file(self.quality_cache_path)
            if type(data) is not dict:
                raise ValueError(f"Invalid quality cache format in {self.quality_cache_path}")
            for key1, val1 in data.items():  # type: ignore[reportUnknownVariableType]
                if type(key1) is not str:
                    raise ValueError(f'"{key1}": expected str, got {type(key1)}')
                if type(val1) is not dict:
                    raise ValueError(f'"{key1}" val: expected dict, got {type(val1)}')
                val1.setdefault("runtimes", [])
                for key2 in ("statuses", "widths", "heights", "bitrates", "framerates", "runtimes", "updated_at"):
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
                            if type(item) not in (float, int) or item < 0:
                                raise ValueError(f'"{key1}" - "bitrates" item {index}: expected non-negative float, got {item}')
                    elif key2 == "framerates":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "framerates": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) not in (float, int) or item < 0:
                                raise ValueError(f'"{key1}" - "framerates" item {index}: expected non-negative float, got {item}')
                    elif key2 == "runtimes":
                        if type(val2) is not list:
                            raise ValueError(f'"{key1}" - "runtimes": expected list, got {type(val2)}')
                        for index, item in enumerate(val2):
                            if type(item) is not dict:
                                raise ValueError(f'"{key1}" - "runtimes" item {index}: expected dict, got {type(item)}')
                            for key3 in ("stop_reason", "runtime"):
                                if key3 not in item:
                                    raise ValueError(f'"{key1}" - "runtimes" item {index} - missing required key "{key3}"')
                                val3 = item[key3]
                                if key3 == "stop_reason":
                                    if val3 not in StopReason:
                                        raise ValueError(f'"{key1}" - "runtimes" item {index} - "stop_reason": expected one of [{", ".join(StopReason)}], got {val3}')
                                elif key3 == "runtime":
                                    if type(val3) not in (float, int) or val3 < 0:
                                        raise ValueError(f'"{key1}" - "runtimes" item {index} - "runtime": expected non-negative float, got {val3}')
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
        """Saves the quality cache to quality_cache.json."""
        return await self._save_json_file(self.quality_cache_path, data)

    async def get_jobs_config(self, *, label: Label | None = None) -> JobsDataImpl | None:
        """Loads the jobs configuration from jobs.json."""
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
                    try:
                        if type(val2) is not str:
                            raise ValueError()
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
        """Saves the jobs configuration to jobs.json."""
        return await self._save_json_file(self.jobs_path, data)

    async def backup_config(self, *, scheduled: bool) -> Path | None:
        """Creates a zip backup of the current configuration files."""
        if scheduled and not self.backup_count:
            Log.debug(Label.CONFIG, "Scheduled backups are disabled (scheduled backups kept is 0). Skipping backup.")
            return
        try:
            base_path = self.backups_scheduled_path if scheduled else self.backups_manual_path
            backup_folder = base_path / f"nexus_tuner_backup_{datetime.now().isoformat(timespec='seconds').replace(':', '-')}"
            await aiofiles.os.makedirs(backup_folder, exist_ok=True)
            backup_path = backup_folder.with_name(f"{backup_folder.name}.zip")
            Log.info(Label.CONFIG, f"Creating backup at {backup_path}")
            async with self.file_lock:
                await aioshutil.copy2(self.settings_path, backup_folder / self.settings_name)
                await aioshutil.copy2(self.providers_path, backup_folder / self.providers_name)
                await aioshutil.copy2(self.discovered_sources_path, backup_folder / self.discovered_sources_name)
                await aioshutil.copy2(self.logical_channels_path, backup_folder / self.logical_channels_name)
                await aioshutil.copy2(self.channel_mappings_path, backup_folder / self.channel_mappings_name)
                await aioshutil.copy2(self.channel_list_path, backup_folder / self.channel_list_name)
                await aioshutil.copy2(self.playlists_path, backup_folder / self.playlists_name)
                await aioshutil.copy2(self.quality_cache_path, backup_folder / self.quality_cache_name)
                await aioshutil.copy2(self.jobs_path, backup_folder / self.jobs_name)
                await aioshutil.make_archive(str(backup_folder), 'zip', backup_folder)
                await aioshutil.rmtree(backup_folder, ignore_errors=True)
            return backup_path
        except Exception as e:
            Log.error(Label.CONFIG, f"Failed to create backup: {e}")

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
