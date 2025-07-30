import asyncio
import aiohttp
from typing import Coroutine, Final, NoReturn, Self, Set, Any

from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.stream import StreamManager
from nexus_stream.utils import Label, LogicalChannelId, LogicalChannelName, VideoKey

# --- Constants ---
SESSION_MONITOR_STARTUP_DELAY: Final[int] = 15
MEDIA_SERVER_API_TIMEOUT: Final[aiohttp.ClientTimeout] = aiohttp.ClientTimeout(total=10)
SESSION_ACTIVE_BUFFER_SECONDS: Final[int] = 60

class GhostSessionMonitor:
    """
    A background task that monitors media servers (Emby/Jellyfin) to find and
    terminate "ghost" streams using asyncio.
    
    A ghost stream is an FFmpeg process that is running on the server but has no
    corresponding active viewing session on any configured media server.
    """
    __slots__ = (
        'config', 'handler', 'stream_manager',
        'display_name_to_lc_id_map', 'ghost_monitor_task',
    )
    
    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> None:
        """
        Initializes the monitor.
        
        The monitor's background task should be started externally using `asyncio.create_task(monitor.run())`.
        """
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.stream_manager: StreamManager = stream_manager
        
        self.display_name_to_lc_id_map: dict[LogicalChannelName, LogicalChannelId] = {}
        self.ghost_monitor_task: asyncio.Task[NoReturn]

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> Self | None:
        """Asynchronous factory for creating and initializing a GhostSessionMonitor instance."""
        if not config.emby_url and not config.jellyfin_url:
            config.info(Label.STARTUP, "No Emby/Jellyfin URL configured. Ghost Session Monitor is disabled.")
            return
        config.info(Label.STARTUP, "Emby/Jellyfin URL found. Ghost Session Monitor is enabled.")
        instance = cls(config, handler, stream_manager)
        instance.ghost_monitor_task = asyncio.create_task(instance._run())
        return instance

    def _build_name_to_id_map(self) -> None:
        """
        Creates a mapping from a channel's display name to its logical_channel_id.
        This is a synchronous, CPU-bound operation on in-memory data.
        """
        self.config.debug(Label.SESSION, "Building channel name to stream ID map...")
        name_map = {channel_data["display_name"]: lc_id
                    for lc_id, channel_data in self.handler.client_facing_channels.items()}
        self.display_name_to_lc_id_map = name_map
        self.config.debug(Label.SESSION, f"Built map with {len(self.display_name_to_lc_id_map)} entries.")

    async def _fetch_sessions_from_server(self, session: aiohttp.ClientSession, base_url: str | None, api_key: str | None, server_type: str) -> list[Any]:
        """Fetches active session data from a single media server asynchronously."""
        if not base_url or not api_key:
            return []
        
        url = f"{base_url.rstrip('/')}/emby/Sessions"
        params: dict[str, str | int] = {"api_key": api_key, "ActiveWithinSeconds": self.config.ghost_check_interval + SESSION_ACTIVE_BUFFER_SECONDS}
        
        try:
            async with session.get(url, params=params, timeout=MEDIA_SERVER_API_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.config.error(Label.SESSION, f"Could not connect to {server_type} at {base_url}: {e}")
            return []

    async def _get_legitimate_stream_ids(self) -> Set[str]:
        """
        Fetches sessions from all configured servers concurrently and returns a set of
        logical channel IDs that are legitimately being watched.
        """
        active_lc_ids: Set[str] = set()
        
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_sessions_from_server(session, self.config.emby_url, self.config.emby_api_key, "Emby"),
                self._fetch_sessions_from_server(session, self.config.jellyfin_url, self.config.jellyfin_api_key, "Jellyfin")
            ]
            results = await asyncio.gather(*tasks)
            all_sessions = [item for sublist in results for item in sublist]

        if not self.display_name_to_lc_id_map:
            return set()

        for session_data in all_sessions:
            if (now_playing := session_data.get("NowPlayingItem")) and now_playing.get("Type") == "TvChannel":
                if (channel_name := now_playing.get("Name")) in self.display_name_to_lc_id_map:
                    lc_id = self.display_name_to_lc_id_map[channel_name]
                    active_lc_ids.add(lc_id)
                    self.config.debug(Label.SESSION, f"Found legitimate session for '{channel_name}' (ID: {lc_id}) on device '{session_data.get('DeviceName', 'Unknown')}'.")
        
        return active_lc_ids

    async def _check_for_ghost_sessions(self) -> None:
        """The main logic loop to find and terminate ghost streams asynchronously."""
        self.config.debug(Label.SESSION, "Running check for ghost sessions...")
        self._build_name_to_id_map()

        try:
            legitimately_active_lc_ids = await self._get_legitimate_stream_ids()
            self.config.debug(Label.SESSION, f"Found {len(legitimately_active_lc_ids)} legitimate sessions: {legitimately_active_lc_ids or 'None'}")
        except Exception as e:
            self.config.error(Label.SESSION, f"Could not get active sessions from media servers: {e}")
            return

        ghost_video_keys: Set[tuple[VideoKey, LogicalChannelName]] = set()
        async with self.stream_manager.stream_process_lock:
            for video_key, data in self.stream_manager.ffmpeg_processes.items():
                if data['is_preview']:
                    continue
                if data['is_long_term'] and data['logical_channel_id'] not in legitimately_active_lc_ids:
                    ghost_video_keys.add((video_key, data['logical_channel_name']))

        if not ghost_video_keys:
            self.config.debug(Label.SESSION, "No ghost sessions found.")
            return

        self.config.warn(Label.SESSION, f"Found {len(ghost_video_keys)} ghost session(s) to terminate: {', '.join(g[0] for g in ghost_video_keys)}")
        
        stop_tasks: list[Coroutine[Any, Any, None]] = []
        for video_key, logical_channel_name in ghost_video_keys:
            self.config.info(Label.SESSION, f"Terminating ghost stream for '{logical_channel_name}' [{video_key}]...")
            stop_tasks.append(self.stream_manager.stop_ffmpeg_process(video_key, logical_channel_name))
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks)

    async def _run(self) -> NoReturn:
        """The main execution loop for the monitor task."""
        self.config.info(Label.STARTUP, "Ghost Session Monitor task started.")
        await asyncio.sleep(SESSION_MONITOR_STARTUP_DELAY)
        
        while True:
            try:
                await self._check_for_ghost_sessions()
            except Exception as e:
                self.config.critical(Label.SESSION, f"Unhandled exception in main check loop: {e}")
            
            await asyncio.sleep(self.config.ghost_check_interval)