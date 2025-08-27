import asyncio
import aiohttp
from typing import Final, NoReturn, Self, Set, Any

from nexus_tuner.config import Config
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.mpegts import MPEGTSStream
from nexus_tuner.stream import StreamManager
from nexus_tuner.utils import Label, Log, LogicalChannelId, LogicalChannelTitle, VideoKey, VideoType

# --- Constants ---
SESSION_MONITOR_STARTUP_DELAY: Final[int] = 15
MEDIA_SERVER_API_TIMEOUT: Final[aiohttp.ClientTimeout] = aiohttp.ClientTimeout(total=20)
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
        'lc_title_to_lc_id_map', 'ghost_monitor_task',
    )
    
    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> None:
        """
        Initializes the monitor.
        
        The monitor's background task should be started externally using `asyncio.create_task(monitor.run())`.
        """
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.stream_manager: StreamManager = stream_manager
        
        self.lc_title_to_lc_id_map: dict[LogicalChannelTitle, LogicalChannelId] = {}
        self.ghost_monitor_task: asyncio.Task[NoReturn]

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> Self | None:
        """Asynchronous factory for creating and initializing a GhostSessionMonitor instance."""
        if not config.emby_url and not config.jellyfin_url:
            return
        Log.info(Label.STARTUP, "Emby/Jellyfin URL found. Ghost Session Monitor is enabled.")
        instance = cls(config, handler, stream_manager)
        instance.ghost_monitor_task = asyncio.create_task(instance._run())
        return instance

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
            Log.error(Label.SESSION, f"Could not connect to {server_type} at {base_url}: {e}")
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

        if not self.lc_title_to_lc_id_map:
            return set()

        for session_data in all_sessions:
            if (now_playing := session_data.get("NowPlayingItem")) and now_playing.get("Type") == "TvChannel":
                if (channel_name := now_playing.get("Name")) in self.lc_title_to_lc_id_map:
                    lc_id = self.lc_title_to_lc_id_map[channel_name]
                    active_lc_ids.add(lc_id)
        
        return active_lc_ids

    async def _check_for_ghost_sessions(self) -> None:
        """The main logic loop to find and terminate ghost streams asynchronously."""
        self.lc_title_to_lc_id_map = {channel_data["logical_channel_title"]: lc_id
                    for lc_id, channel_data in (await self.handler.copy_client_channels()).items()}

        try:
            legitimately_active_lc_ids = await self._get_legitimate_stream_ids()
        except Exception as e:
            Log.error(Label.SESSION, f"Could not get active sessions from media servers: {e}")
            return

        mpegts_video_keys: list[VideoKey] = []
        hls_video_keys: list[VideoKey] = []
        async with self.stream_manager.stream_process_lock:
            for video_key, data in self.stream_manager.ffmpeg_processes.items():
                if data['is_preview']:
                    continue
                if not data['is_long_term']:
                    continue
                if data['logical_channel_id'] in legitimately_active_lc_ids:
                    continue
                video_type = data['video_type']
                Log.warn(Label.SESSION, f"{data['video_name']}: Found ghost session.", video_type)
                if video_type == VideoType.MPEGTS:
                    mpegts_video_keys.append(video_key)
                elif video_type == VideoType.HLS:
                    hls_video_keys.append(video_key)
                else:
                    Log.error(Label.SESSION, f"{data['video_name']}: Unknown video type '{video_type}', cannot terminate.", video_type)
        if not mpegts_video_keys and not hls_video_keys:
            return

        for video_key in mpegts_video_keys:
            if video_key in MPEGTSStream.streams:
                MPEGTSStream.streams[video_key].shutdown()
        await self.stream_manager.stop_ffmpeg_processes(hls_video_keys)

    async def _run(self) -> NoReturn:
        """The main execution loop for the monitor task."""
        Log.info(Label.STARTUP, "Ghost Session Monitor task started.")
        await asyncio.sleep(SESSION_MONITOR_STARTUP_DELAY)
        
        while True:
            try:
                await self._check_for_ghost_sessions()
            except Exception as e:
                Log.critical(Label.SESSION, f"Unhandled exception in main check loop: {e}")
            
            await asyncio.sleep(self.config.ghost_check_interval)
