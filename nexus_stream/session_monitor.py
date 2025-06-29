import asyncio
# Refactor Note: Replaced requests with aiohttp for asynchronous HTTP requests.
import aiohttp
from typing import NoReturn, Set, Any

# Refactor Note: Imports are updated to point to the async versions of the classes.
from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.stream import StreamManager
from nexus_stream.create_stream import VideoKey

# --- Constants ---
MEDIA_SERVER_API_TIMEOUT = 10
SESSION_ACTIVE_BUFFER_SECONDS = 60 # Check for sessions active within interval + this buffer

class GhostSessionMonitor:
    """
    A background task that monitors media servers (Emby/Jellyfin) to find and
    terminate "ghost" streams using asyncio.
    
    A ghost stream is an FFmpeg process that is running on the server but has no
    corresponding active viewing session on any configured media server.
    """
    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> None:
        """
        Initializes the monitor.
        
        The monitor's background task should be started externally using `asyncio.create_task(monitor.run())`.
        """
        self.config = config
        self.handler = handler
        self.stream_manager = stream_manager
        
        # Refactor Note: The threading.Thread is removed. The monitor is now a plain object
        # whose `run` coroutine will be executed as an asyncio.Task by the application's main event loop.
        self.display_name_to_lc_id_map: dict[str, str] = {}

        if self.config.emby_url or self.config.jellyfin_url:
            self.config.log_message("Emby/Jellyfin URL found. Ghost Session Monitor is enabled.", level="INFO")
        else:
            self.config.log_message("No Emby/Jellyfin URL configured. Ghost Session Monitor is disabled.", level="DEBUG")

    def _build_name_to_id_map(self) -> None:
        """
        Creates a mapping from a channel's display name to its logical_channel_id.
        This is a synchronous, CPU-bound operation on in-memory data.
        """
        self.config.log_message("Monitor: Building channel name to stream ID map...", level="DEBUG")
        name_map = {
            channel_data.get("display_name"): lc_id
            for lc_id, channel_data in self.handler.client_facing_channels.items()
            if channel_data.get("display_name")
        }
        self.display_name_to_lc_id_map = name_map
        self.config.log_message(f"Monitor: Built map with {len(self.display_name_to_lc_id_map)} entries.", level="DEBUG")

    # Refactor Note: This method is now async and uses aiohttp for non-blocking network I/O.
    # It accepts a ClientSession to reuse connections, which is a best practice.
    async def _fetch_sessions_from_server(self, session: aiohttp.ClientSession, base_url: str | None, api_key: str | None, server_type: str) -> list[Any]:
        """Fetches active session data from a single media server asynchronously."""
        if not base_url or not api_key:
            return []
        
        url = f"{base_url.rstrip('/')}/emby/Sessions"
        params = {"api_key": api_key, "ActiveWithinSeconds": self.config.ghost_check_interval + SESSION_ACTIVE_BUFFER_SECONDS}
        
        try:
            # Refactor Note: Using async with for the aiohttp request context.
            async with session.get(url, params=params, timeout=MEDIA_SERVER_API_TIMEOUT) as response:
                response.raise_for_status()
                # Refactor Note: Awaiting the .json() coroutine to parse the response body.
                return await response.json()
        # Refactor Note: Catching aiohttp and asyncio specific exceptions.
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.config.log_message(f"Monitor: Could not connect to {server_type} at {base_url}: {e}", level="ERROR")
            return []

    # Refactor Note: This method is now async to await the network calls.
    async def _get_legitimate_stream_ids(self) -> Set[str]:
        """
        Fetches sessions from all configured servers concurrently and returns a set of
        logical channel IDs that are legitimately being watched.
        """
        active_lc_ids: Set[str] = set()
        
        # Refactor Note: Using an async context manager for the aiohttp ClientSession.
        async with aiohttp.ClientSession() as session:
            # Refactor Note: asyncio.gather runs all fetch tasks concurrently for efficiency.
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
                    self.config.log_message(f"Monitor: Found legitimate session for '{channel_name}' (ID: {lc_id}) on device '{session_data.get('DeviceName', 'Unknown')}'.", level="DEBUG")
        
        return active_lc_ids

    # Refactor Note: This method is now async to use the async lock and await other coroutines.
    async def _check_for_ghost_sessions(self) -> None:
        """The main logic loop to find and terminate ghost streams asynchronously."""
        self.config.log_message("Monitor: Running check for ghost sessions...", level="DEBUG")
        self._build_name_to_id_map()

        try:
            # Refactor Note: Awaiting the async helper method.
            legitimately_active_lc_ids = await self._get_legitimate_stream_ids()
            self.config.log_message(f"Monitor: Found {len(legitimately_active_lc_ids)} legitimate sessions: {legitimately_active_lc_ids or 'None'}", level="DEBUG")
        except Exception as e:
            self.config.log_message(f"Monitor: Could not get active sessions from media servers: {e}", level="ERROR")
            return

        ghost_video_keys: Set[tuple[VideoKey, str]] = set()
        async with self.stream_manager.stream_process_lock:
            for video_key, data in self.stream_manager.ffmpeg_processes.items():
                if data['is_preview']:
                    continue
                if data['is_long_term'] and data['logical_channel_id'] not in legitimately_active_lc_ids:
                    ghost_video_keys.add((video_key, data['logical_channel_name']))

        if not ghost_video_keys:
            self.config.log_message("Monitor: No ghost sessions found.", level="DEBUG")
            return

        self.config.log_message(f"Monitor: Found {len(ghost_video_keys)} ghost session(s) to terminate: {', '.join(g[0] for g in ghost_video_keys)}", level="WARN")
        
        # Refactor Note: Use asyncio.gather to stop all ghost streams concurrently.
        stop_tasks = []
        for video_key, logical_channel_name in ghost_video_keys:
            self.config.log_message(f"Monitor: Terminating ghost stream for '{logical_channel_name}' [{video_key}]...", level="INFO")
            stop_tasks.append(self.stream_manager.stop_ffmpeg_process(video_key, logical_channel_name))
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks)

    # Refactor Note: The main loop is now an async method, intended to be run as a background task.
    async def run(self) -> NoReturn:
        """The main execution loop for the monitor task."""
        if not self.config.emby_url and not self.config.jellyfin_url:
            self.config.log_message("Ghost Session Monitor is disabled and will not run.", level="INFO")
            return

        self.config.log_message("Ghost Session Monitor task started.", level="INFO")
        # Refactor Note: Replaced time.sleep with await asyncio.sleep for a non-blocking initial delay.
        await asyncio.sleep(15)
        
        while True:
            try:
                await self._check_for_ghost_sessions()
            except Exception as e:
                self.config.log_message(f"Monitor: Unhandled exception in main check loop: {e}", level="CRITICAL")
            
            # Refactor Note: Replaced time.sleep with await asyncio.sleep for the main non-blocking wait interval.
            await asyncio.sleep(self.config.ghost_check_interval)