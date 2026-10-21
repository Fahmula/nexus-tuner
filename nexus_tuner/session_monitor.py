import asyncio
from datetime import datetime
import aiohttp
from typing import Final, NoReturn, Self, Set, Any

from nexus_tuner.config import Config
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.mpegts import MPEGTSStream
from nexus_tuner.stream import StreamManager
from nexus_tuner.utils import Label, Log, MediaServerId, MediaServerInfo, MediaServerStatus, MediaServerType, StopReason, VideoKey, VideoType

# --- Constants ---
SESSION_MONITOR_STARTUP_DELAY: Final[int] = 15
MEDIA_SERVER_API_TIMEOUT: Final[aiohttp.ClientTimeout] = aiohttp.ClientTimeout(total=20)
MEDIA_SERVER_TEST_TIMEOUT: Final[aiohttp.ClientTimeout] = aiohttp.ClientTimeout(total=10)
SESSION_ACTIVE_BUFFER_SECONDS: Final[int] = 60


class MediaServerError(Exception):
    """A media server responded but not with what was expected."""


MEDIA_SERVER_ERRORS: Final = (aiohttp.ClientError, asyncio.TimeoutError, ValueError, MediaServerError)


def build_media_server_request(server: MediaServerInfo, path: str) -> tuple[str, dict[str, str]]:
    """Returns the URL and auth headers for a media server API path."""
    if server["type"] == MediaServerType.EMBY:
        return f"{server['url']}/emby/{path}", {"X-Emby-Token": server["api_key"]}
    # Recent Jellyfin versions disable the legacy Emby routes and api_key auth by default
    return f"{server['url']}/{path}", {"Authorization": f'MediaBrowser Token="{server["api_key"]}"'}


async def fetch_media_server_json(session: aiohttp.ClientSession, server: MediaServerInfo, path: str, timeout: aiohttp.ClientTimeout, params: dict[str, str] | None = None) -> Any:
    """Fetches JSON from a media server API path, raises one of MEDIA_SERVER_ERRORS on failure."""
    url, headers = build_media_server_request(server, path)
    async with session.get(url, headers=headers, params=params, timeout=timeout) as response:
        if response.status in (401, 403):
            raise MediaServerError(f"HTTP {response.status}, check the API key.")
        response.raise_for_status()
        return await response.json()


def describe_media_server_error(e: BaseException) -> str:
    """Returns a readable description of a media server request failure."""
    if isinstance(e, asyncio.TimeoutError):
        return "Timed out waiting for a response."
    if isinstance(e, aiohttp.ContentTypeError):
        return "Response was not JSON, check the URL."
    return str(e) or type(e).__name__


async def test_media_server_connection(server: MediaServerInfo) -> tuple[bool, str]:
    """Checks that a media server is reachable with its API key, returns whether it is and a message for the user."""
    try:
        async with aiohttp.ClientSession() as session:
            info = await fetch_media_server_json(session, server, "System/Info", MEDIA_SERVER_TEST_TIMEOUT)
    except MEDIA_SERVER_ERRORS as e:
        return False, describe_media_server_error(e)
    if type(info) is not dict:
        return False, "Unexpected response, check the URL and server type."
    return True, f"{info.get('ServerName') or server['name']} v{info.get('Version', '?')}"  # type: ignore[reportUnknownMemberType]


class GhostSessionMonitor:
    """
    A background task that monitors media servers (Emby/Jellyfin) to find and
    terminate "ghost" streams using asyncio.

    A ghost stream is an process that is running on the server but has no
    corresponding active viewing session on any configured media server.
    """
    __slots__ = (
        'config', 'handler', 'stream_manager',
        'title_to_stream_ids', 'ghost_monitor_task',
        'server_status', '_wake',
    )

    def __init__(self, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> None:
        """
        Initializes the monitor.

        The monitor's background task should be started externally using `asyncio.create_task(monitor.run())`.
        """
        self.config: Config = config
        self.handler: ChannelHandler = handler
        self.stream_manager: StreamManager = stream_manager

        self.title_to_stream_ids: dict[str, set[str]] = {}  # Logical channels and source channels can share a title
        self.ghost_monitor_task: asyncio.Task[NoReturn]
        self.server_status: dict[MediaServerId, MediaServerStatus] = {}  # Result of the last check of each server, shown in the UI
        self._wake: asyncio.Event = asyncio.Event()

    @classmethod
    async def create(cls, config: Config, handler: ChannelHandler, stream_manager: StreamManager) -> Self:
        """Asynchronous factory for creating and initializing a GhostSessionMonitor instance.
        The monitor always runs so media servers added in Settings take effect without a restart.
        """
        enabled_count = sum(server["enabled"] for server in config.media_servers)
        Log.info(Label.STARTUP, f"Ghost Session Monitor is enabled with {enabled_count} media server(s).")
        instance = cls(config, handler, stream_manager)
        instance.ghost_monitor_task = asyncio.create_task(instance._run())
        return instance

    def notify_settings_changed(self) -> None:
        """Runs a check immediately so that media server and interval changes apply without waiting."""
        self._wake.set()

    def _set_server_status(self, server: MediaServerInfo, ok: bool, message: str) -> None:
        """Records the result of a check, only logging when a server goes down or recovers to avoid repeating every cycle."""
        previous = self.server_status.get(server["id"])
        self.server_status[server["id"]] = {"ok": ok, "message": message, "checked_at": datetime.now()}
        if ok:
            if previous and not previous["ok"]:
                Log.info(Label.SESSION, f"Media server '{server['name']}' is reachable again.")
        elif previous is None or previous["ok"]:
            Log.warn(Label.SESSION, f"Could not get sessions from media server '{server['name']}' at {server['url']}, no ghost sessions will be stopped until it is reachable: {message}")
        else:
            Log.debug(Label.SESSION, f"Media server '{server['name']}' is still unreachable: {message}")

    async def _fetch_sessions(self, session: aiohttp.ClientSession, server: MediaServerInfo) -> list[Any] | None:
        """Fetches active session data from a single media server, returns None if it could not be fetched."""
        params = {"ActiveWithinSeconds": str(self.config.ghost_check_interval + SESSION_ACTIVE_BUFFER_SECONDS)}
        try:
            sessions = await fetch_media_server_json(session, server, "Sessions", MEDIA_SERVER_API_TIMEOUT, params)
            if type(sessions) is not list:
                raise MediaServerError("Unexpected response, check the URL and server type.")
        except MEDIA_SERVER_ERRORS as e:
            self._set_server_status(server, False, describe_media_server_error(e))
            return None
        self._set_server_status(server, True, f"{len(sessions)} active session(s)")  # type: ignore[reportUnknownArgumentType]
        return sessions  # type: ignore[reportUnknownVariableType]

    async def _get_legitimate_stream_ids(self, servers: list[MediaServerInfo]) -> Set[str] | None:
        """
        Fetches sessions from the servers concurrently and returns a set of logical channel IDs that are
        legitimately being watched, or None if any server could not be reached.
        """
        active_lc_ids: Set[str] = set()

        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(*(self._fetch_sessions(session, server) for server in servers))
        if any(result is None for result in results):
            return None
        all_sessions = [item for sublist in results if sublist for item in sublist]

        if not self.title_to_stream_ids:
            return set()

        for session_data in all_sessions:
            if (now_playing := session_data.get("NowPlayingItem")) and now_playing.get("Type") == "TvChannel":
                if (channel_name := now_playing.get("Name")) in self.title_to_stream_ids:
                    active_lc_ids |= self.title_to_stream_ids[channel_name]

        return active_lc_ids

    async def _check_for_ghost_sessions(self) -> None:
        """The main logic loop to find and terminate ghost streams."""
        servers = [server for server in self.config.media_servers if server["enabled"]]
        server_ids = {server["id"] for server in servers}
        self.server_status = {server_id: status for server_id, status in self.server_status.items() if server_id in server_ids}
        if not servers:
            return

        self.title_to_stream_ids = await self.handler.get_stream_title_map()

        try:
            legitimately_active_lc_ids = await self._get_legitimate_stream_ids(servers)
        except Exception as e:
            Log.error(Label.SESSION, f"Could not get active sessions from media servers: {e}")
            return
        if legitimately_active_lc_ids is None:
            # A stream watched only on an unreachable server would look like a ghost, so nothing is stopped this cycle
            unreachable = [f"'{server['name']}'" for server in servers if not (status := self.server_status.get(server["id"])) or not status["ok"]]
            Log.debug(Label.SESSION, f"Skipping ghost session check, could not reach media server(s) {', '.join(unreachable)}.")
            return

        mpegts_video_keys: list[VideoKey] = []
        hls_video_keys: list[VideoKey] = []
        async with self.stream_manager.stream_process_lock:
            for video_key, data in self.stream_manager.processes.items():
                if data['is_preview']:
                    continue
                if not data['is_long_term']:
                    continue
                if data['logical_channel_id'] in legitimately_active_lc_ids:
                    continue
                video_type = data['video_type']
                if data['is_mpegts_active'] is False:
                    continue
                stream_engine = data['stream_engine']
                Log.warn(Label.SESSION, f"{data['video_name']}: Found ghost session.", (video_type, stream_engine))
                if video_type == VideoType.MPEGTS:
                    mpegts_video_keys.append(video_key)
                elif video_type == VideoType.HLS:
                    hls_video_keys.append(video_key)
                else:
                    Log.error(Label.SESSION, f"{data['video_name']}: Unknown video type '{video_type}', cannot terminate.", (video_type, stream_engine))
        if not mpegts_video_keys and not hls_video_keys:
            return

        for video_key in mpegts_video_keys:
            if video_key in MPEGTSStream.streams:
                MPEGTSStream.streams[video_key].shutdown()
        await self.stream_manager.stop_processes(StopReason.TIMEOUT, hls_video_keys)

    async def _run(self) -> NoReturn:
        """The main execution loop for the monitor task."""
        Log.info(Label.STARTUP, "Ghost Session Monitor task started.")
        await asyncio.sleep(SESSION_MONITOR_STARTUP_DELAY)

        while True:
            try:
                await self._check_for_ghost_sessions()
            except Exception as e:
                Log.critical(Label.SESSION, f"Unhandled exception in main check loop: {e}")

            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.config.ghost_check_interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
