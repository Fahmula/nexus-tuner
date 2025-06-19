import threading
import time
import requests
from typing import Set
from nexus_stream.config import Config
from nexus_stream.create_stream import HLSKey
from nexus_stream.handler import ChannelHandler
from nexus_stream.stream import HLSStreamManager


# --- Constants ---
MEDIA_SERVER_API_TIMEOUT = 10
SESSION_ACTIVE_BUFFER_SECONDS = 60 # Check for sessions active within interval + this buffer

class GhostSessionMonitor:
    """
    A background thread that monitors media servers (Emby/Jellyfin) to find and
    terminate "ghost" HLS streams.
    
    A ghost stream is an FFmpeg process that is running on the server but has no
    corresponding active viewing session on any configured media server. This can
    happen if a client disconnects improperly.
    """
    def __init__(self, config: Config, handler: ChannelHandler, hls_manager: HLSStreamManager) -> None:
        """
        Initializes the monitor.
        
        The monitor will automatically start its background thread if a media
        server URL is found in the configuration.
        
        Args:
            config: The main application Config object.
            handler: The main ChannelHandler object.
            hls_manager: The main HLSStreamManager object.
        """
        self.config = config
        self.handler = handler
        self.hls_manager = hls_manager
        
        self.interval: int = self.config.ghost_check_interval
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.display_name_to_lc_id_map: dict[str, str] = {}

        if self.config.emby_url or self.config.jellyfin_url:
            self.config.log_message("Emby/Jellyfin URL found, starting Ghost Session Monitor thread.", level="INFO")
            self.thread.start()
        else:
            self.config.log_message("No Emby/Jellyfin URL configured. Ghost Session Monitor is disabled.", level="DEBUG")

    def _build_name_to_id_map(self) -> None:
        """
        Creates a mapping from a channel's display name to its logical_channel_id.
        This is crucial for linking a media server session back to a stream process.
        """
        self.config.log_message("Monitor: Building channel name to stream ID map...", level="DEBUG")
        name_map = {
            channel_data.get("display_name"): lc_id
            for lc_id, channel_data in self.handler.client_facing_channels.items()
            if channel_data.get("display_name")
        }
        self.display_name_to_lc_id_map = name_map
        self.config.log_message(f"Monitor: Built map with {len(self.display_name_to_lc_id_map)} entries.", level="DEBUG")

    def _fetch_sessions_from_server(self, base_url: str | None, api_key: str | None, server_type: str) -> list:
        """
        Fetches active session data from a single media server.

        Args:
            base_url: The base URL of the media server.
            api_key: The API key for authentication.
            server_type: A string identifying the server type (e.g., "Emby") for logging.

        Returns:
            A list of session objects from the API, or an empty list on failure.
        """
        if not base_url or not api_key:
            return []
        
        url = f"{base_url.rstrip('/')}/emby/Sessions"
        headers = {'Content-Type': 'application/json'}
        params = {"api_key": api_key, "ActiveWithinSeconds": self.interval + SESSION_ACTIVE_BUFFER_SECONDS}
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=MEDIA_SERVER_API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.config.log_message(f"Monitor: Could not connect to {server_type} at {base_url}: {e}", level="ERROR")
            return []

    def _get_legitimate_stream_ids(self) -> Set[str]:
        """
        Fetches sessions from all configured servers and returns a set of
        logical channel IDs that are legitimately being watched.
        
        Returns:
            A set of string logical channel IDs that have active sessions.
        """
        emby_sessions = self._fetch_sessions_from_server(self.config.emby_url, self.config.emby_api_key, "Emby")
        jellyfin_sessions = self._fetch_sessions_from_server(self.config.jellyfin_url, self.config.jellyfin_api_key, "Jellyfin")
        
        all_sessions = emby_sessions + jellyfin_sessions

        if not self.display_name_to_lc_id_map:
            # This can happen at startup before the handler is fully ready.
            return set()

        active_lc_ids: Set[str] = set()
        for session in all_sessions:
            if (now_playing := session.get("NowPlayingItem")) and now_playing.get("Type") == "TvChannel":
                if (channel_name := now_playing.get("Name")) in self.display_name_to_lc_id_map:
                    lc_id = self.display_name_to_lc_id_map[channel_name]
                    active_lc_ids.add(lc_id)
                    self.config.log_message(f"Monitor: Found legitimate session for '{channel_name}' (ID: {lc_id}) on device '{session.get('DeviceName', 'Unknown')}'.", level="DEBUG")
        
        return active_lc_ids

    def _check_for_ghost_sessions(self) -> None:
        """The main logic loop to find and terminate ghost streams."""
        self.config.log_message("Monitor: Running check for ghost sessions...", level="DEBUG")
        # Rebuild the map on each run to catch any live configuration changes.
        self._build_name_to_id_map()

        try:
            legitimately_active_lc_ids = self._get_legitimate_stream_ids()
            self.config.log_message(f"Monitor: Found {len(legitimately_active_lc_ids)} legitimate sessions: {legitimately_active_lc_ids or 'None'}", level="DEBUG")
        except Exception as e:
            # This is a critical failure, as we can't determine what's legitimate.
            # Abort this check cycle to avoid terminating valid streams.
            self.config.log_message(f"Monitor: Could not get active sessions from media servers: {e}", level="ERROR")
            return

        ghost_hls_keys: Set[tuple[HLSKey, str]] = set()  # A ghost is a stream that is running but NOT in the legitimate list.
        with self.hls_manager.hls_process_lock:
            for hls_key, data in self.hls_manager.hls_ffmpeg_processes.items():
                if data['is_long_term'] and data['logical_channel_id'] not in legitimately_active_lc_ids:
                    ghost_hls_keys.add((hls_key, data['logical_channel_name']))

        if not ghost_hls_keys:
            self.config.log_message("Monitor: No ghost sessions found.", level="DEBUG")
            return

        self.config.log_message(f"Monitor: Found {len(ghost_hls_keys)} ghost session(s) to terminate: {', '.join(g[0] for g in ghost_hls_keys)}", level="WARN")
        for hls_key, logical_channel_name in ghost_hls_keys:
            self.config.log_message(f"Monitor: Terminating ghost stream for '{logical_channel_name}' [{hls_key}]...", level="INFO")
            self.hls_manager.stop_hls_ffmpeg_process(hls_key, logical_channel_name)

    def _run(self) -> None:
        """The main execution loop for the monitor thread."""
        self.config.log_message("Ghost Session Monitor thread started.", level="INFO")
        time.sleep(15) # Initial delay to allow the rest of the app to start up.
        
        while True:
            try:
                self._check_for_ghost_sessions()
            except Exception as e:
                # Top-level catch to ensure the monitoring thread never dies.
                self.config.log_message(f"Monitor: Unhandled exception in main check loop: {e}", level="CRITICAL")
            time.sleep(self.interval)