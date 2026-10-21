from datetime import datetime, timedelta
import re
import html
import hashlib
import asyncio
from typing import TYPE_CHECKING, Any, Final, Self

import aiohttp
from nexus_tuner.config import Config
from nexus_tuner.slots import ProviderSlots
from nexus_tuner.utils import (M3UURL, NEXUS_TUNER_USER_AGENT, ClientChannelInfo, ClientChannelInfos, ClientChannelInfosImpl, ChannelListData, ChannelListDataImpl,
                                ChannelListGroup, ChannelListInfo, ChannelMappings, ChannelMappingsImpl, ChannelMappingsData, ChannelMappingsDataImpl, ChannelNum, DateTimeISO,
                                DiscoveredSource, DiscoveredSourceWithId, DiscoveredSourcesData, DiscoveredSourcesDataImpl, Log, LogicalChannelInfo, LogicalChannelInfoWithId, LogicalChannelTitle, LogicalChannelsDataImpl, Priority, ProviderInfo,
                                ProviderStatuses, SourceInfo, SourceMappingInfoWithId, SourceId, StreamURL, TVGGroupTitle, Label, LogicalChannelId,
                                LogicalChannelsData, M3USource, MaxStreams, ProviderAlias, ProvidersData, ProvidersDataImpl,
                                StreamKey, TVGDisplayTitle, TVGId, TVGLogo, TVGName, VideoType, create_stream_name, create_video_name, run_bg,
                                DEFAULT_PRIORITY, MAIN_PLAYLIST_ID, GroupKey, LCGroupInfo, LCGroupMode, PlaylistChannelCounts, PlaylistEntry, PlaylistId, PlaylistInfo,
                                PlaylistInfoWithId, PlaylistLineupEntry, PlaylistsData, PlaylistsDataImpl, SourceChannelId, SourceChannelInfo, SourceGroupInfo,
                                StreamChannelInfo, create_source_channel_id, default_playlist_info, normalize_group_key)

if TYPE_CHECKING:
    from nexus_tuner.quality_monitor import QualityMonitor

# --- Constants ---
INITIAL_LOGICAL_CHANNEL_ID: Final[LogicalChannelId] = LogicalChannelId("1000")
PROVIDER_FETCH_TIMEOUT: Final[aiohttp.ClientTimeout] = aiohttp.ClientTimeout(total=20)
DISCOVER_SOURCES_INTERVAL: Final[int] = 60 * 60 * 24
HTTP_REGEX: Final[re.Pattern[str]] = re.compile(r'^https?://', re.IGNORECASE)
TVG_NAME_REGEX: Final[re.Pattern[str]] = re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE)
TVG_ID_REGEX: Final[re.Pattern[str]] = re.compile(r'tvg-id="([^"]*)"', re.IGNORECASE)
TVG_LOGO_REGEX: Final[re.Pattern[str]] = re.compile(r'tvg-logo="([^"]*)"', re.IGNORECASE)
TVG_GROUP_TITLE_REGEX: Final[re.Pattern[str]] = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)
NOT_PLAYLIST_ID_REGEX: Final[re.Pattern[str]] = re.compile(r'[^a-z0-9]+')
DEFAULT_LC_GROUP_TITLE: Final[TVGGroupTitle] = TVGGroupTitle("Uncategorized")


def _channel_num_sort_key(channel_num: str) -> tuple[float, str]:
    """Sorts channel numbers numerically, non-numeric numbers are sorted last."""
    try:
        return (float(channel_num), channel_num)
    except ValueError:
        return (float('inf'), channel_num)


class ChannelHandler:
    """
    Manages all channel data, including providers, logical channels, and mappings.
    
    This class is responsible for:
    - Fetching and parsing M3U files from source providers asynchronously.
    - Building a list of "client-facing" channels based on user-defined logical channels and mappings.
    - Generating the main M3U file for clients.
    - Handling provider stream capacity limits using asyncio-native constructs.
    - Providing async methods for the UI to interact with configuration data.
    """
    __slots__ = (
        'label', 'config', 'quality_monitor', '_mutex', '_providers_data', '_discovered_sources_data',
        '_logical_channels_data', '_channel_mappings_data', '_channel_list_data', '_playlists_data',
        '_client_channels', '_source_groups', '_source_channels', '_playlist_entries', '_playlist_lineups', '_playlist_counts',
        '_slots', '_kill_provider_streams', '_pending_streams',
    )
    
    def __init__(self, config: Config) -> None:
        """
        Initializes the ChannelHandler with the provided configuration.
        """
        self.label: Label = Label.STARTUP
        self.config: Config = config
        self.quality_monitor: QualityMonitor | None = None  # Injected after creation
        self._mutex: asyncio.Lock = asyncio.Lock()

        # Data loaded from configuration files
        self._providers_data: ProvidersData = ProvidersDataImpl({})
        self._discovered_sources_data: DiscoveredSourcesData = DiscoveredSourcesDataImpl({})
        self._logical_channels_data: LogicalChannelsData = LogicalChannelsDataImpl({})
        self._channel_mappings_data: ChannelMappingsData = ChannelMappingsDataImpl({})
        self._channel_list_data: ChannelListData = ChannelListDataImpl({})
        self._playlists_data: PlaylistsData = PlaylistsDataImpl({MAIN_PLAYLIST_ID: default_playlist_info()})

        # In-memory processed data
        self._client_channels: ClientChannelInfos = ClientChannelInfosImpl({})
        self._source_groups: dict[GroupKey, SourceGroupInfo] = {}
        self._source_channels: dict[SourceChannelId, SourceChannelInfo] = {}  # Only for groups selected by a playlist
        self._playlist_entries: dict[PlaylistId, tuple[PlaylistEntry, ...]] = {}
        self._playlist_lineups: dict[PlaylistId, tuple[PlaylistLineupEntry, ...]] = {}
        self._playlist_counts: dict[PlaylistId, PlaylistChannelCounts] = {}

        # Slots management
        self._slots: dict[ProviderAlias, ProviderSlots] = {}
        self._kill_provider_streams: set[ProviderAlias] = set()
        self._pending_streams: set[StreamKey] = set()

    @classmethod
    async def create(cls, config: Config) -> Self:
        """Asynchronous factory for creating and initializing a ChannelHandler instance."""
        instance = cls(config)
        await instance._load_and_process_configurations(update_providers=True, force_discover_sources=False)
        instance.label = Label.HANDLER
        return instance

    async def _load_and_process_configurations(self, *, update_providers: bool, force_discover_sources: bool) -> None:
        """
        Loads all data from JSON files and rebuilds the in-memory channel structures.
        """
        logged_waiting = False
        await self._mutex.acquire()
        while self.get_pending_stream_count():
            self._mutex.release()
            if not logged_waiting:
                Log.info(self.label, "Waiting for streams to finish being created before reloading configurations...")
                logged_waiting = True
            await asyncio.sleep(0.01)
            await self._mutex.acquire()
        try:
            Log.info(self.label, "Reloading ChannelHandler configurations")

            new_discovered_sources_data = await self.config.get_discovered_sources_config(label=self.label)
            if new_discovered_sources_data is None:
                if self.label == Label.STARTUP:
                    Log.critical(self.label, "Discovered sources configuration was not found, cannot start ChannelHandler.")
                    raise RuntimeError("Discovered sources configuration invalid or missing.")
                if await self.config.save_discovered_sources_config(self._discovered_sources_data):
                    Log.warn(self.label, "Discovered sources configuration was removed/corrupted after startup, reloaded with data in-memory.")
                else:
                    Log.critical(self.label, "Discovered sources configuration was removed/corrupted after startup, cannot reload.")
            else:
                self._discovered_sources_data = new_discovered_sources_data
            new_logical_channels_data = await self.config.get_logical_channels_config(label=self.label)
            if new_logical_channels_data is None:
                if self.label == Label.STARTUP:
                    Log.critical(self.label, "Logical channels configuration was not found, cannot start ChannelHandler.")
                    raise RuntimeError("Logical channels configuration invalid or missing.")
                if await self.config.save_logical_channels_config(self._logical_channels_data):
                    Log.warn(self.label, "Logical channels configuration was removed/corrupted after startup, reloaded with data in-memory.")
                else:
                    Log.critical(self.label, "Logical channels configuration was removed/corrupted after startup, cannot reload.")
            else:
                self._logical_channels_data = new_logical_channels_data
            new_channel_mappings_data = await self.config.get_channel_mappings_config(label=self.label)
            if new_channel_mappings_data is None:
                if self.label == Label.STARTUP:
                    Log.critical(self.label, "Channel mappings configuration was not found, cannot start ChannelHandler.")
                    raise RuntimeError("Channel mappings configuration invalid or missing.")
                if await self.config.save_channel_mappings_config(self._channel_mappings_data):
                    Log.warn(self.label, "Channel mappings configuration was removed/corrupted after startup, reloaded with data in-memory.")
                else:
                    Log.critical(self.label, "Channel mappings configuration was removed/corrupted after startup, cannot reload.")
            else:
                self._channel_mappings_data = new_channel_mappings_data
            new_channel_list_data = await self.config.get_channel_list_config(label=self.label)
            if new_channel_list_data is None:
                if self.label == Label.STARTUP:
                    Log.critical(self.label, "Channel list configuration was not found, cannot start ChannelHandler.")
                    raise RuntimeError("Channel list configuration invalid or missing.")
                Log.critical(self.label, "Channel list configuration was removed/corrupted after startup, cannot reload.")
            else:
                self._channel_list_data = new_channel_list_data
            new_playlists_data = await self.config.get_playlists_config(label=self.label)
            if new_playlists_data is None:
                if self.label == Label.STARTUP:
                    Log.critical(self.label, "Playlists configuration was not found, cannot start ChannelHandler.")
                    raise RuntimeError("Playlists configuration invalid or missing.")
                if await self.config.save_playlists_config(self._playlists_data):
                    Log.warn(self.label, "Playlists configuration was removed/corrupted after startup, reloaded with data in-memory.")
                else:
                    Log.critical(self.label, "Playlists configuration was removed/corrupted after startup, cannot reload.")
            else:
                self._playlists_data = new_playlists_data

            if update_providers:
                new_providers_data = await self.config.get_providers_config(label=self.label)
                if new_providers_data is None:
                    if self.label == Label.STARTUP:
                        Log.critical(self.label, "Providers configuration was not found, cannot start ChannelHandler.")
                        raise RuntimeError("Providers configuration invalid or missing.")
                    if await self.config.save_providers_config(self._providers_data):
                        Log.warn(self.label, "Providers configuration was removed/corrupted after startup, reloaded with data in-memory.")
                    else:
                        Log.critical(self.label, "Providers configuration was removed/corrupted after startup, cannot reload.")
                else:
                    self._providers_data = new_providers_data
                    self._update_providers_slots()

            prev_discovered_sources = DiscoveredSourcesDataImpl({**self._discovered_sources_data})
            min_updated_at = min([p_data["updated_at"] for p_data in self._providers_data.values()], default=DateTimeISO("0001-01-01"))
            now = datetime.now()
            if force_discover_sources or datetime.fromisoformat(min_updated_at) < now - timedelta(seconds=DISCOVER_SOURCES_INTERVAL):
                Log.info(self.label, "Discovering sources from configured providers...")
                await self._parse_all_provider_m3us_and_populate_discovered_sources()
                if not await self.config.save_discovered_sources_config(self._discovered_sources_data):
                    Log.critical(self.label, "Failed to save discovered sources configuration.")
                new_providers_data = ProvidersDataImpl({alias: {
                    "m3u_url": details["m3u_url"],
                    "max_streams": details["max_streams"],
                    "updated_at": DateTimeISO(now.isoformat())
                } for alias, details in self._providers_data.items()})
                if await self.config.save_providers_config(new_providers_data):
                    self._providers_data = new_providers_data
                else:
                    Log.critical(self.label, "Failed to save updated provider configuration after discovery.")

            # Build in-memory data
            await self._build_client_channels(prev_discovered_sources)
            self._build_source_channels()
            self._build_playlist_outputs()
            
            Log.info(self.label, f"Discovered: {len(self._discovered_sources_data)}, Client-Facing: {len(self._client_channels)}, Source Channels: {len(self._source_channels)}, Playlists: {len(self._playlists_data)}")
        finally:
            self._mutex.release()

    def _parse_source_m3u_lines(self, text: str) -> list[M3USource]:
        """Parses M3U lines into structured channels. Structure:
        #EXTM3U
        #EXTINF: -1, tvg-name="Channel Name", tvg-id="12345", tvg-logo="logo.png", group-title="Group"
        http://example.com/stream.m3u8
        #EXTINF: -1, tvg-name="Another Channel", tvg-id="67890", tvg-logo="logo2.png", group-title="Group 2"
        http://example.com/another_stream.m3u8
        ...
        """
        m3u_sources: list[M3USource] = []
        lines = (line.strip() for line in text.splitlines())
        extm3u = next(lines, None)
        if extm3u is None or not extm3u.startswith("#EXTM3U"):
            Log.error(self.label, "Invalid Extended M3U format: Missing #EXTM3U header.")
            return m3u_sources

        REGEX_DEFAULT: tuple[str, str] = ("", "")
        current_extinf: str | None = None
        for line in lines:
            if line.startswith("#EXTINF:"):
                if current_extinf:
                    Log.warn(self.label, f"Found #EXTINF without a preceding stream URL, skipping: {current_extinf}")
                current_extinf = line
                continue
            if HTTP_REGEX.match(line):
                if not current_extinf:
                    Log.warn(self.label, "Invalid M3U format: Missing #EXTINF line before stream URL.")
                    continue
                tvg_id = (re.search(TVG_ID_REGEX, current_extinf) or REGEX_DEFAULT)[1].strip()
                tvg_name = html.unescape((re.search(TVG_NAME_REGEX, current_extinf) or REGEX_DEFAULT)[1].strip())
                tvg_logo = (re.search(TVG_LOGO_REGEX, current_extinf) or REGEX_DEFAULT)[1].strip()
                group_title = html.unescape((re.search(TVG_GROUP_TITLE_REGEX, current_extinf) or REGEX_DEFAULT)[1].strip())
                display_name_from_extinf = html.unescape(current_extinf.split(',')[-1].strip())

                m3u_sources.append({
                    "tvg_name": TVGName(tvg_name),
                    "display_title": TVGDisplayTitle(display_name_from_extinf),
                    "group_title": TVGGroupTitle(group_title),
                    "tvg_id": TVGId(tvg_id),
                    "tvg_logo": TVGLogo(tvg_logo),
                    "stream_url": StreamURL(line),
                })
                current_extinf = None
        return m3u_sources

    def _generate_source_id(self, provider_alias: ProviderAlias, stream_url: StreamURL) -> SourceId:
        """Creates a stable, unique ID for a source stream."""
        id_material = f"{provider_alias}:{stream_url}"
        return SourceId(f"{provider_alias}:{hashlib.md5(id_material.encode('utf-8')).hexdigest()}")
    
    async def _fetch_and_parse_provider(self, session: aiohttp.ClientSession, provider_alias: ProviderAlias, m3u_url: M3UURL) -> DiscoveredSourcesData:
        """Fetches and parses a single provider's M3U."""
        try:
            discovered_sources = DiscoveredSourcesDataImpl({})
            async with session.get(m3u_url, timeout=PROVIDER_FETCH_TIMEOUT, headers={'User-Agent': NEXUS_TUNER_USER_AGENT}) as response:
                response.raise_for_status()           
                m3u_sources = self._parse_source_m3u_lines(await response.text())

                for m3u_source in m3u_sources:
                    source_id = self._generate_source_id(provider_alias, m3u_source["stream_url"])
                    discovered_sources[source_id] = {
                        "provider_alias": provider_alias,
                        **m3u_source
                    }
                Log.debug(self.label, f"Discovered {len(m3u_sources)} sources from provider '{provider_alias}'.")
                return discovered_sources
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            Log.error(self.label, f"Failed to fetch or parse provider '{provider_alias}': {e}")
            raise
        except BaseException as e:
            Log.error(self.label, f"An unexpected error occurred while processing provider '{provider_alias}': {e}")
            raise

    async def _parse_all_provider_m3us_and_populate_discovered_sources(self) -> None:
        """Uses asyncio.gather to fetch and parse all configured provider M3Us concurrently."""
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_and_parse_provider(session, alias, details["m3u_url"])
                for alias, details in self._providers_data.items()
            ]
            if not tasks:
                Log.warn(self.label, "No providers configured to parse.")
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)
            discovered_sources = DiscoveredSourcesDataImpl({})
            for res in results:
                if not isinstance(res, BaseException):
                    discovered_sources.update(res)
            self._discovered_sources_data = discovered_sources

    async def _build_client_channels(self, prev_discovered_sources: DiscoveredSourcesData) -> None:
        """Builds the final list of channels exposed to clients."""
        name_url_map: dict[str, dict[ProviderAlias, list[tuple[SourceId, StreamURL]]]] = {}
        for source_id, discovered_source in self._discovered_sources_data.items():
            key = f"{discovered_source['group_title']}|{discovered_source['tvg_name']}"
            if key not in name_url_map:
                name_url_map[key] = {}
            alias = discovered_source["provider_alias"]
            if alias not in name_url_map[key]:
                name_url_map[key][alias] = []
            name_url_map[key][alias].append((source_id, discovered_source["stream_url"]))

        new_client_channels = ClientChannelInfosImpl({})
        for logical_channel_id, lc_def in self._logical_channels_data.items():
            stream_name = create_stream_name(lc_def['logical_channel_title'], lc_def['channel_num'])
            processed_sources: list[SourceInfo] = []
            for source_id, source_mapping in self._channel_mappings_data.get(logical_channel_id, {}).items():
                priority = source_mapping["priority"]
                discovered_source = self._discovered_sources_data.get(source_id)
                if discovered_source:
                    processed_sources.append({
                        "source_id": source_id,
                        "priority": priority,
                        "provider_alias": discovered_source["provider_alias"],
                        "stream_url": discovered_source["stream_url"],
                    })
                    continue
                prev_discovered_source = prev_discovered_sources.get(source_id)
                if not prev_discovered_source:
                    Log.warn(self.label, f"{stream_name} ({source_id}): Mapped source 'Unknown Source' not found in discovered sources or previously discovered sources.")
                    continue
                alias = prev_discovered_source["provider_alias"]
                video_name = create_video_name(stream_name, prev_discovered_source['tvg_name'], source_id)
                Log.warn(self.label, f"{video_name}: Mapped source from provider '{alias}' not found in discovered sources.")
                key = f"{prev_discovered_source['group_title']}|{prev_discovered_source['tvg_name']}"
                if key not in name_url_map:
                    Log.warn(self.label, f"{video_name}: Could not match previously discovered source to any current matching tvg-group and tvg-name.")
                    continue
                if not alias or alias not in name_url_map[key]:
                    Log.warn(self.label, f"{video_name}: Could not match previously discovered source to any current provider using matching tvg-group and tvg-name.")
                    continue
                id_urls = name_url_map[key][alias]
                if len(id_urls) > 1:
                    Log.warn(self.label, f"{video_name}: Multiple stream URLs found for previously discovered source, cannot update stream URL using matching tvg-group and tvg-name.")
                    continue
                new_source_id = id_urls[0][0]
                if not await self._update_source_id_for_mapping(logical_channel_id, source_id, new_source_id):
                    Log.error(self.label, f"{video_name}: Failed to update to '{new_source_id}' after successfully matching it to a new source using tvg-group and tvg-name.")
                    continue
                if self.quality_monitor:
                    run_bg(self.quality_monitor.update_source_id(source_id, new_source_id))  # run_bg to prevent deadlocks if quality monitor is waiting on a handler function
                else:
                    Log.warn(self.label, f"{video_name}: Quality monitor not yet initialized, cannot update to '{new_source_id}' in quality cache after successfully matching it to a new source using tvg-group and tvg-name.")
                processed_sources.append({
                    "source_id": new_source_id,
                    "priority": priority,
                    "provider_alias": alias,
                    "stream_url": id_urls[0][1],
                })
                Log.info(self.label, f"{video_name}: Updated to '{new_source_id}' using matching tvg-group and tvg-name.")
            if processed_sources:
                new_client_channels[logical_channel_id] = {
                    "logical_channel_title": lc_def["logical_channel_title"] or LogicalChannelTitle(logical_channel_id),
                    "group_title": lc_def["group_title"] or TVGGroupTitle("Uncategorized"),
                    "tvg_id": lc_def["tvg_id"],
                    "tvg_logo": lc_def["tvg_logo"],
                    "channel_num": lc_def["channel_num"],
                    "sources": processed_sources
                }
            else:
                Log.warn(self.label, f"{stream_name}: No valid mapped sources, it will not be included in the client M3U.")
        self._client_channels = new_client_channels

    async def get_num_providers(self) -> int:
        """Returns the number of configured providers."""
        async with self._mutex:
            return len(self._providers_data)

    async def get_discovered_source(self, source_id: SourceId) -> DiscoveredSource | None:
        """Retrieves a discovered source by its ID."""
        async with self._mutex:
            return self._discovered_sources_data.get(source_id)

    async def get_num_discovered_sources(self) -> int:
        """Returns the number of discovered sources."""
        async with self._mutex:
            return len(self._discovered_sources_data)

    async def get_num_logical_channels(self) -> int:
        """Returns the number of logical channels."""
        async with self._mutex:
            return len(self._logical_channels_data)

    def reset_kill_provider_streams(self) -> set[ProviderAlias]:
        """Resets the kill_provider_streams, returns the aliases that should be killed."""
        if not self._kill_provider_streams:
            return set()
        tmp = self._kill_provider_streams
        self._kill_provider_streams = set()
        return tmp

    def get_pending_stream_count(self) -> int:
        return len(self._pending_streams)

    async def add_pending_stream(self, stream_key: StreamKey) -> bool:
        async with self._mutex:
            if stream_key in self._pending_streams:
                return False
            self._pending_streams.add(stream_key)
            return True

    async def remove_pending_stream(self, stream_key: StreamKey) -> None:
        """Removes a pending stream from the set of pending streams."""
        async with self._mutex:
            self._pending_streams.remove(stream_key)

    def _build_source_channels(self) -> None:
        """Groups discovered sources by their normalized group and name across providers.
        All groups are indexed for the UI, but source channels are only built for groups selected by a playlist.
        """
        selected_groups = {group_key for playlist in self._playlists_data.values() for group_key in playlist["source_groups"]}
        group_titles: dict[GroupKey, TVGGroupTitle] = {}
        group_providers: dict[GroupKey, dict[ProviderAlias, None]] = {}
        group_names: dict[GroupKey, set[str]] = {}
        channel_keys: dict[SourceChannelId, tuple[GroupKey, str]] = {}
        channel_sources: dict[SourceChannelId, list[tuple[SourceId, DiscoveredSource]]] = {}
        skipped = 0
        for source_id, discovered_source in self._discovered_sources_data.items():
            name = discovered_source["tvg_name"].strip() or discovered_source["display_title"].strip()
            if not name:
                skipped += 1
                continue
            group_key = normalize_group_key(discovered_source["group_title"])
            name_key = normalize_group_key(name)
            group_titles.setdefault(group_key, TVGGroupTitle(discovered_source["group_title"].strip()))
            group_providers.setdefault(group_key, {})[discovered_source["provider_alias"]] = None
            group_names.setdefault(group_key, set()).add(name_key)
            if group_key not in selected_groups:
                continue
            source_channel_id = create_source_channel_id(group_key, name_key)
            if channel_keys.setdefault(source_channel_id, (group_key, name_key)) != (group_key, name_key):
                Log.error(self.label, f"Source channel ID collision for '{name}' in group '{discovered_source['group_title']}', skipping source '{source_id}'.")
                continue
            channel_sources.setdefault(source_channel_id, []).append((source_id, discovered_source))
        if skipped:
            Log.debug(self.label, f"Skipped {skipped} discovered sources without a name when building source channels.")

        self._source_groups = {group_key: {
            "key": group_key,
            "display_title": group_titles[group_key],
            "providers": tuple(group_providers[group_key]),
            "channel_count": len(group_names[group_key]),
        } for group_key in group_titles}

        source_channels: dict[SourceChannelId, SourceChannelInfo] = {}
        for source_channel_id, sources in channel_sources.items():
            first_source = sources[0][1]
            source_channels[source_channel_id] = {
                "title": LogicalChannelTitle(first_source["tvg_name"].strip() or first_source["display_title"].strip()),
                "group_title": group_titles[channel_keys[source_channel_id][0]],
                "group_key": channel_keys[source_channel_id][0],
                "tvg_id": next((s["tvg_id"] for _, s in sources if s["tvg_id"]), TVGId("")),
                "tvg_logo": next((s["tvg_logo"] for _, s in sources if s["tvg_logo"]), TVGLogo("")),
                "sources": [{
                    "source_id": source_id,
                    "priority": Priority(DEFAULT_PRIORITY),
                    "provider_alias": s["provider_alias"],
                    "stream_url": s["stream_url"],
                } for source_id, s in sources],
            }
        self._source_channels = source_channels

    def _build_playlist_outputs(self) -> None:
        """Builds the M3U entries and HDHomeRun lineup of every playlist.
        The base URL is added when served as it depends on the URL the client connected with.
        """
        client_channels = sorted(self._client_channels.items(), key=lambda item: (item[1].get("group_title", "zzz").lower(), item[1].get("logical_channel_title", "zzz").lower()))
        logical_channels = sorted(self._logical_channels_data.items(), key=lambda item: _channel_num_sort_key(item[1]["channel_num"]))
        source_channels = sorted(self._source_channels.items(), key=lambda item: (item[1]["group_title"].casefold(), item[1]["title"].casefold(), item[0]))

        playlist_entries: dict[PlaylistId, tuple[PlaylistEntry, ...]] = {}
        playlist_lineups: dict[PlaylistId, tuple[PlaylistLineupEntry, ...]] = {}
        playlist_counts: dict[PlaylistId, PlaylistChannelCounts] = {}
        for playlist_id, playlist in self._playlists_data.items():
            lc_groups = set(playlist["lc_groups"])
            only_include = playlist["lc_group_mode"] == LCGroupMode.ONLY_INCLUDE
            def is_lc_included(group_title: TVGGroupTitle) -> bool:
                return (normalize_group_key(group_title or DEFAULT_LC_GROUP_TITLE) in lc_groups) == only_include

            entries: list[PlaylistEntry] = []
            for logical_channel_id, client_channel in client_channels:
                if not is_lc_included(client_channel["group_title"]):
                    continue
                name = client_channel.get("logical_channel_title", "")
                extinf_parts = [f'tvg-name="{name}"']
                extinf_parts.append(f'tvg-chno="{client_channel['channel_num']}"')
                extinf_parts.append(f'tvg-id="{client_channel['tvg_id']}"')
                extinf_parts.append(f'tvg-logo="{client_channel['tvg_logo']}"')
                extinf_parts.append(f'group-title="{client_channel['group_title']}"')
                entries.append({
                    "extinf": f"#EXTINF:-1 {' '.join(extinf_parts)},{name}",
                    "path": f"/{VideoType.HLS}/{logical_channel_id}/{self.config.stream_engine}/playlist.m3u8",
                })
            num_logical_channels = len(entries)

            source_groups = set(playlist["source_groups"])
            for source_channel_id, source_channel in source_channels:
                if source_channel["group_key"] not in source_groups:
                    continue
                extinf_parts = [f'tvg-name="{source_channel['title'].replace('"', "'")}"']  # Provider values are unescaped when parsed
                extinf_parts.append(f'tvg-id="{source_channel['tvg_id'].replace('"', "'")}"')
                extinf_parts.append(f'tvg-logo="{source_channel['tvg_logo'].replace('"', "'")}"')
                extinf_parts.append(f'group-title="{source_channel['group_title'].replace('"', "'")}"')
                entries.append({
                    "extinf": f"#EXTINF:-1 {' '.join(extinf_parts)},{source_channel['title']}",
                    "path": f"/{VideoType.HLS}/{source_channel_id}/{self.config.stream_engine}/playlist.m3u8",
                })
            playlist_entries[playlist_id] = tuple(entries)
            playlist_counts[playlist_id] = {"logical_channels": num_logical_channels, "source_channels": len(entries) - num_logical_channels}

            playlist_lineups[playlist_id] = tuple({
                "channel_id": logical_channel_id,
                "title": logical_channel["logical_channel_title"],
                "channel_num": logical_channel["channel_num"],
                "source_ids": tuple(self._channel_mappings_data.get(logical_channel_id, {})),
            } for logical_channel_id, logical_channel in logical_channels
              if logical_channel["channel_num"] and is_lc_included(logical_channel["group_title"]))
        self._playlist_entries = playlist_entries
        self._playlist_lineups = playlist_lineups
        self._playlist_counts = playlist_counts

    async def get_client_channel(self, logical_channel_id: LogicalChannelId) -> ClientChannelInfo | None:
        """Retrieves a client channel by its logical channel ID."""
        async with self._mutex:
            return self._client_channels.get(logical_channel_id)

    async def copy_sources_for_client_channel(self, logical_channel_id: LogicalChannelId) -> list[SourceInfo]:
        """Retrieves sources for a logical channel or source channel."""
        async with self._mutex:
            if (client_channel := self._client_channels.get(logical_channel_id)):
                return client_channel["sources"].copy()
            if (source_channel := self._source_channels.get(SourceChannelId(logical_channel_id))):
                return source_channel["sources"].copy()
            return []

    async def resolve_stream_channel(self, channel_id: str) -> StreamChannelInfo | None:
        """Returns the title and channel number used when streaming a logical channel or source channel."""
        async with self._mutex:
            if (logical_channel := self._logical_channels_data.get(LogicalChannelId(channel_id))):
                return {"title": logical_channel["logical_channel_title"], "channel_num": logical_channel["channel_num"]}
            if (source_channel := self._source_channels.get(SourceChannelId(channel_id))):
                return {"title": source_channel["title"], "channel_num": None}

    async def get_source_channel(self, source_channel_id: SourceChannelId) -> SourceChannelInfo | None:
        """Retrieves a source channel by its ID."""
        async with self._mutex:
            return self._source_channels.get(source_channel_id)

    async def get_stream_title_map(self) -> dict[str, set[str]]:
        """Returns the IDs of all client-facing logical channels and source channels by their title."""
        async with self._mutex:
            title_map: dict[str, set[str]] = {}
            for logical_channel_id, client_channel in self._client_channels.items():
                title_map.setdefault(client_channel["logical_channel_title"], set()).add(logical_channel_id)
            for source_channel_id, source_channel in self._source_channels.items():
                title_map.setdefault(source_channel["title"], set()).add(source_channel_id)
            return title_map

    def _update_providers_slots(self) -> None:
        """Initializes or updates provider slots based on config."""
        providers_to_delete: set[ProviderAlias] = set()
        for alias, curr_details in self._slots.items():
            if alias not in self._providers_data:
                Log.info(self.label, f"Removing slots for provider '{alias}' as it no longer exists in configuration.")
                providers_to_delete.add(alias)
                self._kill_provider_streams.add(alias)
                continue
            m3u_url = self._providers_data[alias]["m3u_url"]
            max_streams = self._providers_data[alias]["max_streams"]
            if curr_details.get_total_slots() != max_streams:
                if curr_details.get_m3u_url() != m3u_url:
                    Log.info(self.label, f"Updating slots for provider '{alias}' with new URL and max streams: {curr_details.get_total_slots()} -> {max_streams}.")
                else:
                    Log.info(self.label, f"Updating slots for provider '{alias}' with new max streams: {curr_details.get_total_slots()} -> {max_streams}.")
            elif curr_details.get_m3u_url() != m3u_url:
                Log.info(self.label, f"Updating slots for provider '{alias}' with new URL.")
            else:
                continue
            self._slots[alias] = ProviderSlots(
                alias=alias,
                m3u_url=m3u_url,
                total_slots=max_streams
            )
            self._kill_provider_streams.add(alias)
        for alias in providers_to_delete:
            del self._slots[alias]
        for alias, details in self._providers_data.items():
            if alias in self._slots:
                continue
            max_streams = details["max_streams"]
            self._slots[alias] = ProviderSlots(
                alias=alias,
                m3u_url=details["m3u_url"],
                total_slots=max_streams
            )
            Log.info(self.label, f"Initialized slots for provider '{alias}' with capacity {max_streams}")
        
    async def reload_handler_config(self, *, update_providers: bool = False, force_discover_sources: bool = False) -> None:
        """Public method to trigger a full reload of the handler's configuration."""
        await self._load_and_process_configurations(update_providers=update_providers, force_discover_sources=force_discover_sources)

    async def get_provider_stream_status(self) -> ProviderStatuses:
        """Calculates current stream usage for each provider."""
        async with self._mutex:
            return ProviderStatuses({alias: {
                "alias": alias,
                "m3u_url": provider_slots.get_m3u_url(),
                "active_streams": await provider_slots.get_active_slots(),
                "max_streams": provider_slots.get_total_slots()
            } for alias, provider_slots in self._slots.items()})

    # --- UI Interaction Methods ---
    
    async def add_provider(self, alias: ProviderAlias, m3u_url: M3UURL, max_streams: MaxStreams) -> bool:
        """Adds a new provider to the configuration."""
        if not alias: raise ValueError("Provider alias cannot be empty.")
        if not m3u_url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 0: raise ValueError("Max concurrent streams must be at least 0.")

        async with self._mutex:
            if alias.lower() in {a.lower() for a in self._providers_data.keys()}:
                raise ValueError(f"Provider with alias '{alias}' already exists.")

            new_providers_data = ProvidersDataImpl({**self._providers_data, alias: ProviderInfo({
                "m3u_url": m3u_url,
                "max_streams": max_streams,
                "updated_at": DateTimeISO(datetime.now().isoformat())
            })})
            if not await self.config.save_providers_config(new_providers_data):
                Log.critical(self.label, f"Failed to save new provider configuration for '{alias}'.")
                return False
            self._providers_data = new_providers_data
            self._update_providers_slots()
            return True

    async def update_provider(self, alias: ProviderAlias, m3u_url: M3UURL, max_streams: MaxStreams) -> bool:
        """Updates an existing provider's configuration."""
        if not m3u_url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 0: raise ValueError("Max concurrent streams must be at least 0.")

        async with self._mutex:
            if alias not in self._providers_data: raise ValueError(f"Provider with alias '{alias}' not found.")

            new_providers_data = ProvidersDataImpl({**self._providers_data})
            new_providers_data[alias] = ProviderInfo({
                "m3u_url": m3u_url,
                "max_streams": max_streams,
                "updated_at": self._providers_data[alias]["updated_at"]
            })
            if not await self.config.save_providers_config(new_providers_data):
                Log.critical(self.label, f"Failed to save updated provider configuration for '{alias}'.")
                return False
            self._providers_data = new_providers_data
            self._update_providers_slots()
            return True

    async def delete_provider(self, alias: ProviderAlias) -> bool:
        """Deletes a provider from the configuration."""
        async with self._mutex:
            if alias not in self._providers_data:
                raise ValueError(f"Provider with alias '{alias}' not found.")

            new_discovered_sources = DiscoveredSourcesDataImpl({k: v for k, v in self._discovered_sources_data.items()
                                                                if v["provider_alias"] != alias})
            if not await self.config.save_discovered_sources_config(new_discovered_sources):
                Log.critical(self.label, f"Failed to save discovered sources configuration when deleting provider '{alias}'.")
                return False
            self._discovered_sources_data = new_discovered_sources

            new_providers_data = ProvidersDataImpl({**self._providers_data})
            del new_providers_data[alias]
            if not await self.config.save_providers_config(new_providers_data):
                Log.critical(self.label, f"Failed to save updated provider configuration after deleting '{alias}'.")
                return False
            self._providers_data = new_providers_data
            self._update_providers_slots()
            return True

    async def get_discovered_sources_for_ui(self) -> list[DiscoveredSourceWithId]:
        """Gets a list of discovered sources."""
        async with self._mutex:
            all_sources = [DiscoveredSourceWithId({**source, "source_id": source_id})
                            for source_id, source in self._discovered_sources_data.items()]
        return sorted(all_sources, key=lambda x: (x["provider_alias"], (x["tvg_name"] or x["display_title"]).lower()))

    async def get_logical_channels_for_ui(self) -> list[LogicalChannelInfoWithId]:
        """Returns a copy of the logical channels data sorted by channel number."""
        async with self._mutex:
            return sorted((LogicalChannelInfoWithId({**lc, "logical_channel_id": logical_channel_id})
                   for logical_channel_id, lc in self._logical_channels_data.items()), key=lambda x: _channel_num_sort_key(x["channel_num"]))

    async def get_logical_channel_by_id(self, logical_channel_id: LogicalChannelId) -> LogicalChannelInfo | None:
        """Gets a logical channel by its ID."""
        async with self._mutex:
            return self._logical_channels_data.get(logical_channel_id)

    async def get_logical_channel_with_id(self, logical_channel_id: LogicalChannelId) -> LogicalChannelInfoWithId | None:
        """Gets a logical channel with its ID for UI display."""
        async with self._mutex:
            lc_info = self._logical_channels_data.get(logical_channel_id)
            if lc_info:
                return LogicalChannelInfoWithId({**lc_info, "logical_channel_id": logical_channel_id})

    def _generate_next_logical_channel_id(self) -> LogicalChannelId:
        """Generates the next available ID."""
        existing_id_strings = self._logical_channels_data.keys()
        if not existing_id_strings: return INITIAL_LOGICAL_CHANNEL_ID
        numeric_ids = [int(id_str) for id_str in existing_id_strings]
        return LogicalChannelId(str(max(numeric_ids) + 1))

    async def add_logical_channel(self, raw_lc_data: LogicalChannelInfo) -> LogicalChannelId | None:
        """Adds a new logical channel to the configuration."""
        async with self._mutex:
            new_lc_id = self._generate_next_logical_channel_id()
            new_lc_data = LogicalChannelInfo({**raw_lc_data})
            new_data = LogicalChannelsDataImpl({**self._logical_channels_data})
            new_data[new_lc_id] = new_lc_data
            if not await self.config.save_logical_channels_config(new_data):
                Log.critical(self.label, f"Failed to save new logical channel configuration: {new_lc_data}")
                return
            self._logical_channels_data = new_data
            return new_lc_id

    async def update_logical_channel(self, logical_channel_id: LogicalChannelId, updated_lc_data: LogicalChannelInfo) -> bool:
        """Updates a logical channel in the configuration."""
        async with self._mutex:
            if logical_channel_id not in self._logical_channels_data:
                Log.error(self.label, f"Failed to find Logical Channel ID {logical_channel_id} for update: {updated_lc_data}")
                return False
            new_data = LogicalChannelsDataImpl({**self._logical_channels_data})
            new_data[logical_channel_id] = LogicalChannelInfo({**updated_lc_data})
            if not await self.config.save_logical_channels_config(new_data):
                Log.critical(self.label, f"Failed to save updated logical channel configuration for {logical_channel_id}: {updated_lc_data}")
                return False
            self._logical_channels_data = new_data
            return True

    async def delete_logical_channel(self, logical_channel_id: LogicalChannelId) -> bool:
        """Deletes a logical channel from the configuration."""
        async with self._mutex:
            if logical_channel_id not in self._logical_channels_data:
                Log.error(self.label, f"Failed to delete logical channel {logical_channel_id}: not found.")
                return False
            if logical_channel_id in self._channel_mappings_data:
                new_mappings = ChannelMappingsDataImpl({k: ChannelMappingsImpl({x: y for x, y in v.items()})
                                                        for k, v in self._channel_mappings_data.items()})
                del new_mappings[logical_channel_id]
                if not await self.config.save_channel_mappings_config(new_mappings):
                    Log.critical(self.label, f"Failed to save channel mappings when deleting logical channel {logical_channel_id}. Restoring original mappings.")
                    return False
                self._channel_mappings_data = new_mappings

            new_data = LogicalChannelsDataImpl({**self._logical_channels_data})
            del_lc_data = new_data.pop(logical_channel_id)
            if not await self.config.save_logical_channels_config(new_data):
                Log.critical(self.label, f"Failed to save updated logical channels configuration after deleting {logical_channel_id}: {del_lc_data}")
                return False
            self._logical_channels_data = new_data
            return True

    async def get_source_priority(self, source_id: SourceId) -> Priority | None:
        """Retrieves the priority of a source."""
        async with self._mutex:
            for channel_mapping in self._channel_mappings_data.values():
                if source_id in channel_mapping:
                    return channel_mapping[source_id]["priority"]

    async def get_sources_mapped_elsewhere(self, logical_channel_id: LogicalChannelId | None) -> dict[SourceId, str]:
        """Returns the sources with their channel numbers that are mapped to channels other than the specified logical channel."""
        async with self._mutex:
            sources_mapped_elsewhere: dict[SourceId, str] = {}
            for lc_id, mappings in self._channel_mappings_data.items():
                if lc_id == logical_channel_id:
                    continue
                logical_channel = self._logical_channels_data[lc_id]
                for source_id in mappings:
                    if source_id in sources_mapped_elsewhere:
                        sources_mapped_elsewhere[source_id] += f" #{logical_channel['channel_num']}"
                    else:
                        sources_mapped_elsewhere[source_id] = f"#{logical_channel['channel_num']}"
            return sources_mapped_elsewhere

    async def copy_channel_mappings_data(self) -> ChannelMappingsDataImpl:
        """Returns a copy of the channel mappings data."""
        async with self._mutex:
            return ChannelMappingsDataImpl({k: ChannelMappingsImpl({x: y for x, y in v.items()})
                                            for k, v in self._channel_mappings_data.items()})

    async def get_channel_mappings_for_ui(self, logical_channel_id: LogicalChannelId) -> list[SourceMappingInfoWithId]:
        """Returns a list of source mappings for a logical channel."""
        async with self._mutex:
            mappings = self._channel_mappings_data.get(logical_channel_id, ChannelMappingsImpl({}))
            return [SourceMappingInfoWithId({**mapping, "source_id": source_id})
                    for source_id, mapping in mappings.items()]

    async def get_mappings_for_logical_channel(self, logical_channel_id: LogicalChannelId) -> ChannelMappings | None:
        """Retrieves mappings for a logical channel."""
        async with self._mutex:
            return self._channel_mappings_data.get(logical_channel_id)

    async def _update_source_id_for_mapping(self, logical_channel_id: LogicalChannelId, old_source_id: SourceId, new_source_id: SourceId) -> bool:
        """Updates a source ID in the mappings for a logical channel."""
        if logical_channel_id not in self._channel_mappings_data:
            Log.error(self.label, f"Logical channel {logical_channel_id} not found for updating source ID.")
            return False
        mappings = self._channel_mappings_data[logical_channel_id]
        if old_source_id not in mappings:
            Log.error(self.label, f"Source ID {old_source_id} not found in mappings for logical channel {logical_channel_id}.")
            return False
        new_mappings_data = ChannelMappingsDataImpl({k: ChannelMappingsImpl({x: y for x, y in v.items()})
                                                    for k, v in self._channel_mappings_data.items()})
        new_mappings_data[logical_channel_id][new_source_id] = new_mappings_data[logical_channel_id].pop(old_source_id)
        if not await self.config.save_channel_mappings_config(new_mappings_data):
            Log.critical(self.label, f"Failed to save updated channel mappings after changing source ID {old_source_id} to {new_source_id} for logical channel {logical_channel_id}.")
            return False
        self._channel_mappings_data = new_mappings_data
        return True

    async def update_mappings_for_logical_channel(self, logical_channel_id: LogicalChannelId, new_mappings: list[SourceMappingInfoWithId]) -> bool:
        """Updates mappings for a logical channel."""
        async with self._mutex:
            new_mappings_data = ChannelMappingsDataImpl({k: ChannelMappingsImpl({x: y for x, y in v.items()})
                                                        for k, v in self._channel_mappings_data.items()})
            new_mappings_data[logical_channel_id] = ChannelMappingsImpl({m["source_id"]: {"priority": m["priority"], "offset": m["offset"]}
                                                                         for m in new_mappings})
            if not await self.config.save_channel_mappings_config(new_mappings_data):
                Log.critical(self.label, f"Failed to save updated channel mappings for logical channel {logical_channel_id}.")
                return False
            self._channel_mappings_data = new_mappings_data
            return True

    def copy_channel_list_data(self) -> ChannelListData:
        """Returns a copy of the channel list data."""
        return ChannelListDataImpl({k: [c for c in v] for k, v in self._channel_list_data.items()})  # No mutex since we never modify this data

    def find_matching_predefined_channel(self, channel_name: LogicalChannelTitle, channel_num: ChannelNum) -> ChannelListInfo | None:
        """Finds a matching predefined channel based on name or number."""
        channel_list_data = self.copy_channel_list_data()
        for _, channel_list in channel_list_data.items():
            for pre_channel in channel_list:
                if channel_name == pre_channel["title"]: return pre_channel
        for _, channel_list in channel_list_data.items():
            for pre_channel in channel_list:
                if channel_name in pre_channel["aliases"]: return pre_channel
        for _, channel_list in channel_list_data.items():
            for pre_channel in channel_list:
                pre_channel_title = pre_channel["title"]
                if channel_name in pre_channel_title: return pre_channel
                if pre_channel_title and pre_channel_title in channel_name: return pre_channel
                for pre_channel_name in pre_channel["aliases"]:
                    if channel_name in pre_channel_name: return pre_channel
                    if pre_channel_name in channel_name: return pre_channel
        for _, channel_list in channel_list_data.items():
            for pre_channel in channel_list:
                if channel_num == pre_channel["num"]: return pre_channel

    def search_predefined_channel_nums(self, raw_query: str) -> list[ChannelListGroup]:
        """Searches the predefined channel lists for channels matching the number."""
        if not raw_query:
            return []
        query_num = raw_query.strip()
        matches: list[ChannelListGroup] = []
        for group, channels in self.copy_channel_list_data().items():
            for channel in channels:
                if query_num in channel["num"]:
                    matches.append({**channel, 'group': group})

        def sort_key(x: ChannelListGroup) -> tuple[int, float, str, str]:
            channel_num = x["num"]
            channel_title = x["title"]
            if channel_num == query_num:
                return (0, 0, channel_num, channel_title)  # Exact match gets highest priority
            if channel_num.startswith(query_num):
                return (1, len(channel_num), channel_num, channel_title)  # Prefix match, shorter numbers first
            try:
                return (2, float(channel_num), channel_num, channel_title)  # Convert to float for proper numeric sorting
            except ValueError:
                return (2, float('inf'), channel_num, channel_title)  # Non-numeric
        
        matches.sort(key=sort_key)
        return matches[:10]

    def search_predefined_channel_names(self, raw_query: str) -> list[ChannelListGroup]:
        """Searches the predefined channel lists for channels matching the query."""
        if not raw_query:
            return []
        query = raw_query.strip().lower()
        matches: list[ChannelListGroup] = []
        found_with: dict[str, str] = {}
        for group, channels in self.copy_channel_list_data().items():
            for channel in channels:
                title = channel["title"].lower()
                words = query.split()
                if all(word in title for word in words):
                    matches.append({**channel, 'group': group})
                    found_with[channel['title']] = title
                    continue
                for name in channel["aliases"]:
                    title = name.lower()
                    if all(word in title for word in words):
                        matches.append({**channel, 'group': group})
                        found_with[channel["title"]] = title
                        break

        def sort_key(x: ChannelListGroup) -> tuple[int, float, int, str]:
            channel_title = found_with[x["title"]]
            pos = channel_title.find(query)
            if pos == -1:
                pos = float('inf')
            if channel_title == query:
                return (0, pos, len(channel_title), x["title"])
            return (1, pos, len(channel_title), x["title"])
        
        matches.sort(key=sort_key)
        return matches[:10]

    async def get_provider_slots(self, alias: ProviderAlias) -> ProviderSlots | None:
        """Returns the provider slots configuration."""
        async with self._mutex:
            return self._slots.get(alias)

    async def copy_client_channels(self) -> ClientChannelInfos:
        """Returns a copy of the client-facing channels."""
        async with self._mutex:
            return ClientChannelInfosImpl({**self._client_channels})

    # --- Playlists ---

    async def get_playlist(self, playlist_id: PlaylistId) -> PlaylistInfo | None:
        """Gets a playlist by its ID."""
        async with self._mutex:
            return self._playlists_data.get(playlist_id)

    async def get_playlists_for_ui(self) -> list[PlaylistInfoWithId]:
        """Returns all playlists with the main playlist first, then sorted by name."""
        async with self._mutex:
            return sorted((PlaylistInfoWithId({**playlist, "playlist_id": playlist_id}) for playlist_id, playlist in self._playlists_data.items()),
                          key=lambda x: (x["playlist_id"] != MAIN_PLAYLIST_ID, x["name"].casefold()))

    async def get_playlist_channel_counts(self) -> dict[PlaylistId, PlaylistChannelCounts]:
        """Returns the number of logical channels and source channels in each playlist's M3U."""
        async with self._mutex:
            return {**self._playlist_counts}

    async def get_playlist_entries(self, playlist_id: PlaylistId) -> tuple[PlaylistEntry, ...] | None:
        """Returns the M3U entries of a playlist, the base URL must be prepended to each path."""
        async with self._mutex:
            return self._playlist_entries.get(playlist_id)

    async def get_playlist_lineup(self, playlist_id: PlaylistId) -> tuple[PlaylistLineupEntry, ...] | None:
        """Returns the HDHomeRun lineup of a playlist, only logical channels with a channel number are included."""
        async with self._mutex:
            return self._playlist_lineups.get(playlist_id)

    def _generate_playlist_id(self, name: str) -> PlaylistId:
        """Generates a unique, URL-safe ID from the playlist name."""
        base_id = NOT_PLAYLIST_ID_REGEX.sub('-', name.casefold()).strip('-')[:40].strip('-') or "playlist"
        playlist_id = PlaylistId(base_id)
        suffix = 2
        while playlist_id in self._playlists_data or playlist_id == MAIN_PLAYLIST_ID:
            playlist_id = PlaylistId(f"{base_id}-{suffix}")
            suffix += 1
        return playlist_id

    async def add_playlist(self, playlist: PlaylistInfo) -> PlaylistId | None:
        """Adds a new playlist to the configuration."""
        async with self._mutex:
            playlist_id = self._generate_playlist_id(playlist["name"])
            new_data = PlaylistsDataImpl({**self._playlists_data, playlist_id: PlaylistInfo({**playlist})})
            if not await self.config.save_playlists_config(new_data):
                Log.critical(self.label, f"Failed to save new playlist configuration: {playlist}")
                return
            self._playlists_data = new_data
            return playlist_id

    async def update_playlist(self, playlist_id: PlaylistId, playlist: PlaylistInfo) -> bool:
        """Updates an existing playlist, the ID never changes so client URLs stay valid."""
        async with self._mutex:
            if playlist_id not in self._playlists_data:
                raise ValueError(f"Playlist '{playlist_id}' not found.")
            new_data = PlaylistsDataImpl({**self._playlists_data, playlist_id: PlaylistInfo({**playlist})})
            if not await self.config.save_playlists_config(new_data):
                Log.critical(self.label, f"Failed to save updated playlist configuration for '{playlist_id}': {playlist}")
                return False
            self._playlists_data = new_data
            return True

    async def delete_playlist(self, playlist_id: PlaylistId) -> bool:
        """Deletes a playlist, the main playlist cannot be deleted."""
        async with self._mutex:
            if playlist_id == MAIN_PLAYLIST_ID:
                raise ValueError("The main playlist cannot be deleted.")
            if playlist_id not in self._playlists_data:
                raise ValueError(f"Playlist '{playlist_id}' not found.")
            new_data = PlaylistsDataImpl({k: v for k, v in self._playlists_data.items() if k != playlist_id})
            if not await self.config.save_playlists_config(new_data):
                Log.critical(self.label, f"Failed to save playlists configuration after deleting '{playlist_id}'.")
                return False
            self._playlists_data = new_data
            return True

    async def get_lc_groups_for_ui(self) -> list[LCGroupInfo]:
        """Returns the groups used by logical channels with their channel counts."""
        async with self._mutex:
            groups: dict[GroupKey, dict[str, Any]] = {}
            for logical_channel in self._logical_channels_data.values():
                group_title = logical_channel["group_title"].strip() or DEFAULT_LC_GROUP_TITLE
                group = groups.setdefault(normalize_group_key(group_title), {"display_title": group_title, "channel_count": 0})
                group["channel_count"] += 1
        return sorted((LCGroupInfo({"key": key, "display_title": TVGGroupTitle(group["display_title"]), "channel_count": group["channel_count"]})
                       for key, group in groups.items()), key=lambda x: x["display_title"].casefold())

    async def get_source_groups_for_ui(self) -> list[SourceGroupInfo]:
        """Returns all discovered source groups merged across providers."""
        async with self._mutex:
            source_groups = list(self._source_groups.values())
        return sorted(source_groups, key=lambda x: x["display_title"].casefold())
