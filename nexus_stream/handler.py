from datetime import datetime, timedelta
import re
import html
import hashlib
import asyncio
from typing import Callable, Final, Self

import aiohttp
from nexus_stream.config import Config
from nexus_stream.slots import ProviderSlots
from nexus_stream.utils import (M3UURL, NEXUS_STREAM_USER_AGENT, ClientChannelInfos, ClientChannelInfosImpl, ChannelListData, ChannelListDataImpl,
                                ChannelListGroup, ChannelListInfo, ChannelMappings, ChannelMappingsImpl, ChannelMappingsData, ChannelMappingsDataImpl, ChannelNum, DateTimeISO,
                                DiscoveredSource, DiscoveredSourceWithId, DiscoveredSourcesData, DiscoveredSourcesDataImpl, LogicalChannelInfo, LogicalChannelInfoWithId, LogicalChannelTitle, LogicalChannelsDataImpl, Priority, ProviderInfo,
                                ProviderStatuses, SourceInfo, SourceMappingInfoWithId, SourceId, StreamURL, TVGGroupTitle, Label, LogicalChannelId,
                                LogicalChannelsData, M3USource, MainM3UPlaylist, MaxStreams, ProviderAlias, ProvidersData, ProvidersDataImpl,
                                StreamKey, TVGDisplayTitle, TVGId, TVGLogo, TVGName, VideoType, create_stream_key)

# --- Constants ---
INITIAL_LOGICAL_CHANNEL_ID: Final[LogicalChannelId] = LogicalChannelId("1000")
PROVIDER_FETCH_TIMEOUT: Final[aiohttp.ClientTimeout] = aiohttp.ClientTimeout(total=20)
DISCOVER_SOURCES_INTERVAL: Final[int] = 60 * 60 * 24
HTTP_REGEX: Final[re.Pattern[str]] = re.compile(r'^https?://', re.IGNORECASE)
TVG_NAME_REGEX: Final[re.Pattern[str]] = re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE)
TVG_ID_REGEX: Final[re.Pattern[str]] = re.compile(r'tvg-id="([^"]*)"', re.IGNORECASE)
TVG_LOGO_REGEX: Final[re.Pattern[str]] = re.compile(r'tvg-logo="([^"]*)"', re.IGNORECASE)
TVG_GROUP_TITLE_REGEX: Final[re.Pattern[str]] = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)


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
        'label', 'config', '_mutex', '_providers_data', '_discovered_sources_data',
        '_logical_channels_data', '_channel_mappings_data', '_channel_list_data',
        '_client_channels', '_main_m3u_playlist',
        '_slots', '_kill_provider_streams', '_pending_streams',
    )
    
    def __init__(self, config: Config) -> None:
        """
        Initializes the ChannelHandler with the provided configuration.
        """
        self.label: Label = Label.STARTUP
        self.config: Config = config
        self._mutex: asyncio.Lock = asyncio.Lock()

        # Data loaded from configuration files
        self._providers_data: ProvidersData = ProvidersDataImpl({})
        self._discovered_sources_data: DiscoveredSourcesData = DiscoveredSourcesDataImpl({})
        self._logical_channels_data: LogicalChannelsData = LogicalChannelsDataImpl({})
        self._channel_mappings_data: ChannelMappingsData = ChannelMappingsDataImpl({})
        self._channel_list_data: ChannelListData = ChannelListDataImpl({})

        # In-memory processed data
        self._client_channels: ClientChannelInfos = ClientChannelInfosImpl({})
        self._main_m3u_playlist: MainM3UPlaylist = MainM3UPlaylist("#EXTM3U\n")

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
        Loads all data from JSON files and rebuilds the in-memory channel structures asynchronously.
        """
        logged_waiting = False
        await self._mutex.acquire()
        while self.get_pending_stream_count():
            self._mutex.release()
            if not logged_waiting:
                self.config.warn(self.label, "Waiting for streams to finish being created before reloading configurations...")
                logged_waiting = True
            await asyncio.sleep(0.01)
            await self._mutex.acquire()
        try:
            self.config.info(self.label, "Reloading ChannelHandler configurations")

            self._discovered_sources_data = await self.config.get_discovered_sources_config()
            self._logical_channels_data = await self.config.get_logical_channels_config()
            self._channel_mappings_data = await self.config.get_channel_mappings_config()
            self._channel_list_data = await self.config.get_channel_list_config()

            if update_providers:
                self._providers_data = await self.config.get_providers_config()
                self._update_providers_slots()

            prev_discovered_sources = DiscoveredSourcesDataImpl({**self._discovered_sources_data})
            min_updated_at = min([p_data["updated_at"] or DateTimeISO("0001-01-01") for p_data in self._providers_data.values()], default=DateTimeISO("0001-01-01"))
            now = datetime.now()
            if force_discover_sources or datetime.fromisoformat(min_updated_at) < now - timedelta(seconds=DISCOVER_SOURCES_INTERVAL):
                self.config.info(self.label, "Discovering sources from configured providers...")
                await self._parse_all_provider_m3us_and_populate_discovered_sources()
                if not await self.config.save_discovered_sources_config(self._discovered_sources_data):
                    self.config.critical(self.label, "Failed to save discovered sources configuration.")
                new_providers_data = ProvidersDataImpl({alias: {
                    "m3u_url": details["m3u_url"],
                    "max_streams": details["max_streams"],
                    "updated_at": DateTimeISO(now.isoformat())
                } for alias, details in self._providers_data.items()})
                if await self.config.save_providers_config(new_providers_data):
                    self._providers_data = new_providers_data
                else:
                    self.config.critical(self.label, "Failed to save updated provider configuration after discovery.")

            # Build in-memory data
            self._build_client_channels(prev_discovered_sources)
            self.generate_main_client_m3u()
            
            self.config.info(self.label,
                f"ChannelHandler ready. Discovered: {len(self._discovered_sources_data)}, "
                f"Client-Facing: {len(self._client_channels)}"
            )
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
            self.config.error(self.label, "Invalid Extended M3U format: Missing #EXTM3U header.")
            return m3u_sources

        REGEX_DEFAULT: tuple[str, str] = ("", "")
        current_extinf: str | None = None
        for line in lines:
            if line.startswith("#EXTINF:"):
                if current_extinf:
                    self.config.warn(self.label, f"Found #EXTINF without a preceding stream URL, skipping: {current_extinf}")
                current_extinf = line
                continue
            if HTTP_REGEX.match(line):
                if not current_extinf:
                    self.config.warn(self.label, "Invalid M3U format: Missing #EXTINF line before stream URL.")
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
                    "tvg_log": TVGLogo(tvg_logo),
                    "stream_url": StreamURL(line),
                })
                current_extinf = None
        return m3u_sources

    def _generate_source_id(self, provider_alias: ProviderAlias, stream_url: StreamURL) -> SourceId:
        """Creates a stable, unique ID for a source stream. (Sync - pure function)"""
        id_material = f"{provider_alias}:{stream_url}"
        return SourceId(f"{provider_alias}:{hashlib.md5(id_material.encode('utf-8')).hexdigest()}")
    
    async def _fetch_and_parse_provider(self, session: aiohttp.ClientSession, provider_alias: ProviderAlias, m3u_url: M3UURL) -> DiscoveredSourcesData:
        """Fetches and parses a single provider's M3U asynchronously."""
        try:
            discovered_sources = DiscoveredSourcesDataImpl({})
            async with session.get(m3u_url, timeout=PROVIDER_FETCH_TIMEOUT, headers={'User-Agent': NEXUS_STREAM_USER_AGENT}) as response:
                response.raise_for_status()           
                m3u_sources = self._parse_source_m3u_lines(await response.text())

                for m3u_source in m3u_sources:
                    source_id = self._generate_source_id(provider_alias, m3u_source["stream_url"])
                    discovered_sources[source_id] = {
                        "provider_alias": provider_alias,
                        **m3u_source
                    }
                self.config.info(self.label, f"Discovered {len(m3u_sources)} sources from provider '{provider_alias}'.")
                return discovered_sources
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.config.error(self.label, f"Failed to fetch or parse provider '{provider_alias}': {e}")
            raise
        except BaseException as e:
            self.config.error(self.label, f"An unexpected error occurred while processing provider '{provider_alias}': {e}")
            raise

    async def _parse_all_provider_m3us_and_populate_discovered_sources(self) -> None:
        """Uses asyncio.gather to fetch and parse all configured provider M3Us concurrently."""
        self.config.info(self.label, "Starting to parse all provider M3Us...")
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_and_parse_provider(session, alias, details["m3u_url"])
                for alias, details in self._providers_data.items()
            ]
            if not tasks:
                self.config.debug(self.label, "No providers configured to parse.")
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)
            discovered_sources = DiscoveredSourcesDataImpl({})
            for res in results:
                if not isinstance(res, BaseException):
                    discovered_sources.update(res)
            self._discovered_sources_data = discovered_sources

        self.config.info(self.label, f"Finished parsing. Total discovered sources: {len(self._discovered_sources_data)}")

    def _build_client_channels(self, prev_discovered_sources: DiscoveredSourcesData) -> None:
        """Builds the final list of channels exposed to clients."""
        self.config.info(self.label, "Building client-facing channels...")
        new_client_channels = ClientChannelInfosImpl({})
        for logical_channel_id, lc_def in self._logical_channels_data.items():
            mapped_sources_for_lc = [(source_mapping["priority"], source_id)
                                     for source_id, source_mapping in self._channel_mappings_data.get(logical_channel_id, {}).items()]
            mapped_sources_for_lc.sort(key=lambda x: x[0]) 
            processed_sources: list[SourceInfo] = []
            for priority, source_id in mapped_sources_for_lc:
                discovered_source = self._discovered_sources_data.get(source_id)
                if discovered_source:
                    processed_sources.append({
                        "source_id": source_id,
                        "priority": priority,
                        "provider_alias": discovered_source["provider_alias"],
                        "stream_url": discovered_source["stream_url"],
                    })
                else:
                    if source_id in prev_discovered_sources:
                        prev_discovered_source = prev_discovered_sources[source_id]
                        source_name = f"'{prev_discovered_source['display_title'] or prev_discovered_source['tvg_name']}' ({source_id})"
                    else:
                        source_name = f"'Unknown Source' ({source_id})"
                    self.config.warn(self.label, f"Mapped source {source_name} for '{lc_def.get('logical_channel_title', logical_channel_id)}'{f' ({lc_def['channel_num']})' if 'channel_num' in lc_def else ''} not found in discovered sources.")

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
                self.config.warn(self.label, f"No valid mapped sources for logical channel '{logical_channel_id}'. It will not be included in the client M3U.")
        self._client_channels = new_client_channels
        self.config.info(self.label, f"Built {len(self._client_channels)} client-facing channels.")

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

    async def add_pending_stream(self, logical_channel_id: LogicalChannelId, video_type: VideoType) -> bool:
        stream_key = create_stream_key(video_type, logical_channel_id)
        async with self._mutex:
            if stream_key in self._pending_streams:
                return False
            self._pending_streams.add(stream_key)
            return True

    async def remove_pending_stream(self, logical_channel_id: LogicalChannelId, video_type: VideoType) -> None:
        """Removes a pending stream from the set of pending streams."""
        async with self._mutex:
            self._pending_streams.remove(create_stream_key(video_type, logical_channel_id))

    def generate_main_client_m3u(self) -> None:
        """Generates the main M3U content to be served to clients. (Sync - CPU-bound)"""
        m3u_lines = ["#EXTM3U x-tvg-url=\"\""]
        if not self.config.nexus_url:
            self.config.error(self.label, "NEXUS_URL not set. Client M3U URLs will be incorrect.")
            m3u_lines.extend(["#EXTINF:-1,Error: NEXUS_URL not configured", "http://error.invalid/stream"])
            self._main_m3u_playlist = MainM3UPlaylist("\n".join(m3u_lines) + "\n")
            return

        client_channels = [(logical_channel_id, logical_channel) for logical_channel_id, logical_channel in self._client_channels.items()]
        client_channels.sort(key=lambda item: (item[1].get("group_title", "zzz").lower(), item[1].get("logical_channel_title", "zzz").lower()))

        for logical_channel_id, logical_channel in client_channels:
            name = logical_channel.get("logical_channel_title", "")
            extinf_parts = [f'tvg-name="{name}"']
            if ch_num := logical_channel.get("channel_num"): extinf_parts.append(f'tvg-chno="{ch_num}"')
            if tvg_id := logical_channel.get("tvg_id"): extinf_parts.append(f'tvg-id="{tvg_id}"')
            if logo := logical_channel.get("tvg_logo"): extinf_parts.append(f'tvg-logo="{logo}"')
            if group := logical_channel.get("group_title"): extinf_parts.append(f'group-title="{group}"')
            
            m3u_lines.append(f"#EXTINF:-1 {' '.join(extinf_parts)},{name}")
            m3u_lines.append(f"{self.config.nexus_url}/{VideoType.HLS}/{logical_channel_id}/playlist.m3u8")
        
        self._main_m3u_playlist = MainM3UPlaylist("\n".join(m3u_lines) + "\n")
        self.config.info(self.label, f"Generated main client M3U with {len(self._client_channels)} channels.")

    def get_sources_for_client_channel(self, logical_channel_id: LogicalChannelId) -> list[SourceInfo]:
        """Retrieves source stream URLs for a channel."""
        return self._client_channels.get(logical_channel_id, {}).get("sources", [])

    def _update_providers_slots(self) -> None:
        """Initializes or updates provider slots based on config asynchronously."""
        providers_to_delete: set[ProviderAlias] = set()
        for alias, curr_details in self._slots.items():
            if alias not in self._providers_data:
                self.config.info(self.label, f"Removing slots for provider '{alias}' as it no longer exists in configuration.")
                providers_to_delete.add(alias)
                self._kill_provider_streams.add(alias)
                continue
            m3u_url = self._providers_data[alias]["m3u_url"]
            max_streams = self._providers_data[alias]["max_streams"]
            if curr_details.get_m3u_url() == m3u_url and curr_details.get_total_slots() == max_streams:
                continue
            self.config.info(self.label, f"Updating slots for provider '{alias}' with new URL or max streams.")
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
            self.config.info(self.label, f"Initialized slots for provider '{alias}' with capacity {max_streams}")
        
    async def reload_handler_config(self, *, update_providers: bool = False, force_discover_sources: bool = False) -> None:
        """Public method to trigger a full async reload of the handler's configuration."""
        await self._load_and_process_configurations(update_providers=update_providers, force_discover_sources=force_discover_sources)

    async def get_provider_stream_status(self) -> ProviderStatuses:
        """Calculates current stream usage for each provider asynchronously."""
        async with self._mutex:
            return ProviderStatuses({alias: {
                "alias": alias,
                "m3u_url": provider_slots.get_m3u_url(),
                "active_streams": await provider_slots.get_active_slots(),
                "max_streams": provider_slots.get_total_slots()
            } for alias, provider_slots in self._slots.items()})

    # --- UI Interaction Methods ---
    
    async def add_provider(self, alias: ProviderAlias, m3u_url: M3UURL, max_streams: MaxStreams) -> bool:
        """Adds a new provider to the configuration asynchronously."""
        if not alias: raise ValueError("Provider alias cannot be empty.")
        if not m3u_url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 0: raise ValueError("Max concurrent streams must be at least 0.")

        async with self._mutex:
            if alias.lower() in {a.lower() for a in self._providers_data.keys()}:
                raise ValueError(f"Provider with alias '{alias}' already exists.")

            new_providers_data = ProvidersDataImpl({**self._providers_data, alias: ProviderInfo({
                "m3u_url": m3u_url,
                "max_streams": max_streams,
                "updated_at": None
            })})
            if not await self.config.save_providers_config(new_providers_data):
                self.config.critical(self.label, f"Failed to save new provider configuration for '{alias}'.")
                return False
            self._providers_data = new_providers_data
            self._update_providers_slots()
            return True

    async def update_provider(self, alias: ProviderAlias, m3u_url: M3UURL, max_streams: MaxStreams) -> bool:
        """Updates an existing provider's configuration asynchronously."""
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
                self.config.critical(self.label, f"Failed to save updated provider configuration for '{alias}'.")
                return False
            self._providers_data = new_providers_data
            self._update_providers_slots()
            return True

    async def delete_provider(self, alias: ProviderAlias) -> bool:
        """Deletes a provider from the configuration asynchronously."""
        async with self._mutex:
            if alias not in self._providers_data: raise ValueError(f"Provider with alias '{alias}' not found.")

            new_providers_data = ProvidersDataImpl({**self._providers_data})
            del new_providers_data[alias]
            if not await self.config.save_providers_config(new_providers_data):
                self.config.critical(self.label, f"Failed to save updated provider configuration after deleting '{alias}'.")
                return False
            self._providers_data = new_providers_data
            self._update_providers_slots()
            return True

    async def get_discovered_sources_for_ui(self) -> list[DiscoveredSourceWithId]:
        """Gets a list of discovered sources."""
        async with self._mutex:
            all_sources = [DiscoveredSourceWithId({**source, "source_id": source_id})
                            for source_id, source in self._discovered_sources_data.items()]
        return sorted(all_sources, key=lambda x: (x["provider_alias"], (x.get("tvg_name","") or x.get("display_title","")).lower()))

    async def get_logical_channels_for_ui(self, key: Callable[[LogicalChannelInfo], str] | None = None) -> list[LogicalChannelInfoWithId]:
        """Returns a copy of the logical channels data."""
        async with self._mutex:
            res = [LogicalChannelInfoWithId({**lc, "logical_channel_id": logical_channel_id})
                   for logical_channel_id, lc in self._logical_channels_data.items()]
        if key:
            res.sort(key=key)
        return res

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
            return

    def _generate_next_logical_channel_id(self) -> LogicalChannelId:
        """Generates the next available ID. (Sync - CPU-bound)"""
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
                self.config.critical(self.label, f"Failed to save new logical channel configuration: {new_lc_data}")
                return
            self._logical_channels_data = new_data
            return new_lc_id

    async def update_logical_channel(self, logical_channel_id: LogicalChannelId, updated_lc_data: LogicalChannelInfo) -> bool:
        """Updates a logical channel in the configuration."""
        async with self._mutex:
            if logical_channel_id not in self._logical_channels_data:
                self.config.error(self.label, f"Failed to find Logical Channel ID {logical_channel_id} for update: {updated_lc_data}")
                return False
            new_data = LogicalChannelsDataImpl({**self._logical_channels_data})
            new_data[logical_channel_id] = LogicalChannelInfo({**updated_lc_data})
            if not await self.config.save_logical_channels_config(new_data):
                self.config.critical(self.label, f"Failed to save updated logical channel configuration for {logical_channel_id}: {updated_lc_data}")
                return False
            self._logical_channels_data = new_data
            return True

    async def delete_logical_channel(self, logical_channel_id: LogicalChannelId) -> bool:
        """Deletes a logical channel from the configuration."""
        async with self._mutex:
            if logical_channel_id not in self._logical_channels_data:
                self.config.error(self.label, f"Failed to delete logical channel {logical_channel_id}: not found.")
                return False
            if logical_channel_id in self._channel_mappings_data:
                new_mappings = ChannelMappingsDataImpl({k: ChannelMappingsImpl({x: y for x, y in v.items()})
                                                        for k, v in self._channel_mappings_data.items()})
                del new_mappings[logical_channel_id]
                if not await self.config.save_channel_mappings_config(new_mappings):
                    self.config.critical(self.label, f"Failed to save channel mappings when deleting logical channel {logical_channel_id}. Restoring original mappings.")
                    return False
                self._channel_mappings_data = new_mappings

            new_data = LogicalChannelsDataImpl({**self._logical_channels_data})
            del_lc_data = new_data.pop(logical_channel_id)
            if not await self.config.save_logical_channels_config(new_data):
                self.config.critical(self.label, f"Failed to save updated logical channels configuration after deleting {logical_channel_id}: {del_lc_data}")
                return False
            self._logical_channels_data = new_data
            return True

    async def get_source_priority(self, source_id: SourceId) -> Priority | None:
        """Retrieves the priority of a source."""
        async with self._mutex:
            for channel_mapping in self._channel_mappings_data.values():
                if source_id in channel_mapping:
                    return channel_mapping[source_id]["priority"]
            return None

    async def get_all_mapped_source_ids(self) -> set[SourceId]:
        """Returns a set of all source IDs that are mapped to logical channels."""
        async with self._mutex:
            return {source_id for mappings in self._channel_mappings_data.values() for source_id in mappings}        

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

    async def update_mappings_for_logical_channel(self, logical_channel_id: LogicalChannelId, new_mappings: list[SourceMappingInfoWithId]) -> bool:
        """Updates mappings for a logical channel."""
        async with self._mutex:
            new_mappings_data = ChannelMappingsDataImpl({k: ChannelMappingsImpl({x: y for x, y in v.items()})
                                                    for k, v in self._channel_mappings_data.items()})
            new_mappings_data[logical_channel_id] = ChannelMappingsImpl({m["source_id"]: {"priority": m["priority"]}
                                                                         for m in new_mappings if "source_id" in m})
            if not await self.config.save_channel_mappings_config(new_mappings_data):
                self.config.critical(self.label, f"Failed to save updated channel mappings for logical channel {logical_channel_id}.")
                return False
            self._channel_mappings_data = new_mappings_data
            return True

    async def copy_channel_list_data(self) -> ChannelListData:
        """Returns a copy of the channel list data."""
        async with self._mutex:
            return ChannelListDataImpl({k: [c for c in v] for k, v in self._channel_list_data.items()})

    async def get_channel_lists(self) -> tuple[tuple[ChannelListInfo, ...], ...]:
        """Returns a tuple of tuples containing channel list groups."""
        return tuple(tuple(c for c in v) for v in self._channel_list_data.values())

    async def find_matching_predefined_channel(self, channel_name: LogicalChannelTitle, channel_num: ChannelNum) -> ChannelListInfo | None:
        """Finds a matching predefined channel based on name or number."""
        async with self._mutex:
            predefined_channel_lists = await self.get_channel_lists()
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                if channel_name == pre_channel.get('title'): return pre_channel
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                if channel_name in pre_channel.get('aliases', []): return pre_channel
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                pre_channel_title = pre_channel.get('title', '')
                if channel_name in pre_channel_title: return pre_channel
                if pre_channel_title and pre_channel_title in channel_name: return pre_channel
                for pre_channel_name in pre_channel.get('aliases', []):
                    if channel_name in pre_channel_name: return pre_channel
                    if pre_channel_name in channel_name: return pre_channel
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                if channel_num == pre_channel.get('num'): return pre_channel
        return None

    async def search_predefined_channels(self, raw_query: str) -> list[ChannelListGroup]:
        """Searches the predefined channel lists for channels matching the query."""
        if not raw_query:
            return []
        query = raw_query.strip().lower()
        matches: list[ChannelListGroup] = []
        found_with: dict[str, str] = {}
        for group, channels in (await self.copy_channel_list_data()).items():
            for channel in channels:
                title = channel.get('title', '').lower()
                words = query.split()
                if all(word in title for word in words):
                    matches.append({**channel, 'group': group})
                    found_with[channel['title']] = channel['title']
                    continue
                for name in channel.get('aliases', []):
                    title = name.lower()
                    if all(word in title for word in words):
                        matches.append({**channel, 'group': group})
                        found_with[channel.get('title', channel.get('num', ''))] = name
                        break
        matches.sort(key=lambda x: found_with[x.get('title', x.get('num', ''))].lower().find(query))
        return matches[:10]

    async def get_provider_slots(self, alias: ProviderAlias) -> ProviderSlots | None:
        """Returns the provider slots configuration."""
        async with self._mutex:
            return self._slots.get(alias)

    async def copy_client_channels(self) -> ClientChannelInfos:
        """Returns a copy of the client-facing channels."""
        async with self._mutex:
            return ClientChannelInfosImpl({**self._client_channels})

    async def get_main_m3u_playlist(self) -> MainM3UPlaylist:
        """Returns the main M3U playlist."""
        async with self._mutex:
            return self._main_m3u_playlist
