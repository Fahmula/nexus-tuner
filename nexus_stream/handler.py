from datetime import datetime, timedelta
import re
import html
import hashlib
import asyncio
from typing import Final, Self, cast

import aiohttp
from nexus_stream.config import Config
from nexus_stream.slots import ProviderSlots
from nexus_stream.utils import (DEFAULT_PRIORITY, M3UURL, NEXUS_STREAM_USER_AGENT, ChannelInfos, ChannelInfosMutable, ChannelListData,
                                ChannelListGroup, ChannelListInfo, ChannelMappingsData, ChannelMappingsDataMutable, ChannelNum, DateTimeISO,
                                DiscoveredSource, DiscoveredSourcesData, DiscoveredSourcesDataMutable, LogicalChannelInfo, LogicalChannelName,
                                ProviderStatuses, SourceInfo, SourcePriority, SourceServiceId, StreamURL, TVGGroupTitle, Label, LogicalChannelId,
                                LogicalChannelsData, M3USource, MainM3UPlaylist, MaxStreams, ProviderAlias, ProviderInfoMutable, ProvidersData,
                                ProvidersDataMutable, ProvidersSourceData, StreamKey, TVGDisplayName, TVGId, TVGLogo, TVGName, VideoType, create_stream_key, run_bg)

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
        'label', '_loading', 'config', '_mutex',
        'providers_data', 'discovered_source_services_data',
        'logical_channels_data', 'channel_mappings_data', 'channel_list_data',
        'client_facing_channels', 'main_m3u_playlist',
        'slots', '_kill_provider_streams', 'pending_streams',
    )
    
    def __init__(self, config: Config) -> None:
        """
        Initializes the ChannelHandler with the provided configuration.
        """
        self.label: Label = Label.STARTUP
        self._loading: bool = False
        self.config: Config = config
        self._mutex: asyncio.Lock = asyncio.Lock()

        # Data loaded from configuration files
        self.providers_data: ProvidersData = ProvidersData({})
        self.discovered_source_services_data: DiscoveredSourcesData = DiscoveredSourcesData({})
        self.logical_channels_data: LogicalChannelsData = LogicalChannelsData(())
        self.channel_mappings_data: ChannelMappingsData = ChannelMappingsData({})
        self.channel_list_data: ChannelListData = ChannelListData({})

        # In-memory processed data
        self.client_facing_channels: ChannelInfos = ChannelInfos({})
        self.main_m3u_playlist: MainM3UPlaylist = MainM3UPlaylist("#EXTM3U\n")

        # Slots management
        self.slots: dict[ProviderAlias, ProviderSlots] = {}
        self._kill_provider_streams: set[ProviderAlias] = set()
        self.pending_streams: set[StreamKey] = set()

    @classmethod
    async def create(cls, config: Config) -> Self:
        """Asynchronous factory for creating and initializing a ChannelHandler instance."""
        instance = cls(config)
        await instance._load_and_process_configurations(update_providers=True, force_discover_sources=False)
        instance.label = Label.HANDLER
        return instance

    def is_loading(self) -> bool:
        """Returns True if the handler is currently loading configurations."""
        return self._loading

    async def reset_kill_provider_streams(self) -> set[ProviderAlias]:
        """Resets the kill_provider_streams, returns the aliases that should be killed."""
        async with self._mutex:
            if not self._kill_provider_streams:
                return set()
            tmp = self._kill_provider_streams
            self._kill_provider_streams = set()
            return tmp

    def _generate_source_service_id(self, provider_alias: ProviderAlias, actual_stream_url: StreamURL) -> SourceServiceId:
        """Creates a stable, unique ID for a source stream. (Sync - pure function)"""
        id_material = f"{provider_alias}:{actual_stream_url}"
        return SourceServiceId(f"{provider_alias}:{hashlib.md5(id_material.encode('utf-8')).hexdigest()}")

    async def _load_and_process_configurations(self, *, update_providers: bool, force_discover_sources: bool) -> None:
        """
        Loads all data from JSON files and rebuilds the in-memory channel structures asynchronously.
        """
        self._loading = True
        self.config.info(self.label, "Reloading ChannelHandler configurations")

        self.discovered_source_services_data = await self.config.get_discovered_source_services_config()
        self.logical_channels_data = await self.config.get_logical_channels_config()
        self.channel_mappings_data = await self.config.get_channel_mappings_config()
        self.channel_list_data = await self.config.get_channel_list_config()

        if update_providers:
            self.providers_data = (await self.config.get_providers_config()).get("source_m3u_providers", ProvidersData({}))
            await self._update_providers_slots()

        prev_discovered_source_services: DiscoveredSourcesData = DiscoveredSourcesData({k: v for k, v in self.discovered_source_services_data.items()})
        min_updated_at = min([p_data["updated_at"] or DateTimeISO("0001-01-01") for p_data in self.providers_data.values()], default=DateTimeISO("0001-01-01"))
        now = datetime.now()
        if force_discover_sources or datetime.fromisoformat(min_updated_at) < now - timedelta(seconds=DISCOVER_SOURCES_INTERVAL):
            self.config.info(self.label, "Discovering source services from configured providers...")
            await self._parse_all_provider_m3us_and_populate_discovered_services()
            await self.config.save_discovered_source_services_config(self.discovered_source_services_data)
            for alias in self.providers_data:
                cast(ProviderInfoMutable, self.providers_data[alias])["updated_at"] = DateTimeISO(now.isoformat())
            await self._save_providers_for_ui(ProvidersSourceData({"source_m3u_providers": self.providers_data}), update_slots=False)

        # Build in-memory data
        self._build_client_facing_channels(prev_discovered_source_services)
        self.generate_main_client_m3u()
        
        self._loading = False
        self.config.info(self.label,
            f"ChannelHandler ready. Discovered: {len(self.discovered_source_services_data)}, "
            f"Client-Facing: {len(self.client_facing_channels)}"
        )

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
                    "original_tvg_name": TVGName(tvg_name),
                    "original_display_name_extinf": TVGDisplayName(display_name_from_extinf),
                    "original_group_title": TVGGroupTitle(group_title),
                    "original_tvg_id": TVGId(tvg_id),
                    "original_tvg_logo": TVGLogo(tvg_logo),
                    "actual_stream_url": StreamURL(line),
                })
                current_extinf = None
        return m3u_sources
    
    async def _fetch_and_parse_provider(self, session: aiohttp.ClientSession, provider_alias: ProviderAlias, m3u_url: M3UURL) -> None:
        """Fetches and parses a single provider's M3U asynchronously."""
        try:
            async with session.get(m3u_url, timeout=PROVIDER_FETCH_TIMEOUT, headers={'User-Agent': NEXUS_STREAM_USER_AGENT}) as response:
                response.raise_for_status()           
                m3u_sources = self._parse_source_m3u_lines(await response.text())

                async with self._mutex:
                    for m3u_source in m3u_sources:
                        service_id = self._generate_source_service_id(provider_alias, m3u_source["actual_stream_url"])
                        cast(DiscoveredSourcesDataMutable, self.discovered_source_services_data)[service_id] = {
                            "id": service_id,
                            "provider_alias": provider_alias,
                            **m3u_source
                        }
                self.config.info(self.label, f"Discovered {len(m3u_sources)} sources from provider '{provider_alias}'.")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.config.error(self.label, f"Failed to fetch or parse provider '{provider_alias}': {e}")
            raise
        except Exception as e:
            self.config.error(self.label, f"An unexpected error occurred while processing provider '{provider_alias}': {e}")
            raise

    async def _parse_all_provider_m3us_and_populate_discovered_services(self) -> None:
        """Uses asyncio.gather to fetch and parse all configured provider M3Us concurrently."""
        self.config.info(self.label, "Starting to parse all provider M3Us...")
        cast(DiscoveredSourcesDataMutable, self.discovered_source_services_data).clear()

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_and_parse_provider(session, alias, details["url"])
                for alias, details in self.providers_data.items()
            ]
            if not tasks:
                self.config.debug(self.label, "No providers with URLs configured to parse.")
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result, alias in zip(results, self.providers_data):
                if isinstance(result, Exception):
                    self.config.error(self.label, f"A background task for provider '{alias}' failed: {result}")
        
        self.config.info(self.label, f"Finished parsing. Total discovered source services: {len(self.discovered_source_services_data)}")

    def _build_client_facing_channels(self, prev_discovered_source_services: DiscoveredSourcesData) -> None:
        """Builds the final list of channels exposed to clients. (Sync - CPU-bound)"""
        self.config.info(self.label, "Building client-facing channels...")
        cast(ChannelInfosMutable, self.client_facing_channels).clear()

        for lc_def in self.logical_channels_data:
            logical_channel_id = lc_def["logical_channel_id"]

            mapped_sources_for_lc = self.get_mappings_for_logical_channel(logical_channel_id)
            mapped_sources_for_lc.sort(key=lambda x: x.get("priority", DEFAULT_PRIORITY))
            processed_sources: list[SourceInfo] = []
            for mapping in mapped_sources_for_lc:
                source_id = mapping.get("source_service_id")
                priority = mapping.get("priority", DEFAULT_PRIORITY)
                discovered_service = self.discovered_source_services_data.get(source_id)
                if discovered_service:
                    processed_sources.append({
                        "source_service_id": source_id,
                        "priority": priority,
                        "provider_alias": discovered_service["provider_alias"],
                        "actual_stream_url": discovered_service["actual_stream_url"],
                    })
                else:
                    if source_id in prev_discovered_source_services:
                        prev_discovered_source = prev_discovered_source_services[source_id]
                        source_name = f"'{prev_discovered_source['original_display_name_extinf'] or prev_discovered_source['original_tvg_name']}' ({source_id})"
                    else:
                        source_name = f"'Unknown Source' ({source_id})"
                    self.config.warn(self.label, f"Mapped source {source_name} for '{lc_def.get('display_name', logical_channel_id)}'{f' ({lc_def['channel_num']})' if 'channel_num' in lc_def else ''} not found in discovered services.")

            if processed_sources:
                cast(ChannelInfosMutable, self.client_facing_channels)[logical_channel_id] = {
                    "logical_channel_id": logical_channel_id,
                    "display_name": lc_def["display_name"] or LogicalChannelName(logical_channel_id),
                    "group_title": lc_def["group_title"] or TVGGroupTitle("Uncategorized"),
                    "tvg_id": lc_def["tvg_id"],
                    "tvg_logo": lc_def["tvg_logo"],
                    "channel_num": lc_def["channel_num"],
                    "sources": processed_sources
                }
            else:
                self.config.warn(self.label, f"No valid mapped sources for logical channel '{logical_channel_id}'. It will not be included in the client M3U.")
        self.config.info(self.label, f"Built {len(self.client_facing_channels)} client-facing channels.")

    async def get_pending_stream_count(self) -> int:
        async with self._mutex:
            return len(self.pending_streams)

    async def add_pending_stream(self, logical_channel_id: LogicalChannelId, video_type: VideoType) -> bool:
        stream_key = create_stream_key(video_type, logical_channel_id)
        async with self._mutex:
            if stream_key in self.pending_streams:
                return False
            self.pending_streams.add(stream_key)
            return True

    async def _remove_pending_stream(self, logical_channel_id: LogicalChannelId, video_type: VideoType) -> None:
        async with self._mutex:
            self.pending_streams.remove(create_stream_key(video_type, logical_channel_id))

    def remove_pending_stream(self, logical_channel_id: LogicalChannelId, video_type: VideoType) -> None:
        """Removes a pending stream from the set of pending streams. (Sync - CPU-bound)"""
        run_bg(self._remove_pending_stream(logical_channel_id, video_type))

    def generate_main_client_m3u(self) -> None:
        """Generates the main M3U content to be served to clients. (Sync - CPU-bound)"""
        m3u_lines = ["#EXTM3U x-tvg-url=\"\""]
        if not self.config.nexus_url:
            self.config.error(self.label, "NEXUS_URL not set. Client M3U URLs will be incorrect.")
            m3u_lines.extend(["#EXTINF:-1,Error: NEXUS_URL not configured", "http://error.invalid/stream"])
            self.main_m3u_playlist = MainM3UPlaylist("\n".join(m3u_lines) + "\n")
            return

        sorted_channels = sorted(self.client_facing_channels.values(), key=lambda item: (item.get("group_title", "zzz").lower(), item.get("display_name", "zzz").lower()))

        for lc_data in sorted_channels:
            name = lc_data.get("display_name", "")
            extinf_parts = [f'tvg-name="{name}"']
            if ch_num := lc_data.get("channel_num"): extinf_parts.append(f'tvg-chno="{ch_num}"')
            if tvg_id := lc_data.get("tvg_id"): extinf_parts.append(f'tvg-id="{tvg_id}"')
            if logo := lc_data.get("tvg_logo"): extinf_parts.append(f'tvg-logo="{logo}"')
            if group := lc_data.get("group_title"): extinf_parts.append(f'group-title="{group}"')
            
            m3u_lines.append(f"#EXTINF:-1 {' '.join(extinf_parts)},{name}")
            m3u_lines.append(f"{self.config.nexus_url}/{VideoType.HLS}/{lc_data['logical_channel_id']}/playlist.m3u8")
        
        self.main_m3u_playlist = MainM3UPlaylist("\n".join(m3u_lines) + "\n")
        self.config.info(self.label, f"Generated main client M3U with {len(self.client_facing_channels)} channels.")

    def get_sources_for_client_facing_channel(self, logical_channel_id: LogicalChannelId) -> list[SourceInfo]:
        """Retrieves source stream URLs for a channel."""
        return self.client_facing_channels.get(logical_channel_id, {}).get("sources", [])

    async def _update_providers_slots(self) -> None:
        """Initializes or updates provider slots based on config asynchronously."""
        async with self._mutex:
            providers_to_delete: set[ProviderAlias] = set()
            for alias, curr_details in self.slots.items():
                if alias not in self.providers_data:
                    self.config.info(self.label, f"Removing slots for provider '{alias}' as it no longer exists in configuration.")
                    providers_to_delete.add(alias)
                    self._kill_provider_streams.add(alias)
                    continue
                m3u_url = self.providers_data[alias]["url"]
                max_streams = self.providers_data[alias]["max_concurrent_streams"]
                if curr_details.get_m3u_url() == m3u_url and curr_details.get_total_slots() == max_streams:
                    continue
                self.config.info(self.label, f"Updating slots for provider '{alias}' with new URL or max streams.")
                self.slots[alias] = ProviderSlots(
                    alias=alias,
                    m3u_url=m3u_url,
                    total_slots=max_streams
                )
                self._kill_provider_streams.add(alias)
            for alias in providers_to_delete:
                del self.slots[alias]
            for alias, details in self.providers_data.items():
                if alias in self.slots:
                    continue
                max_streams = details["max_concurrent_streams"]
                self.slots[alias] = ProviderSlots(
                    alias=alias,
                    m3u_url=details["url"],
                    total_slots=max_streams
                )
                self.config.info(self.label, f"Initialized slots for provider '{alias}' with capacity {max_streams}")
        
    async def reload_handler_config(self, *, update_providers: bool = False, force_discover_sources: bool = False) -> None:
        """Public method to trigger a full async reload of the handler's configuration."""
        await self._load_and_process_configurations(update_providers=update_providers, force_discover_sources=force_discover_sources)

    async def get_provider_stream_status(self) -> ProviderStatuses:
        """Calculates current stream usage for each provider asynchronously."""
        async with self._mutex:
            return ProviderStatuses({alias: {"alias": alias, "url": provider_slots.get_m3u_url(), "active_streams": await provider_slots.get_active_slots(), "max_concurrent_streams": provider_slots.get_total_slots()}
                                    for alias, provider_slots in self.slots.items()})

    # --- UI Interaction Methods ---
    def search_predefined_channels(self, raw_query: str) -> list[ChannelListGroup]:
        """Searches the predefined channel list. (Sync - CPU-bound)"""
        if not raw_query: return []
        query = raw_query.strip().lower()
        matches: list[ChannelListGroup] = []
        found_with: dict[str, str] = {}
        for group, channels in self.channel_list_data.items():
            for channel in channels:
                title = channel.get('title', '').lower()
                words = query.split()
                if all(word in title for word in words):
                    matches.append({**channel, 'group': group})
                    found_with[channel['title']] = channel['title']
                    continue
                for name in channel.get('names', []):
                    title = name.lower()
                    if all(word in title for word in words):
                        matches.append({**channel, 'group': group})
                        found_with[channel.get('title', channel.get('num', ''))] = name
                        break
        matches.sort(key=lambda x: found_with[x.get('title', x.get('num', ''))].lower().find(query))
        return matches[:10]

    def find_matching_predefined_channel(self, channel_name: LogicalChannelName, channel_num: ChannelNum) -> ChannelListInfo | None:
        """Finds a matching predefined channel. (Sync - CPU-bound)"""
        predefined_channel_lists = list(self.channel_list_data.values())
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                if channel_name == pre_channel.get('title'): return pre_channel
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                if channel_name in pre_channel.get('names', []): return pre_channel
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                if channel_num == pre_channel.get('num'): return pre_channel
        for channel_list in predefined_channel_lists:
            for pre_channel in channel_list:
                pre_channel_title = pre_channel.get('title', '')
                if channel_name in pre_channel_title: return pre_channel
                if pre_channel_title and pre_channel_title in channel_name: return pre_channel
                for pre_channel_name in pre_channel.get('names', []):
                    if channel_name in pre_channel_name: return pre_channel
                    if pre_channel_name in channel_name: return pre_channel
        return None

    def filter_sources(self, raw_query: str, service: DiscoveredSource) -> bool:
        """Filters sources based on a query. (Sync - pure function)"""
        tvg_name = service.get('original_tvg_name', '').lower()
        display_name = service.get('original_display_name_extinf', '').lower()
        for raw_q in raw_query.split(" OR "):
            words = raw_q.strip().lower().split()
            if all(word in tvg_name or word in display_name for word in words):
                return True
        return False

    def _generate_next_logical_channel_id(self) -> LogicalChannelId:
        """Generates the next available ID. (Sync - CPU-bound)"""
        existing_id_strings = [lc['logical_channel_id'] for lc in self.logical_channels_data]
        if not existing_id_strings: return INITIAL_LOGICAL_CHANNEL_ID
        numeric_ids = [int(id_str) for id_str in existing_id_strings]
        return LogicalChannelId(str(max(numeric_ids) + 1))

    async def _save_providers_for_ui(self, new_providers_data: ProvidersSourceData, *, update_slots: bool) -> bool:
        """Saves the provider configuration and updates internal state asynchronously."""
        if "source_m3u_providers" not in new_providers_data:
            return False 
        save_successful = await self.config.save_providers_config(new_providers_data)
        if save_successful and update_slots:
            await self._update_providers_slots()
        return save_successful
    
    async def add_provider(self, alias: ProviderAlias, url: M3UURL, max_streams: MaxStreams) -> bool:
        """Adds a new provider to the configuration asynchronously."""
        if not alias: raise ValueError("Provider alias cannot be empty.")
        if not url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 0: raise ValueError("Max concurrent streams must be at least 0.")
        if alias.lower() in {a.lower() for a in self.providers_data.keys()}:
            raise ValueError(f"Provider with alias '{alias}' already exists.")

        try:
            cast(ProvidersDataMutable, self.providers_data)[alias] = {"url": url, "max_concurrent_streams": max_streams, "updated_at": None}
            save_data = ProvidersSourceData({"source_m3u_providers": self.providers_data})
            assert await self._save_providers_for_ui(save_data, update_slots=True)
            return True
        except BaseException as e:
            del cast(ProvidersDataMutable, self.providers_data)[alias]
            if isinstance(e, Exception):
                return False
            raise

    async def update_provider(self, alias: ProviderAlias, url: M3UURL, max_streams: MaxStreams) -> bool:
        """Updates an existing provider's configuration asynchronously."""
        if not url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 0: raise ValueError("Max concurrent streams must be at least 0.")
        if alias not in self.providers_data: raise ValueError(f"Provider with alias '{alias}' not found.")

        original_data = self.providers_data[alias].copy()
        try:
            cast(ProviderInfoMutable, self.providers_data[alias])["url"] = url
            cast(ProviderInfoMutable, self.providers_data[alias])["max_concurrent_streams"] = max_streams
            save_data = ProvidersSourceData({"source_m3u_providers": self.providers_data})
            assert await self._save_providers_for_ui(save_data, update_slots=True)
            return True
        except BaseException as e:
            cast(ProvidersDataMutable, self.providers_data)[alias] = original_data
            if isinstance(e, Exception):
                return False
            raise

    async def delete_provider(self, alias: ProviderAlias) -> bool:
        """Deletes a provider from the configuration asynchronously."""
        if alias not in self.providers_data: raise ValueError(f"Provider with alias '{alias}' not found.")

        provider_to_delete = self.providers_data[alias]
        try:
            del cast(ProvidersDataMutable, self.providers_data)[alias]
            save_data = ProvidersSourceData({"source_m3u_providers": self.providers_data})
            assert await self._save_providers_for_ui(save_data, update_slots=True)
            return True
        except BaseException as e:
            cast(ProvidersDataMutable, self.providers_data)[alias] = provider_to_delete
            if isinstance(e, Exception):
                return False
            raise

    def get_all_logical_channels_for_ui(self) -> LogicalChannelsData:
        """Gets a list of all logical channels."""
        return LogicalChannelsData(tuple(sorted(self.logical_channels_data, key=lambda x: x.get("display_name","").lower())))

    def get_logical_channel_by_id(self, logical_channel_id: LogicalChannelId) -> LogicalChannelInfo | None:
        """Gets a logical channel by its ID."""
        return next((lc for lc in self.logical_channels_data if lc.get("logical_channel_id") == logical_channel_id), None)

    async def add_logical_channel(self, raw_lc_data: LogicalChannelInfo) -> LogicalChannelId | None:
        """Adds a new logical channel to the configuration."""
        new_lc_id = self._generate_next_logical_channel_id()
        new_lc_data = LogicalChannelInfo({**raw_lc_data, "logical_channel_id": new_lc_id})
        new_data = LogicalChannelsData((*self.logical_channels_data, new_lc_data))
        if not await self.config.save_logical_channels_config(new_data):
            self.config.error(self.label, f"Failed to save new logical channel configuration: {new_lc_data}")
            return
        self.logical_channels_data = new_data
        return new_lc_id

    async def update_logical_channel(self, updated_lc_data: LogicalChannelInfo) -> bool:
        """Updates a logical channel in the configuration."""
        if not any(lc["logical_channel_id"] == updated_lc_data["logical_channel_id"] for lc in self.logical_channels_data):
            return False
        new_data = LogicalChannelsData(tuple(updated_lc_data if lc["logical_channel_id"] == updated_lc_data["logical_channel_id"]
                                                               else lc for lc in self.logical_channels_data))
        res = await self.config.save_logical_channels_config(new_data)
        if res:
            self.logical_channels_data = new_data
        else:
            self.config.error(self.label, f"Failed to save updated logical channel configuration: {updated_lc_data}")
        return res

    async def delete_logical_channel(self, logical_channel_id: LogicalChannelId) -> bool:
        """Deletes a logical channel from the configuration."""
        original_len = len(self.logical_channels_data)
        self.logical_channels_data = LogicalChannelsData(tuple(lc for lc in self.logical_channels_data if lc["logical_channel_id"] != logical_channel_id))
        
        if logical_channel_id in self.channel_mappings_data:
            del cast(ChannelMappingsDataMutable, self.channel_mappings_data)[logical_channel_id]
            await self.config.save_channel_mappings_config(self.channel_mappings_data)
        
        return len(self.logical_channels_data) < original_len and await self.config.save_logical_channels_config(self.logical_channels_data)

    def get_mappings_for_logical_channel(self, logical_channel_id: LogicalChannelId) -> list[SourcePriority]:
        """Retrieves mappings for a logical channel."""
        return [mapping for mapping in self.channel_mappings_data.get(logical_channel_id, [])]

    async def update_mappings_for_logical_channel(self, logical_channel_id: LogicalChannelId, new_mappings_list: list[SourcePriority]) -> bool:
        """Updates mappings for a logical channel."""
        cast(ChannelMappingsDataMutable, self.channel_mappings_data)[logical_channel_id] = new_mappings_list
        return await self.config.save_channel_mappings_config(self.channel_mappings_data)

    def copy_mappings_channel_mappings(self) -> ChannelMappingsDataMutable:
        """Returns a copy of the channel mappings data."""
        return ChannelMappingsDataMutable(cast(ChannelMappingsDataMutable, self.channel_mappings_data).copy())

    def get_all_discovered_source_services_for_ui(self) -> list[DiscoveredSource]:
        """Gets a list of discovered services."""
        all_services = list(self.discovered_source_services_data.values())
        return sorted(all_services, key=lambda x: (x["provider_alias"], (x.get("original_tvg_name","") or x.get("original_display_name_extinf","")).lower()))
