import re
import html
import hashlib
import asyncio
from typing import Any, Self

import aiohttp
from nexus_stream.config import Config, VideoType, NEXUS_STREAM_USER_AGENT
from nexus_stream.slots import ProviderName, ProviderSlots

# --- Constants ---
DEFAULT_PRIORITY = 5
PROVIDER_FETCH_TIMEOUT = 20
TVG_NAME_REGEX = re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE)
TVG_ID_REGEX = re.compile(r'tvg-id="([^"]*)"', re.IGNORECASE)
TVG_LOGO_REGEX = re.compile(r'tvg-logo="([^"]*)"', re.IGNORECASE)
GROUP_TITLE_REGEX = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)


class ChannelHandler:
    """
    Manages all channel data, including providers, logical channels, and mappings.
    
    This class is responsible for:
    - Fetching and parsing M3U files from source providers asynchronously.
    - Building a list of "client-facing" channels based on user-defined logical channels and mappings.
    - Generating the master M3U file for clients.
    - Handling provider stream capacity limits using asyncio-native constructs.
    - Providing async methods for the UI to interact with configuration data.
    """
    def __init__(self, config: Config) -> None:
        """
        Initializes the ChannelHandler. NOTE: This is now a lightweight, synchronous constructor.
        """
        self._startup = True
        self._loading = False
        self.config = config
        self._mutex = asyncio.Lock()

        # Data loaded from configuration files
        self.providers_data_from_json: dict[str, Any] = {}
        self.logical_channels_data_from_json: list[dict[str, Any]] = []
        self.channel_mappings_data_from_json: dict[str, Any] = {}
        self.predefined_channel_list: dict[str, Any] = {}
        
        # In-memory processed data
        self.discovered_source_services: dict[str, dict[str, Any]] = {} 
        self.client_facing_channels: dict[str, dict[str, Any]] = {}
        self.master_m3u_content: str = "#EXTM3U\n"

        self.slots: dict[ProviderName, ProviderSlots] = {}
        self._kill_provider_streams: set[str] = set()
        self.pending_streams: set[str] = set()

    @classmethod
    async def create(cls, config: Config) -> Self:
        """Asynchronous factory for creating and initializing a ChannelHandler instance."""
        instance = cls(config)
        await instance._load_and_process_configurations(update_providers=True)
        instance._startup = False
        return instance

    def is_loading(self) -> bool:
        """Returns True if the handler is currently loading configurations."""
        return self._loading

    async def reset_kill_provider_streams(self) -> set[str]:
        """Resets the kill_provider_streams, returns the aliases that should be killed."""
        async with self._mutex:
            if not self._kill_provider_streams:
                return set()
            tmp = self._kill_provider_streams
            self._kill_provider_streams = set()
            return tmp

    def _generate_source_service_id(self, provider_alias: str, actual_stream_url: str) -> str:
        """Creates a stable, unique ID for a source stream. (Sync - pure function)"""
        id_material = f"{provider_alias}:{actual_stream_url}"
        return f"{provider_alias}:{hashlib.md5(id_material.encode('utf-8')).hexdigest()}"

    async def _load_and_process_configurations(self, *, update_providers: bool) -> None:
        """
        Loads all data from JSON files and rebuilds the in-memory channel structures asynchronously.
        """
        self._loading = True
        self.config.log_message("Loading/Reloading ChannelHandler configurations", level="INFO")

        if update_providers:
            providers_config = await self.config.get_providers_config()
            self.providers_data_from_json = providers_config.get("source_m3u_providers", {})
            await self._update_providers_slots()

        self.logical_channels_data_from_json = await self.config.get_logical_channels_config()
        self.channel_mappings_data_from_json = await self.config.get_channel_mappings_config()
        self.predefined_channel_list = await self.config.get_channel_list_config()

        self.discovered_source_services.clear()
        self.client_facing_channels.clear()

        await self._parse_all_provider_m3us_and_populate_discovered_services()
        self._build_client_facing_channels()
        self.generate_master_client_m3u()
        
        self._loading = False
        self.config.log_message(
            f"ChannelHandler ready. Discovered: {len(self.discovered_source_services)}, "
            f"Client-Facing: {len(self.client_facing_channels)}",
            level="INFO"
        )

    def _parse_source_m3u_lines(self, lines: list[str]) -> list[dict[str, str]]:
        """Parses M3U lines into structured channels. (Sync - CPU-bound)"""
        parsed_channels: list[dict[str, str]] = []
        current_extinf = None
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#EXTM3U"):
                continue
            
            if line.startswith("#EXTINF:"):
                current_extinf = line
            elif current_extinf and (line.startswith("http://") or line.startswith("https://")):
                tvg_name = (re.search(TVG_NAME_REGEX, current_extinf) or [None, ""])[1]
                tvg_id = (re.search(TVG_ID_REGEX, current_extinf) or [None, ""])[1]
                tvg_logo = (re.search(TVG_LOGO_REGEX, current_extinf) or [None, ""])[1]
                group_title = (re.search(GROUP_TITLE_REGEX, current_extinf) or [None, ""])[1]
                display_name_from_extinf = current_extinf.split(',')[-1]

                parsed_channels.append({
                    "original_tvg_name": html.unescape(tvg_name.strip()),
                    "original_display_name_extinf": html.unescape(display_name_from_extinf.strip()),
                    "original_group_title": html.unescape(group_title.strip()),
                    "original_tvg_id": tvg_id.strip(),
                    "original_tvg_logo": tvg_logo.strip(),
                    "actual_stream_url": line.strip()
                })
                current_extinf = None
        return parsed_channels
    
    async def _fetch_and_parse_provider(self, session: aiohttp.ClientSession, provider_alias: str, m3u_url: str) -> None:
        """Fetches and parses a single provider's M3U asynchronously."""
        try:
            async with session.get(m3u_url, timeout=PROVIDER_FETCH_TIMEOUT, headers={'User-Agent': NEXUS_STREAM_USER_AGENT}) as response:
                response.raise_for_status()
                text = await response.text()
                
                parsed_channels = self._parse_source_m3u_lines(text.splitlines())

                async with self._mutex:
                    for p_channel in parsed_channels:
                        service_id = self._generate_source_service_id(provider_alias, p_channel["actual_stream_url"])
                        self.discovered_source_services[service_id] = {
                            "id": service_id,
                            "provider_alias": provider_alias,
                            **p_channel
                        }
                self.config.log_message(f"Discovered {len(parsed_channels)} services from provider '{provider_alias}'.", level="INFO")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.config.log_message(f"Failed to fetch or parse provider '{provider_alias}': {e}", level="ERROR")
            raise
        except Exception as e:
            self.config.log_message(f"An unexpected error occurred while processing provider '{provider_alias}': {e}", level="ERROR")
            raise

    async def _parse_all_provider_m3us_and_populate_discovered_services(self) -> None:
        """Uses asyncio.gather to fetch and parse all configured provider M3Us concurrently."""
        self.config.log_message("Starting to parse all provider M3Us...", level="INFO")
        self.discovered_source_services.clear()

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_and_parse_provider(session, alias, details.get("url"))
                for alias, details in self.providers_data_from_json.items() if details.get("url")
            ]
            if not tasks:
                self.config.log_message("No providers with URLs configured to parse.", level="DEBUG")
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)

            provider_aliases = [alias for alias, details in self.providers_data_from_json.items() if details.get("url")]
            for result, alias in zip(results, provider_aliases):
                if isinstance(result, Exception):
                    self.config.log_message(f"A background task for provider '{alias}' failed: {result}", level="ERROR")
        
        self.config.log_message(f"Finished parsing. Total discovered source services: {len(self.discovered_source_services)}", level="INFO")

    def _build_client_facing_channels(self) -> None:
        """Builds the final list of channels exposed to clients. (Sync - CPU-bound)"""
        self.config.log_message("Building client-facing channels...", level="INFO")
        self.client_facing_channels.clear()

        for lc_def in self.logical_channels_data_from_json:
            logical_channel_id = lc_def.get("logical_channel_id")
            if not logical_channel_id:
                self.config.log_message(f"Skipping logical channel with missing ID: {lc_def.get('display_name', 'N/A')}", level="WARN")
                continue

            mapped_sources_for_lc = self.channel_mappings_data_from_json.get(logical_channel_id, [])
            processed_sources: list[dict[str, Any]] = []
            for mapping in sorted(mapped_sources_for_lc, key=lambda x: x.get("priority", DEFAULT_PRIORITY)):
                source_id = mapping.get("source_service_id")
                priority = mapping.get("priority", DEFAULT_PRIORITY)
                discovered_service = self.discovered_source_services.get(source_id)
                if discovered_service:
                    processed_sources.append({
                        "source_service_id": source_id,
                        "priority": priority,
                        "provider_alias": discovered_service["provider_alias"],
                        "actual_stream_url": discovered_service["actual_stream_url"],
                    })
                else:
                    self.config.log_message(f"Mapped source '{source_id}' for '{lc_def.get('display_name', logical_channel_id)}'{f' ({lc_def['channel_num']})' if 'channel_num' in lc_def else ''} not found in discovered services.", level="WARN")

            if processed_sources:
                self.client_facing_channels[logical_channel_id] = {
                    "logical_channel_id": logical_channel_id,
                    "display_name": lc_def.get("display_name", logical_channel_id),
                    "group_title": lc_def.get("group_title", "Uncategorized"),
                    "tvg_id": lc_def.get("tvg_id", ""),
                    "tvg_logo": lc_def.get("tvg_logo", ""),
                    "channel_num": lc_def.get("channel_num", ""),
                    "sources": processed_sources
                }
            else:
                 self.config.log_message(f"No valid mapped sources for LC '{logical_channel_id}'. It will not be included in the client M3U.", level="WARN")
        self.config.log_message(f"Built {len(self.client_facing_channels)} client-facing channels.", level="INFO")

    async def get_pending_stream_count(self) -> int:
        async with self._mutex:
            return len(self.pending_streams)

    async def add_pending_stream(self, logical_channel_id: str, video_type: VideoType) -> bool:
        key = f"{video_type}_{logical_channel_id}"
        async with self._mutex:
            if key in self.pending_streams:
                return False
            self.pending_streams.add(key)
            return True

    async def remove_pending_stream(self, logical_channel_id: str, video_type: VideoType) -> None:
        async with self._mutex:
            self.pending_streams.remove(f"{video_type}_{logical_channel_id}")

    def generate_master_client_m3u(self) -> None:
        """Generates the master M3U content to be served to clients. (Sync - CPU-bound)"""
        m3u_lines = ["#EXTM3U x-tvg-url=\"\""]
        if not self.config.nexus_url:
            self.config.log_message("NEXUS_URL not set. Client M3U URLs will be incorrect.", level="ERROR")
            m3u_lines.extend(["#EXTINF:-1,Error: NEXUS_URL not configured", "http://error.invalid/stream"])
            self.master_m3u_content = "\n".join(m3u_lines) + "\n"
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
        
        self.master_m3u_content = "\n".join(m3u_lines) + "\n"
        self.config.log_message(f"Generated master client M3U with {len(self.client_facing_channels)} channels.", level="INFO")

    def get_sources_for_client_facing_channel(self, logical_channel_id: str) -> list[dict[str, Any]]:
        """Retrieves source stream URLs for a channel. (Sync - in-memory lookup)"""
        channel_data = self.client_facing_channels.get(logical_channel_id)
        return channel_data.get("sources", []) if channel_data else []
    
    async def _update_providers_slots(self) -> None:
        """Initializes or updates provider slots based on config asynchronously."""
        async with self._mutex:
            providers_to_delete: set[ProviderName] = set()
            for alias, curr_details in self.slots.items():
                if alias not in self.providers_data_from_json:
                    self.config.log_message(f"Removing slots for provider '{alias}' as it no longer exists in configuration.", level="INFO")
                    providers_to_delete.add(alias)
                    self._kill_provider_streams.add(alias)
                    continue
                m3u_url = self.providers_data_from_json[alias].get("url", "")
                max_streams = self.providers_data_from_json[alias].get("max_concurrent_streams", 1)
                if curr_details.get_m3u_url() == m3u_url and curr_details.get_total_slots() == max_streams:
                    continue
                self.config.log_message(f"Updating slots for provider '{alias}' with new URL or max streams.", level="INFO")
                self.slots[alias] = ProviderSlots(
                    name=ProviderName(alias),
                    m3u_url=m3u_url,
                    total_slots=max_streams
                )
                self._kill_provider_streams.add(alias)
            for alias in providers_to_delete:
                del self.slots[alias]
            for alias, details in self.providers_data_from_json.items():
                if alias in self.slots:
                    continue
                name = ProviderName(alias)
                max_streams = details.get("max_concurrent_streams", 1)
                self.slots[name] = ProviderSlots(
                    name=name,
                    m3u_url=details.get("url", ""),
                    total_slots=max_streams
                )
                self.config.log_message(f"Initialized slots for provider '{alias}' with capacity {max_streams}", level="INFO")
        
    async def reload_handler_config(self, *, update_providers: bool) -> None:
        """Public method to trigger a full async reload of the handler's configuration."""
        await self._load_and_process_configurations(update_providers=update_providers)

    async def get_provider_stream_status(self) -> dict[str, dict[str, int]]:
        """Calculates current stream usage for each provider asynchronously."""
        status_report: dict[str, dict[str, int]] = {}
        async with self._mutex:
            for alias, provider_slots in self.slots.items():                
                status_report[alias] = {
                    "active": provider_slots.get_active_slots(),
                    "max": provider_slots.get_total_slots()
                }
        return status_report

    async def get_total_stream_status_for_ui(self) -> tuple[int, int]:
        """Returns (total_active, total_max) streams asynchronously."""
        detailed_status = await self.get_provider_stream_status()
        total_active = sum(status['active'] for status in detailed_status.values())
        total_max = sum(status['max'] for status in detailed_status.values())
        return int(total_active), int(total_max)
    
    async def get_active_stream_status_for_logging(self, provider_alias: str) -> str:
        detailed_status = await self.get_provider_stream_status()
        provider_status = detailed_status.get(provider_alias, {'active': 0, 'max': 0})
        return f"{provider_status['active']}/{provider_status['max']}"

    # --- UI Interaction Methods ---
    def search_predefined_channels(self, raw_query: str) -> list[dict[str, Any]]:
        """Searches the predefined channel list. (Sync - CPU-bound)"""
        if not raw_query: return []
        query = raw_query.strip().lower()
        matches: list[dict[str, Any]] = []
        found_with: dict[str, str] = {}
        for group, channels in self.predefined_channel_list.items():
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

    def find_matching_predefined_channel(self, channel_name: str, channel_num: str) -> dict[str, Any]:
        """Finds a matching predefined channel. (Sync - CPU-bound)"""
        predefined_channel_lists = list(self.predefined_channel_list.values())
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
        return {}

    def filter_sources(self, raw_query: str, service: dict[str, str]) -> bool:
        """Filters sources based on a query. (Sync - pure function)"""
        tvg_name = service.get('original_tvg_name', '').lower()
        display_name = service.get('original_display_name_extinf', '').lower()
        for raw_q in raw_query.split(" OR "):
            words = raw_q.strip().lower().split()
            if all(word in tvg_name or word in display_name for word in words):
                return True
        return False

    def _generate_next_logical_channel_id(self) -> str:
        """Generates the next available ID. (Sync - CPU-bound)"""
        existing_id_strings = [lc['logical_channel_id'] for lc in self.logical_channels_data_from_json if lc.get('logical_channel_id')]
        if not existing_id_strings: return '1000'
        numeric_ids = [int(id_str) for id_str in existing_id_strings]
        return str(max(numeric_ids) + 1)

    async def get_all_providers_for_ui(self) -> list[dict[str, Any]]:
        all_providers = self.providers_data_from_json
        provider_status = await self.get_provider_stream_status()
        providers_display: list[dict[str, Any]] = [
            {
                "alias": alias,
                "url": details.get("url", ""),
                "max_concurrent_streams": details.get("max_concurrent_streams", 1),
                "active_streams": provider_status.get(alias, {}).get("active", 0)
            } for alias, details in sorted(all_providers.items())
        ]
        return providers_display

    async def _save_providers_for_ui(self, new_providers_data: dict[str, Any]) -> bool:
        """Saves the provider configuration and updates internal state asynchronously."""
        if "source_m3u_providers" not in new_providers_data:
            return False 
        save_successful = await self.config.save_providers_config(new_providers_data)
        if save_successful:
            await self._update_providers_slots()
        return save_successful
    
    async def add_provider(self, alias: str, url: str, max_streams: int) -> dict[str, Any] | None:
        """Adds a new provider to the configuration asynchronously."""
        alias = alias.strip()
        if not alias: raise ValueError("Provider alias cannot be empty.")
        if not url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 1: raise ValueError("Max concurrent streams must be at least 1.")
        if alias.lower() in {a.lower() for a in self.providers_data_from_json.keys()}:
            raise ValueError(f"Provider with alias '{alias}' already exists.")

        self.providers_data_from_json[alias] = {"url": url, "max_concurrent_streams": max_streams}
        save_data = {"source_m3u_providers": self.providers_data_from_json}
        if await self._save_providers_for_ui(save_data):
            return {"alias": alias, "url": url, "max_concurrent_streams": max_streams, "active_streams": 0}
        else:
            del self.providers_data_from_json[alias]
            return None

    async def update_provider(self, alias: str, url: str, max_streams: int) -> bool:
        """Updates an existing provider's configuration asynchronously."""
        alias = alias.strip()
        if not alias: raise ValueError("Provider alias cannot be empty.")
        if not url: raise ValueError("Provider URL cannot be empty.")
        if max_streams < 1: raise ValueError("Max concurrent streams must be at least 1.")
        if alias not in self.providers_data_from_json: raise ValueError(f"Provider with alias '{alias}' not found.")

        original_data = self.providers_data_from_json[alias].copy()
        self.providers_data_from_json[alias]["url"] = url
        self.providers_data_from_json[alias]["max_concurrent_streams"] = max_streams
        save_data = {"source_m3u_providers": self.providers_data_from_json}
        if await self._save_providers_for_ui(save_data):
            return True
        else:
            self.providers_data_from_json[alias] = original_data
            return False

    async def delete_provider(self, alias: str) -> bool:
        """Deletes a provider from the configuration asynchronously."""
        alias = alias.strip()
        if not alias: raise ValueError("Provider alias cannot be empty.")
        if alias not in self.providers_data_from_json: raise ValueError(f"Provider with alias '{alias}' not found.")
        
        provider_to_delete = self.providers_data_from_json.pop(alias)
        save_data = {"source_m3u_providers": self.providers_data_from_json}
        if await self._save_providers_for_ui(save_data):
            return True
        else:
            self.providers_data_from_json[alias] = provider_to_delete
            return False

    def get_all_logical_channels_for_ui(self) -> list[dict[str, Any]]:
        """Gets a list of all logical channels. (Sync - in-memory lookup)"""
        return sorted(self.logical_channels_data_from_json, key=lambda x: x.get("display_name","").lower())

    def get_logical_channel_by_id(self, logical_channel_id: str) -> dict[str, Any] | None:
        """Gets a logical channel by its ID. (Sync - in-memory lookup)"""
        return next((lc for lc in self.logical_channels_data_from_json if lc.get("logical_channel_id") == logical_channel_id), None)

    async def add_logical_channel(self, lc_data: dict[str, Any]) -> str:
        """Adds a new logical channel to the configuration asynchronously."""
        new_id = self._generate_next_logical_channel_id()
        lc_data['logical_channel_id'] = new_id
        self.logical_channels_data_from_json.append(lc_data)
        await self.config.save_logical_channels_config(self.logical_channels_data_from_json)
        return new_id

    async def update_logical_channel(self, logical_channel_id: str, updated_lc_data: dict[str, Any]) -> bool:
        """Updates a logical channel in the configuration asynchronously."""
        for i, lc in enumerate(self.logical_channels_data_from_json):
            if lc.get("logical_channel_id") == logical_channel_id:
                updated_lc_data["logical_channel_id"] = logical_channel_id 
                self.logical_channels_data_from_json[i] = updated_lc_data
                return await self.config.save_logical_channels_config(self.logical_channels_data_from_json)
        return False

    async def delete_logical_channel(self, logical_channel_id: str) -> bool:
        """Deletes a logical channel from the configuration asynchronously."""
        original_len = len(self.logical_channels_data_from_json)
        self.logical_channels_data_from_json = [lc for lc in self.logical_channels_data_from_json if lc.get("logical_channel_id") != logical_channel_id]
        
        if logical_channel_id in self.channel_mappings_data_from_json:
            del self.channel_mappings_data_from_json[logical_channel_id]
            await self.config.save_channel_mappings_config(self.channel_mappings_data_from_json)
        
        return len(self.logical_channels_data_from_json) < original_len and await self.config.save_logical_channels_config(self.logical_channels_data_from_json)

    def get_mappings_for_logical_channel(self, logical_channel_id: str) -> list[dict[str, Any]]:
        """Retrieves mappings for a logical channel. (Sync - in-memory lookup)"""
        return self.channel_mappings_data_from_json.get(logical_channel_id, [])

    async def update_mappings_for_logical_channel(self, logical_channel_id: str, new_mappings_list: list[dict[str, Any]]) -> bool:
        """Updates mappings for a logical channel asynchronously."""
        self.channel_mappings_data_from_json[logical_channel_id] = new_mappings_list
        return await self.config.save_channel_mappings_config(self.channel_mappings_data_from_json)

    def get_all_discovered_source_services_for_ui(self) -> list[dict[str, Any]]:
        """Gets a list of discovered services. (Sync - in-memory lookup)"""
        all_services = list(self.discovered_source_services.values())
        self.config.log_message(f"Returning {len(all_services)} services for UI.", level="DEBUG")
        return sorted(all_services, key=lambda x: (x["provider_alias"], (x.get("original_tvg_name","") or x.get("original_display_name_extinf","")).lower()))