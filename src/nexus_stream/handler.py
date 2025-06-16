import re
import requests
import html
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

# Forward-declare Config to avoid circular import issues with type hints
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from nexus_stream.config import Config

# --- Constants ---
PROVIDER_FETCH_TIMEOUT = 20
PROVIDER_PARSE_MAX_WORKERS = 10
NEXUS_USER_AGENT = 'NexusStream/1.0'

class ChannelHandler:
    """
    Manages all channel data, including providers, logical channels, and mappings.
    
    This class is responsible for:
    - Fetching and parsing M3U files from source providers.
    - Building a list of "client-facing" channels based on user-defined logical channels and mappings.
    - Generating the master M3U file for clients.
    - Handling provider stream capacity limits.
    - Providing methods for the UI to interact with configuration data.

    Key Data Structures:
    - `discovered_source_services`: A dictionary mapping a unique service ID to a
      dict of its parsed M3U data (e.g., name, logo, group, original URL).
    - `client_facing_channels`: A dictionary mapping a logical_channel_id to a
      fully-processed logical channel, including its list of prioritized source URLs.
    """
    def __init__(self, config: 'Config'):
        """
        Initializes the ChannelHandler.

        Args:
            config: The main application Config object.
        """
        self.config = config
        self.base_url = self.config.base_url
        self.stream_lock = threading.RLock()

        # Data loaded from configuration files
        self.providers_data_from_json: dict[str, Any] = {}
        self.logical_channels_data_from_json: list[dict[str, Any]] = []
        self.channel_mappings_data_from_json: dict[str, Any] = {}
        self.predefined_channel_list: dict[str, Any] = {}
        
        # In-memory processed data
        self.discovered_source_services: dict[str, dict[str, Any]] = {} 
        self.client_facing_channels: dict[str, dict[str, Any]] = {}
        self.master_m3u_content: str = "#EXTM3U\n"

        self.provider_semaphores: dict[str, threading.Semaphore] = {}
        
        self._load_and_process_configurations()

    def _generate_source_service_id(self, provider_alias: str, actual_stream_url: str) -> str:
        """Creates a stable, unique ID for a source stream."""
        id_material = f"{provider_alias}:{actual_stream_url}"
        return f"{provider_alias}:{hashlib.md5(id_material.encode('utf-8')).hexdigest()}"

    def _load_and_process_configurations(self) -> None:
        """
        Loads all data from JSON files and rebuilds the in-memory channel structures.
        This is the main "reload" function for the handler.
        """
        self.config.log_message("Loading/Reloading ChannelHandler configurations", level="INFO")
        
        self.providers_data_from_json = self.config.get_providers_config().get("source_m3u_providers", {})

        with self.stream_lock:
            self.provider_semaphores.clear()
            for alias, details in self.providers_data_from_json.items():
                max_streams = details.get("max_concurrent_streams", 1)
                self.provider_semaphores[alias] = threading.Semaphore(max_streams)
                self.config.log_message(f"Initialized semaphore for provider '{alias}' with capacity {max_streams}", level="DEBUG")

        self.logical_channels_data_from_json = self.config.get_logical_channels_config()
        self.channel_mappings_data_from_json = self.config.get_channel_mappings_config()
        self.predefined_channel_list = self.config.get_channel_list_config()

        self.discovered_source_services.clear()
        self.client_facing_channels.clear()

        self._parse_all_provider_m3us_and_populate_discovered_services()
        self._build_client_facing_channels()
        self.generate_master_client_m3u()
        
        self.config.log_message(
            f"ChannelHandler ready. Discovered: {len(self.discovered_source_services)}, "
            f"Client-Facing: {len(self.client_facing_channels)}",
            level="INFO"
        )

    def _parse_source_m3u_lines(self, lines: list[str]) -> list[dict[str, str]]:
        """Parses the text lines of an M3U file into a structured list of channels."""
        parsed_channels = []
        current_extinf = None
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#EXTM3U"):
                continue
            
            if line.startswith("#EXTINF:"):
                current_extinf = line
            elif current_extinf and (line.startswith("http://") or line.startswith("https://")):
                tvg_name = (re.search(r'tvg-name="([^"]*)"', current_extinf, re.IGNORECASE) or [None, ""])[1]
                group_title = (re.search(r'group-title="([^"]*)"', current_extinf, re.IGNORECASE) or [None, ""])[1]
                tvg_id = (re.search(r'tvg-id="([^"]*)"', current_extinf, re.IGNORECASE) or [None, ""])[1]
                tvg_logo = (re.search(r'tvg-logo="([^"]*)"', current_extinf, re.IGNORECASE) or [None, ""])[1]
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
    
    def _fetch_and_parse_provider(self, provider_alias: str, m3u_url: str) -> None:
        """Fetches a single provider's M3U, parses it, and populates the discovered services."""
        try:
            session = requests.Session()
            session.headers.update({'User-Agent': NEXUS_USER_AGENT})
            response = session.get(m3u_url, timeout=PROVIDER_FETCH_TIMEOUT)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or 'utf-8'
            
            parsed_channels = self._parse_source_m3u_lines(response.text.splitlines())

            with self.stream_lock:
                for p_channel in parsed_channels:
                    service_id = self._generate_source_service_id(provider_alias, p_channel["actual_stream_url"])
                    self.discovered_source_services[service_id] = {
                        "id": service_id,
                        "provider_alias": provider_alias,
                        **p_channel
                    }
            self.config.log_message(f"Discovered {len(parsed_channels)} services from provider '{provider_alias}'.", level="INFO")
        except requests.RequestException as e:
            self.config.log_message(f"Failed to fetch or parse provider '{provider_alias}' ({m3u_url}): {e}", level="ERROR")
        except Exception as e:
            self.config.log_message(f"An unexpected error occurred while processing provider '{provider_alias}': {e}", level="ERROR")

    def _parse_all_provider_m3us_and_populate_discovered_services(self) -> None:
        """Uses a thread pool to fetch and parse all configured provider M3Us concurrently."""
        self.config.log_message("Starting to parse all provider M3Us...", level="INFO")
        self.discovered_source_services.clear()

        with ThreadPoolExecutor(max_workers=PROVIDER_PARSE_MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._fetch_and_parse_provider, alias, details.get("url")): alias
                for alias, details in self.providers_data_from_json.items() if details.get("url")
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    provider_alias = futures[future]
                    self.config.log_message(f"A background task for provider '{provider_alias}' failed: {e}", level="ERROR")
        
        self.config.log_message(f"Finished parsing. Total discovered source services: {len(self.discovered_source_services)}", level="INFO")

    def _build_client_facing_channels(self) -> None:
        """Builds the final list of channels exposed to clients based on logical channel definitions and mappings."""
        self.config.log_message("Building client-facing channels...", level="INFO")
        self.client_facing_channels.clear()

        for lc_def in self.logical_channels_data_from_json:
            logical_channel_id = lc_def.get("logical_channel_id")
            if not logical_channel_id:
                self.config.log_message(f"Skipping logical channel with missing ID: {lc_def.get('display_name', 'N/A')}", level="WARN")
                continue

            mapped_sources_for_lc = self.channel_mappings_data_from_json.get(logical_channel_id, [])
            processed_sources = []
            for mapping in sorted(mapped_sources_for_lc, key=lambda x: x.get("priority", 0)):
                source_id = mapping.get("source_service_id")
                discovered_service = self.discovered_source_services.get(source_id)
                if discovered_service:
                    processed_sources.append({
                        "provider_alias": discovered_service["provider_alias"],
                        "actual_stream_url": discovered_service["actual_stream_url"],
                    })
                else:
                    self.config.log_message(f"Mapped source '{source_id}' for LC '{logical_channel_id}' not found in discovered services.", level="WARN")

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

    def generate_master_client_m3u(self) -> None:
        """Generates the master M3U content to be served to clients."""
        m3u_lines = ["#EXTM3U x-tvg-url=\"\""]
        if not self.base_url:
            self.config.log_message("BASE_URL not set. Client M3U URLs will be incorrect.", level="ERROR")
            m3u_lines.extend(["#EXTINF:-1,Error: BASE_URL not configured", "http://error.invalid/stream"])
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
            m3u_lines.append(f"{self.base_url}/hls/{lc_data['logical_channel_id']}/playlist.m3u8")
        
        self.master_m3u_content = "\n".join(m3u_lines) + "\n"
        self.config.log_message(f"Generated master client M3U with {len(self.client_facing_channels)} channels.", level="INFO")

    def get_sources_for_client_facing_channel(self, logical_channel_id: str) -> list[dict[str, str]]:
        """Retrieves the list of source stream URLs for a given client-facing channel ID."""
        channel_data = self.client_facing_channels.get(logical_channel_id)
        return channel_data.get("sources", []) if channel_data else []

    def _get_max_streams_for_provider(self, provider_alias: str) -> int:
        """Gets the configured maximum concurrent streams for a provider."""
        return self.providers_data_from_json.get(provider_alias, {}).get("max_concurrent_streams", 1)
        
    def reload_handler_config(self) -> None:
        """Public method to trigger a full reload of the handler's configuration."""
        self._load_and_process_configurations()

    def get_provider_stream_status(self) -> dict[str, dict[str, int]]:
        """
        Calculates the current stream usage for each provider by inspecting the semaphores.
        This is the single source of truth for UI and logs.

        Returns:
            A dictionary where keys are provider aliases and values are dicts
            containing 'active' and 'max' stream counts.
            e.g., {'provider_a': {'active': 1, 'max': 2}, ...}
        """
        status_report = {}
        with self.stream_lock: # Lock to safely iterate over provider_semaphores
            for alias, semaphore in self.provider_semaphores.items():
                # Get the max streams from the original config
                max_streams = self._get_max_streams_for_provider(alias)
                
                # The semaphore's internal _value is the number of AVAILABLE slots.
                # Active streams = Max Streams - Available Slots.
                available_slots = semaphore._value
                active_streams = max_streams - available_slots
                
                status_report[alias] = {
                    "active": active_streams,
                    "max": max_streams
                }
        return status_report

    def get_total_stream_status_for_ui(self) -> tuple[int, int]:
        """
        Returns a tuple of (total_active_streams, total_max_streams) for UI display,
        derived directly from the semaphore states.
        """
        detailed_status = self.get_provider_stream_status()
        total_active = sum(status['active'] for status in detailed_status.values())
        total_max = sum(status['max'] for status in detailed_status.values())
        return int(total_active), int(total_max)
    
    def get_active_stream_status_for_logging(self, provider_alias: str) -> dict[str, int]:
        detailed_status = self.get_provider_stream_status()[provider_alias]
        active = detailed_status['active']
        max_streams = detailed_status['max']
        return f"{active}/{max_streams}"

    # --- UI Interaction Methods ---
    def search_predefined_channels(self, query: str) -> list[dict[str, Any]]:
        """Searches the predefined channel list"""
        if not query:
            return []
        
        query = query.lower()
        matches = []
        for group, channels in self.predefined_channel_list.items():
            for channel in channels:
                if query in channel.get('name', '').lower():
                    matches.append({**channel, 'group': group})
        # Sort matches by how early the query appears in the name
        matches.sort(key=lambda x: x['name'].lower().find(query))
        return matches[:10] # Return a max of 10 suggestions

    def _generate_next_logical_channel_id(self) -> str:
        """Generates the next available auto-incrementing ID as a string, starting at '1000'."""
        
        existing_id_strings = [
            lc.get('logical_channel_id') 
            for lc in self.logical_channels_data_from_json 
            if lc.get('logical_channel_id') is not None
        ]
        
        if not existing_id_strings:
            return '1000'

        numeric_ids = [int(id_str) for id_str in existing_id_strings]
        
        next_id_as_int = max(numeric_ids) + 1
        
        return str(next_id_as_int)

    def get_all_providers_for_ui(self) -> dict[str, Any]:
        all_providers = self.providers_data_from_json
        providers_display = [
            {
                "alias": alias,
                "url": details.get("url", ""),
                "max_concurrent_streams": details.get("max_concurrent_streams", 1),
                "active_streams": self.get_provider_stream_status().get(alias, {}).get("active", 0)
            } for alias, details in sorted(all_providers.items())
        ]

        return providers_display

    def save_providers_for_ui(self, new_providers_data: dict[str, Any]) -> bool:
        if "source_m3u_providers" not in new_providers_data:
            return False 
        return self.config.save_providers_config(new_providers_data)

    def add_provider(self, alias: str, url: str, max_streams: int) -> dict[str, Any]:
        """
        Adds a new provider to the configuration.
        
        On success, returns the newly created provider dictionary.
        On failure (like a save error), returns None.
        Raises ValueError for invalid input or duplicate alias.
        """
        alias = alias.strip()
        if not alias:
            raise ValueError("Provider alias cannot be empty.")
        if not url:
            raise ValueError("Provider URL cannot be empty.")
        if max_streams < 1:
            raise ValueError("Max concurrent streams must be at least 1.")

        if alias.lower() in {a.lower() for a in self.providers_data_from_json.keys()}:
            raise ValueError(f"Provider with alias '{alias}' already exists.")

        provider_data_to_save = {
            "url": url,
            "max_concurrent_streams": max_streams
        }
        
        self.providers_data_from_json[alias] = provider_data_to_save

        save_successful = self.config.save_providers_config({"source_m3u_providers": self.providers_data_from_json})

        if save_successful:

            provider_to_render = {
                "alias": alias,
                "url": url,
                "max_concurrent_streams": max_streams,
                "active_streams": 0
            }
            return provider_to_render
        else:
            del self.providers_data_from_json[alias]
            return None

    def update_provider(self, alias: str, url: str, max_streams: int) -> bool:
        """Updates an existing provider's configuration."""
        alias = alias.strip()
        if not alias:
            raise ValueError("Provider alias cannot be empty.")
        if not url:
            raise ValueError("Provider URL cannot be empty.")
        if max_streams < 1:
            raise ValueError("Max concurrent streams must be at least 1.")

        if alias not in self.providers_data_from_json:
            raise ValueError(f"Provider with alias '{alias}' not found.")

        self.providers_data_from_json[alias]["url"] = url
        self.providers_data_from_json[alias]["max_concurrent_streams"] = max_streams
        return self.config.save_providers_config({"source_m3u_providers": self.providers_data_from_json})

    def delete_provider(self, alias: str) -> bool:
        """Deletes a provider from the configuration."""
        alias = alias.strip()
        if not alias:
            raise ValueError("Provider alias cannot be empty.")

        if alias not in self.providers_data_from_json:
            raise ValueError(f"Provider with alias '{alias}' not found.")
        
        del self.providers_data_from_json[alias]
        return self.config.save_providers_config({"source_m3u_providers": self.providers_data_from_json})

    def get_all_logical_channels_for_ui(self) -> list[dict[str, Any]]:
        """Gets a list of all logical channels."""
        return sorted(self.logical_channels_data_from_json, key=lambda x: x.get("display_name","").lower())

    def get_logical_channel_by_id(self, logical_channel_id: str) -> dict[str, Any] | None:
        """Gets a logical channel by its logical channel ID."""
        return next((lc for lc in self.logical_channels_data_from_json if lc.get("logical_channel_id") == logical_channel_id), None)

    def add_logical_channel(self, lc_data: dict[str, Any]) -> bool:
        """Adds a new logical channel to the configuration and returns its internal ID."""
        new_id = self._generate_next_logical_channel_id()
        lc_data['logical_channel_id'] = new_id
        self.logical_channels_data_from_json.append(lc_data)
        self.config.save_logical_channels_config(self.logical_channels_data_from_json)
        return new_id

    def update_logical_channel(self, logical_channel_id: str, updated_lc_data: dict[str, Any]) -> bool:
        """Updates a logical channel in the configuration."""
        for i, lc in enumerate(self.logical_channels_data_from_json):
            if lc.get("logical_channel_id") == logical_channel_id:
                updated_lc_data["logical_channel_id"] = logical_channel_id 
                self.logical_channels_data_from_json[i] = updated_lc_data
                return self.config.save_logical_channels_config(self.logical_channels_data_from_json)
        return False

    def delete_logical_channel(self, logical_channel_id: str) -> bool:
        """Deletes a logical channel from the configuration."""
        original_len = len(self.logical_channels_data_from_json)
        self.logical_channels_data_from_json = [lc for lc in self.logical_channels_data_from_json if lc.get("logical_channel_id") != logical_channel_id]
        
        # Also remove any associated mappings
        if logical_channel_id in self.channel_mappings_data_from_json:
            del self.channel_mappings_data_from_json[logical_channel_id]
            self.config.save_channel_mappings_config(self.channel_mappings_data_from_json)
        
        return len(self.logical_channels_data_from_json) < original_len and self.config.save_logical_channels_config(self.logical_channels_data_from_json)

    def get_mappings_for_logical_channel(self, logical_channel_id: str) -> list[dict[str, Any]]:
        """Retrieves the mappings for a logical channel."""
        return self.channel_mappings_data_from_json.get(logical_channel_id, [])

    def update_mappings_for_logical_channel(self, logical_channel_id: str, new_mappings_list: list[dict[str, Any]]) -> bool:
        """Updates the mappings for a logical channel."""
        if logical_channel_id in self.channel_mappings_data_from_json:
            del self.channel_mappings_data_from_json[logical_channel_id]
        self.channel_mappings_data_from_json[logical_channel_id] = new_mappings_list
        return self.config.save_channel_mappings_config(self.channel_mappings_data_from_json)

    def get_all_discovered_source_services_for_ui(self) -> list[dict[str, Any]]:
        """Gets a list of discovered services."""
        all_services = list(self.discovered_source_services.values())
        self.config.log_message(f"Returning {len(all_services)} services for UI.", level="DEBUG")
        return sorted(all_services, key=lambda x: (x["provider_alias"], (x.get("original_tvg_name","") or x.get("original_display_name_extinf","")).lower()))