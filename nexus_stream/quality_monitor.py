import threading
import time
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor
from typing import NoReturn
from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName


# Try to have these add up to 100
RESOLUTION_WEIGHT = 50
BITRATE_WEIGHT = 30
FRAMERATE_WEIGHT = 20
RELIABILITY_WEIGHT = 0  # Since we test multiple streams at once when selecting, reliability is not critical

# These cap the maximum values for each metric, any higher will return the same score
# Essentially, values *_NORM+ will all return the weights above
RESOLTUION_NORM = 2160
BITRATE_NORM = 12_000_000
FRAMERATE_NORM = 60

QUALITY_MONITOR_INTERVAL = 300
QUALITY_MONITOR_TIMEOUT = 5
MAX_HISTORY_PER_SERVICE = 10
MAX_HISTORY_FOR_STATUSES = 100


class QualityMonitor:
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config = config
        self.handler = handler
        self._mutex = threading.Lock()

        self._quality_scores: dict[str, dict[str, float]] = {}

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def get_quality_scores(self) -> dict[str, dict[str, float]]:
        """Returns the current quality scores for all services."""
        with self._mutex:
            return self._quality_scores

    def _run(self) -> NoReturn:
        """The main execution loop for the monitor thread."""
        self._build_quality_scores(self.config.get_service_quality_cache())
        self.config.log_message("Quality Monitor thread started.", level="INFO")

        while True:
            time.sleep(QUALITY_MONITOR_INTERVAL)
            try:
                self._analyze_mapped_services()
            except Exception as e:
                self.config.log_message(f"Quality Monitor: Unhandled exception in main check loop: {e}", level="CRITICAL")
            self.config.log_message(f"Quality Monitor: Cycle complete. Sleeping for {QUALITY_MONITOR_INTERVAL} seconds.", level="INFO")

    def _get_stream_info(self, service_url: str) -> dict[str, str | float | int] | None:
        """Extracts stream information such as bitrate, resolution, and frame rate using ffprobe."""
        duration = QUALITY_MONITOR_TIMEOUT  # Doesn't seem to have an effect
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,r_frame_rate",
            "-show_entries", "packet=pts_time,size",
            "-read_intervals", f"%+{duration}",
            "-of", "json",
            service_url
        ]

        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 3)
        info = json.loads(proc.stdout)

        stream = info.get('streams', [{}])[0]
        width = int(stream.get('width', 0))
        height = int(stream.get('height', 0))
        fr_str = stream.get('r_frame_rate', '0/1')
        nums = fr_str.split('/')
        frame_rate = float(nums[0]) / float(nums[1]) if len(nums) == 2 else float(nums[0])

        packets = info.get('packets', [])
        if not packets:
            return None

        sizes, times = zip(*((float(pkt['size']), float(pkt['pts_time'])) for pkt in packets))
        duration_s = max(times) - min(times)
        if duration_s <= 0:
            return None
        total_bytes = sum(sizes)
        bitrate = (total_bytes * 8) / duration_s

        return {
            "status": "online",
            "width": width,
            "height": height,
            "bitrate": bitrate,
            "framerate": frame_rate
        }

    def _run_single_probe(self, service_id: str, service_url: str, provider_alias: ProviderName, acquire_timeout: float) -> tuple[str, dict[str, str | float | int]]:
        """
        Probes a single stream URL. It will politely wait for a slot
        for a limited time, yielding to higher-priority stream requests.
        """
        provider_slots = self.handler.slots.get(provider_alias)
        if not provider_slots:
            return service_id, {"status": "error", "reason": "Slot not found"}

        end_time = time.monotonic() + acquire_timeout
        slot_acquired = False
        while time.monotonic() < end_time:
            if self.handler.get_pending_stream_count() == 0:
                slot_acquired = provider_slots.acquire(blocking=False)  # non-blocking acquire so that streams are prioritized
                if slot_acquired:
                    break
            else:
                end_time = time.monotonic() + acquire_timeout  # Wait indefinitely if there are pending streams
            time.sleep(1)
        
        if not slot_acquired:
            return service_id, {"status": "skipped", "reason": "Provider busy (active streams?)"}

        try:
            stream_info = self._get_stream_info(service_url)
            if not stream_info:
                return service_id, {"status": "offline", "reason": "No stream info available"}
            return service_id, stream_info
        
        except subprocess.TimeoutExpired:
            return service_id, {"status": "offline", "reason": "Probe timed out"}
        except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError) as e:
            return service_id, {"status": "offline", "reason": f"Probe failed: {e}"}
        finally:
            if slot_acquired:
                provider_slots.release()
            time.sleep(3)  # Give some time before the next probe to avoid overwhelming the provider
    
    def _analyze_mapped_services(self) -> None:
        """The main logic to find and probe all mapped services, running concurrently."""
        self.config.log_message("Quality Monitor: Starting stream quality analysis cycle.", level="INFO")
        
        all_mappings = self.handler.channel_mappings_data_from_json.values()
        if not all_mappings:
            self.config.log_message("Quality Monitor: No mapped services to analyze.", level="INFO")
            return

        max_streams = sum(status["max"] for status in self.handler.get_provider_stream_status().values())
        acquire_timeout = max_streams * QUALITY_MONITOR_TIMEOUT  # If only 1 slot is available gives enough time

        # Organize probe tasks by logical channel
        probe_tasks: list[list[tuple[str, str, ProviderName, float]]] = []
        seen_tasks: set[str] = set()
        for services in all_mappings:
            channel_tasks: list[tuple[str, str, ProviderName, float]] = []
            for service in services:
                service_id = service["source_service_id"]
                service_details = self.handler.discovered_source_services.get(service_id)
                if not service_details:
                    continue
                if service_id in seen_tasks:
                    continue
                seen_tasks.add(service_id)
                channel_tasks.append(
                    (service_id, service_details['actual_stream_url'], service_details['provider_alias'], acquire_timeout)
                )
            if channel_tasks:
                probe_tasks.append(channel_tasks)
        
        # Process one logical channel at a time
        all_results: list[tuple[str, dict[str, str | float | int]]] = []
        with ThreadPoolExecutor(max_workers=max_streams) as executor:
            for channel_tasks in probe_tasks:
                all_results.extend(executor.map(lambda p: self._run_single_probe(*p), channel_tasks))

        self.config.log_message(f"Quality Monitor: {len(all_results)} probes complete. Processing results.", level="DEBUG")

        quality_cache = self.config.get_service_quality_cache()

        for service_id, result in all_results:
            service_entry = quality_cache.setdefault(service_id, {
            "statuses": [],
            "widths": [],
            "heights": [],
            "bitrates": [],
            "framerates": []
            })

            if result['status'] == 'offline':
                self.config.log_message(f"Quality Monitor: Service {service_id} is offline: {result['reason']}", level="WARN")
                service_entry["statuses"].append("offline")
                if len(service_entry["statuses"]) > MAX_HISTORY_FOR_STATUSES:
                    service_entry["statuses"].pop(0)
                continue
            elif result['status'] != 'online':
                continue

            service_entry["statuses"].append("online")
            if len(service_entry["statuses"]) > MAX_HISTORY_FOR_STATUSES:
                service_entry["statuses"].pop(0)
            width = result.get("width", 0)
            if width:
                service_entry["widths"].append(width)
                if len(service_entry["widths"]) > MAX_HISTORY_PER_SERVICE:
                    service_entry["widths"].pop(0)
            height = result.get("height", 0)
            if height:
                service_entry["heights"].append(height)
                if len(service_entry["heights"]) > MAX_HISTORY_PER_SERVICE:
                    service_entry["heights"].pop(0)
            bitrate = result.get("bitrate", 0)
            if bitrate:
                service_entry["bitrates"].append(bitrate)
                if len(service_entry["bitrates"]) > MAX_HISTORY_PER_SERVICE:
                    service_entry["bitrates"].pop(0)
            framerate = result.get("framerate", 0)
            if framerate:
                service_entry["framerates"].append(framerate)
                if len(service_entry["framerates"]) > MAX_HISTORY_PER_SERVICE:
                    service_entry["framerates"].pop(0)

        self.config.save_service_quality_cache(quality_cache)
        self._build_quality_scores(quality_cache)

    def _build_quality_scores(self, quality_cache: dict[str, dict[str, list[str | float | int]]]) -> None:
        """
        Calculate quality scores for each source based on resolution, bitrate, framerate, and reliability.
        
        Quality metrics are scored and weighted using constants defined at the top of the file:
        - Resolution (height): 0-RESOLUTION_WEIGHT points (normalized against RESOLTUION_NORM)
        - Bitrate: 0-BITRATE_WEIGHT points (normalized against BITRATE_NORM)
        - Framerate: 0-FRAMERATE_WEIGHT points (normalized against FRAMERATE_NORM)
        - Reliability: 0-RELIABILITY_WEIGHT points (percentage of time online)
        
        Returns:
            Dictionary mapping source_service_id to dict containing:
            - total_score: float - Combined quality score
            - resolution_score: float - Score for resolution (0-RESOLUTION_WEIGHT)
            - bitrate_score: float - Score for bitrate (0-BITRATE_WEIGHT)
            - framerate_score: float - Score for framerate (0-FRAMERATE_WEIGHT)
            - reliability_score: float - Score for reliability (0-RELIABILITY_WEIGHT)
            - uptime_10: float - Uptime over last 10 status checks (0.0-1.0)
            - uptime_100: float - Uptime over last 100 status checks (0.0-1.0)
        """
        self.config.log_message("Quality Monitor: Building quality scores from cache.", level="DEBUG")
        
        final_scores: dict[str, dict[str, float]] = {}
        for service_id, cache_entry in quality_cache.items():
            # Calculate average metrics
            heights: list[float] = cache_entry["heights"]
            avg_height: float = sum(heights) / len(heights) if heights else 0.0
            
            bitrates: list[float] = cache_entry["bitrates"]
            avg_bitrate: float = sum(bitrates) / len(bitrates) if bitrates else 0.0
            
            framerates: list[float] = cache_entry["framerates"]
            avg_framerate: float = sum(framerates) / len(framerates) if framerates else 0.0
            
            statuses: list[str] = cache_entry["statuses"]
            reliability: float = sum(1 for s in statuses if s == "online") / len(statuses) if statuses else 0.0
            
            # Calculate uptime metrics
            uptime_10: float = 0.0
            if statuses:
                recent_10 = statuses[-10:] if len(statuses) >= 10 else statuses
                uptime_10 = sum(1 for s in recent_10 if s == "online") / len(recent_10)
            
            uptime_100: float = 0.0
            if statuses:
                recent_100 = statuses[-100:] if len(statuses) >= 100 else statuses
                uptime_100 = sum(1 for s in recent_100 if s == "online") / len(recent_100)
            
            # Calculate normalized scores
            height_score: float = RESOLUTION_WEIGHT * min(avg_height / float(RESOLTUION_NORM), 1.0)
            bitrate_score: float = BITRATE_WEIGHT * min(avg_bitrate / float(BITRATE_NORM), 1.0)
            framerate_score: float = FRAMERATE_WEIGHT * min(avg_framerate / float(FRAMERATE_NORM), 1.0)
            reliability_score: float = RELIABILITY_WEIGHT * reliability
            
            # Store final scores
            final_scores[service_id] = {
                "total_score": height_score + bitrate_score + framerate_score + reliability_score,
                "resolution_score": height_score,
                "bitrate_score": bitrate_score,
                "framerate_score": framerate_score,
                "reliability_score": reliability_score,
                "uptime_10": uptime_10,
                "uptime_100": uptime_100
            }

        with self._mutex:
            self._quality_scores = final_scores
        self.config.log_message("Quality Monitor: Quality scores built successfully.", level="DEBUG")
