import threading
import time
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor
from nexus_stream.config import Config
from nexus_stream.handler import ChannelHandler
from nexus_stream.slots import ProviderName

# Constants

class QualityMonitor:
    def __init__(self, config: Config, handler: ChannelHandler) -> None:
        self.config = config
        self.handler = handler
        self.interval: int = 300
        self.max_history_per_service: int = 10
        self.max_history_for_statuses: int = 100
        self.timeout: int = 5
        
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        """The main execution loop for the monitor thread."""
        self.config.log_message("Quality Monitor thread started.", level="INFO")
        time.sleep(10)

        while True:
            try:
                self._analyze_mapped_services()
            except Exception as e:
                self.config.log_message(f"Quality Monitor: Unhandled exception in main check loop: {e}", level="CRITICAL")
            
            self.config.log_message(f"Quality Monitor: Cycle complete. Sleeping for {self.interval} seconds.", level="INFO")
            time.sleep(self.interval)

    def get_stream_info(self, service_url: str) -> dict[str, str | float | int] | None:
        """Extracts stream information such as bitrate, resolution, and frame rate using ffprobe."""
        duration = self.timeout  # Doesn't seem to have an effect
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
            stream_info = self.get_stream_info(service_url)
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
        target_service_ids = {mapping['source_service_id'] for mappings_list in all_mappings for mapping in mappings_list}

        if not target_service_ids:
            self.config.log_message("Quality Monitor: No mapped services to analyze.", level="INFO")
            return

        max_streams = sum(status["max"] for status in self.handler.get_provider_stream_status().values())
        acquire_timeout = max_streams * self.timeout  # If only 1 slot is available gives enough time

        probe_tasks: list[tuple[str, str, ProviderName, float]] = []
        for service_id in target_service_ids:
            service_details = self.handler.discovered_source_services.get(service_id)
            if service_details:
                probe_tasks.append(
                    (service_id, service_details['actual_stream_url'], service_details['provider_alias'], acquire_timeout)
                )

        with ThreadPoolExecutor(max_workers=max_streams) as executor:
            results_iterator = executor.map(lambda p: self._run_single_probe(*p), probe_tasks)
        all_results = list(results_iterator)

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
                self.config.log_message(f"Quality Monitor: Service {service_id} is offline: {result['reason']}", level="WARNING")
                service_entry["statuses"].append("offline")
                if len(service_entry["statuses"]) > self.max_history_for_statuses:
                    service_entry["statuses"].pop(0)
                continue
            elif result['status'] != 'online':
                continue

            service_entry["statuses"].append("online")
            if len(service_entry["statuses"]) > self.max_history_for_statuses:
                service_entry["statuses"].pop(0)
            width = result.get("width", 0)
            if width:
                service_entry["widths"].append(width)
                if len(service_entry["widths"]) > self.max_history_per_service:
                    service_entry["widths"].pop(0)
            height = result.get("height", 0)
            if height:
                service_entry["heights"].append(height)
                if len(service_entry["heights"]) > self.max_history_per_service:
                    service_entry["heights"].pop(0)
            bitrate = result.get("bitrate", 0)
            if bitrate:
                service_entry["bitrates"].append(bitrate)
                if len(service_entry["bitrates"]) > self.max_history_per_service:
                    service_entry["bitrates"].pop(0)
            framerate = result.get("framerate", 0)
            if framerate:
                service_entry["framerates"].append(framerate)
                if len(service_entry["framerates"]) > self.max_history_per_service:
                    service_entry["framerates"].pop(0)

        self.config.save_service_quality_cache(quality_cache)