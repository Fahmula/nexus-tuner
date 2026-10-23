"""
The main Quart application file for NexusTuner.

This file initializes the Quart app and its async components (Config, ChannelHandler, 
StreamManager, GhostSessionMonitor) and defines all the web routes for:
- Serving the main M3U playlist.
- Handling HLS and MPEGTS streaming requests asynchronously.
- HDHomeRun emulation endpoints.
- Providing a web-based user interface (UI) for configuration and management.
- A manual reload endpoint.
"""

from collections import deque
import signal
import sys
from types import FrameType
if sys.version_info < (3, 14):
    raise RuntimeError("NexusTuner requires Python 3.14 or higher.")

import asyncio
import aiofiles.os
import json
import math
import os
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncGenerator, Dict, Final, cast

from quart import Quart, Response, abort, flash, redirect, render_template, request, url_for
from quart import send_from_directory  # type: ignore
from werkzeug.wrappers import Response as WerkzeugResponse
from werkzeug.datastructures import ImmutableMultiDict

from nexus_tuner.config import SETTINGS_INT_FIELDS, Config, validate_media_server
from nexus_tuner.create_stream import CreateStream
from nexus_tuner.handler import ChannelHandler
from nexus_tuner.mpegts import MPEGTSStream
from nexus_tuner.quality_monitor import QualityMonitor
from nexus_tuner.session_monitor import GhostSessionMonitor, test_media_server_connection
from nexus_tuner.stream import StreamManager
from nexus_tuner.scheduler import Scheduler
from nexus_tuner.utils import (Log, LogicalChannelFormDetails, PreviewId, ProcessInfoMutable, Runtime, StreamEngine, StopReason, Uptime, VideoKey, background_tasks, CREATE_STREAM_DEADLINE, CREATE_STREAM_POLL_INTERVAL,
                                DEFAULT_PRIORITY, M3UURL, NEXUS_TUNER_PORT, NEXUS_TUNER_VERSION, ChannelNum, DiscoveredSource,
                                Label, LogicalChannelId, LogicalChannelInfo, LogicalChannelInfoWithId, LogicalChannelMetrics, LogicalChannelTitle, MaxStreams, 
                                PercentDisplay, Priority, ProviderAlias, ProviderStatus, QualityScores, SourceInfo, SourceMappingInfoWithId, SourceMetrics,
                                SourceId, TVGGroupTitle, TVGId, TVGLogo, VideoType, create_preview_id, create_stream_key, create_stream_name, duration_to_str, get_source_id_from_preview, is_valid_url, run_bg, sort_sources,
                                MAIN_PLAYLIST_ID, BaseURL, GroupKey, LCGroupMode, PlaylistId, PlaylistInfo, PlaylistName, normalize_group_key, playlist_device_id,
                                CONFIG_DIR, MediaServerId, MediaServerInfo, MediaServerType, create_media_server_id)

# --- Constants ---
PLAYLIST_POLL_INTERVAL: Final[float] = 0.2         # Seconds to wait between checking for a new playlist
HIGHEST_PRIORITY_SOURCES_NUM: Final[int] = 8       # Maximum number of sources to consider for quality metrics
NUM_SOURCES_PER_PAGE: Final[int] = 100             # Number of sources to display per page in the UI
MULTI_SEARCH_QUERY_DELIMITER: Final[str] = " OR "  # Delimiter for multi-word search queries

# --- App Initialization ---
app = Quart(__name__)
app.secret_key = os.urandom(24)

config: Config
handler: ChannelHandler
stream_manager: StreamManager
ghost_monitor: GhostSessionMonitor
quality_monitor: QualityMonitor
scheduler: Scheduler


@app.before_serving
async def startup() -> None:
    """
    Asynchronous startup function. Initializes all core components and starts background tasks.
    This is the standard Quart pattern for handling async setup.
    """
    global config, handler, stream_manager, ghost_monitor, quality_monitor, scheduler
    try:
        config = await Config.create()
        handler = await ChannelHandler.create(config)
        quality_monitor = await QualityMonitor.create(config, handler)
        handler.quality_monitor = quality_monitor
        stream_manager = await StreamManager.create(config, handler, quality_monitor)
        quality_monitor.stream_manager = stream_manager
        ghost_monitor = await GhostSessionMonitor.create(config, handler, stream_manager)
        scheduler = await Scheduler.create(config, handler, quality_monitor)
        Log.info(Label.SERVER, f"Started on {config.nexus_url}")
    except BaseException as e:
        if Log.initialized:
            Log.critical(Label.SERVER, f"Could not initialize application: {e}")
        print(f"FATAL: Could not initialize application - {e}", file=sys.stderr)
        sys.exit(1)

    global prev_sigterm_handler, prev_sigint_handler, prev_sighup_handler, prev_sigquit_handler
    prev_sigterm_handler = signal.signal(signal.SIGTERM, handle_signal)
    prev_sigint_handler = signal.signal(signal.SIGINT, handle_signal)
    prev_sighup_handler = signal.signal(signal.SIGHUP, handle_signal)
    prev_sigquit_handler = signal.signal(signal.SIGQUIT, handle_signal)

    app.jinja_env.filters["duration_to_str"] = duration_to_str  # type: ignore


prev_sigterm_handler = signal.getsignal(signal.SIGTERM)
prev_sigint_handler = signal.getsignal(signal.SIGINT)
prev_sighup_handler = signal.getsignal(signal.SIGHUP)
prev_sigquit_handler = signal.getsignal(signal.SIGQUIT)


def handle_signal(signum: int, frame: FrameType | None) -> None:
    """Handles termination signals to gracefully shut down the application."""
    sig = signal.Signals(signum)
    Log.info(Label.SERVER, f"Received {sig.name}, shutting down NexusTuner v{NEXUS_TUNER_VERSION}...")
    for mpegts_stream in MPEGTSStream.streams.values():
        mpegts_stream.shutdown()  # We need to close any active connections for @app.after_serving to trigger
    if sig == signal.SIGTERM and callable(prev_sigterm_handler):
        prev_sigterm_handler(sig, frame)
    elif sig == signal.SIGINT and callable(prev_sigint_handler):
        prev_sigint_handler(sig, frame)
    elif sig == signal.SIGHUP and callable(prev_sighup_handler):
        prev_sighup_handler(sig, frame)
    elif sig == signal.SIGQUIT and callable(prev_sigquit_handler):
        prev_sigquit_handler(sig, frame)


@app.after_serving
async def shutdown() -> None:
    """Handles graceful shutdown of the application."""
    if "scheduler" in globals():
        scheduler.shutdown()
    if "stream_manager" in globals():
        await stream_manager.stop_processes(StopReason.SHUTDOWN)
    if "config" in globals():
        await config.cleanup_tmp_dir()
    await asyncio.gather(*background_tasks, return_exceptions=True)


async def calculate_channel_metrics(mapped_sources: list[SourceMappingInfoWithId], all_quality_scores: QualityScores) -> LogicalChannelMetrics:
    """Calculates uptime metrics for a channel."""
    sort_sources(mapped_sources, all_quality_scores, reverse=False)
    uptime_scores: list[Uptime] = []
    runtime_scores: list[Runtime] = []
    for mapped_source in mapped_sources[:HIGHEST_PRIORITY_SOURCES_NUM]:
        quality_score = all_quality_scores.get(mapped_source["source_id"])
        if not quality_score:
            continue
        uptime_scores.append(quality_score["uptime"])
        if quality_score["runtime"]:
            runtime_scores.append(quality_score["runtime"])

    discovered_mappings = 0
    for source in mapped_sources:
        if await handler.get_discovered_source(source["source_id"]):
            discovered_mappings += 1
    return LogicalChannelMetrics({
        "health_score": PercentDisplay(int((1 - math.prod([(1 - score) for score in uptime_scores])) * 100)) if uptime_scores else None,
        "lowest_uptime": PercentDisplay(int(min(uptime_scores) * 100)) if uptime_scores else None,
        "lowest_runtime": min(runtime_scores) if runtime_scores else None,
        "enabled_mappings": len(mapped_sources),
        "discovered_mappings": discovered_mappings,
    })

def filter_sources(raw_query: str, discovered_source: DiscoveredSource) -> bool:
    """Filters sources based on a query."""
    tvg_name = discovered_source["tvg_name"].lower()
    display_title = discovered_source["display_title"].lower()
    for raw_q in raw_query.split(MULTI_SEARCH_QUERY_DELIMITER):
        words = raw_q.strip().lower().split()
        if all(word in tvg_name or word in display_title for word in words):
            return True
    return False


async def calculate_logical_channel_form_details(*, logical_channel_id: LogicalChannelId | None, search_query: str | None, filter_query: str | None, current_page: int) -> LogicalChannelFormDetails:
    all_quality_scores = await quality_monitor.get_quality_scores()
    current_mappings = await handler.get_channel_mappings_for_ui(logical_channel_id) if logical_channel_id else []
    channel_metrics = await calculate_channel_metrics(current_mappings, all_quality_scores)

    all_discovered_sources = await handler.get_discovered_sources_for_ui()
    all_sources_map = {s['source_id']: s for s in all_discovered_sources}

    mapped_sources: list[DiscoveredSource] = []
    all_mapped_source_ids: set[SourceId] = set()
    all_source_metrics: dict[SourceId, SourceMetrics] = {}
    for mapping in current_mappings:
        source_id = mapping['source_id']
        all_mapped_source_ids.add(source_id)
        if source_id in all_sources_map:
            source_details = all_sources_map[source_id].copy()
            mapped_sources.append(source_details)
            raw_uptime = all_quality_scores.get(source_id, {}).get('uptime')
            raw_runtime = all_quality_scores.get(source_id, {}).get('runtime')
            all_source_metrics[source_id] = SourceMetrics({
                "priority": mapping["priority"],
                "uptime": PercentDisplay(int(raw_uptime * 100)) if raw_uptime is not None else None,
                "runtime": raw_runtime if raw_runtime else None,
                "offset": mapping["offset"],
            })
    
    unmapped_sources: list[DiscoveredSource] = []
    if filter_query and search_query:
        for discovered_source in all_discovered_sources:
            if discovered_source['source_id'] not in all_mapped_source_ids and filter_sources(search_query, discovered_source):
                unmapped_sources.append(discovered_source)
                source_id = discovered_source['source_id']
                raw_uptime = all_quality_scores.get(source_id, {}).get('uptime')
                raw_runtime = all_quality_scores.get(source_id, {}).get('runtime')
                all_source_metrics[source_id] = SourceMetrics({
                    "priority": Priority(DEFAULT_PRIORITY),
                    "uptime": PercentDisplay(int(raw_uptime * 100)) if raw_uptime is not None else None,
                    "runtime": raw_runtime if raw_runtime else None,
                    "offset": None,
                })

    total_unmapped_sources = len(unmapped_sources)
    total_pages = math.ceil(total_unmapped_sources / NUM_SOURCES_PER_PAGE) if NUM_SOURCES_PER_PAGE > 0 else 1
    if current_page < 1:
        current_page = 1
    elif current_page > total_pages and total_pages > 0:
        current_page = total_pages
    start_index = (current_page - 1) * NUM_SOURCES_PER_PAGE
    unmapped_sources_for_page = unmapped_sources[start_index:start_index + NUM_SOURCES_PER_PAGE]

    return LogicalChannelFormDetails(
        channel_metrics=channel_metrics,
        all_source_metrics=all_source_metrics,
        mapped_sources=mapped_sources,
        unmapped_sources_for_page=unmapped_sources_for_page,
        total_unmapped_sources=total_unmapped_sources,
        total_pages=total_pages,
        current_page=current_page
    )


@app.context_processor
def inject_global_vars() -> Dict[str, datetime | str | list[str]]:
    """Injects global variables into the context of all templates."""
    return {
        'now': datetime.now(UTC),
        'app_version': NEXUS_TUNER_VERSION,
        'config_problems': config.get_path_problems(),
    }


def abort_if_engine_unusable(stream_engine: StreamEngine, video_type: VideoType) -> None:
    """Aborts before trying every source when the stream engine's executables cannot be used."""
    if (problem := config.get_engine_problem(stream_engine)):
        msg = f"Cannot create stream: {problem} Fix it in Settings."
        Log.error(Label.SERVER, msg, (video_type, stream_engine))
        abort(503, msg)


# --- Streaming Endpoints ---


@app.route(f'/{VideoType.MPEGTS}/<string:logical_channel_id>')
async def serve_mpegts_stream(logical_channel_id: LogicalChannelId, stream_response: bool = True) -> Response | VideoKey:
    """Serves a channel stream using MPEGTS format.
    If stream_response is True, it returns a generator that the client connects to, otherwise it simply creates the stream
    and returns the video key for later use.
    """
    added_pending_stream = False
    loop = asyncio.get_running_loop()
    end_time = loop.time() + CREATE_STREAM_DEADLINE
    stream_engine = config.stream_engine
    stream_key = create_stream_key(stream_engine, VideoType.MPEGTS, logical_channel_id)
    try:
        while not await handler.add_pending_stream(stream_key):
            if loop.time() > end_time:
                msg = f"Exceeded timeout while waiting for earlier request for {stream_key} to complete."
                Log.error(Label.SERVER, msg, (VideoType.MPEGTS, stream_engine))
                abort(503, msg)
            await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        stream_channel = await handler.resolve_stream_channel(logical_channel_id)
        if not stream_channel:
            msg = f"Channel {logical_channel_id} not found for MPEGTS."
            Log.error(Label.SERVER, msg, (VideoType.MPEGTS, stream_engine))
            abort(404, msg)
        logical_channel_title = stream_channel['title']
        channel_num = stream_channel['channel_num']

        lc_id_processes = await stream_manager.get_processes_from_logical_id(logical_channel_id, video_type=VideoType.MPEGTS, stream_engine=stream_engine, long_term_only=True)
        if len(lc_id_processes):
            video_key, p_info = lc_id_processes.popitem()
            video_name = p_info['video_name']
            if p_info['is_mpegts_active']:
                Log.info(Label.SERVER, f"{video_name}: Client connecting to shared stream.", (VideoType.MPEGTS, stream_engine))
            else:
                Log.info(Label.SERVER, f"{video_name}: Client reconnected to stream.", (VideoType.MPEGTS, stream_engine))
        else:
            abort_if_engine_unusable(stream_engine, VideoType.MPEGTS)
            create_stream_obj = await CreateStream.create(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_title, channel_num, VideoType.MPEGTS, stream_engine)
            res = await create_stream_obj.result()
            match res[0]:
                case True:
                    video_key, video_name = res[1:]
                case False:
                    code, msg = res[1:]
                    Log.error(Label.SERVER, msg, (VideoType.MPEGTS, stream_engine))
                    abort(code, msg)

        if not stream_response:
            Log.info(Label.SERVER, f"{video_name}: Recreated stream.", (VideoType.MPEGTS, stream_engine))
            return video_key

        async def stream_generator() -> AsyncGenerator[bytes, None]:
            async def recreate_stream() -> Response | VideoKey:
                return await serve_mpegts_stream(logical_channel_id, stream_response=False)
            try:
                mpegts_stream, reader_id = await MPEGTSStream.register(config, stream_manager, logical_channel_id, video_key, video_name, stream_engine, recreate_stream)
            except Exception as e:
                msg = f"{video_name}: Failed to register stream - {e}"
                Log.error(Label.SERVER, msg, (VideoType.MPEGTS, stream_engine))
                abort(500, msg)

            try:
                while True:
                    yield await mpegts_stream.read(reader_id)
            except asyncio.CancelledError as e:
                Log.info(Label.SERVER, f"{video_name}: Client #{reader_id} disconnected from stream.", (VideoType.MPEGTS, None))
            except BaseException as e:
                Log.error(Label.SERVER, f"{video_name}: Client #{reader_id} unexpected error in stream - {e}", (VideoType.MPEGTS, None))
                raise
            finally:
                mpegts_stream.unregister(reader_id)

        response = Response(stream_generator(), mimetype='video/mp2t')
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.timeout = None
        return response
    finally:
        if added_pending_stream:
            await handler.remove_pending_stream(stream_key)


@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/<string:stream_engine>/playlist.m3u8')
async def serve_hls_playlist(logical_channel_id: LogicalChannelId | PreviewId, stream_engine: StreamEngine, logical_channel_title: LogicalChannelTitle | None = None, channel_num: ChannelNum | None = None, sources: list[SourceInfo] | None = None) -> Response:
    """Serves the HLS playlist for a channel."""
    if stream_engine not in tuple(StreamEngine):
        abort(404, f"Unknown stream engine '{stream_engine}'.")
    run_bg(stream_manager.record_video_access(logical_channel_id, VideoType.HLS, stream_engine))
    added_pending_stream = False
    loop = asyncio.get_running_loop()
    end_time = loop.time() + CREATE_STREAM_DEADLINE
    stream_key = create_stream_key(stream_engine, VideoType.HLS, logical_channel_id)
    try:
        while not await handler.add_pending_stream(stream_key):
            if loop.time() > end_time:
                msg = f"Exceeded timeout while waiting for earlier request for {stream_key} to complete."
                Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
                abort(503, msg)
            await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        if logical_channel_title is None:
            stream_channel = await handler.resolve_stream_channel(logical_channel_id)
            if not stream_channel:
                msg = f"Channel {logical_channel_id} not found for HLS."
                Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
                abort(404, msg)
            logical_channel_title = stream_channel['title']
            channel_num = stream_channel['channel_num']
        else:
            channel_num = channel_num

        lc_id_processes = await stream_manager.get_processes_from_logical_id(logical_channel_id, video_type=VideoType.HLS, stream_engine=stream_engine, long_term_only=True)
        if len(lc_id_processes):
            video_key, p_info = lc_id_processes.popitem()
            video_name = p_info['video_name']
        else:
            abort_if_engine_unusable(stream_engine, VideoType.HLS)
            create_stream_obj = await CreateStream.create(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_title, channel_num, VideoType.HLS, stream_engine, sources)
            res = await create_stream_obj.result()
            match res[0]:
                case True:
                    video_key, video_name = res[1:]
                case False:
                    code, msg = res[1:]
                    Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
                    abort(code, msg)

        playlist_path = await stream_manager.get_hls_playlist_path(video_key)
        if not playlist_path:
            msg = f"{video_name}: Internal error: HLS playlist path not found."
            Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
            abort(500, msg)

        end_time = loop.time() + config.process_start_timeout
        while loop.time() < end_time:
            to_cleanup = False
            async with stream_manager.stream_process_lock:
                if video_key not in stream_manager.processes:
                    to_cleanup = True
                elif stream_manager.processes[video_key]['process'].returncode is not None:
                    to_cleanup = True
                    if not stream_manager.processes[video_key]["stop_reason"]:
                        cast(ProcessInfoMutable, stream_manager.processes[video_key])["stopped_at"] = datetime.now()
                        cast(ProcessInfoMutable, stream_manager.processes[video_key])["stop_reason"] = StopReason.ERROR
                        Log.debug(Label.STREAM, f"{video_name}: Updated stopped timestamp with {StopReason.ERROR}.", (VideoType.HLS, stream_engine))
            if to_cleanup:
                msg = f"{video_name}: Process terminated unexpectedly."
                Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
                await stream_manager.stop_process(video_key)
                abort(503, msg)

            if await aiofiles.os.path.exists(playlist_path) and (await aiofiles.os.stat(playlist_path)).st_size > 0:
                try:
                    response = await send_from_directory(str(playlist_path.parent), playlist_path.name, mimetype="application/vnd.apple.mpegurl")
                    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                    response.headers["Pragma"] = "no-cache"
                    response.headers["Expires"] = "0"
                    return response
                except Exception as e:
                    msg = f"{video_name}: Error serving HLS playlist {playlist_path} - {e}"
                    Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
                    abort(500, msg)
            await asyncio.sleep(PLAYLIST_POLL_INTERVAL)

        msg = f"{video_name}: HLS playlist was not available after {config.process_start_timeout} seconds."
        Log.error(Label.SERVER, msg, (VideoType.HLS, stream_engine))
        abort(408, msg)
    finally:
        if added_pending_stream:
            await handler.remove_pending_stream(stream_key)


@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/<string:stream_engine>/<path:segment_filename>')
async def serve_hls_segment(logical_channel_id: LogicalChannelId, stream_engine: StreamEngine, segment_filename: str) -> Response:
    """Serves an HLS video segment (.ts file)."""
    run_bg(stream_manager.record_video_access(logical_channel_id, VideoType.HLS, stream_engine, segment_filename=segment_filename))
    if not segment_filename.endswith(".ts") or ".." in segment_filename:
        Log.error(Label.SERVER, f"Invalid segment filename for channel '{logical_channel_id}': {segment_filename}", (VideoType.HLS, None))
        abort(400, f"Invalid segment filename for channel '{logical_channel_id}': {segment_filename}")
    
    segment_path = await stream_manager.get_hls_segment_path(logical_channel_id, VideoType.HLS, stream_engine, segment_filename)
    if not segment_path or not await aiofiles.os.path.isfile(segment_path):
        Log.error(Label.SERVER, f"HLS segment not found for channel '{logical_channel_id}' with filename '{segment_filename}'.", (VideoType.HLS, None))
        abort(404, f"HLS segment not found for channel '{logical_channel_id}'")

    response = await send_from_directory(str(segment_path.parent), segment_path.name, mimetype="video/mp2t")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/ui/source-preview/<path:source_id>")
async def ui_player_for_source(source_id: SourceId, logical_channel_id: LogicalChannelId | None = None) -> str:
    lc_id_from_qs = request.args.get("logical_channel_id")
    if lc_id_from_qs:
        logical_channel_id = LogicalChannelId(lc_id_from_qs)
    preview_id = create_preview_id(source_id, logical_channel_id)
    if logical_channel_id:
        logical_channel = await handler.get_logical_channel_by_id(logical_channel_id)
        channel_num = logical_channel['channel_num'] if logical_channel else None
    else:
        channel_num = None
    discovered_source = await handler.get_discovered_source(source_id)
    if not discovered_source:
        msg = f"Source ID '{source_id}' not found for preview."
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        abort(404, msg)
    source_name = discovered_source["display_title"] or discovered_source["tvg_name"] or "Preview"
    tvg_logo = discovered_source["tvg_logo"]
    stream_engine = config.stream_engine
    playlist_url = url_for('serve_hls_preview', preview_id=preview_id, stream_engine=stream_engine, channel_num=channel_num)
    return await render_template("_video_player_modal.html", playlist_url=playlist_url, preview_id=preview_id, stream_engine=stream_engine, source_id=source_id, source_name=source_name, tvg_logo=tvg_logo)


@app.route(f'/{VideoType.HLS}/<string:preview_id>/<string:stream_engine>/preview.m3u8')
async def serve_hls_preview(preview_id: PreviewId, stream_engine: StreamEngine, channel_num: ChannelNum | None = None) -> Response:
    """Serves a preview HLS playlist for a channel."""
    ch_num_from_qs = request.args.get("channel_num")
    if ch_num_from_qs:
        channel_num = ChannelNum(ch_num_from_qs)
    source_id = get_source_id_from_preview(preview_id)
    discovered_source = await handler.get_discovered_source(source_id)

    if not discovered_source:
        msg = f"Preview requested for non-existent source ID {source_id}."
        Log.error(Label.SERVER, msg, (VideoType.HLS, None))
        abort(404, msg)

    sources: list[SourceInfo] = [{
        'source_id': source_id,
        'priority': await handler.get_source_priority(source_id) or Priority(DEFAULT_PRIORITY),
        'provider_alias': discovered_source['provider_alias'],
        'stream_url': discovered_source['stream_url']
    }]

    return await serve_hls_playlist(preview_id, stream_engine, logical_channel_title=LogicalChannelTitle('Preview'), channel_num=channel_num, sources=sources)


@app.route("/<string:video_type>/<string:logical_channel_id>/<string:stream_engine>/stop", methods=["POST"])
async def stop_stream(video_type: VideoType, logical_channel_id: LogicalChannelId, stream_engine: StreamEngine) -> Response:
    """Stops the stream for a logical channel."""
    if video_type == VideoType.MPEGTS:
        for mpegts_stream in MPEGTSStream.streams.values():
            if mpegts_stream.logical_channel_id == logical_channel_id and mpegts_stream.stream_engine == stream_engine:
                mpegts_stream.shutdown()
    await stream_manager.stop_processes_with_logical_channel_id(logical_channel_id, video_type, stream_engine, StopReason.MANUAL)
    return Response(status=204)


def get_request_base_url(playlist: PlaylistInfo | None = None) -> str:
    """Returns the base URL for client-facing URLs, using the playlist's override if set,
    otherwise the URL the client connected with so that e.g. clients connecting over a VPN get working URLs.
    """
    if playlist and playlist["base_url"]:
        return playlist["base_url"].rstrip("/")
    host = request.headers.get("X-Forwarded-Host", "").split(",")[0].strip() or request.host
    if not host:
        return config.nexus_url
    scheme = request.headers.get("X-Forwarded-Proto", "").split(",")[0].strip() or request.scheme
    return f"{scheme}://{host}"


def get_playlist_urls(playlist_id: PlaylistId, playlist: PlaylistInfo) -> dict[str, str]:
    """Returns the M3U and HDHomeRun URLs of a playlist as seen by the current client."""
    base_url = get_request_base_url(playlist)
    if playlist_id == MAIN_PLAYLIST_ID:
        return {"m3u": f"{base_url}/playlist.m3u", "hdhomerun": base_url}
    return {"m3u": f"{base_url}/playlists/{playlist_id}/playlist.m3u", "hdhomerun": f"{base_url}/playlists/{playlist_id}"}


async def serve_playlist_m3u_for(playlist_id: PlaylistId) -> Response:
    """Serves the M3U of a playlist with URLs using the playlist's base URL."""
    playlist = await handler.get_playlist(playlist_id)
    entries = await handler.get_playlist_entries(playlist_id)
    if playlist is None or entries is None:
        msg = f"Playlist '{playlist_id}' not found."
        Log.error(Label.SERVER, msg)
        abort(404, msg)
    base_url = get_request_base_url(playlist)
    m3u_lines = ["#EXTM3U x-tvg-url=\"\""]
    for entry in entries:
        m3u_lines.append(entry["extinf"])
        m3u_lines.append(f"{base_url}{entry['path']}")
    return Response("\n".join(m3u_lines) + "\n", mimetype="application/x-mpegurl")


@app.route("/playlist.m3u")
async def serve_main_playlist() -> Response:
    """Serves the main M3U playlist for clients."""
    return await serve_playlist_m3u_for(MAIN_PLAYLIST_ID)


@app.route("/playlists/<string:playlist_id>/playlist.m3u")
async def serve_playlist_m3u(playlist_id: PlaylistId) -> Response:
    """Serves the M3U playlist for clients of a specific playlist."""
    return await serve_playlist_m3u_for(playlist_id)


# --- UI Endpoints ---


@app.route("/")
@app.route("/ui")
async def ui_main_dashboard() -> str:
    """Renders the main dashboard page."""
    return await render_template("ui_dashboard.html",
                           provider_count=await handler.get_num_providers(),
                           discovered_sources_count=await handler.get_num_discovered_sources(),
                           logical_channels_count=await handler.get_num_logical_channels())


@app.route("/ui/flash-messages")
async def ui_flash_messages() -> str:
    """Renders just the flash messages partial for HTMX updates."""
    return await render_template("_flash_messages.html")


@app.route("/ui/provider-status")
async def ui_provider_status() -> str:
    statuses = await handler.get_provider_stream_status()
    active_streams = sum(status['active_streams'] for status in statuses.values())
    max_total_streams = sum(status['max_streams'] for status in statuses.values())
    return await render_template("_provider_status_bar.html", active_streams=active_streams, max_total_streams=max_total_streams)


@app.route("/ui/providers", methods=["GET"])
async def ui_providers_manage() -> str:
    all_providers = sorted((await handler.get_provider_stream_status()).values(), key=lambda p: p['alias'])
    all_processes = await stream_manager.get_process_statuses()
    return await render_template("ui_providers.html", providers=all_providers, processes=all_processes)


@app.route("/ui/providers/streams/<string:alias>")
async def ui_provider_streams_row(alias: ProviderAlias) -> str:
    providers = await handler.get_provider_stream_status()
    provider = providers.get(alias)
    if not provider:
        abort(404)
    all_processes = await stream_manager.get_process_statuses()
    return await render_template("_provider_streams_rows.html",
                                 provider=provider,
                                 processes=all_processes)


@app.route("/ui/providers/add", methods=["GET", "POST"])
async def ui_provider_add() -> Response | str:
    if request.method == "POST":
        form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
        alias = ProviderAlias(form_data.get("alias", "").strip())
        m3u_url = M3UURL(form_data.get("m3u_url", "").strip())
        max_streams_str = form_data.get("max_streams", "")
        try:
            if ";" in alias:
                raise ValueError("Alias must not contain semicolons (';').")
            max_streams = MaxStreams(int(max_streams_str))
            if not is_valid_url(m3u_url):
                raise ValueError(f"Invalid URL format: {m3u_url}")
            if not await handler.add_provider(alias, m3u_url, max_streams):
                raise ValueError(f"Failed to add provider '{alias}'.")
            Log.info(Label.SERVER, f"Provider '{alias}' added with max streams {max_streams}.")
            await flash(f"Provider '{alias}' added successfully, use 'Reload Providers & Sources' on the dashboard to discover sources.", "success")
            all_providers = sorted((await handler.get_provider_stream_status()).values(), key=lambda p: p['alias'])
            table_body_html = await render_template("_providers_table_body.html", providers=all_providers)
            form_removal_html = '<div id="add-provider-form-wrapper" hx-swap-oob="true"></div>'
            response = Response(table_body_html + form_removal_html)
        except ValueError as e:
            msg = f"Failed to add provider '{alias}': {e}"
            Log.error(Label.SERVER, msg)
            await flash(msg, "error")
            response = Response(await render_template("_provider_add_form.html", alias=alias, m3u_url=m3u_url, max_streams=max_streams_str))
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        response.headers["HX-Refresh"] = "true"
        return response
    return await render_template("_provider_add_form.html")


@app.route("/ui/providers/edit/<string:alias>", methods=["GET", "PUT"])
async def ui_provider_edit(alias: ProviderAlias) -> Response | str:
    provider = (await handler.get_provider_stream_status()).get(alias)
    if not provider:
        return ""

    if request.method == "GET":
        if request.args.get('cancel') == 'true':
            return await render_template("_provider_row.html", provider=provider)
        return await render_template("_provider_edit_form.html", provider=provider)

    form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
    m3u_url = M3UURL(form_data.get("m3u_url", "").strip())
    max_streams_str = form_data.get("max_streams", "")
    try:
        max_streams = MaxStreams(int(max_streams_str))
        if not is_valid_url(m3u_url):
            raise ValueError(f"Invalid URL format: {m3u_url}")
        if not await handler.update_provider(alias, m3u_url, max_streams):
            raise ValueError(f"Failed to update provider '{alias}'.")
        Log.info(Label.SERVER, f"Provider '{alias}' updated with max streams {max_streams}.")
        await flash(f"Provider '{alias}' updated successfully, use 'Reload Providers & Sources' on the dashboard to re-discover sources.", "success")
        updated_provider_data = ProviderStatus({**provider, "m3u_url": m3u_url, "max_streams": max_streams})
        response = Response(await render_template("_provider_row.html", provider=updated_provider_data))
    except ValueError as e:
        msg = f"Failed to update provider '{alias}': {e}"
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        response = Response(await render_template("_provider_edit_form.html", provider={**provider, "m3u_url": m3u_url, "max_streams": max_streams_str}))
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/providers/delete/<string:alias>", methods=["DELETE"])
async def ui_provider_delete(alias: ProviderAlias) -> Response:
    try:
        if not await handler.delete_provider(alias):
            raise ValueError(f"Failed to delete provider '{alias}'.")
        Log.info(Label.SERVER, f"Provider '{alias}' deleted successfully.")
        await flash(f"Provider '{alias}' deleted successfully, use 'Reload Providers & Sources' on the dashboard to re-discover sources.", "success")
        response = Response("", 200)
    except ValueError as e:
        msg = f"Failed to delete provider '{alias}': {e}"
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        response = Response("", 400)
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    response.headers["HX-Refresh"] = "true"
    return response


@app.route("/ui/sources")
async def ui_sources_list() -> str:
    per_page = request.args.get('per_page', NUM_SOURCES_PER_PAGE, type=int)
    page = request.args.get('page', 1, type=int)
    sources_unfiltered = await handler.get_discovered_sources_for_ui()
    providers = sorted(list(set(s['provider_alias'] for s in sources_unfiltered)))
    filter_provider = request.args.get('provider_alias', '')
    filter_name = request.args.get('name_filter', '').lower()
    sources_filtered = [s for s in sources_unfiltered if (not filter_provider or s['provider_alias'] == filter_provider) and (not filter_name or filter_name in s["tvg_name"].lower() or filter_name in s["display_title"].lower())]
    total_items = len(sources_filtered)
    total_pages = math.ceil(total_items / per_page)
    sources_for_page = sources_filtered[(page - 1) * per_page:page * per_page]
    return await render_template("ui_sources.html", sources=sources_for_page, providers=providers, current_provider=filter_provider, current_name_filter=filter_name, current_page=page, total_pages=total_pages, total_items=total_items, per_page=per_page)


@app.route("/ui/logical-channels")
async def ui_logical_channels_list() -> str:
    """Renders the list of all configured logical channels."""
    channels = await handler.get_logical_channels_for_ui()
    all_quality_scores = await quality_monitor.get_quality_scores()

    all_channel_metrics: dict[LogicalChannelId, LogicalChannelMetrics] = {}
    for channel in channels:
        mapped_sources = await handler.get_channel_mappings_for_ui(channel['logical_channel_id'])
        all_channel_metrics[channel["logical_channel_id"]] = await calculate_channel_metrics(mapped_sources, all_quality_scores)

    return await render_template("ui_logical_channels.html", channels=channels, all_channel_metrics=all_channel_metrics)


@app.route("/ui/logical-channels/delete/<string:logical_channel_id>", methods=["POST"])
async def ui_logical_channel_delete(logical_channel_id: LogicalChannelId) -> Response | WerkzeugResponse:
    channel = await handler.get_logical_channel_by_id(logical_channel_id)
    if channel:
        stream_name = create_stream_name(channel['logical_channel_title'], channel['channel_num'])
        if await handler.delete_logical_channel(logical_channel_id):
            msg = f"{stream_name}: Deleted successfully."
            Log.info(Label.SERVER, msg)
            await flash(msg, "success")
            await handler.reload_handler_config()
        else:
            msg = f"{stream_name}: Failed to delete."
            Log.error(Label.SERVER, msg)
            await flash(msg, "error")
    else:
        msg = f"Logical Channel with ID '{logical_channel_id}' not found for deletion."
        Log.error(Label.SERVER, msg)
        await flash(msg, "warning")
    return redirect(url_for('ui_logical_channels_list'))


@app.route("/ui/logical-channels/form/", methods=["GET", "POST"])
@app.route("/ui/logical-channels/form/<string:logical_channel_id>", methods=["GET", "POST"])
async def ui_logical_channel_form(logical_channel_id: LogicalChannelId | None = None) -> Response | WerkzeugResponse | str:
    """Handles adding/editing a logical channel and its mappings."""
    if request.method == "POST":
        form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore

        logical_channel_title: LogicalChannelTitle = LogicalChannelTitle(form_data.get("logical_channel_title", "").strip())
        channel_num: ChannelNum = ChannelNum(form_data.get("channel_num", "").strip())
        group_title: TVGGroupTitle = TVGGroupTitle(form_data.get("group_title", "Uncategorized").strip())
        tvg_id: TVGId = TVGId(form_data.get("tvg_id", "").strip())
        tvg_logo: TVGLogo = TVGLogo(form_data.get("tvg_logo", "").strip())

        if not logical_channel_title or not channel_num:
            msg = "Display Name and Channel Number are required."
            Log.error(Label.SERVER, msg)
            await flash(msg, "error")
            return redirect(request.url)
        try:
            int(channel_num)
        except ValueError:
            msg = f"Channel Number must be a valid integer, received: {channel_num}"
            Log.error(Label.SERVER, msg)
            await flash(msg, "error")
            return redirect(request.url)
        submitted_id = form_data.get("logical_channel_id", "")
        logical_channel_id = LogicalChannelId(submitted_id) if submitted_id else None

        source_ids: list[SourceId] = [SourceId(source_id) for source_id in form_data.getlist('mapping_source_id')]
        prev_mappings = await handler.get_mappings_for_logical_channel(logical_channel_id) if logical_channel_id else None
        mappings_to_save: list[SourceMappingInfoWithId] = []
        for source_id in source_ids:
            try:
                mappings_to_save.append(SourceMappingInfoWithId({
                    'source_id': source_id,
                    'priority': Priority(int(form_data.get(f"priority_{source_id}", DEFAULT_PRIORITY))),
                    'offset': prev_mappings.get(source_id, {}).get('offset') if prev_mappings else None,
                }))
            except (ValueError, TypeError):
                msg = f"Skipping mapping with invalid priority for source '{source_id}'."
                Log.warn(Label.SERVER, msg)
                await flash(msg, "warning")
        sort_sources(mappings_to_save, await quality_monitor.get_quality_scores(), reverse=False)

        if not tvg_logo:
            for mapping in mappings_to_save:
                discovered_source = await handler.get_discovered_source(mapping['source_id'])
                if discovered_source and discovered_source['tvg_logo']:
                    tvg_logo = discovered_source['tvg_logo']
                    break

        stream_name = create_stream_name(logical_channel_title, channel_num)
        if logical_channel_id:
            lc_data: LogicalChannelInfo = {
                "logical_channel_title": logical_channel_title,
                "channel_num": channel_num,
                "group_title": group_title,
                "tvg_id": tvg_id,
                "tvg_logo": tvg_logo,
            }
            if not await handler.update_logical_channel(logical_channel_id, lc_data):
                await flash(f"{stream_name}: Failed to update.", "error")
                return redirect(request.url)
            if await handler.update_mappings_for_logical_channel(logical_channel_id, mappings_to_save):
                msg = f"{stream_name}: Updated with {len(mappings_to_save)} mappings."
                Log.info(Label.SERVER, msg)
                await flash(msg, "success")
            else:
                msg = f"{stream_name}: Updated info but failed to update {len(mappings_to_save)} mappings."
                Log.warn(Label.SERVER, msg)
                await flash(msg, "warning")
            await handler.reload_handler_config()
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=logical_channel_id))
        else:
            lc_data: LogicalChannelInfo = {
                "logical_channel_title": logical_channel_title,
                "channel_num": channel_num,
                "group_title": group_title,
                "tvg_id": tvg_id,
                "tvg_logo": tvg_logo,
            }
            new_id = await handler.add_logical_channel(lc_data)
            if not new_id:
                await flash(f"{stream_name}: Failed to create channel.", "error")
                return redirect(request.url)
            if mappings_to_save:
                if await handler.update_mappings_for_logical_channel(new_id, mappings_to_save):
                    msg = f"{stream_name}: Created with {len(mappings_to_save)} mappings."
                    Log.info(Label.SERVER, msg)
                    await flash(msg, "success")
                else:
                    msg = f"{stream_name}: Created but failed to add {len(mappings_to_save)} mappings."
                    Log.warn(Label.SERVER, msg)
                    await flash(msg, "warning")
            else:
                msg = f"{stream_name}: Created with no mappings."
                Log.info(Label.SERVER, msg)
                await flash(msg, "success")
            await handler.reload_handler_config()
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=new_id))

    # --- GET Request Handling ---

    is_htmx_source_list_request = (request.headers.get('HX-Request') and request.headers.get('HX-Target') == 'source-list-container')
    channel: LogicalChannelInfoWithId | None = None
    if logical_channel_id:
        channel = await handler.get_logical_channel_with_id(logical_channel_id)
        if not channel:
            msg = f"Logical Channel with ID '{logical_channel_id}' not found."
            Log.error(Label.SERVER, msg)
            await flash(msg, "error")
            return redirect(url_for('ui_logical_channels_list'))
    
    search_query = request.args.get('search_query')
    if channel and search_query is None and not is_htmx_source_list_request:
        predefined_channel = handler.find_matching_predefined_channel(channel['logical_channel_title'], channel['channel_num'])
        if predefined_channel:
            search_query = MULTI_SEARCH_QUERY_DELIMITER.join(predefined_channel['aliases']) if predefined_channel["aliases"] else predefined_channel["title"]
    filter_query = search_query.strip().lower() if search_query else None
    logical_channel_form_details = await calculate_logical_channel_form_details(
        logical_channel_id=logical_channel_id,
        search_query=search_query,
        filter_query=filter_query,
        current_page=request.args.get('page', 1, type=int),
    )

    template_to_render = "_source_list_content.html" if is_htmx_source_list_request else "ui_logical_channel_form.html"
    return await render_template(
        template_to_render,
        channel=channel,
        channel_metrics=logical_channel_form_details["channel_metrics"],
        all_source_metrics=logical_channel_form_details["all_source_metrics"],
        unmapped_sources_for_page=logical_channel_form_details["unmapped_sources_for_page"],
        mapped_sources=logical_channel_form_details["mapped_sources"],
        sources_mapped_elsewhere=await handler.get_sources_mapped_elsewhere(logical_channel_id),
        current_page=logical_channel_form_details["current_page"],
        total_pages=logical_channel_form_details["total_pages"],
        total_unmapped_sources=logical_channel_form_details["total_unmapped_sources"],
        search_query=search_query,
        filter_query=filter_query,
    )


@app.route("/ui/playlists")
async def ui_playlists_list() -> str:
    """Renders the list of all playlists with their client URLs."""
    playlists = await handler.get_playlists_for_ui()
    channel_counts = await handler.get_playlist_channel_counts()
    playlist_urls = {p["playlist_id"]: get_playlist_urls(p["playlist_id"], p) for p in playlists}
    return await render_template("ui_playlists.html", playlists=playlists, channel_counts=channel_counts, playlist_urls=playlist_urls,
                                 main_playlist_id=MAIN_PLAYLIST_ID, detected_base_url=get_request_base_url())


@app.route("/ui/playlists/delete/<string:playlist_id>", methods=["POST"])
async def ui_playlist_delete(playlist_id: PlaylistId) -> Response | WerkzeugResponse:
    playlist = await handler.get_playlist(playlist_id)
    try:
        if not await handler.delete_playlist(playlist_id):
            raise RuntimeError("failed to save configuration")
        msg = f"Playlist '{playlist['name'] if playlist else playlist_id}': Deleted successfully."
        Log.info(Label.SERVER, msg)
        await flash(msg, "success")
        await handler.reload_handler_config()
    except ValueError as e:
        Log.warn(Label.SERVER, f"Cannot delete playlist '{playlist_id}': {e}")
        await flash(str(e), "warning")
    except RuntimeError as e:
        msg = f"Playlist '{playlist_id}': Failed to delete - {e}."
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
    return redirect(url_for('ui_playlists_list'))


@app.route("/ui/playlists/form/", methods=["GET", "POST"])
@app.route("/ui/playlists/form/<string:playlist_id>", methods=["GET", "POST"])
async def ui_playlist_form(playlist_id: PlaylistId | None = None) -> Response | WerkzeugResponse | str:
    """Handles adding/editing a playlist."""
    if request.method == "POST":
        form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
        name = PlaylistName(form_data.get("name", "").strip())
        lc_group_mode = form_data.get("lc_group_mode", LCGroupMode.INCLUDE_ALL_EXCEPT)
        base_url = BaseURL(form_data.get("base_url", "").strip().rstrip("/"))
        lc_groups = tuple(dict.fromkeys(normalize_group_key(group) for group in form_data.getlist("lc_groups")))
        source_groups = tuple(dict.fromkeys(normalize_group_key(group) for group in form_data.getlist("source_groups")))

        error = None
        if not name:
            error = "Playlist name is required."
        elif lc_group_mode not in tuple(LCGroupMode):
            error = f"Invalid logical channel group mode '{lc_group_mode}'."
        elif base_url and not is_valid_url(base_url):
            error = f"Invalid base URL '{base_url}', expected e.g. 'http://100.64.0.1:{NEXUS_TUNER_PORT}' or leave empty to detect automatically."
        if error:
            Log.error(Label.SERVER, f"Playlist form error: {error}")
            await flash(error, "error")
            return redirect(request.url)

        new_playlist: PlaylistInfo = {
            "name": name,
            "lc_group_mode": LCGroupMode(lc_group_mode),
            "lc_groups": lc_groups,
            "source_groups": source_groups,
            "base_url": base_url,
        }
        if playlist_id:
            try:
                saved = await handler.update_playlist(playlist_id, new_playlist)
            except ValueError as e:
                await flash(str(e), "warning")
                return redirect(url_for('ui_playlists_list'))
            if not saved:
                await flash(f"Playlist '{name}': Failed to save.", "error")
                return redirect(request.url)
            msg = f"Playlist '{name}': Updated successfully."
        else:
            new_playlist_id = await handler.add_playlist(new_playlist)
            if not new_playlist_id:
                await flash(f"Playlist '{name}': Failed to create.", "error")
                return redirect(request.url)
            playlist_id = new_playlist_id
            msg = f"Playlist '{name}': Created successfully."
        Log.info(Label.SERVER, msg)
        await flash(msg, "success")
        await handler.reload_handler_config()
        return redirect(url_for('ui_playlist_form', playlist_id=playlist_id))

    playlist: PlaylistInfo | None = None
    if playlist_id:
        playlist = await handler.get_playlist(playlist_id)
        if not playlist:
            await flash(f"Playlist '{playlist_id}' not found.", "warning")
            return redirect(url_for('ui_playlists_list'))
    lc_groups = await handler.get_lc_groups_for_ui()
    source_groups = await handler.get_source_groups_for_ui()
    selected_lc_groups: set[GroupKey] = set(playlist["lc_groups"]) if playlist else set()
    selected_source_groups: set[GroupKey] = set(playlist["source_groups"]) if playlist else set()
    stale_lc_groups = sorted(selected_lc_groups - {group["key"] for group in lc_groups})
    stale_source_groups = sorted(selected_source_groups - {group["key"] for group in source_groups})
    # Selected groups are shown first so they are easy to review
    source_groups.sort(key=lambda group: group["key"] not in selected_source_groups)
    channel_counts = (await handler.get_playlist_channel_counts()).get(playlist_id) if playlist_id else None
    return await render_template("ui_playlist_form.html",
                                 playlist=playlist, playlist_id=playlist_id, main_playlist_id=MAIN_PLAYLIST_ID,
                                 lc_groups=lc_groups, source_groups=source_groups,
                                 selected_lc_groups=selected_lc_groups, selected_source_groups=selected_source_groups,
                                 stale_lc_groups=stale_lc_groups, stale_source_groups=stale_source_groups,
                                 playlist_urls=get_playlist_urls(playlist_id, playlist) if playlist_id and playlist else None,
                                 channel_counts=channel_counts, detected_base_url=get_request_base_url(),
                                 lc_group_modes=LCGroupMode)


# --- Settings ---


async def apply_settings_side_effects(changed: set[str]) -> None:
    """Applies changed settings that are not read each time they are used."""
    if "stream_engine" in changed:
        await handler.reload_handler_config()  # Playlist entries contain the stream engine
    if changed & {"media_servers", "ghost_check_interval"}:
        ghost_monitor.notify_settings_changed()


async def render_media_servers_table_body() -> str:
    return await render_template("_media_servers_table_body.html", media_servers=config.media_servers, server_status=ghost_monitor.server_status)


def parse_media_server_form(form_data: ImmutableMultiDict[str, str], server_id: MediaServerId) -> MediaServerInfo:
    """Builds a media server from the form, keeping the saved API key when the field is left empty."""
    api_key = form_data.get("api_key", "").strip()
    if not api_key and (existing := config.get_media_server(server_id)):
        api_key = existing["api_key"]
    return {
        "id": server_id,
        "name": form_data.get("name", "").strip(),
        "type": cast(MediaServerType, form_data.get("type", "")),
        "url": form_data.get("url", "").strip(),
        "api_key": api_key,
        "enabled": "enabled" in form_data,
    }


@app.route("/ui/settings", methods=["GET", "POST"])
async def ui_settings() -> Response | WerkzeugResponse | str:
    """Renders and saves the application settings."""
    form_settings: dict[str, Any] | None = None
    if request.method == "POST":
        form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
        form_settings = {
            "nexus_url": form_data.get("nexus_url", ""),
            "ffmpeg_path": form_data.get("ffmpeg_path", ""),
            "vlc_path": form_data.get("vlc_path", ""),
            "stream_engine": form_data.get("stream_engine", ""),
            "tls_verify": "tls_verify" in form_data,
            "remove_captions_and_subtitles": "remove_captions_and_subtitles" in form_data,
        }
        for key in SETTINGS_INT_FIELDS:
            form_settings[key] = form_data.get(key, "").strip()
        try:
            changes = dict(form_settings)
            for key, (display_name, _) in SETTINGS_INT_FIELDS.items():
                try:
                    changes[key] = int(form_settings[key])
                except ValueError:
                    raise ValueError(f"{display_name}: expected a whole number, got '{form_settings[key]}'.")
            changed = await config.update_settings(changes)
        except (ValueError, RuntimeError) as e:
            Log.error(Label.SERVER, f"Settings form error: {e}")
            await flash(f"Settings were not saved. {e}", "error")
        else:
            await apply_settings_side_effects(changed)
            await flash("Settings saved." if changed else "No settings were changed.", "success")
            return redirect(url_for('ui_settings'))

    return await render_template("ui_settings.html",
                                 settings={**config.settings, **(form_settings or {})},
                                 media_servers=config.media_servers, server_status=ghost_monitor.server_status,
                                 engine_problems={engine: config.get_engine_problem(engine) for engine in StreamEngine},
                                 stream_engines=StreamEngine, nexus_port=NEXUS_TUNER_PORT, config_dir=CONFIG_DIR,
                                 settings_path=config.settings_path, detected_base_url=get_request_base_url())


@app.route("/ui/settings/media-servers/add", methods=["GET", "POST"])
async def ui_settings_media_server_add() -> Response | str:
    if request.method == "GET":
        return await render_template("_media_server_add_form.html", server=None, media_server_types=MediaServerType)

    form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
    server = parse_media_server_form(form_data, create_media_server_id())
    try:
        await config.add_media_server(server)
    except (ValueError, RuntimeError) as e:
        Log.error(Label.SERVER, f"Failed to add media server '{server['name']}': {e}")
        await flash(f"Media server was not added. {e}", "error")
        response = Response(await render_template("_media_server_add_form.html", server=server, media_server_types=MediaServerType))
        response.headers["HX-Retarget"] = "#add-media-server-form-wrapper"
        response.headers["HX-Reswap"] = "outerHTML"
    else:
        await apply_settings_side_effects({"media_servers"})
        await flash(f"Media server '{server['name']}' added.", "success")
        form_removal_html = '<div id="add-media-server-form-wrapper" hx-swap-oob="true"></div>'
        response = Response(await render_media_servers_table_body() + form_removal_html)
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/settings/media-servers/edit/<string:server_id>", methods=["GET", "PUT"])
async def ui_settings_media_server_edit(server_id: MediaServerId) -> Response | str:
    existing = config.get_media_server(server_id)
    if not existing:
        return await render_media_servers_table_body()

    if request.method == "GET":
        if request.args.get('cancel') == 'true':
            return await render_template("_media_server_row.html", server=existing, status=ghost_monitor.server_status.get(server_id))
        return await render_template("_media_server_edit_form.html", server=existing, media_server_types=MediaServerType)

    form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
    server = parse_media_server_form(form_data, server_id)
    try:
        await config.update_media_server(server)
    except (ValueError, RuntimeError) as e:
        Log.error(Label.SERVER, f"Failed to update media server '{existing['name']}': {e}")
        await flash(f"Media server was not saved. {e}", "error")
        response = Response(await render_template("_media_server_edit_form.html", server=server, media_server_types=MediaServerType))
    else:
        await apply_settings_side_effects({"media_servers"})
        await flash(f"Media server '{server['name']}' saved.", "success")
        updated = config.get_media_server(server_id) or server
        response = Response(await render_template("_media_server_row.html", server=updated, status=ghost_monitor.server_status.get(server_id)))
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/settings/media-servers/delete/<string:server_id>", methods=["DELETE"])
async def ui_settings_media_server_delete(server_id: MediaServerId) -> Response:
    existing = config.get_media_server(server_id)
    try:
        await config.delete_media_server(server_id)
    except (ValueError, RuntimeError) as e:
        Log.error(Label.SERVER, f"Failed to delete media server '{server_id}': {e}")
        await flash(f"Media server was not deleted. {e}", "error")
    else:
        await apply_settings_side_effects({"media_servers"})
        await flash(f"Media server '{existing['name'] if existing else server_id}' deleted.", "success")
    response = Response(await render_media_servers_table_body())
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/settings/media-servers/test", methods=["POST"])
async def ui_settings_media_server_test() -> str:
    """Tests the connection to a saved media server, or to the values entered in its form."""
    form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
    server_id = MediaServerId(form_data.get("server_id", ""))
    saved_server = config.get_media_server(server_id) if server_id else None
    if saved_server and "url" not in form_data:
        server = saved_server
    else:
        server = parse_media_server_form(form_data, server_id or create_media_server_id())
    try:
        server = validate_media_server({**server, "name": server["name"] or "Test", "enabled": True})
    except ValueError as e:
        ok, message = False, str(e)
    else:
        ok, message = await test_media_server_connection(server)
    Log.info(Label.SERVER, f"Media server connection test for '{server['name']}' at {server['url']}: {'OK' if ok else 'failed'} - {message}")
    return await render_template("_media_server_test_result.html", ok=ok, message=message)


@app.route("/ui/channels/populate-from-suggestion")
async def ui_channel_populate_from_suggestion() -> str:
    """
    Called when a user clicks a channel suggestion.
    Returns multiple OOB fragments to:
    1. Populate the channel details form.
    2. Populate the source mapping card with pre-filtered results.
    3. Clear the suggestion dropdown.
    """
    prefilled_data: dict[str, str] = {
        'logical_channel_id': request.args.get('logical_channel_id', ''),
        'logical_channel_title': request.args.get('title', ''),
        'channel_num': request.args.get('num', ''),
        'group_title': request.args.get('group', 'Uncategorized'),
    }
    form_html = await render_template("_logical_channel_form_fields.html", channel=prefilled_data)
    logical_channel_id = LogicalChannelId(prefilled_data['logical_channel_id']) or None

    search_query = prefilled_data['logical_channel_title']
    for group, channel_list in handler.copy_channel_list_data().items():
        for pre_channel in channel_list:
            if prefilled_data['logical_channel_title'] != pre_channel["title"]:
                continue
            if prefilled_data['channel_num'] != pre_channel["num"]:
                continue
            if prefilled_data['group_title'] != group:
                continue
            search_query = MULTI_SEARCH_QUERY_DELIMITER.join(pre_channel["aliases"])
            break
    filter_query = prefilled_data['logical_channel_title'].strip().lower()
    logical_channel_form_details = await calculate_logical_channel_form_details(
        logical_channel_id=logical_channel_id,
        search_query=search_query,
        filter_query=filter_query,
        current_page=1,
    )

    search_card_html = await render_template(
        "_source_mapping_card.html",
        channel=(await handler.get_logical_channel_with_id(logical_channel_id) or {}) if logical_channel_id else {},
        channel_metrics=logical_channel_form_details["channel_metrics"],
        all_source_metrics=logical_channel_form_details["all_source_metrics"],
        unmapped_sources_for_page=logical_channel_form_details["unmapped_sources_for_page"],
        mapped_sources=logical_channel_form_details["mapped_sources"],
        sources_mapped_elsewhere=await handler.get_sources_mapped_elsewhere(logical_channel_id),
        current_page=logical_channel_form_details["current_page"],
        total_pages=logical_channel_form_details["total_pages"],
        total_unmapped_sources=logical_channel_form_details["total_unmapped_sources"],
        search_query=search_query,
        filter_query=filter_query,
    )
    oob_search_card = f'<div id="source-mapping-section" hx-swap-oob="innerHTML">{search_card_html}</div>'
    clear_title_suggestions_html = '<div id="channel-title-suggestion-box" hx-swap-oob="true"></div>'
    clear_num_suggestions_html = '<div id="channel-num-suggestion-box" hx-swap-oob="true"></div>'

    return form_html + oob_search_card + clear_title_suggestions_html + clear_num_suggestions_html


@app.route("/ui/channels/suggest", methods=["GET"])
async def ui_channel_suggest() -> str:
    if len(query := request.args.get('logical_channel_title', '')):
        suggestions = handler.search_predefined_channel_names(query)
    elif len(query := request.args.get('channel_num', '')) > 0:
        suggestions = handler.search_predefined_channel_nums(query)
    else:
        return ""
    return await render_template("_channel_suggestions.html", logical_channel_id=request.args.get('logical_channel_id'), suggestions=suggestions)


@app.route("/ui/logical-channels/analyze-mappings/<string:logical_channel_id>", methods=["POST"])
async def ui_analyze_mappings(logical_channel_id: LogicalChannelId) -> Response:
    """Analyzes the mappings for a logical channel."""
    channel = await handler.get_logical_channel_by_id(logical_channel_id)
    if not channel:
        msg = f"Logical Channel with ID '{logical_channel_id}' not found for analysis."
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        response = Response("", 404)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response
    stream_name = create_stream_name(channel['logical_channel_title'], channel['channel_num'])
    sources = await handler.get_channel_mappings_for_ui(logical_channel_id)
    if not sources:
        msg = f"{stream_name}: No mappings found."
        Log.info(Label.SERVER, msg)
        await flash(msg, "info")
        response = Response("", 204)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response

    res = await quality_monitor.analyze_mapped_sources(logical_channel_id)
    if res == 1:
        msg = f"{stream_name}: Quality analysis completed for {len(sources)} mapping(s) but failed to measure relative offset."
        Log.warn(Label.SERVER, msg)
        await flash(msg, "warning")
        response = Response("", 202)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response
    elif res == 0:
        msg = f"{stream_name}: Failed to analyze mappings."
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        response = Response("", 500)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response
    elif isinstance(res, str):
        msg = f"{stream_name}: Quality analysis already in progress for {res}."
        Log.warn(Label.SERVER, msg)
        await flash(msg, "warning")
        response = Response("", 409)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response

    await flash(f"{stream_name}: Quality analysis completed for {len(sources)} mapping(s)", "success")
    response = Response("", 200)
    response.headers["HX-Refresh"] = "true"
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/logical-channels/remove-dead-mappings/<string:logical_channel_id>", methods=["DELETE"])
async def ui_remove_dead_mappings(logical_channel_id: LogicalChannelId) -> Response:
    """Removes dead mappings from logical channels."""
    channel = await handler.get_logical_channel_by_id(logical_channel_id)
    if not channel:
        msg = f"Logical Channel with ID '{logical_channel_id}' not found for dead mapping removal."
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        response = Response("", 404)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response
    stream_name = create_stream_name(channel['logical_channel_title'], channel['channel_num'])
    discovered_mappings: list[SourceMappingInfoWithId] = []
    removed_count = 0
    for source in await handler.get_channel_mappings_for_ui(logical_channel_id):
        if await handler.get_discovered_source(source['source_id']):
            discovered_mappings.append(source)
        else:
            if not await quality_monitor.remove_source(source['source_id']):
                msg = f"{stream_name}: Failed to remove dead source {source['source_id']} from quality monitor."
                Log.error(Label.SERVER, msg)
                await flash(msg, "error")
                response = Response("", 500)
                response.headers["HX-Refresh"] = "true"
                response.headers["HX-Trigger"] = "flashMessagesUpdated"
                return response
            removed_count += 1
    if not await handler.update_mappings_for_logical_channel(logical_channel_id, discovered_mappings):
        msg = f"{stream_name}: Failed to update mappings after removing dead sources."
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")
        response = Response("", 500)
        response.headers["HX-Refresh"] = "true"
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response

    if removed_count > 0:
        msg = f"{stream_name}: Removed {removed_count} dead mapping(s)."
        Log.info(Label.SERVER, msg)
        await flash(msg, "success")
    else:
        msg = f"{stream_name}: No dead mappings found to remove."
        Log.info(Label.SERVER, msg)
        await flash(msg, "info")

    response = Response("", 200)
    response.headers["HX-Refresh"] = "true"
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/logs/modal/<int:num_lines>")
async def ui_logs_modal(num_lines: int) -> str:
    prev_date = datetime.now() - timedelta(days=1)
    if num_lines < 1:
        num_lines = 1
    log_lines: list[str] = []
    verbose_log_path = config.logs_dir / Log.LOG_FILE_NAME_VERBOSE
    try:
        async with aiofiles.open(verbose_log_path, 'r') as f:
            log_lines = list(deque(await f.readlines(), maxlen=num_lines))
        while len(log_lines) < num_lines:
            prev_verbose_log_path = verbose_log_path.with_name(f"{Log.LOG_FILE_NAME_VERBOSE}.{prev_date.strftime('%Y-%m-%d')}")
            if not prev_verbose_log_path.exists():
                break
            async with aiofiles.open(prev_verbose_log_path, 'r') as f:
                log_lines = list(deque(await f.readlines() + log_lines, maxlen=num_lines))
            if len(log_lines) >= num_lines:
                break
            prev_date -= timedelta(days=1)
    except FileNotFoundError as e:
        log_lines = [f"Error: Log file not found in '{config.logs_dir}' - {e}"]
    except Exception as e:
        log_lines = [f"An error occurred while reading the log file: {e}"]
    return await render_template("_logs_modal_content.html", log_lines=log_lines, num_lines=num_lines)


# --- HDHomeRun Emulation Endpoints ---


async def hdhomerun_discover_for(playlist_id: PlaylistId) -> Response:
    """Emulates the HDHomeRun device discovery API for a playlist."""
    playlist = await handler.get_playlist(playlist_id)
    if playlist is None:
        abort(404, f"Playlist '{playlist_id}' not found.")
    base_url = get_request_base_url(playlist)
    if playlist_id == MAIN_PLAYLIST_ID:
        friendly_name = "NexusTuner"
        device_base_url = base_url
    else:
        friendly_name = f"NexusTuner - {playlist['name']}"
        device_base_url = f"{base_url}/playlists/{playlist_id}"
    response_dict: dict[str, str | int] = {
        "FriendlyName": friendly_name,
        "DeviceAuth": "nexus-tuner",
        "ModelNumber": NEXUS_TUNER_VERSION,
        "FirmwareName": f"nexus-tuner_{NEXUS_TUNER_VERSION}",
        "FirmwareVersion": NEXUS_TUNER_VERSION,
        "DeviceID": playlist_device_id(playlist_id),
        "Manufacturer": "nexus-tuner",
        "BaseURL": device_base_url,
        "LineupURL": f"{device_base_url}/lineup.json",
        "TunerCount": sum(p["max_streams"] for p in (await handler.get_provider_stream_status()).values())
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


async def hdhomerun_lineup_status_for(playlist_id: PlaylistId) -> Response:
    """Returns the status of the lineup for a playlist."""
    if await handler.get_playlist(playlist_id) is None:
        abort(404, f"Playlist '{playlist_id}' not found.")
    response_dict: dict[str, int | str | list[str]] = {
        "ScanInProgress": 0,
        "ScanPossible": 0,
        "Source": "Cable",
        "SourceList": ["Cable"]
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


async def hdhomerun_lineup_for(playlist_id: PlaylistId) -> Response:
    """Returns the channel lineup of a playlist in HDHomeRun format, only logical channels with a channel number are included."""
    playlist = await handler.get_playlist(playlist_id)
    lineup_entries = await handler.get_playlist_lineup(playlist_id)
    if playlist is None or lineup_entries is None:
        abort(404, f"Playlist '{playlist_id}' not found.")
    base_url = get_request_base_url(playlist)
    lineup: list[dict[str, str | int]] = []
    quality_scores = await quality_monitor.get_quality_scores()
    for entry in lineup_entries:
        is_hd = 1 if any(quality_scores.get(source_id, {}).get('height', 0) >= 720 for source_id in entry["source_ids"]) else 0
        lineup.append({
            "GuideNumber": entry["channel_num"],
            "GuideName": entry["title"],
            "HD": is_hd,
            "URL": f"{base_url}/{VideoType.MPEGTS}/{entry['channel_id']}"
        })
    return Response(json.dumps(lineup), mimetype="application/json")


@app.route('/discover.json')
async def hdhomerun_discover() -> Response:
    """Emulates HDHomeRun device discovery API endpoint."""
    return await hdhomerun_discover_for(MAIN_PLAYLIST_ID)


@app.route('/lineup_status.json')
async def hdhomerun_lineup_status() -> Response:
    """Returns the status of the lineup."""
    return await hdhomerun_lineup_status_for(MAIN_PLAYLIST_ID)


@app.route('/lineup.json')
async def hdhomerun_lineup() -> Response:
    """Returns the channel lineup in HDHomeRun format."""
    return await hdhomerun_lineup_for(MAIN_PLAYLIST_ID)


@app.route('/playlists/<string:playlist_id>/discover.json')
async def hdhomerun_playlist_discover(playlist_id: PlaylistId) -> Response:
    """Emulates HDHomeRun device discovery API endpoint for a specific playlist."""
    return await hdhomerun_discover_for(playlist_id)


@app.route('/playlists/<string:playlist_id>/lineup_status.json')
async def hdhomerun_playlist_lineup_status(playlist_id: PlaylistId) -> Response:
    """Returns the status of the lineup for a specific playlist."""
    return await hdhomerun_lineup_status_for(playlist_id)


@app.route('/playlists/<string:playlist_id>/lineup.json')
async def hdhomerun_playlist_lineup(playlist_id: PlaylistId) -> Response:
    """Returns the channel lineup in HDHomeRun format for a specific playlist."""
    return await hdhomerun_lineup_for(playlist_id)


# --- PWA Endpoints ---


@app.route('/manifest.json')
async def serve_manifest() -> Response:
    """Serves the web app manifest file."""
    return await send_from_directory("public", "manifest.json", mimetype="application/json")


@app.route('/sw.js')
async def serve_service_worker() -> Response:
    """Serves the service worker JavaScript file."""
    return await send_from_directory("public", "sw.js", mimetype="application/javascript")


@app.route('/icon-<int:size>.png')
async def serve_icon(size: int) -> Response:
    """Serves the app icon in various sizes."""
    return await send_from_directory("public", f"icon-{size}.png", mimetype="image/png")


@app.route('/favicon.ico')
async def serve_favicon() -> Response:
    """Serves the favicon.ico file."""
    return await send_from_directory("public", "favicon.ico", mimetype="image/x-icon")


@app.route('/screenshots/<path:filename>')
async def serve_screenshot(filename: str) -> Response:
    """Serves screenshots from the public directory."""
    return await send_from_directory("public/screenshots", filename, mimetype="image/png")


# --- Miscellaneous Endpoints ---


@app.route("/reload", methods=["POST"])
async def reload_configuration() -> Response:
    """Triggers a full reload of all configurations and channel data."""
    form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
    update_providers = form_data.get("update_providers", "false").lower() == "true"
    force_discover_sources = form_data.get("force_discover_sources", "false").lower() == "true"

    Log.info(Label.SERVER, f"Received request to reload configuration via UI with params={{update_providers={update_providers}, force_discover_sources={force_discover_sources}}}")
    try:
        if (changed_settings := await config.reload_settings()) is None:
            await flash(f"Could not reload {config.settings_name}, kept the current settings. See the logs for details.", "warning")
        elif changed_settings & {"media_servers", "ghost_check_interval"}:
            ghost_monitor.notify_settings_changed()
        await handler.reload_handler_config(update_providers=update_providers, force_discover_sources=force_discover_sources)
        await quality_monitor.reload_quality_scores()
        if force_discover_sources:
            await flash("Successfully reloaded configuration and refreshed discovered sources!", "success")
        else:
            await flash("Successfully reloaded configuration!", "success")
    except Exception as e:
        msg = f"An error occurred during manual reload: {e}"
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")

    response = Response("")
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    response.headers["HX-Refresh"] = "true"
    return response


@app.route("/backup", methods=["POST"])
async def backup_configuration() -> Response:
    """Triggers an backup of the current configuration files."""
    try:
        backup_path = await config.backup_config(scheduled=False)
        if backup_path:
            await flash(f"Backup created successfully at {backup_path}", "success")
        else:
            await flash("Failed to create backup.", "error")
    except Exception as e:
        msg = f"An error occurred during backup: {e}"
        Log.error(Label.SERVER, msg)
        await flash(msg, "error")

    response = Response("")
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route('/ping')
async def ping() -> Response:
    """Simple endpoint to check if the server is running."""
    return Response(status=200)


@app.route('/robots.txt')
async def serve_robots_txt() -> Response:
    """Serves the robots.txt file."""
    return await send_from_directory("public", "robots.txt", mimetype="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=NEXUS_TUNER_PORT, use_reloader=False)
