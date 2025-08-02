"""
The main Quart application file for NexusStream.

This file initializes the Quart app and its async components (Config, ChannelHandler, 
StreamManager, GhostSessionMonitor) and defines all the web routes for:
- Serving the main M3U playlist.
- Handling HLS and MPEG-TS streaming requests asynchronously.
- HDHomeRun emulation endpoints.
- Providing a web-based user interface (UI) for configuration and management.
- A manual reload endpoint.
"""

import asyncio
import aiofiles.os
import json
import math
import os
import sys
from collections import deque
from datetime import UTC, datetime
from typing import AsyncGenerator, Dict, Final, cast

from quart import Quart, Response, abort, flash, redirect, render_template, request, url_for
from quart import send_from_directory  # type: ignore
from werkzeug.wrappers import Response as WerkzeugResponse
from werkzeug.datastructures import ImmutableMultiDict

from nexus_stream.config import Config
from nexus_stream.create_stream import CreateStream
from nexus_stream.handler import ChannelHandler
from nexus_stream.mpegts import MPEGTSStream
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.session_monitor import GhostSessionMonitor
from nexus_stream.stream import StreamManager
from nexus_stream.scheduler import Scheduler
from nexus_stream.utils import (CREATE_STREAM_DEADLINE, CREATE_STREAM_POLL_INTERVAL,
                                DEFAULT_PRIORITY, M3UURL, NEXUS_STREAM_PORT, NEXUS_STREAM_VERSION, ChannelNum, DiscoveredSource,
                                Label, LogicalChannelId, LogicalChannelInfo, LogicalChannelMetrics, LogicalChannelName, MaxStreams, 
                                PercentDisplay, Priority, ProviderAlias, ProviderStatus, QualityScores, SourceInfo, SourceMetrics,
                                SourcePriority, SourceServiceId, TVGGroupTitle, TVGId, TVGLogo, VideoType, is_valid_url, run_bg, sort_sources)

# --- Constants ---
PLAYLIST_POLL_INTERVAL: Final[float] = 0.2         # Seconds to wait between checking for a new playlist
UI_SEARCH_MIN_CHARS: Final[int] = 3                # Minimum characters for a UI search
UI_SEARCH_MAX_RESULTS: Final[int] = 50             # Max results to return in a UI search
HIGHEST_PRIORITY_SOURCES_NUM: Final[int] = 8       # Maximum number of sources to consider for quality metrics
MULTI_SEARCH_QUERY_DELIMITER: Final[str] = " OR "  # Delimiter for multi-word search queries

# --- App Initialization ---
app = Quart(__name__)
app.secret_key = os.urandom(24)

config: Config
handler: ChannelHandler
stream_manager: StreamManager
ghost_monitor: GhostSessionMonitor | None
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
        stream_manager = await StreamManager.create(config, handler)
        ghost_monitor = await GhostSessionMonitor.create(config, handler, stream_manager)
        quality_monitor = await QualityMonitor.create(config, handler)
        scheduler = await Scheduler.create(config, handler, quality_monitor)
    except BaseException as e:
        print(f"FATAL: Could not initialize application: {e}", file=sys.stderr)
        sys.exit(1)


@app.after_serving
async def shutdown() -> None:
    """Handles graceful shutdown of the application."""
    if "scheduler" in globals():
        scheduler.shutdown()
    if "stream_manager" in globals():
        await stream_manager.stop_ffmpeg_processes()
    if "config" in globals():
        await config.clean_up_hls_segments()


async def calculate_channel_metrics(mapped_services: list[SourcePriority], all_quality_scores: QualityScores) -> LogicalChannelMetrics:
    """Calculates uptime metrics for a channel. (Sync, CPU-bound logic)."""
    uptime_scores: list[float] = []
    if mapped_services:
        for service_object in mapped_services[:HIGHEST_PRIORITY_SOURCES_NUM]:
            service_id = service_object.get('source_service_id')
            if not service_id: continue
            raw_uptime = all_quality_scores.get(service_id, {}).get('uptime')
            if raw_uptime is not None: uptime_scores.append(raw_uptime)

    discovered_mappings = 0
    for service in mapped_services:
        if await handler.get_discovered_source(service["source_service_id"]):
            discovered_mappings += 1
    return LogicalChannelMetrics({
        "health_score": PercentDisplay(int((1 - math.prod([(1 - score) for score in uptime_scores])) * 100)) if uptime_scores else None,
        "lowest_uptime": PercentDisplay(int(min(uptime_scores) * 100)) if uptime_scores else None,
        "enabled_mappings": len(mapped_services),
        "discovered_mappings": discovered_mappings,
    })

def filter_sources(raw_query: str, service: DiscoveredSource) -> bool:
    """Filters sources based on a query. (Sync - pure function)"""
    tvg_name = service.get('tvg_name', '').lower()
    display_name = service.get('display_title', '').lower()
    for raw_q in raw_query.split(MULTI_SEARCH_QUERY_DELIMITER):
        words = raw_q.strip().lower().split()
        if all(word in tvg_name or word in display_name for word in words):
            return True
    return False


@app.context_processor
def inject_global_vars() -> Dict[str, datetime | str]:
    """Injects global variables into the context of all templates."""
    return {
        'now': datetime.now(UTC),
        'app_version': NEXUS_STREAM_VERSION
    }


# --- Core Streaming and Playlist Endpoints ---


@app.route(f'/{VideoType.MPEGTS}/<string:logical_channel_id>')
async def serve_mpegts_stream(logical_channel_id: LogicalChannelId, stream_response: bool = True) -> Response:
    """Serves a channel stream using MPEG-TS format asynchronously.
    If stream_response is True, it returns a generator that the client connects to, otherwise it simply creates the stream.
    """
    added_pending_stream = False
    loop = asyncio.get_running_loop()
    end_time = loop.time() + CREATE_STREAM_DEADLINE
    try:
        while not await handler.add_pending_stream(logical_channel_id, VideoType.MPEGTS):
            if loop.time() > end_time:
                msg = f"Exceeded timeout while waiting for earlier request for MPEGTS {logical_channel_id} to complete."
                config.error(VideoType.MPEGTS, msg)
                abort(503, msg)
            await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        logical_channel = await handler.get_logical_channel_by_id(logical_channel_id)
        if not logical_channel:
            msg = f"Logical channel {logical_channel_id} not found for MPEGTS."
            config.error(VideoType.MPEGTS, msg)
            abort(404, msg)
        logical_channel_name = logical_channel['display_name']

        lc_id_processes = await stream_manager.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=VideoType.MPEGTS, long_term_only=True)
        if len(lc_id_processes):
            video_key, p_info = lc_id_processes.popitem()
            if p_info['is_mpegts_active']:
                config.info(VideoType.MPEGTS, f"Client connecting to shared MPEGTS stream for '{logical_channel_name}' with key '{video_key}'.")
            else:
                config.info(VideoType.MPEGTS, f"Client reconnected to MPEGTS stream for '{logical_channel_name}' with key '{video_key}'.")
        else:
            create_stream_task = await CreateStream.create(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_name, VideoType.MPEGTS)
            res = await create_stream_task.result()
            if isinstance(res, tuple):
                code, msg = res
                config.error(VideoType.MPEGTS, msg)
                abort(code, msg)
            video_key = res

        if not stream_response:
            config.info(VideoType.MPEGTS, f"Recreated MPEGTS stream for channel '{logical_channel_name}' with key '{video_key}'.")
            return Response(status=204)

        async def stream_generator() -> AsyncGenerator[bytes, None]:
            async def recreate_stream() -> None:
                await serve_mpegts_stream(logical_channel_id, stream_response=False)
            try:
                mpegts_stream, reader_id = await MPEGTSStream.register(config, stream_manager, video_key, recreate_stream=recreate_stream)
            except Exception as e:
                msg = f"Failed to register MPEGTS stream for '{logical_channel_name}' with key '{video_key}': {e}"
                config.error(VideoType.MPEGTS, msg)
                abort(500, msg)

            try:
                while True:
                    yield await mpegts_stream.read(reader_id)
            except asyncio.CancelledError as e:
                config.info(VideoType.MPEGTS, f"Client disconnected from MPEGTS stream for '{logical_channel_name}' with key '{video_key}'.")
                raise
            except BaseException as e:
                config.error(VideoType.MPEGTS, f"Unexpected error in MPEGTS stream for '{logical_channel_name}' with key '{video_key}': {e}")
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
            await handler.remove_pending_stream(logical_channel_id, VideoType.MPEGTS)


@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/preview.m3u8')
async def serve_hls_preview(logical_channel_id: LogicalChannelId) -> Response:
    """Serves a preview HLS playlist for a channel asynchronously."""
    source_service_id = SourceServiceId(logical_channel_id.replace("preview_", ""))
    source_service = await handler.get_discovered_source(source_service_id)
    
    if not source_service:
        msg = f"Preview requested for non-existent source service ID {source_service}."
        config.error(VideoType.HLS, msg)
        abort(404, msg)

    sources: list[SourceInfo] = [{
        'source_service_id': source_service_id,
        'priority': await handler.get_source_priority(source_service_id) or Priority(DEFAULT_PRIORITY),
        'provider_alias': source_service['provider_alias'],
        'stream_url': source_service['stream_url']
    }]

    return await serve_hls_playlist(logical_channel_id, logical_channel_name=LogicalChannelName('Preview'), sources=sources)


@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/playlist.m3u8')
async def serve_hls_playlist(logical_channel_id: LogicalChannelId, logical_channel_name: LogicalChannelName | None = None, sources: list[SourceInfo] | None = None) -> Response:
    """Serves the HLS playlist for a channel asynchronously."""
    run_bg(stream_manager.record_video_access(logical_channel_id, VideoType.HLS))
    added_pending_stream = False
    loop = asyncio.get_running_loop()
    end_time = loop.time() + CREATE_STREAM_DEADLINE
    try:
        while not await handler.add_pending_stream(logical_channel_id, VideoType.HLS):
            if loop.time() > end_time:
                msg = f"Exceeded timeout while waiting for earlier request for HLS {logical_channel_id} to complete."
                config.error(VideoType.HLS, msg)
                abort(503, msg)
            await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        if logical_channel_name is None:
            logical_channel = await handler.get_logical_channel_by_id(logical_channel_id)
            if not logical_channel:
                msg = f"Logical channel {logical_channel_id} not found for HLS."
                config.error(VideoType.HLS, msg)
                abort(404, msg)
            logical_channel_name = logical_channel['display_name']

        lc_id_processes = await stream_manager.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=VideoType.HLS, long_term_only=True)
        if len(lc_id_processes):
            video_key = lc_id_processes.popitem()[0]
        else:
            create_stream_task = await CreateStream.create(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_name, VideoType.HLS, sources)
            res = await create_stream_task.result()
            if isinstance(res, tuple):
                code, msg = res
                config.error(VideoType.HLS, msg)
                abort(code, msg)
            video_key = res

        playlist_path = await stream_manager.get_hls_playlist_path(video_key)
        if not playlist_path:
            msg = f"Internal error: HLS playlist path not found for channel '{logical_channel_name}' with key '{video_key}'."
            config.error(VideoType.HLS, msg)
            abort(500, msg)

        end_time = loop.time() + config.ffmpeg_start_timeout
        while loop.time() < end_time:
            to_cleanup = False
            async with stream_manager.stream_process_lock:
                if video_key not in stream_manager.ffmpeg_processes or stream_manager.ffmpeg_processes[video_key]['process'].returncode is not None:
                    to_cleanup = True
            if to_cleanup:
                msg = f"HLS FFmpeg process for '{logical_channel_name}' with key '{video_key}' terminated unexpectedly."
                config.error(VideoType.HLS, msg)
                await stream_manager.stop_ffmpeg_process(video_key, logical_channel_name)
                abort(503, msg)

            if await aiofiles.os.path.exists(playlist_path) and (await aiofiles.os.stat(playlist_path)).st_size > 0:
                try:
                    response = await send_from_directory(str(playlist_path.parent), playlist_path.name, mimetype="application/vnd.apple.mpegurl")
                    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                    response.headers["Pragma"] = "no-cache"
                    response.headers["Expires"] = "0"
                    return response
                except Exception as e:
                    msg = f"Error serving HLS playlist {playlist_path} for '{logical_channel_name}' with key '{video_key}': {e}"
                    config.error(VideoType.HLS, msg)
                    abort(500, msg)
            await asyncio.sleep(PLAYLIST_POLL_INTERVAL)

        msg = f"HLS playlist for '{logical_channel_name}' with key '{video_key}' was not available after {config.ffmpeg_start_timeout} seconds."
        config.error(VideoType.HLS, msg)
        abort(408, msg)
    finally:
        if added_pending_stream:
            await handler.remove_pending_stream(logical_channel_id, VideoType.HLS)


@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/<path:segment_filename>')
async def serve_hls_segment(logical_channel_id: LogicalChannelId, segment_filename: str) -> Response:
    """Serves an HLS video segment (.ts file) asynchronously."""
    run_bg(stream_manager.record_video_access(logical_channel_id, VideoType.HLS, segment_filename=segment_filename))
    if not segment_filename.endswith(".ts") or ".." in segment_filename:
        abort(400, f"Invalid segment filename: {segment_filename}")
    
    segment_path = await stream_manager.get_hls_segment_path(logical_channel_id, VideoType.HLS, segment_filename)
    if not segment_path or not await aiofiles.os.path.isfile(segment_path):
        abort(404, f"HLS segment not found for channel '{logical_channel_id}'")

    return await send_from_directory(str(segment_path.parent), segment_path.name, mimetype="video/mp2t")


@app.route("/<string:video_type>/<string:logical_channel_id>/stop", methods=["POST"])
async def stop_stream(video_type: VideoType, logical_channel_id: LogicalChannelId) -> Response:
    """Stops the stream for a logical channel asynchronously."""
    await stream_manager.stop_ffmpeg_processes_with_logical_channel_id(logical_channel_id, video_type)
    return Response(status=204)


@app.route("/playlist.m3u")
async def serve_main_playlist() -> Response:
    """Serves the main M3U playlist for clients."""
    return Response(await handler.get_main_m3u_playlist(), mimetype="application/x-mpegurl")


@app.route("/reload", methods=["POST"])
async def reload_configuration() -> Response:
    """Triggers a full async reload of all configurations and channel data."""
    form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
    update_providers = form_data.get("update_providers", "false").lower() == "true"
    force_discover_sources = form_data.get("force_discover_sources", "false").lower() == "true"

    config.info(Label.SERVER, f"Received request to reload configuration via UI with params={{update_providers={update_providers}, force_discover_sources={force_discover_sources}}}")
    try:
        await handler.reload_handler_config(update_providers=update_providers, force_discover_sources=force_discover_sources)
        if force_discover_sources:
            await flash("Successfully reloaded configuration and refreshed discovered source services!", "success")
        else:
            await flash("Successfully reloaded configuration!", "success")
    except Exception as e:
        config.error(Label.SERVER, f"An error occurred during manual reload: {e}")
        await flash(f"An error occurred during reload: {e}", "error")
    
    response = Response("")
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/backup", methods=["POST"])
async def backup_configuration() -> Response:
    """Triggers an async backup of the current configuration files."""
    try:
        backup_path = await config.backup_config(scheduled=False)
        if backup_path:
            await flash(f"Backup created successfully at {backup_path}", "success")
        else:
            await flash("Failed to create backup.", "error")
    except Exception as e:
        config.error(Label.SERVER, f"An error occurred during backup: {e}")
        await flash(f"An error occurred during backup: {e}", "error")

    response = Response("")
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/flash-messages")
async def ui_flash_messages() -> str:
    """Renders just the flash messages partial for HTMX updates."""
    return await render_template("_flash_messages.html")


# --- UI Endpoints ---


@app.route("/")
@app.route("/ui")
async def ui_main_dashboard() -> str:
    """Renders the main dashboard page."""
    return await render_template("ui_dashboard.html",
                           provider_count=await handler.get_num_providers(),
                           discovered_services_count=await handler.get_num_discovered_sources(),
                           logical_channels_count=await handler.get_num_logical_channels())


@app.route("/ui/providers", methods=["GET"])
async def ui_providers_manage() -> str:
    all_providers = sorted((await handler.get_provider_stream_status()).values(), key=lambda p: p['alias'])
    return await render_template("ui_providers.html", providers=all_providers)


@app.route("/ui/providers/add", methods=["GET", "POST"])
async def ui_provider_add() -> Response | str:
    if request.method == "POST":
        form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore
        alias = ProviderAlias(form_data.get("alias", "").strip())
        m3u_url = M3UURL(form_data.get("m3u_url", "").strip())
        max_streams_str = form_data.get("max_streams", "")
        try:
            max_streams = MaxStreams(int(max_streams_str))
            if not is_valid_url(m3u_url):
                raise ValueError(f"Invalid URL format: {m3u_url}")
            if await handler.add_provider(alias, m3u_url, max_streams):
                await flash(f"Provider '{alias}' added successfully.", "success")
                all_providers = sorted((await handler.get_provider_stream_status()).values(), key=lambda p: p['alias'])
                table_body_html = await render_template("_providers_table_body.html", providers=all_providers)
                form_removal_html = '<div id="add-provider-form-wrapper" hx-swap-oob="true"></div>'
                response = Response(table_body_html + form_removal_html)
            else:
                raise ValueError(f"Failed to add provider '{alias}'.")
        except ValueError as e:
            await flash(f"Failed to add provider '{alias}`: {e}", "error")
            response = Response(await render_template("_provider_add_form.html", alias=alias, m3u_url=m3u_url, max_streams=max_streams_str))
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
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
        if await handler.update_provider(alias, m3u_url, max_streams):
            await flash(f"Provider '{alias}' updated successfully.", "success")
            updated_provider_data = ProviderStatus({**provider, "m3u_url": m3u_url, "max_streams": max_streams})
            response = Response(await render_template("_provider_row.html", provider=updated_provider_data))
        else:
            raise ValueError(f"Failed to update provider '{alias}'.")
    except ValueError as e:
        await flash(f"Failed to update provider '{alias}': {e}", "error")
        response = Response(await render_template("_provider_edit_form.html", provider={**provider, "m3u_url": m3u_url, "max_streams": max_streams_str}))
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/providers/delete/<string:alias>", methods=["DELETE"])
async def ui_provider_delete(alias: ProviderAlias) -> Response:
    try:
        if await handler.delete_provider(alias):
            await flash(f"Provider '{alias}' deleted successfully.", "success")
            response = Response("", 200)
        else:
            raise ValueError(f"Failed to delete provider '{alias}'.")
    except ValueError as e:
        await flash(f"Failed to delete provider '{alias}': {e}", "error")
        response = Response("", 400)
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/provider-status")
async def ui_provider_status() -> str:
    statues = await handler.get_provider_stream_status()
    active_streams = sum(status['active_streams'] for status in statues.values())
    max_total_streams = sum(status['max_streams'] for status in statues.values())
    return await render_template("_provider_status_bar.html", active_streams=active_streams, max_total_streams=max_total_streams)


@app.route("/ui/source-services")
async def ui_source_services_list() -> str:
    per_page = request.args.get('per_page', 100, type=int)
    page = request.args.get('page', 1, type=int)
    services_unfiltered = await handler.get_all_discovered_source_services_for_ui()
    providers = sorted(list(set(s['provider_alias'] for s in services_unfiltered)))
    filter_provider = request.args.get('provider_alias', '')
    filter_name = request.args.get('name_filter', '').lower()
    services_filtered = [s for s in services_unfiltered if (not filter_provider or s['provider_alias'] == filter_provider) and (not filter_name or filter_name in s.get('tvg_name', '').lower() or filter_name in s.get('display_title', '').lower())]
    total_items = len(services_filtered)
    total_pages = math.ceil(total_items / per_page)
    services_for_page = services_filtered[(page - 1) * per_page:page * per_page]
    return await render_template("ui_source_services.html", services=services_for_page, providers=providers, current_provider=filter_provider, current_name_filter=filter_name, current_page=page, total_pages=total_pages, total_items=total_items, per_page=per_page)


@app.route("/ui/logical-channels")
async def ui_logical_channels_list() -> str:
    """Renders the list of all configured logical channels."""
    channels = await handler.copy_logical_channels_data(key=lambda x: x.get("display_name","").lower())
    all_quality_scores = await quality_monitor.get_quality_scores()

    all_channel_metrics: dict[LogicalChannelId, LogicalChannelMetrics] = {}
    for channel in channels:
        mapped_services = await handler.copy_mappings_for_logical_channel(channel['logical_channel_id'])
        sort_sources(mapped_services, all_quality_scores, reverse=False)
        all_channel_metrics[channel["logical_channel_id"]] = await calculate_channel_metrics(mapped_services, all_quality_scores)

    return await render_template("ui_logical_channels.html", channels=channels, all_channel_metrics=all_channel_metrics)


@app.route("/ui/logical-channels/form/", methods=["GET", "POST"])
@app.route("/ui/logical-channels/form/<string:logical_channel_id>", methods=["GET", "POST"])
async def ui_logical_channel_form(logical_channel_id: LogicalChannelId | None = None) -> Response | WerkzeugResponse | str:
    """Handles adding/editing a logical channel and its mappings asynchronously."""
    if request.method == "POST":
        form_data = cast(ImmutableMultiDict[str, str], await request.form)  # type: ignore

        logical_channel_name: LogicalChannelName = LogicalChannelName(form_data.get("display_name", "").strip())
        channel_num: ChannelNum = ChannelNum(form_data.get("channel_num", "").strip())
        group_title: TVGGroupTitle = TVGGroupTitle(form_data.get("group_title", "Uncategorized").strip())
        tvg_id: TVGId = TVGId(form_data.get("tvg_id", "").strip())
        tvg_log: TVGLogo = TVGLogo(form_data.get("tvg_logo", "").strip())

        if not logical_channel_name or not channel_num:
            await flash("Display Name and Channel Number are required.", "error") 
            return redirect(request.url)
        try:
            int(channel_num)
        except ValueError:
            await flash(f"Channel Number must be a valid integer, received: {channel_num}", "error")
            return redirect(request.url)

        mappings_to_save: list[SourcePriority] = []
        for service_id_str in form_data.getlist('mapping_service_id'):
            try:
                mappings_to_save.append(SourcePriority({
                    'source_service_id': SourceServiceId(service_id_str),
                    'priority': Priority(int(form_data.get(f"priority_{service_id_str}", DEFAULT_PRIORITY)))
                }))
            except (ValueError, TypeError):
                await flash(f"Skipping a mapping with invalid priority for service '{service_id_str}'.", "warning")

        channel_log = f"'{logical_channel_name}' ({channel_num})"
        submitted_id = form_data.get("logical_channel_id")
        if submitted_id:
            lc_data: LogicalChannelInfo = {
                "logical_channel_id": LogicalChannelId(submitted_id),
                "display_name": logical_channel_name,
                "channel_num": channel_num,
                "group_title": group_title,
                "tvg_id": tvg_id,
                "tvg_logo": tvg_log,
            }
            if not await handler.update_logical_channel(lc_data):
                await flash(f"Failed to update channel {channel_log}.", "error")
                return redirect(request.url)
            if await handler.update_mappings_for_logical_channel(lc_data["logical_channel_id"], mappings_to_save):
                await flash(f"Channel {channel_log} updated with {len(mappings_to_save)} mappings.", "success")
            else:
                await flash(f"Successfully updated channel {channel_log}, but failed to update {len(mappings_to_save)} mappings.", "warning")
            await handler.reload_handler_config()
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=submitted_id))
        else:
            lc_data: LogicalChannelInfo = {
                "logical_channel_id": LogicalChannelId("0"),
                "display_name": logical_channel_name,
                "channel_num": channel_num,
                "group_title": group_title,
                "tvg_id": tvg_id,
                "tvg_logo": tvg_log,
            }
            new_id = await handler.add_logical_channel(lc_data)
            if not new_id:
                await flash(f"Failed to create channel {channel_log}.", "error")
                return redirect(request.url)
            if mappings_to_save:
                if await handler.update_mappings_for_logical_channel(new_id, mappings_to_save):
                    await flash(f"Channel {channel_log} created with {len(mappings_to_save)} mappings.", "success")
                else:
                    await flash(f"Successfully created channel {channel_log}, but failed to add {len(mappings_to_save)} mappings.", "warning")
            else:
                await flash(f"Channel {channel_log} created with no mappings.", "success")
            await handler.reload_handler_config()
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=new_id))

    # --- GET Request Handling ---

    is_htmx_service_list_request = (request.headers.get('HX-Request') and request.headers.get('HX-Target') == 'service-list-container')
    channel: LogicalChannelInfo | None = None
    if logical_channel_id:
        channel = await handler.get_logical_channel_by_id(logical_channel_id)
        if not channel:
            await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
            return redirect(url_for('ui_logical_channels_list'))
    
    search_query = request.args.get('search_query')
    if channel and search_query is None and not is_htmx_service_list_request:
        predefined_channel = await handler.find_matching_predefined_channel(channel['display_name'], channel['channel_num'])
        if predefined_channel:
            search_query = MULTI_SEARCH_QUERY_DELIMITER.join(predefined_channel['names']) if predefined_channel.get('names') else predefined_channel.get('title', channel.get('display_name'))

    filter_query = search_query.strip().lower() if search_query else None
    all_services = await handler.get_all_discovered_source_services_for_ui()
    all_quality_scores = await quality_monitor.get_quality_scores()

    all_services_map = {s['source_id']: s for s in all_services}
    mapped_services: list[DiscoveredSource] = []
    all_mapped_service_ids: set[SourceServiceId] = set()
    source_metrics: dict[SourceServiceId, SourceMetrics] = {}
    current_mappings = await handler.copy_mappings_for_logical_channel(logical_channel_id) if logical_channel_id else []
    sort_sources(current_mappings, all_quality_scores, reverse=False)
    services_mapped_elsewhere = await handler.get_all_mapped_service_ids()
    for mapping in current_mappings:
        service_id = mapping['source_service_id']
        services_mapped_elsewhere.discard(service_id)
        all_mapped_service_ids.add(service_id)
        if service_id in all_services_map:
            service_details = all_services_map[service_id].copy()
            raw_score = all_quality_scores.get(service_id, {}).get('uptime', None)
            source_metrics[service_id] = SourceMetrics({
                "priority": mapping["priority"],
                "uptime": PercentDisplay(int(raw_score * 100)) if raw_score is not None else None
            })
            mapped_services.append(service_details)
    channel_metrics = await calculate_channel_metrics(current_mappings, all_quality_scores)
    
    unmapped_suggestions: list[DiscoveredSource] = []
    if filter_query and search_query:
        for service in all_services:
            if service['source_id'] not in all_mapped_service_ids and filter_sources(search_query, service):
                service_id = service['source_id']
                raw_score = all_quality_scores.get(service_id, {}).get('uptime', None)
                source_metrics[service_id] = SourceMetrics({
                    "priority": Priority(DEFAULT_PRIORITY),
                    "uptime": PercentDisplay(int(raw_score * 100)) if raw_score is not None else None
                })
                unmapped_suggestions.append(service)

    page = request.args.get('page', 1, type=int)
    per_page = 100
    total_unmapped_items = len(unmapped_suggestions)
    total_pages = math.ceil(total_unmapped_items / per_page) if per_page > 0 else 1
    start_index = (page - 1) * per_page
    unmapped_suggestions_for_page = unmapped_suggestions[start_index:start_index + per_page]

    template_to_render = "_service_list_content.html" if is_htmx_service_list_request else "ui_logical_channel_form.html"

    return await render_template(
        template_to_render, channel=channel, channel_metrics=channel_metrics, source_metrics=source_metrics,
        unmapped_suggestions_for_page=unmapped_suggestions_for_page,
        mapped_services=mapped_services, services_mapped_elsewhere=services_mapped_elsewhere,
        current_page=page, total_pages=total_pages, total_unmapped_items=total_unmapped_items,
        search_query=search_query, filter_query=filter_query,
    )


@app.route("/ui/logical-channels/delete/<string:logical_channel_id>", methods=["POST"])
async def ui_logical_channel_delete(logical_channel_id: LogicalChannelId) -> Response | WerkzeugResponse:
    channel = await handler.get_logical_channel_by_id(logical_channel_id)
    if channel:
        channel_log = f"'{channel['display_name']}' ({channel['channel_num']})"
        if await handler.delete_logical_channel(logical_channel_id):
            await flash(f"Channel {channel_log} deleted.", "success")
            await handler.reload_handler_config()
        else:
            await flash(f"Error deleting channel {channel_log}.", "error")
    else:
        await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "warning")
    return redirect(url_for('ui_logical_channels_list'))


@app.route("/ui/logical-channels/analyze-mappings/<string:logical_channel_id>", methods=["POST"])
async def ui_analyze_mappings(logical_channel_id: LogicalChannelId) -> Response:
    """Analyzes the mappings for a logical channel asynchronously."""
    channel = await handler.get_logical_channel_by_id(logical_channel_id)
    if not channel:
        await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
        return Response("", 404)
    services = await handler.copy_mappings_for_logical_channel(logical_channel_id)
    if not services:
        await flash(f"No mappings found for logical channel '{logical_channel_id}'.", "info")
        return Response("", 204)

    channel_log = f"'{channel['display_name']}' ({channel['channel_num']})"
    await quality_monitor.analyze_mapped_services(logical_channel_id)
    await flash(f"Quality analysis completed for {len(services)} mapping(s) in {channel_log}", "success")

    response = Response("", 200)
    response.headers["HX-Refresh"] = "true"
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/logical-channels/remove-dead-mappings/<string:logical_channel_id>", methods=["DELETE"])
async def ui_remove_dead_mappings(logical_channel_id: LogicalChannelId) -> Response:
    """Removes dead mappings from logical channels asynchronously."""
    channel = await handler.get_logical_channel_by_id(logical_channel_id)
    if not channel:
        await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
        return Response("", 404)
    discovered_mappings: list[SourcePriority] = []
    removed_count = 0
    for service in await handler.copy_mappings_for_logical_channel(logical_channel_id):
        if await handler.get_discovered_source(service['source_service_id']):
            discovered_mappings.append(service)
        else:
            if not await quality_monitor.remove_source_service(service['source_service_id']):
                await flash(f"Failed to remove dead service {service['source_service_id']} from quality monitor.", "error")
                return Response("", 500)
            removed_count += 1
    if not await handler.update_mappings_for_logical_channel(logical_channel_id, discovered_mappings):
        await flash(f"Failed to update mappings for logical channel '{logical_channel_id}' after removing dead services.", "error")
        return Response("", 500)

    channel_log = f"'{channel['display_name']}' ({channel['channel_num']})"
    if removed_count > 0:
        await flash(f"Removed {removed_count} dead mapping(s) from {channel_log}.", "success")
    else:
        await flash(f"No dead mappings found to remove from {channel_log}.", "info")

    response = Response("", 200)
    response.headers["HX-Refresh"] = "true"
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/channels/populate-from-suggestion")
async def ui_channel_populate_from_suggestion() -> str:
    """
    Called when a user clicks a channel suggestion.
    Returns multiple OOB fragments to:
    1. Populate the channel details form.
    2. Populate the service mapping card with pre-filtered results.
    3. Clear the suggestion dropdown.
    """
    # 1. Populate the channel details form with pre-filled data
    prefilled_data = {
        'display_name': request.args.get('title', ''),
        'channel_num': request.args.get('num', ''),
        'group_title': request.args.get('group', 'Uncategorized'),
        'tvg_logo': ''
    }
    form_html = await render_template("_logical_channel_form_fields.html", channel=prefilled_data)

    # 2. Pre-filter services based on the suggested name
    filter_query = prefilled_data['display_name'].strip().lower()
    all_services = await handler.get_all_discovered_source_services_for_ui()
    services_mapped_elsewhere = await handler.get_all_mapped_service_ids()
    unmapped_suggestions: list[DiscoveredSource] = []

    search_query = prefilled_data['display_name']
    for channel_list in await handler.get_channel_lists():
        for pre_channel in channel_list:
            if search_query == pre_channel.get('title'):  # Only need this check since we are populating this
                search_query = MULTI_SEARCH_QUERY_DELIMITER.join(pre_channel.get('names', []))
                break

    if filter_query:
        for service in all_services:
            if filter_sources(search_query, service):
                unmapped_suggestions.append(service)

    # 3. Paginate the pre-filtered results
    page = 1
    per_page = 100
    total_unmapped_items = len(unmapped_suggestions)
    total_pages = math.ceil(total_unmapped_items / per_page) if per_page > 0 else 1
    unmapped_suggestions_for_page = unmapped_suggestions[:per_page]

    # 4. OOB swap to update the entire service mapping card, now with data.
    search_card_html = await render_template(
        "_service_mapping_card.html",
        search_query=search_query,
        channel={},
        unmapped_suggestions_for_page=unmapped_suggestions_for_page,
        mapped_services=[], # New channel has no mapped services
        services_mapped_elsewhere=services_mapped_elsewhere,
        current_page=page,
        total_pages=total_pages,
        total_unmapped_items=total_unmapped_items,
        filter_query=filter_query
    )
    oob_search_card = f'<div id="service-mapping-section" hx-swap-oob="innerHTML">{search_card_html}</div>'

    # 5. OOB swap to clear the suggestion dropdown
    clear_suggestions_html = '<div id="suggestion-box" hx-swap-oob="true"></div>'

    return form_html + oob_search_card + clear_suggestions_html


@app.route("/ui/channels/suggest", methods=["GET"])
async def ui_channel_suggest() -> str:
    query = request.args.get('display_name', '')
    if len(query) < 2: return ""
    suggestions = await handler.search_predefined_channels(query)
    return await render_template("_channel_suggestions.html", suggestions=suggestions)


@app.route("/ui/logs/modal")
async def ui_logs_modal() -> str:
    log_lines = []
    log_file_path = config.logs_dir / 'app.log'
    try:
        async with aiofiles.open(log_file_path, 'r') as f:
            log_lines = list(deque(await f.readlines(), 1000))
    except FileNotFoundError:
        log_lines = [f"Error: Log file not found at '{log_file_path}'."]
    except Exception as e:
        log_lines = [f"An error occurred while reading the log file: {e}"]
    return await render_template("_logs_modal_content.html", log_lines=log_lines)


@app.route("/ui/service-preview/<path:service_id>")
async def ui_player_for_service(service_id: SourceServiceId) -> str:
    source_service = await handler.get_discovered_source(service_id)
    if not source_service:
        await flash(f"Error: source service ID not found.", "error")
        abort(404, f"Source service ID '{service_id}' not found.")
    service_name = source_service.get('display_title', source_service.get('tvg_name', 'Preview'))
    logical_channel_id = f"preview_{service_id}"
    playlist_url = url_for('serve_hls_preview', logical_channel_id=logical_channel_id)
    return await render_template("_video_player_modal.html", playlist_url=playlist_url, logical_channel_id=logical_channel_id, service_name=service_name)


# --- HDHomeRun Emulation Endpoints ---


@app.route('/discover.json')
async def hdhomerun_discover() -> Response:
    """Emulates HDHomeRun device discovery API endpoint."""
    
    response_dict: dict[str, str | int] = {
        "FriendlyName": "NexusStream",
        "DeviceAuth": "nexus-stream",
        "ModelNumber": NEXUS_STREAM_VERSION,
        "FirmwareName": f"nexus-stream_{NEXUS_STREAM_VERSION}",
        "FirmwareVersion": NEXUS_STREAM_VERSION,
        "DeviceID": "12345678",
        "Manufacturer": "nexus-stream",
        "BaseURL": f"{config.nexus_url}",
        "LineupURL": f"{config.nexus_url}/lineup.json",
        "TunerCount": sum(p["max_streams"] for p in (await handler.get_provider_stream_status()).values())
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


@app.route('/lineup_status.json')
async def hdhomerun_lineup_status() -> Response:
    """Returns the status of the lineup."""
    response_dict: dict[str, int | str | list[str]] = {
        "ScanInProgress": 0,
        "ScanPossible": 0,
        "Source": "Cable",
        "SourceList": ["Cable"]
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


@app.route('/lineup.json')
async def hdhomerun_lineup() -> Response:
    """Returns the channel lineup in HDHomeRun format."""
    lineup: list[dict[str, str | int]] = []
    quality_scores = await quality_monitor.get_quality_scores()
    for channel in await handler.copy_logical_channels_data():
        channel_number = channel.get('channel_num', '')
        if not channel_number:
            continue
        is_hd = 1
        for mapping in await handler.copy_mappings_for_logical_channel(channel['logical_channel_id']):
            if quality_scores.get(mapping['source_service_id'], {}).get('height', 0) >= 720:
                break
        else:
            is_hd = 0
        lineup.append({
            "GuideNumber": channel_number,
            "GuideName": channel.get('display_name', channel_number),
            "HD": is_hd,
            "URL": f"{config.nexus_url}/{VideoType.MPEGTS}/{channel['logical_channel_id']}"
        })
    return Response(json.dumps(lineup), mimetype="application/json")


# --- Miscellaneous Endpoints ---


@app.route('/ping')
async def ping() -> Response:
    """Simple endpoint to check if the server is running."""
    return Response(status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=NEXUS_STREAM_PORT, use_reloader=False)
