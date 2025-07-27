"""
The main Quart application file for NexusStream.

This file initializes the Quart app and its async components (Config, ChannelHandler, 
StreamManager, GhostSessionMonitor) and defines all the web routes for:
- Serving the master M3U playlist.
- Handling HLS and MPEG-TS streaming requests asynchronously.
- HDHomeRun emulation endpoints.
- Providing a web-based user interface (UI) for configuration and management.
- A manual reload endpoint.
"""

import asyncio
import aiofiles
import json
import math
import os
import sys
from collections import deque
from datetime import UTC, datetime
from typing import Any, AsyncGenerator, Dict

from quart import (Quart, Response, abort, flash, redirect, render_template,
                   request, send_from_directory, url_for)

from nexus_stream.config import Config, NEXUS_STREAM_VERSION
from nexus_stream.create_stream import (CREATE_STREAM_DEADLINE, CREATE_STREAM_POLL_INTERVAL, 
                                        CreateStream, VideoType, sort_sources)
from nexus_stream.handler import ChannelHandler, DEFAULT_PRIORITY
from nexus_stream.mpegts import MPEGTSStream
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.session_monitor import GhostSessionMonitor
from nexus_stream.stream import StreamManager

# --- Constants ---
PLAYLIST_POLL_INTERVAL = 0.2  # Seconds to wait between checking for a new playlist
UI_SEARCH_MIN_CHARS = 3       # Minimum characters for a UI search
UI_SEARCH_MAX_RESULTS = 50    # Max results to return in a UI search

# --- App Initialization ---
app = Quart(__name__)
app.secret_key = os.urandom(24)

config: Config
handler: ChannelHandler
stream_manager: StreamManager
ghost_monitor: GhostSessionMonitor
quality_monitor: QualityMonitor

@app.before_serving
async def startup() -> None:
    """
    Asynchronous startup function. Initializes all core components and starts background tasks.
    This is the standard Quart pattern for handling async setup.
    """
    global config, handler, stream_manager, ghost_monitor, quality_monitor
    try:
        config = await Config.create()
        handler = await ChannelHandler.create(config)
        stream_manager = StreamManager(config, handler)
        ghost_monitor = GhostSessionMonitor(config, handler, stream_manager)
        quality_monitor = await QualityMonitor.create(config, handler)

        stream_manager.start_cleanup_task()
        asyncio.create_task(ghost_monitor.run())
        asyncio.create_task(quality_monitor.run())

    except (ValueError, Exception) as e:
        print(f"FATAL: Could not initialize application: {e}", file=sys.stderr)
        sys.exit(1)

@app.after_serving
async def shutdown():
    """Handles graceful shutdown of the application."""
    if stream_manager:
        await stream_manager.stop_ffmpeg_processes()
    if config:
        await config.clean_up_hls_segments()

def calculate_channel_metrics(channel: dict[str, Any], mapped_services: list[dict[str, Any]], all_quality_scores: dict[str, dict[str, float]]) -> None:
    """Calculates uptime metrics for a channel. (Sync, CPU-bound logic)."""
    uptime_scores: list[float] = []
    if mapped_services:
        for service_object in mapped_services[:8]:
            service_id = service_object.get('source_service_id')
            if not service_id: continue
            raw_uptime = all_quality_scores.get(service_id, {}).get('uptime')
            if raw_uptime is not None: uptime_scores.append(raw_uptime)

    if uptime_scores:
        channel['lowest_uptime'] = int(min(uptime_scores) * 100)
        prob_all_services_fail = math.prod([(1 - score) for score in uptime_scores])
        channel['health_score'] = int((1 - prob_all_services_fail) * 100)
    else:
        channel['lowest_uptime'] = None
        channel['health_score'] = None

    channel['enabled_mappings'] = len(mapped_services)
    channel['discovered_mappings'] = sum(1 for service in mapped_services if service['source_service_id'] in handler.discovered_source_services_data)

@app.context_processor
def inject_global_vars() -> Dict[str, Any]:
    """Injects global variables into the context of all templates."""
    return {
        'now': datetime.now(UTC),
        'app_version': NEXUS_STREAM_VERSION
    }

# --- Core Streaming and Playlist Endpoints ---

@app.route(f'/{VideoType.MPEGTS}/<string:logical_channel_id>')
async def serve_mpegts_stream(logical_channel_id: str, stream_response: bool = True) -> Response:
    """Serves a channel stream using MPEG-TS format asynchronously.
    If stream_response is True, it returns a generator that the client connects to, otherwise it simply creates the stream.
    """
    added_pending_stream = False
    loop = asyncio.get_running_loop()
    end_time = loop.time() + CREATE_STREAM_DEADLINE
    try:
        while not await handler.add_pending_stream(logical_channel_id, VideoType.MPEGTS):
            if loop.time() > end_time:
                msg = f"[{VideoType.MPEGTS}] Exceeded timeout while waiting for earlier request for MPEGTS {logical_channel_id} to complete."
                config.log_message(msg, level="ERROR")
                abort(503, msg)
            await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        logical_channel = handler.get_logical_channel_by_id(logical_channel_id)
        if not logical_channel:
            msg = f"[{VideoType.MPEGTS}] Logical channel {logical_channel_id} not found for MPEGTS."
            config.log_message(msg, level="ERROR")
            abort(404, msg)
        logical_channel_name = str(logical_channel['display_name'])

        lc_id_processes = await stream_manager.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=VideoType.MPEGTS, long_term_only=True)
        if len(lc_id_processes):
            video_key, p_info = lc_id_processes.popitem()
            if p_info['is_mpegts_active']:
                config.log_message(f"[{VideoType.MPEGTS}] Client connecting to shared MPEGTS stream for '{logical_channel_name}' with key '{video_key}'.", level="INFO")
            else:
                config.log_message(f"[{VideoType.MPEGTS}] Client reconnected to MPEGTS stream for '{logical_channel_name}' with key '{video_key}'.", level="INFO")
        else:
            create_stream_task = await CreateStream.create(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_name, VideoType.MPEGTS)
            res = await create_stream_task.result()
            if isinstance(res, tuple):
                code, msg_text = res
                msg = f"[{VideoType.MPEGTS}] {msg_text}"
                config.log_message(msg, level="ERROR")
                abort(code, msg)
            video_key = res

        if not stream_response:
            config.log_message(f"[{VideoType.MPEGTS}] Recreated MPEGTS stream for channel '{logical_channel_name}' with key '{video_key}'.", level="INFO")
            return Response(status=204)

        async def stream_generator() -> AsyncGenerator[bytes, None]:
            async def recreate_stream() -> None:
                await serve_mpegts_stream(logical_channel_id, stream_response=False)
            try:
                mpegts_stream, reader_id = await MPEGTSStream.register(config, stream_manager, video_key, recreate_stream=recreate_stream)
            except Exception as e:
                msg = f"[{VideoType.MPEGTS}] {e}"
                config.log_message(msg, level="ERROR")
                abort(500, msg)

            try:
                while True:
                    yield await mpegts_stream.read(reader_id)
            except asyncio.CancelledError as e:
                config.log_message(f"[{VideoType.MPEGTS}] Client disconnected from MPEGTS stream for '{logical_channel_name}' with key '{video_key}'.")
                raise
            except BaseException as e:
                config.log_message(f"[{VideoType.MPEGTS}] Unexpected error in MPEGTS stream for '{logical_channel_name}' with key '{video_key}': {e}", level="ERROR")
                raise
            finally:
                await mpegts_stream.unregister(reader_id)

        response = Response(stream_generator(), mimetype='video/mp2t')
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.timeout = None
        return response
    finally:
        if added_pending_stream:
            await handler.remove_pending_stream(logical_channel_id, video_type=VideoType.MPEGTS)

@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/preview.m3u8')
async def serve_hls_preview(logical_channel_id: str) -> Response:
    """Serves a preview HLS playlist for a channel asynchronously."""
    source_service_id = logical_channel_id.replace("preview_", "")
    source_service = handler.discovered_source_services_data.get(source_service_id, None)
    
    if not source_service:
        msg = f"[{VideoType.HLS}] Preview requested for non-existent source service ID {source_service}."
        config.log_message(msg, level="ERROR")
        abort(404, msg)

    priority = DEFAULT_PRIORITY
    for channels in handler.channel_mappings_data.values():
        for channel in channels:
            if channel['source_service_id'] == source_service_id:
                priority = channel['priority']
                break

    sources: list[dict[str, Any]] = [{
        'source_service_id': source_service['id'], 'priority': priority,
        'provider_alias': source_service['provider_alias'],
        'actual_stream_url': source_service['actual_stream_url']
    }]
    logical_channel_name = source_service.get('original_display_name_extinf', source_service.get('original_tvg_name', 'Preview'))

    return await serve_hls_playlist(logical_channel_id, logical_channel_name=logical_channel_name, sources=sources)

@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/playlist.m3u8')
async def serve_hls_playlist(logical_channel_id: str, logical_channel_name: str | None = None, sources: list[dict[str, Any]] | None = None) -> Response:
    """Serves the HLS playlist for a channel asynchronously."""
    asyncio.create_task(stream_manager.record_video_access(logical_channel_id, VideoType.HLS))
    added_pending_stream = False
    loop = asyncio.get_running_loop()
    end_time = loop.time() + CREATE_STREAM_DEADLINE
    try:
        while not await handler.add_pending_stream(logical_channel_id, VideoType.HLS):
            if loop.time() > end_time:
                msg = f"[{VideoType.HLS}] Exceeded timeout while waiting for earlier request for HLS {logical_channel_id} to complete."
                config.log_message(msg, level="ERROR")
                abort(503, msg)
            await asyncio.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        if logical_channel_name is None:
            logical_channel = handler.get_logical_channel_by_id(logical_channel_id)
            if not logical_channel:
                msg = f"[{VideoType.HLS}] Logical channel {logical_channel_id} not found for HLS."
                config.log_message(msg, level="ERROR")
                abort(404, msg)
            logical_channel_name = str(logical_channel['display_name'])

        lc_id_processes = await stream_manager.get_ffmpeg_processes_from_logical_id(logical_channel_id, video_type=VideoType.HLS, long_term_only=True)
        if len(lc_id_processes):
            video_key = lc_id_processes.popitem()[0]
        else:
            create_stream_task = await CreateStream.create(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_name, VideoType.HLS, sources)
            res = await create_stream_task.result()
            if isinstance(res, tuple):
                code, msg_text = res
                msg = f"[{VideoType.HLS}] {msg_text}"
                config.log_message(msg, level="ERROR")
                abort(code, msg)
            video_key = res

        playlist_path = await stream_manager.get_hls_playlist_path(video_key)
        if not playlist_path:
            msg = f"[{VideoType.HLS}] Internal error: HLS playlist path not found for channel '{logical_channel_name}' with key '{video_key}'."
            config.log_message(msg, level="ERROR")
            abort(500, msg)

        end_time = loop.time() + config.ffmpeg_start_timeout
        while loop.time() < end_time:
            to_cleanup = False
            async with stream_manager.stream_process_lock:
                if video_key not in stream_manager.ffmpeg_processes or stream_manager.ffmpeg_processes[video_key]['process'].returncode is not None:
                    to_cleanup = True
            if to_cleanup:
                msg = f"[{VideoType.HLS}] HLS FFmpeg process for '{logical_channel_name}' with key '{video_key}' terminated unexpectedly."
                config.log_message(msg, level="ERROR")
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
                    msg = f"[{VideoType.HLS}] Error serving HLS playlist {playlist_path} for '{logical_channel_name}' with key '{video_key}': {e}"
                    config.log_message(msg, level="ERROR")
                    abort(500, msg)
            await asyncio.sleep(PLAYLIST_POLL_INTERVAL)

        msg = f"[{VideoType.HLS}] HLS playlist for '{logical_channel_name}' with key '{video_key}' was not available after {config.ffmpeg_start_timeout} seconds."
        config.log_message(msg, level="ERROR")
        abort(408, msg)
    finally:
        if added_pending_stream:
            await handler.remove_pending_stream(logical_channel_id, video_type=VideoType.HLS)

@app.route(f'/{VideoType.HLS}/<string:logical_channel_id>/<path:segment_filename>')
async def serve_hls_segment(logical_channel_id: str, segment_filename: str) -> Response:
    """Serves an HLS video segment (.ts file) asynchronously."""
    asyncio.create_task(stream_manager.record_video_access(logical_channel_id, VideoType.HLS, segment_filename=segment_filename))
    if not segment_filename.endswith(".ts") or ".." in segment_filename:
        abort(400, f"Invalid segment filename: {segment_filename}")
    
    segment_path = await stream_manager.get_hls_segment_path(logical_channel_id, VideoType.HLS, segment_filename)
    if not segment_path or not await aiofiles.os.path.isfile(segment_path):
        abort(404, f"HLS segment not found for channel '{logical_channel_id}'")

    return await send_from_directory(str(segment_path.parent), segment_path.name, mimetype="video/mp2t")

@app.route("/<string:video_type>/<string:logical_channel_id>/stop", methods=["POST"])
async def stop_stream(video_type: str, logical_channel_id: str) -> Response:
    """Stops the stream for a logical channel asynchronously."""
    await stream_manager.stop_ffmpeg_processes_with_logical_channel_id(logical_channel_id, VideoType(video_type))
    return Response(status=204)

@app.route("/playlist.m3u")
async def serve_master_playlist() -> Response:
    """Serves the master M3U playlist for clients."""
    return Response(handler.master_m3u_content, mimetype="application/x-mpegurl")

@app.route("/reload", methods=["POST"])
async def reload_configuration() -> Response:
    """Triggers a full async reload of all configurations and channel data."""
    form_data = await request.form
    update_providers = form_data.get("update_providers", "false").lower() == "true"
    force_discover_sources = form_data.get("force_discover_sources", "false").lower() == "true"

    config.log_message(f"Received request to reload configuration via UI with params={{update_providers={update_providers}, force_discover_sources={force_discover_sources}}}", level="INFO")
    try:
        await handler.reload_handler_config(update_providers=update_providers, force_discover_sources=force_discover_sources)
        if force_discover_sources:
            await flash("Successfully reloaded configuration and refreshed discovered source services!", "success")
        else:
            await flash("Successfully reloaded configuration!", "success")
    except Exception as e:
        config.log_message(f"An error occurred during manual reload: {e}", level="ERROR")
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
        config.log_message(f"An error occurred during backup: {e}", level="ERROR")
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
                           provider_count=len(handler.providers_data),
                           discovered_services_count=len(handler.discovered_source_services_data),
                           logical_channels_count=len(handler.logical_channels_data))

@app.route("/ui/logical-channels")
async def ui_logical_channels_list() -> str:
    """Renders the list of all configured logical channels."""
    channels = handler.get_all_logical_channels_for_ui()
    all_quality_scores = await quality_monitor.get_quality_scores()

    for channel in channels:
        mapped_services = handler.get_mappings_for_logical_channel(channel['logical_channel_id'])
        sort_sources(mapped_services, all_quality_scores, reverse=False)
        calculate_channel_metrics(channel, mapped_services, all_quality_scores)

    return await render_template("ui_logical_channels.html", channels=channels, handler=handler)

@app.route("/ui/logical-channels/form/", methods=["GET", "POST"])
@app.route("/ui/logical-channels/form/<string:logical_channel_id>", methods=["GET", "POST"])
async def ui_logical_channel_form(logical_channel_id: str | None = None):
    """Handles adding/editing a logical channel and its mappings asynchronously."""
    if request.method == "POST":
        form_data = await request.form
        submitted_id = form_data.get("logical_channel_id")
        is_update = bool(submitted_id)

        lc_data = {
            "display_name": form_data.get("display_name", "").strip(),
            "channel_num": form_data.get("channel_num", "").strip(),
            "group_title": form_data.get("group_title", "Uncategorized").strip(),
            "tvg_id": form_data.get("tvg_id", "").strip(),
            "tvg_logo": form_data.get("tvg_logo", "").strip()
        }
        channel_log = f"'{lc_data.get('display_name', 'Unknown Channel')}'{f' ({lc_data['channel_num']})' if 'channel_num' in lc_data else ''}"

        if not lc_data['display_name'] or not lc_data['channel_num']:
            await flash("Display Name and Channel Number are required.", "error") 
            return redirect(request.url)

        mappings_to_save = []
        for service_id_str in form_data.getlist('mapping_service_id'):
            try:
                mappings_to_save.append({
                    'source_service_id': service_id_str,
                    'priority': int(form_data.get(f"priority_{service_id_str}", '5'))
                })
            except (ValueError, TypeError):
                await flash(f"Skipping a mapping with invalid priority for service '{service_id_str}'.", "warning")

        if is_update:
            await handler.update_logical_channel(submitted_id, lc_data)
            await handler.update_mappings_for_logical_channel(submitted_id, mappings_to_save)
            await flash(f"Channel {channel_log} updated.", "success")
            await handler.reload_handler_config()
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=submitted_id))
        else:
            new_id = await handler.add_logical_channel(lc_data)
            if new_id:
                if mappings_to_save:
                    await handler.update_mappings_for_logical_channel(new_id, mappings_to_save)
                await flash(f"Channel {channel_log} created.", "success")
                await handler.reload_handler_config()
                return redirect(url_for('ui_logical_channel_form', logical_channel_id=new_id))
            else:
                await flash("Error creating channel.", "error")
                return await render_template("ui_logical_channel_form.html", channel=lc_data)

    # --- GET Request Handling ---
    is_htmx_service_list_request = (request.headers.get('HX-Request') and request.headers.get('HX-Target') == 'service-list-container')
    channel = {}
    if logical_channel_id:
        channel = handler.get_logical_channel_by_id(logical_channel_id)
        if not channel:
            await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
            return redirect(url_for('ui_logical_channels_list'))
    
    search_query = request.args.get('search_query')
    if search_query is None and not is_htmx_service_list_request and logical_channel_id:
        predefined_channel = handler.find_matching_predefined_channel(channel['display_name'], channel['channel_num'])
        search_query = " OR ".join(predefined_channel['names']) if predefined_channel.get('names') else predefined_channel.get('title', channel.get('display_name'))

    filter_query = search_query.strip().lower() if search_query else None
    all_services = handler.get_all_discovered_source_services_for_ui()
    all_quality_scores = await quality_monitor.get_quality_scores()
    
    other_mappings:dict[str, list[dict[str, Any]]] = handler.channel_mappings_data.copy()
    current_mappings = other_mappings.pop(logical_channel_id, []) if logical_channel_id else []
    sort_sources(current_mappings, all_quality_scores, reverse=False)
    services_mapped_elsewhere: set[str] = {mapping['source_service_id'] for mappings in other_mappings.values() for mapping in mappings}
    all_services_map = {s['id']: s for s in all_services}
    mapped_services: list[dict[str, Any]] = []
    all_mapped_service_ids: set[str] = set()
    for mapping in current_mappings:
        service_id = mapping['source_service_id']
        all_mapped_service_ids.add(service_id)
        if service_id in all_services_map:
            service_details = all_services_map[service_id].copy()
            service_details['priority'] = mapping['priority']
            raw_score = all_quality_scores.get(service_id, {}).get('uptime', None)
            service_details['uptime'] = int(raw_score * 100) if raw_score is not None else None
            mapped_services.append(service_details)
    calculate_channel_metrics(channel, current_mappings, all_quality_scores)
    
    unmapped_suggestions = []
    if filter_query:
        for service in all_services:
            if service['id'] not in all_mapped_service_ids and handler.filter_sources(search_query, service):
                service_id = service['id']
                raw_score = all_quality_scores.get(service_id, {}).get('uptime', None)
                service['uptime'] = int(raw_score * 100) if raw_score is not None else None
                unmapped_suggestions.append(service)

    page = request.args.get('page', 1, type=int)
    per_page = 100
    total_unmapped_items = len(unmapped_suggestions)
    total_pages = math.ceil(total_unmapped_items / per_page) if per_page > 0 else 1
    start_index = (page - 1) * per_page
    unmapped_suggestions_for_page = unmapped_suggestions[start_index:start_index + per_page]

    template_to_render = "_service_list_content.html" if is_htmx_service_list_request else "ui_logical_channel_form.html"

    return await render_template(
        template_to_render, channel=channel, unmapped_suggestions_for_page=unmapped_suggestions_for_page,
        mapped_services=mapped_services, services_mapped_elsewhere=services_mapped_elsewhere,
        current_page=page, total_pages=total_pages, total_unmapped_items=total_unmapped_items,
        search_query=search_query, filter_query=filter_query,
    )

@app.route("/ui/logical-channels/analyze-mappings/<string:logical_channel_id>", methods=["POST"])
async def ui_analyze_mappings(logical_channel_id: str) -> Response:
    """Analyzes the mappings for a logical channel asynchronously."""
    channel = handler.get_logical_channel_by_id(logical_channel_id)
    if not channel:
        await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
        return Response("", 404)
    services = handler.get_mappings_for_logical_channel(logical_channel_id)
    if not services:
        await flash(f"No mappings found for logical channel '{logical_channel_id}'.", "info")
        return Response("", 204)

    channel_log = f"'{channel.get('display_name', 'Unknown Channel')}'{f' ({channel['channel_num']})' if 'channel_num' in channel else ''}"
    await quality_monitor.analyze_logical_channel(logical_channel_id)
    await flash(f"Quality analysis completed for {len(services)} mapping(s) in {channel_log}", "success")

    response = Response("", 200)
    response.headers["HX-Refresh"] = "true"
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response

@app.route("/ui/logical-channels/remove-dead-mappings/<string:logical_channel_id>", methods=["DELETE"])
async def ui_remove_dead_mappings(logical_channel_id: str) -> Response:
    """Removes dead mappings from logical channels asynchronously."""
    channel = handler.get_logical_channel_by_id(logical_channel_id)
    if not channel:
        await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
        return Response("", 404)
    discovered: list[dict[str, Any]] = []
    removed_count = 0
    for service in handler.get_mappings_for_logical_channel(logical_channel_id):
        if service['source_service_id'] in handler.discovered_source_services_data:
            discovered.append(service)
        else:
            await quality_monitor.remove_source_service(service['source_service_id'])
            removed_count += 1
    await handler.update_mappings_for_logical_channel(logical_channel_id, discovered)

    channel_log = f"'{channel.get('display_name', 'Unknown Channel')}'{f' ({channel['channel_num']})' if 'channel_num' in channel else ''}"
    if removed_count > 0:
        await flash(f"Removed {removed_count} dead mapping(s) from {channel_log}.", "success")
    else:
        await flash(f"No dead mappings found to remove from {channel_log}.", "info")

    response = Response("", 200)
    response.headers["HX-Refresh"] = "true"
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response

@app.route("/ui/source-services")
async def ui_source_services_list() -> str:
    per_page = request.args.get('per_page', 100, type=int)
    page = request.args.get('page', 1, type=int)
    services_unfiltered = handler.get_all_discovered_source_services_for_ui()
    providers = sorted(list(set(s['provider_alias'] for s in services_unfiltered)))
    filter_provider = request.args.get('provider_alias', '')
    filter_name = request.args.get('name_filter', '').lower()
    services_filtered = [s for s in services_unfiltered if (not filter_provider or s['provider_alias'] == filter_provider) and (not filter_name or filter_name in s.get('original_tvg_name', '').lower() or filter_name in s.get('original_display_name_extinf', '').lower())]
    total_items = len(services_filtered)
    total_pages = math.ceil(total_items / per_page)
    services_for_page = services_filtered[(page - 1) * per_page:page * per_page]
    return await render_template("ui_source_services.html", services=services_for_page, providers=providers, current_provider=filter_provider, current_name_filter=filter_name, current_page=page, total_pages=total_pages, total_items=total_items, per_page=per_page)

@app.route("/ui/providers", methods=["GET"])
async def ui_providers_manage() -> str:
    all_providers = await handler.get_all_providers_for_ui()
    return await render_template("ui_providers.html", providers=all_providers)

@app.route("/ui/providers/add", methods=["GET", "POST"])
async def ui_provider_add() -> Response | str:
    if request.method == "POST":
        form_data = await request.form
        alias = form_data.get("alias", "").strip()
        url = form_data.get("url", "").strip()
        max_streams_str = form_data.get("max_concurrent_streams", "1")
        try:
            max_streams = int(max_streams_str)
            if await handler.add_provider(alias, url, max_streams):
                await flash(f"Provider '{alias}' added successfully.", "success")
                all_providers = await handler.get_all_providers_for_ui()
                table_body_html = await render_template("_providers_table_body.html", providers=all_providers)
                form_removal_html = '<div id="add-provider-form-wrapper" hx-swap-oob="true"></div>'
                response = Response(table_body_html + form_removal_html)
            else:
                raise ValueError(f"Failed to add provider '{alias}'.")
        except ValueError as e:
            await flash(str(e), "error")
            response = Response(await render_template("_provider_add_form.html", alias=alias, url=url, max_concurrent_streams=max_streams_str))
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response
    return await render_template("_provider_add_form.html")

@app.route("/ui/providers/edit/<string:alias>", methods=["GET", "PUT"])
async def ui_provider_edit(alias: str) -> str:
    providers = await handler.get_all_providers_for_ui()
    provider = next((p for p in providers if p['alias'] == alias), None)
    if not provider: return ""

    if request.method == "GET":
        if request.args.get('cancel') == 'true':
            return await render_template("_provider_row.html", provider=provider)
        return await render_template("_provider_edit_form.html", provider=provider)
    
    elif request.method == "PUT":
        form_data = await request.form
        url = form_data.get("url", "").strip()
        max_streams_str = form_data.get("max_concurrent_streams", "1")
        try:
            max_streams = int(max_streams_str)
            if await handler.update_provider(alias, url, max_streams):
                await flash(f"Provider '{alias}' updated successfully.", "success")
                updated_provider_data = {**provider, "url": url, "max_concurrent_streams": max_streams}
                response = Response(await render_template("_provider_row.html", provider=updated_provider_data))
            else:
                raise ValueError(f"Failed to update provider '{alias}'.")
        except ValueError as e:
            await flash(str(e), "error")
            response = Response(await render_template("_provider_edit_form.html", provider={**provider, "url": url, "max_concurrent_streams": max_streams_str}))
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response

@app.route("/ui/providers/delete/<string:alias>", methods=["DELETE"])
async def ui_provider_delete(alias: str) -> tuple[str, int]:
    try:
        if await handler.delete_provider(alias):
            await flash(f"Provider '{alias}' deleted successfully.", "success")
            response = Response("", 200)
        else:
            raise ValueError(f"Failed to delete provider '{alias}'.")
    except ValueError as e:
        await flash(str(e), "error")
        response = Response("", 400)
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response

@app.route("/ui/provider-status")
async def ui_provider_status() -> str:
    active, max_total = await handler.get_total_stream_status_for_ui()
    return await render_template("_provider_status_bar.html", active_streams=active, max_total_streams=max_total)

@app.route("/ui/channels/populate-from-suggestion")
async def ui_channel_populate_from_suggestion():
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
    all_services = handler.get_all_discovered_source_services_for_ui()
    services_mapped_elsewhere: set[str] = {mapping['source_service_id'] for mappings in handler.channel_mappings_data.values() for mapping in mappings}
    unmapped_suggestions: list[dict[str, Any]] = []

    search_query = prefilled_data['display_name']
    for channel_list in handler.channel_list_data.values():
        for pre_channel in channel_list:
            if search_query == pre_channel.get('title'):  # Only need this check since we are populating this
                search_query = " OR ".join(pre_channel.get('names', []))
                break

    if filter_query:
        for service in all_services:
            if handler.filter_sources(search_query, service):
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
    suggestions = handler.search_predefined_channels(query)
    return await render_template("_channel_suggestions.html", suggestions=suggestions)

@app.route("/ui/logical-channels/delete/<string:logical_channel_id>", methods=["POST"])
async def ui_logical_channel_delete(logical_channel_id: str) -> Response:
    channel = handler.get_logical_channel_by_id(logical_channel_id)
    if channel:
        channel_log = f"'{channel.get('display_name', 'Unknown Channel')}'{f' ({channel['channel_num']})' if 'channel_num' in channel else ''}"
        if await handler.delete_logical_channel(logical_channel_id):
            await flash(f"Channel {channel_log} deleted.", "success")
            await handler.reload_handler_config()
        else:
            await flash(f"Error deleting channel {channel_log}.", "error")
    else:
        await flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "warning")
    return redirect(url_for('ui_logical_channels_list'))

@app.route("/ui/logs/modal")
async def ui_logs_modal() -> str:
    log_lines = []
    log_file_path = config.logs_dir / 'app.log'
    try:
        async with aiofiles.open(log_file_path, 'r') as f:
            log_lines = list(deque(await f.readlines(), 200))
    except FileNotFoundError:
        log_lines = [f"Error: Log file not found at '{log_file_path}'."]
    except Exception as e:
        log_lines = [f"An error occurred while reading the log file: {e}"]
    return await render_template("_logs_modal_content.html", log_lines=log_lines)

@app.route("/ui/service-preview/<path:service_id>")
async def ui_player_for_service(service_id: str) -> str:
    source_service = handler.discovered_source_services_data.get(service_id)
    if not source_service:
        await flash(f"Error: source service ID not found.", "error")
        abort(404, f"Source service ID '{service_id}' not found.")
    service_name = source_service.get('original_display_name_extinf', source_service.get('original_tvg_name', 'Preview'))
    logical_channel_id = f"preview_{service_id}"
    playlist_url = url_for('serve_hls_preview', logical_channel_id=logical_channel_id)
    return await render_template("_video_player_modal.html", playlist_url=playlist_url, logical_channel_id=logical_channel_id, service_name=service_name)

# --- HDHomeRun Emulation Endpoints ---

@app.route('/discover.json')
async def hdhomerun_discover() -> Response:
    """Emulates HDHomeRun device discovery API endpoint."""
    _, max_streams = await handler.get_total_stream_status_for_ui()
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
        "TunerCount": max_streams
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


@app.route('/lineup_status.json')
async def hdhomerun_lineup_status() -> Response:
    """Returns the status of the lineup."""
    response_dict: dict[str, int | str | list[str]] = {
        "ScanInProgress": 1 if handler.is_loading() else 0,
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
    for channel in handler.logical_channels_data:
        channel_number = channel.get('channel_num', '')
        if not channel_number:
            continue
        is_hd = 1
        for mapping in handler.channel_mappings_data.get(channel['logical_channel_id'], []):
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=os.getenv("NEXUS_PORT", 4040), use_reloader=False)