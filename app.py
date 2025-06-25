"""
The main Flask application file for NexusStream.

This file initializes the Flask app and its components (Config, ChannelHandler, 
StreamManager, GhostSessionMonitor) and defines all the web routes for:
- Serving the master M3U playlist.
- Handling HLS streaming requests (playlists and segments).
- HDHomeRun endpoints.
- Providing a web-based user interface (UI) for configuration and management.
- A manual reload endpoint.
"""

import json
import os
import signal
import subprocess
import sys
import time
import math
from datetime import datetime, UTC
from collections import deque
from typing import Any, Generator

from flask import (Flask, Response, abort, flash, redirect, render_template,
                   request, send_from_directory, url_for)

from nexus_stream.config import Config
from nexus_stream.create_stream import CREATE_STREAM_POLL_INTERVAL, CreateStream, VideoType, sort_sources
from nexus_stream.handler import ChannelHandler, DEFAULT_PRIORITY
from nexus_stream.quality_monitor import QualityMonitor
from nexus_stream.session_monitor import GhostSessionMonitor
from nexus_stream.stream import StreamManager

# --- Constants ---
PLAYLIST_POLL_INTERVAL = 0.2  # Seconds to wait between checking for a new playlist
UI_SEARCH_MIN_CHARS = 3       # Minimum characters for a UI search
UI_SEARCH_MAX_RESULTS = 50    # Max results to return in a UI search

# --- App Initialization ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

try:
    config = Config()
except Exception as e:
    print(f"FATAL: Could not initialize configuration: {e}")
    exit(1)

try:
    handler = ChannelHandler(config)
    stream_manager = StreamManager(config, handler)
    GhostSessionMonitor(config, handler, stream_manager)
    quality_monitor = QualityMonitor(config, handler)
except ValueError as e:
    # A fatal error during startup should be logged and cause an exit
    config.log_message(f"FATAL: Could not initialize application due to a configuration error: {e}", level="FATAL")
    exit(2)
except Exception as e:
    config.log_message(f"FATAL: An unexpected error occurred during application startup: {e}", level="FATAL")
    exit(3)


def calculate_channel_uptime(channel: dict[str, Any], mapped_services: list[dict[str, Any]], all_quality_scores: dict[str, dict[str, float]]) -> None:
    """Calculates uptime metrics for a channel based on its mapped services."""
    uptime_scores: list[float] = []
    if mapped_services:
        for service_object in mapped_services[:8]:  # Assuming 2s to test a stream, 16s is the max time clients will wait
            service_id = service_object.get('source_service_id')
            if not service_id:
                continue
            
            raw_uptime = all_quality_scores.get(service_id, {}).get('uptime')
            if raw_uptime is not None:
                uptime_scores.append(raw_uptime)

    if uptime_scores:
        # METRIC 1: Lowest Uptime (Weakest Link)
        lowest_raw = min(uptime_scores)
        channel['lowest_uptime'] = int(lowest_raw * 100)

        # METRIC 2: Health (Overall Uptime with Failover)
        # This calculates 1 - (Prob of A failing * Prob of B failing * ...)
        # We use math.prod for a clean way to multiply all items in a list.
        prob_all_services_fail = math.prod([(1 - score) for score in uptime_scores])
        overall_health_raw = 1 - prob_all_services_fail
        channel['health_score'] = int(overall_health_raw * 100)

    else:
        # If no scores exist, set both metrics to None
        channel['lowest_uptime'] = None
        channel['health_score'] = None


@app.context_processor
def inject_now() -> dict[str, datetime]:
    """Injects the current UTC time into all templates for display purposes."""
    return {'now': datetime.now(UTC)}


# --- Core Streaming and Playlist Endpoints ---

@app.route(f'/{VideoType.HLS.value}/<string:logical_channel_id>/preview.m3u8')
def serve_hls_preview(logical_channel_id: str) -> Response:
    """Serves a preview HLS playlist for a channel."""
    source_service_id = logical_channel_id.replace("preview_", "")
    source_service = handler.discovered_source_services.get(source_service_id, None)
    
    if not source_service:
        msg = f"[{request.method} {request.path}] Preview requested for non-existent source service ID {source_service}."
        config.log_message(msg, level="ERROR")
        abort(404, msg)

    priority = DEFAULT_PRIORITY
    for channels in handler.channel_mappings_data_from_json.values():
        for channel in channels:
            if channel['source_service_id'] == source_service_id:
                priority = channel['priority']
                break

    sources: list[dict[str, Any]] = [{
        'source_service_id': source_service['id'],
        'priority': priority,
        'provider_alias': source_service['provider_alias'],
        'actual_stream_url': source_service['actual_stream_url']
    }]

    logical_channel_name = source_service.get('original_display_name_extinf', source_service.get('original_tvg_name', 'Preview'))

    return serve_hls_playlist(logical_channel_id, logical_channel_name=logical_channel_name, sources=sources)

@app.route(f'/{VideoType.HLS.value}/<string:logical_channel_id>/playlist.m3u8')
def serve_hls_playlist(logical_channel_id: str, logical_channel_name: str | None = None, sources: list[dict[str, Any]] | None = None) -> Response:
    """
    Serves the HLS playlist for a channel.
    
    This is the primary entry point for a client starting a stream. It triggers
    the StreamManager to start the FFmpeg process if it's not already running.
    It then waits for the playlist file to become available before serving it.
    """
    stream_manager.record_video_access(logical_channel_id)
    added_pending_stream = False
    end_time = time.monotonic() + 10
    try:
        while not handler.add_pending_stream(logical_channel_id):
            if time.monotonic() > end_time:
                msg = f"[{request.method} {request.path}] Exceeded timeout while waiting for earlier request for {logical_channel_id} to complete."
                config.log_message(msg, level="ERROR")
                abort(503, msg)
            time.sleep(CREATE_STREAM_POLL_INTERVAL)
        added_pending_stream = True

        if logical_channel_name is None:
            logical_channel = handler.get_logical_channel_by_id(logical_channel_id)
            if not logical_channel:
                msg = f"[{request.method} {request.path}] Logical channel {logical_channel_id} not found."
                config.log_message(msg, level="ERROR")
                abort(404, msg)
            logical_channel_name = str(logical_channel['display_name'])

        lc_id_processes = stream_manager.get_ffmpeg_processes_from_logical_id(logical_channel_id, long_term_only=True)
        if len(lc_id_processes):
            video_key = lc_id_processes.popitem()[0]
        else:
            res = CreateStream(config, handler, stream_manager, quality_monitor, logical_channel_id, logical_channel_name, VideoType.HLS, sources).result()
            if isinstance(res, tuple):
                code = res[0]
                msg = f"[{request.method} {request.path}] {res[1]}"
                config.log_message(msg, level="ERROR")
                abort(code, msg)
            video_key = res

        playlist_path = stream_manager.get_hls_playlist_path(video_key)
        if not playlist_path:
            msg = f"[{request.method} {request.path}] Internal error: HLS playlist path not found for logical channel '{logical_channel_name}' with key '{video_key}'."
            config.log_message(msg, level="ERROR")
            abort(500, msg)

        end_time = time.monotonic() + config.ffmpeg_start_timeout
        while time.monotonic() < end_time:
            with stream_manager.stream_process_lock:
                if video_key not in stream_manager.ffmpeg_processes or stream_manager.ffmpeg_processes[video_key]['process'].poll() is not None:
                    msg = f"[{request.method} {request.path}] FFmpeg process for '{logical_channel_name}' with key '{video_key}' terminated unexpectedly while waiting for playlist."
                    config.log_message(msg, level="ERROR")
                    stream_manager.stop_ffmpeg_process(video_key, logical_channel_name)
                    abort(503, msg)

            if playlist_path.exists() and playlist_path.stat().st_size > 0:
                try:
                    response = send_from_directory(str(playlist_path.parent), playlist_path.name, mimetype="application/vnd.apple.mpegurl")
                    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                    response.headers["Pragma"] = "no-cache"
                    response.headers["Expires"] = "0"
                    return response
                except Exception as e:
                    msg = f"[{request.method} {request.path}] Error serving HLS playlist {playlist_path}: {e}"
                    config.log_message(msg, level="ERROR")
                    abort(500, msg)
            time.sleep(PLAYLIST_POLL_INTERVAL)

        msg = f"[{request.method} {request.path}] HLS playlist for logical channel '{logical_channel_name}' with key '{video_key}' was not available after {config.ffmpeg_start_timeout} seconds."
        config.log_message(msg, level="ERROR")
        abort(408, msg)
    finally:
        if added_pending_stream:
            handler.remove_pending_stream(logical_channel_id)


@app.route(f'/{VideoType.HLS.value}/<string:logical_channel_id>/<path:segment_filename>')
def serve_hls_segment(logical_channel_id: str, segment_filename: str) -> Response:
    """Serves an HLS video segment (.ts file)."""
    stream_manager.record_video_access(logical_channel_id)
    if not segment_filename.endswith(".ts") or ".." in segment_filename:
        msg = f"[{request.method} {request.path}] Invalid segment filename: {segment_filename}"
        config.log_message(msg, level="ERROR")
        abort(400, msg)
    
    segment_path = stream_manager.get_hls_segment_path(logical_channel_id, segment_filename)
    if not segment_path:
        msg = f"[{request.method} {request.path}] HLS segment path not found for logical channel '{logical_channel_id}' and segment '{segment_filename}'."
        config.log_message(msg, level="ERROR")
        abort(404, msg)
    if not segment_path.is_file():
        msg = f"[{request.method} {request.path}] HLS segment file not found for logical channel '{logical_channel_id}' and segment '{segment_path}'."
        config.log_message(msg, level="ERROR")
        abort(404, msg)

    return send_from_directory(str(segment_path.parent), segment_path.name, mimetype="video/mp2t")


@app.route("/<string:logical_channel_id>/stop", methods=["POST"])
def stop_stream(logical_channel_id: str) -> Response:
    """Stops the stream for a logical channel."""
    stream_manager.stop_ffmpeg_processes_with_logical_channel_id(logical_channel_id)
    return Response(status=204)


@app.route("/playlist.m3u")
def serve_master_playlist() -> Response:
    """Serves the master M3U playlist for clients."""
    return Response(handler.master_m3u_content, mimetype="application/x-mpegurl")


@app.route("/reload", methods=["POST"])
def reload_configuration() -> Response:
    """Triggers a full reload of all configurations and channel data."""
    config.log_message("Received request to reload configuration via UI.", level="INFO")
    try:
        handler.reload_handler_config(update_providers=True)
        flash("Configuration and source services reloaded successfully!", "success")
    except Exception as e:
        config.log_message(f"An error occurred during manual reload: {e}", level="ERROR")
        flash(f"An error occurred during reload: {e}", "error")
    response = Response(render_template("_flash_messages.html"))
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/flash-messages")
def ui_flash_messages() -> str:
    """Renders just the flash messages partial for HTMX updates."""
    return render_template("_flash_messages.html")


# --- UI Endpoints ---

@app.route("/")
@app.route("/ui")
def ui_main_dashboard() -> str:
    """Renders the main dashboard page."""
    return render_template("ui_dashboard.html",
                           provider_count=len(handler.providers_data_from_json),
                           discovered_services_count=len(handler.discovered_source_services),
                           logical_channels_count=len(handler.logical_channels_data_from_json))

@app.route("/ui/logical-channels")
def ui_logical_channels_list() -> str:
    """Renders the list of all configured logical channels."""
    channels = handler.get_all_logical_channels_for_ui()
    all_quality_scores = quality_monitor.get_quality_scores()

    for channel in channels:
        mapped_services = handler.get_mappings_for_logical_channel(channel['logical_channel_id'])
        sort_sources(mapped_services, all_quality_scores, reverse=False)
        calculate_channel_uptime(channel, mapped_services, all_quality_scores)

    return render_template("ui_logical_channels.html", channels=channels, handler=handler)

@app.route("/ui/logical-channels/form/", methods=["GET", "POST"])
@app.route("/ui/logical-channels/form/<string:logical_channel_id>", methods=["GET", "POST"])
def ui_logical_channel_form(logical_channel_id: str | None = None):
    """Handles adding/editing a logical channel and its mappings in a unified form."""
    if request.method == "POST":
        # Get the ID from the form, which could be empty for new channels
        submitted_id = request.form.get("logical_channel_id")
        is_update = bool(submitted_id)

        lc_data = {
            "display_name": request.form.get("display_name", "").strip(),
            "channel_num": request.form.get("channel_num", "").strip(),
            "group_title": request.form.get("group_title", "Uncategorized").strip(),
            "tvg_id": request.form.get("tvg_id", "").strip(),
            "tvg_logo": request.form.get("tvg_logo", "").strip()
        }

        if not lc_data['display_name'] or not lc_data['channel_num']:
            flash("Display Name and Channel Number are required.", "error")
            # Re-render form with user's data; need to reconstruct the page state
            return redirect(request.url)

        mappings_to_save = []
        service_ids_to_map = request.form.getlist('mapping_service_id')
        for service_id_str in service_ids_to_map:
            try:
                mappings_to_save.append({
                    'source_service_id': service_id_str,
                    'priority': int(request.form.get(f"priority_{service_id_str}", '5'))
                })
            except (ValueError, TypeError):
                flash(f"Skipping a mapping with invalid priority (service_id: '{service_id_str}').", "warning")

        if is_update:  # This is an UPDATE of an existing channel
            handler.update_logical_channel(submitted_id, lc_data)
            handler.update_mappings_for_logical_channel(submitted_id, mappings_to_save)
            flash(f"Channel '{lc_data['display_name']}' and its mappings updated successfully.", "success")
            handler.reload_handler_config(update_providers=False)
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=submitted_id))
        else:  # This is a CREATE for a new channel
            new_id = handler.add_logical_channel(lc_data)
            if new_id:
                if mappings_to_save:
                    handler.update_mappings_for_logical_channel(new_id, mappings_to_save)
                flash(f"Channel '{lc_data['display_name']}' created successfully.", "success")
                handler.reload_handler_config(update_providers=False)
                return redirect(url_for('ui_logical_channel_form', logical_channel_id=new_id))
            else:
                flash("Error creating channel. A channel with that number may already exist.", "error")
                return render_template("ui_logical_channel_form.html", channel=lc_data)

    # --- GET Request Handling ---
    # Check if this is an HTMX request targeting the service list (for search or pagination)
    is_htmx_service_list_request = (
        request.headers.get('HX-Request') and 
        request.headers.get('HX-Target') == 'service-list-container'
    )

    # 1. Load the main channel object if we are editing
    channel = {}
    predefined_channel: dict[str, str] = {}
    if logical_channel_id:
        channel = handler.get_logical_channel_by_id(logical_channel_id)
        if not channel:
            flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
            return redirect(url_for('ui_logical_channels_list'))
        predefined_channel = handler.find_matching_predefined_channel(channel['display_name'], channel['channel_num'])
    # Priority 1: An explicit search query from the user (e.g., typing in the box).
    search_query = request.args.get('search_query')

    # Priority 2: If no explicit search, and it's the initial page load for an
    # existing channel, use the channel's name as the default search term.
    if search_query is None and not is_htmx_service_list_request and logical_channel_id:
        if predefined_channel.get('names'):
            search_query = " OR ".join(predefined_channel['names'])
        else:
            search_query = predefined_channel.get('title', channel.get('display_name'))

    filter_query = search_query.strip().lower() if search_query else None

    # 3. Prepare data for the service list
    unmapped_suggestions = []
    mapped_services: list[dict[str, Any]] = []
    page = request.args.get('page', 1, type=int)
    per_page = 100

    # Load all services and mappings
    all_services = handler.get_all_discovered_source_services_for_ui()
    all_quality_scores = quality_monitor.get_quality_scores()
    other_mappings:dict[str, list[dict[str, Any]]] = handler.channel_mappings_data_from_json.copy()
    current_mappings = other_mappings.pop(logical_channel_id, []) if logical_channel_id else []
    sort_sources(current_mappings, all_quality_scores, reverse=False)
    services_mapped_elsewhere: set[str] = {mapping['source_service_id'] for mappings in other_mappings.values() for mapping in mappings}
    all_services_map = {s['id']: s for s in all_services}

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
    calculate_channel_uptime(channel, current_mappings, all_quality_scores)

    # Populate unmapped suggestions ONLY if there is a filter query.
    if filter_query:
        for service in all_services:
            # Check if the service is NOT already mapped
            if service['id'] not in all_mapped_service_ids:
                # Check if it matches the user's filter/search criteria
                if handler.filter_sources(search_query, service):
                    service_id = service['id']
                    raw_score = all_quality_scores.get(service_id, {}).get('uptime', None)
                    service['uptime'] = int(raw_score * 100) if raw_score is not None else None
                    unmapped_suggestions.append(service)

    # Paginate the results
    total_unmapped_items = len(unmapped_suggestions)
    total_pages = math.ceil(total_unmapped_items / per_page) if per_page > 0 else 1
    if page > total_pages and total_pages > 0: page = total_pages
    if page < 1: page = 1
    start_index = (page - 1) * per_page
    unmapped_suggestions_for_page = unmapped_suggestions[start_index:start_index + per_page]

    # 4. Determine which template to render
    template_to_render = "_service_list_content.html" if is_htmx_service_list_request else "ui_logical_channel_form.html"

    return render_template(
        template_to_render,
        channel=channel,
        unmapped_suggestions_for_page=unmapped_suggestions_for_page,
        mapped_services=mapped_services,
        services_mapped_elsewhere=services_mapped_elsewhere,
        current_page=page,
        total_pages=total_pages,
        total_unmapped_items=total_unmapped_items,
        search_query=search_query, 
        filter_query=filter_query,
    )

@app.route("/ui/source-services")
def ui_source_services_list() -> str:
    """Renders a filterable list of all discovered source services."""
    
    per_page = request.args.get('per_page', 100, type=int)
    if per_page not in [100, 250, 500, 1000]:
        per_page = 100

    page = request.args.get('page', 1, type=int)

    services_unfiltered = handler.get_all_discovered_source_services_for_ui()
    providers = sorted(list(set(s['provider_alias'] for s in services_unfiltered)))

    filter_provider = request.args.get('provider_alias', '')
    filter_name = request.args.get('name_filter', '').lower()

    services_filtered = services_unfiltered
    if filter_provider:
        services_filtered = [s for s in services_filtered if s['provider_alias'] == filter_provider]
    if filter_name:
        services_filtered = [s for s in services_filtered if filter_name in s.get('original_tvg_name', '').lower() or filter_name in s.get('original_display_name_extinf', '').lower()]

    total_items = len(services_filtered)
    total_pages = math.ceil(total_items / per_page)
    
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    
    services_for_page = services_filtered[start_index:end_index]
    services_for_page = services_filtered[start_index:end_index]

    return render_template("ui_source_services.html",
                           services=services_for_page,
                           providers=providers,
                           current_provider=filter_provider,
                           current_name_filter=filter_name,
                           current_page=page,
                           total_pages=total_pages,
                           total_items=total_items,
                           per_page=per_page)


@app.route("/ui/providers", methods=["GET"])
def ui_providers_manage() -> str:
    """Renders the provider management page."""
    all_providers = handler.get_all_providers_for_ui()

    return render_template("ui_providers.html", providers=all_providers)


# --- HTMX Partial Routes ---

@app.route("/ui/providers/add", methods=["GET", "POST"])
def ui_provider_add() -> Response | str:
    """Handles adding a new provider or rendering the add form."""
    if request.method == "POST":
        alias = request.form.get("alias", "").strip()
        url = request.form.get("url", "").strip()
        max_streams_str = request.form.get("max_concurrent_streams", "1")
        
        try:
            max_streams = int(max_streams_str)
            new_provider_object = handler.add_provider(alias, url, max_streams)
            if new_provider_object:
                flash(f"Provider '{alias}' added successfully.", "success")

                all_providers = handler.get_all_providers_for_ui()

                table_body_html = render_template("_providers_table_body.html", providers=all_providers)

                form_removal_html = '<div id="add-provider-form-wrapper" hx-swap-oob="true"></div>'

                response = Response(table_body_html + form_removal_html)
                response.headers["HX-Trigger"] = "flashMessagesUpdated"
                return response
            else:
                flash(f"Failed to add provider '{alias}'.", "error")
                response = Response(render_template("_provider_add_form.html", alias=alias, url=url, max_concurrent_streams=max_streams_str))
        except ValueError as e:
            flash(str(e), "error")
            response = Response(render_template("_provider_add_form.html", alias=alias, url=url, max_concurrent_streams=max_streams_str))
        
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response
    else: # GET request
        return render_template("_provider_add_form.html")


@app.route("/ui/providers/edit/<string:alias>", methods=["GET", "PUT"])
def ui_provider_edit(alias: str) -> str:
    """Handles editing a provider."""
    providers = handler.get_all_providers_for_ui()
    for provider in providers:
        if provider['alias'] == alias:
            break
    if not provider:
        flash(f"Provider '{alias}' not found.", "error")
        return "" # HTMX will remove the row if not found

    if request.method == "GET":
        if request.args.get('cancel'):
            # This is a CANCEL request. Find the provider and return the display row.
            if provider:
                return render_template("_provider_row.html", provider=provider)
            else:
                return ""
        
        return render_template("_provider_edit_form.html", provider=provider)
    
    elif request.method == "PUT":
        url = request.form.get("url", "").strip()
        max_streams_str = request.form.get("max_concurrent_streams", "1")
        
        try:
            max_streams = int(max_streams_str)
            if handler.update_provider(alias, url, max_streams):
                flash(f"Provider '{alias}' updated successfully.", "success")
                # Return the updated row for HTMX to swap
                updated_provider_data = {
                    "alias": alias,
                    "url": url,
                    "max_concurrent_streams": max_streams,
                    "active_streams":  handler.get_provider_stream_status()[alias]['active']
                }
                response = Response(render_template("_provider_row.html", provider=updated_provider_data))
            else:
                flash(f"Failed to update provider '{alias}'.", "error")
                response = Response(render_template("_provider_edit_form.html", provider={
                    "alias": alias, "url": url, "max_concurrent_streams": max_streams_str,
                    "active_streams":  handler.get_provider_stream_status()[alias]['active']
                }))
        except ValueError as e:
            flash(str(e), "error")
            response = Response(render_template("_provider_edit_form.html", provider={
                "alias": alias, "url": url, "max_concurrent_streams": max_streams_str,
                "active_streams":  handler.get_provider_stream_status()[alias]['active']
            }))
        
        response.headers["HX-Trigger"] = "flashMessagesUpdated"
        return response


@app.route("/ui/providers/delete/<string:alias>", methods=["DELETE"])
def ui_provider_delete(alias: str) -> tuple[str, int]:
    """Handles deleting a provider."""
    try:
        if handler.delete_provider(alias):
            flash(f"Provider '{alias}' deleted successfully.", "success")
            response = Response("", 200) # HTMX will remove the element
        else:
            flash(f"Failed to delete provider '{alias}'.", "error")
            response = Response("", 400) # Indicate failure to HTMX
    except ValueError as e:
        flash(str(e), "error")
        response = Response("", 400) # Indicate failure to HTMX
    
    response.headers["HX-Trigger"] = "flashMessagesUpdated"
    return response


@app.route("/ui/provider-status")
def ui_provider_status() -> str:
    """Renders the provider stream status bar partial."""
    active, max_total = handler.get_total_stream_status_for_ui()
    return render_template("_provider_status_bar.html",
                           active_streams=active,
                           max_total_streams=max_total)

@app.route("/ui/channels/populate-from-suggestion")
def ui_channel_populate_from_suggestion():
    """
    Called when a user clicks a channel suggestion.
    Returns multiple OOB fragments to:
    1. Populate the channel details form.
    2. Populate the service mapping card with pre-filtered results.
    3. Clear the suggestion dropdown.
    """
    prefilled_data = {
        'display_name': request.args.get('title', ''),
        'channel_num': request.args.get('num', ''),
        'group_title': request.args.get('group', 'Uncategorized'),
        'tvg_logo': ''
    }
    # This becomes the main response, targeting #form-content-wrapper
    form_html = render_template("_logical_channel_form_fields.html", channel=prefilled_data)

    # 2. Pre-filter services based on the suggested name
    filter_query = prefilled_data['display_name'].strip().lower()
    all_services = handler.get_all_discovered_source_services_for_ui()
    services_mapped_elsewhere: set[str] = {mapping['source_service_id'] for mappings in handler.channel_mappings_data_from_json.values() for mapping in mappings}
    unmapped_suggestions: list[dict[str, Any]] = []

    search_query = prefilled_data['display_name']
    for channel_list in handler.predefined_channel_list.values():
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
    search_card_html = render_template(
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
def ui_channel_suggest() -> str:
    """Provides channel suggestions based on user input for the display_name field."""
    query = request.args.get('display_name', '')
    if len(query) < 2:
        return "" # Return empty to clear suggestions
    suggestions = handler.search_predefined_channels(query)
    return render_template("_channel_suggestions.html", suggestions=suggestions)


@app.route("/ui/logical-channels/delete/<string:logical_channel_id>", methods=["POST"])
def ui_logical_channel_delete(logical_channel_id: str) -> Response:
    """Handles the deletion of a logical channel and its mappings."""
    channel = handler.get_logical_channel_by_id(logical_channel_id)
    if channel:
        if handler.delete_logical_channel(logical_channel_id):
            flash(f"Logical Channel '{channel['display_name']}' deleted.", "success")
            handler.reload_handler_config(update_providers=False)
        else:
            flash(f"Error deleting logical channel '{channel['display_name']}'.", "error")
    else:
        flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "warning")
    return redirect(url_for('ui_logical_channels_list'))

@app.route("/ui/logs/modal")
def ui_logs_modal():
    """Renders the inner content for the log viewer modal."""
    log_lines = []
    log_file_path = config.logs_dir / 'app.log'
    try:
        with open(log_file_path, 'r') as f:
            # Efficiently read the last 200 lines of the file
            log_lines = list(deque(f, 200))
    except FileNotFoundError:
        log_lines = [f"Error: Log file not found at '{log_file_path}'."]
    except Exception as e:
        log_lines = [f"An error occurred while reading the log file: {e}"]

    return render_template("_logs_modal_content.html", log_lines=log_lines)


@app.route("/ui/service-preview/<path:service_id>")
def ui_player_for_service(service_id: str) -> str:
    """
    Returns an HTML fragment containing an HLS.js video player configured
    to play a specific source service.
    """
    source_service = handler.discovered_source_services.get(service_id)
    if not source_service:
        flash(f"Error source service ID not found.", "error")
    service_name = source_service.get('original_display_name_extinf', source_service.get('original_tvg_name', 'Preview') )
    logical_channel_id = f"preview_{service_id}"
    playlist_url = url_for('serve_hls_preview', logical_channel_id=logical_channel_id)

    return render_template("_video_player_modal.html", 
                           playlist_url=playlist_url, logical_channel_id=logical_channel_id, service_name=service_name)


# --- HDHomeRun Emulation Endpoints ---

@app.route('/discover.json')
def hdhomerun_discover() -> Response:
    """Emulates HDHomeRun device discovery API endpoint."""
    response_dict: dict[str, str | int] = {
        "FriendlyName": "NexusStream",
        "DeviceAuth": "nexus-stream",
        "ModelNumber": "2.0.0",
        "FirmwareName": "nexus-stream_2.0.0",
        "FirmwareVersion": "2.0.0",
        "DeviceID": "12345678",
        "Manufacturer": "nexus-stream",
        "BaseURL": f"{config.nexus_url}",
        "LineupURL": f"{config.nexus_url}/lineup.json",
        "TunerCount": sum(status["max"] for status in handler.get_provider_stream_status().values())
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


@app.route('/lineup_status.json')
def hdhomerun_lineup_status() -> Response:
    """Returns the status of the lineup."""
    response_dict: dict[str, int | str | list[str]] = {
        "ScanInProgress": 1 if handler.is_loading() else 0,
        "ScanPossible": 0,
        "Source": "Cable",
        "SourceList": ["Cable"]
    }
    return Response(json.dumps(response_dict), mimetype="application/json")


@app.route('/lineup.json')
def hdhomerun_lineup() -> Response:
    """Returns the channel lineup in HDHomeRun format."""
    lineup: list[dict[str, str | int]] = []
    quality_scores = quality_monitor.get_quality_scores()
    for channel in handler.logical_channels_data_from_json:
        channel_number = channel.get('channel_num', '')
        if not channel_number:
            continue
        service_ids = [m['source_service_id'] for m in handler.channel_mappings_data_from_json.get(channel['logical_channel_id'], [])]
        lineup.append({
            "GuideNumber": channel_number,
            "GuideName": channel.get('display_name', channel_number),
            "HD": 1 if any(quality_scores.get(service_id, {}).get('height', '') >= 720 for service_id in service_ids) else 0,
            "URL": f"{config.nexus_url}/{VideoType.MPEGTS.value}/{channel['logical_channel_id']}"
        })
    return Response(json.dumps(lineup), mimetype="application/json")


@app.route(f'/{VideoType.MPEGTS.value}/<string:logical_channel_id>')
def serve_mpegts_stream(logical_channel_id: str) -> Response:
    """Serves a channel stream using MPEG-TS format."""
    cmd = [
        config.ffmpeg_path,
        "-hide_banner", "-loglevel", "error",
        "-i", f"{config.nexus_url}/hls/{logical_channel_id}/playlist.m3u8",
        "-c", "copy",
        "-f", "mpegts",
        "pipe:1"
    ]
    
    def generate() -> Generator[Any, Any, None]:
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        except Exception as e:
            msg = f"[{request.method} {request.path}] Failed to start FFmpeg process for logical channel '{logical_channel_id}': {e}"
            config.log_message(msg, level="ERROR")
            abort(503, msg)
        try:
            while True:
                if not process.stdout:
                    msg = f"[{request.method} {request.path}] FFmpeg process for logical channel '{logical_channel_id}' failed to start."
                    config.log_message(msg, level="ERROR")
                    abort(503, msg)
                chunk = process.stdout.read(64*1024)
                if not chunk:
                    config.log_message(f"[{request.method} {request.path}] FFmpeg process for logical channel '{logical_channel_id}' terminated.", level="INFO")
                    break
                yield chunk
        finally:
            process.kill()
    return Response(generate(), mimetype="video/mp2t")


def signal_handler(signum: int, _: object) -> None:
    """Handles signals"""
    config.log_message(f"Received {signal.Signals(signum).name}, exiting...", level="INFO")
    stream_manager.stop_ffmpeg_processes()
    config.clean_up_hls_segments()
    sys.exit(0)


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.nexus_port, debug=True, use_reloader=False)
