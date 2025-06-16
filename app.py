"""
The main Flask application file for NexusStream.

This file initializes the Flask app and its components (Config, ChannelHandler, 
HLSStreamManager, GhostSessionMonitor) and defines all the web routes for:
- Serving the master M3U playlist.
- Handling HLS streaming requests (playlists and segments).
- Providing a web-based user interface (UI) for configuration and management.
- A manual reload endpoint.
"""

import os
import time
import math
from datetime import datetime, UTC
from collections import deque

from flask import (Flask, Response, abort, flash, redirect, render_template,
                   request, send_from_directory, url_for)

from nexus_stream.handler import ChannelHandler
from nexus_stream.config import Config
from nexus_stream.stream import HLSStreamManager
from nexus_stream.session_monitor import GhostSessionMonitor

# --- Constants ---
PLAYLIST_POLL_INTERVAL = 0.2  # Seconds to wait between checking for a new playlist
UI_SEARCH_MIN_CHARS = 3       # Minimum characters for a UI search
UI_SEARCH_MAX_RESULTS = 50    # Max results to return in a UI search

# --- App Initialization ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

try:
    config = Config()
    handler = ChannelHandler(config)
    hls_manager = HLSStreamManager(config, handler)
    GhostSessionMonitor(config, handler, hls_manager)
except ValueError as e:
    # A fatal error during startup should be logged and cause an exit
    config.log_message(f"FATAL: Could not initialize application due to a configuration error: {e}", level="FATAL")
    exit(1)
except Exception as e:
    config.log_message(f"FATAL: An unexpected error occurred during application startup: {e}", level="FATAL")
    exit(1)


@app.context_processor
def inject_now() -> dict[str, datetime]:
    """Injects the current UTC time into all templates for display purposes."""
    return {'now': datetime.now(UTC)}


# --- Core Streaming and Playlist Endpoints ---

@app.route('/hls/<string:logical_channel_id>/playlist.m3u8')
def serve_hls_playlist(logical_channel_id: str) -> Response:
    """
    Serves the HLS playlist for a channel.
    
    This is the primary entry point for a client starting a stream. It triggers
    the HLSStreamManager to start the FFmpeg process if it's not already running.
    It then waits for the playlist file to become available before serving it.
    """
    hls_manager.record_hls_access(logical_channel_id)

    logical_channel_name = handler.get_logical_channel_by_id(logical_channel_id)['display_name']

    with hls_manager.hls_process_lock:
        is_running = logical_channel_id in hls_manager.hls_ffmpeg_processes and \
                     hls_manager.hls_ffmpeg_processes[logical_channel_id]['process'].poll() is None

    if not is_running:
        sources_to_try = handler.get_sources_for_client_facing_channel(logical_channel_id)
        if not sources_to_try:
            abort(404, f"Logical channel '{logical_channel_id}' not found or has no sources.")

        started_successfully = False
        for source in sources_to_try:
            if hls_manager.ensure_stream_is_active(
                logical_channel_id=logical_channel_id,
                actual_url=source['actual_stream_url'],
                provider_alias=source['provider_alias']
            ):
                started_successfully = True
                break
            config.log_message(f"Failed to start '{logical_channel_name}' from provider '{source['provider_alias']}'. Trying next source.", level="WARN")

        if not started_successfully:
            abort(503, f"Could not start HLS stream for '{logical_channel_id}' with any available source.")

    playlist_path = hls_manager.get_hls_playlist_path(logical_channel_id)
    if not playlist_path:
        abort(500, "Internal error: HLS playlist path not found after activation.")

    start_wait = time.monotonic()
    while time.monotonic() - start_wait < config.ffmpeg_start_timeout:
        with hls_manager.hls_process_lock:
            if logical_channel_id not in hls_manager.hls_ffmpeg_processes or hls_manager.hls_ffmpeg_processes[logical_channel_id]['process'].poll() is not None:
                config.log_message(f"FFmpeg for '{logical_channel_name}' terminated while waiting for playlist.", level="ERROR")
                hls_manager.stop_hls_ffmpeg_process(logical_channel_id)
                abort(503, "HLS stream generation failed; the process terminated unexpectedly.")

        if playlist_path.exists() and playlist_path.stat().st_size > 0:
            try:
                response = send_from_directory(str(playlist_path.parent), playlist_path.name, mimetype="application/vnd.apple.mpegurl")
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
                return response
            except Exception as e:
                config.log_message(f"Error serving HLS playlist {playlist_path}: {e}", level="ERROR")
                abort(500)
        time.sleep(PLAYLIST_POLL_INTERVAL)

    abort(408, "HLS playlist was not available after the timeout period.")


@app.route('/hls/<string:logical_channel_id>/<path:segment_filename>')
def serve_hls_segment(logical_channel_id: str, segment_filename: str) -> Response:
    """Serves an HLS video segment (.ts file)."""
    if not segment_filename.endswith(".ts") or ".." in segment_filename:
        abort(400, "Invalid segment filename.")
    
    segment_path = hls_manager.get_hls_segment_path(logical_channel_id, segment_filename)
    if not segment_path or not segment_path.is_file():
        config.log_message(f"HLS segment not found: {segment_path}", level="WARN")
        abort(404)
    
    return send_from_directory(str(segment_path.parent), segment_path.name, mimetype="video/mp2t")


@app.route("/playlist.m3u")
def serve_master_playlist() -> Response:
    """Serves the master M3U playlist for clients."""
    return Response(handler.master_m3u_content, mimetype="application/x-mpegurl")


@app.route("/reload", methods=["POST"])
def reload_configuration() -> Response:
    """Triggers a full reload of all configurations and channel data."""
    config.log_message("Received request to reload configuration via UI.", level="INFO")
    try:
        handler.reload_handler_config()
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
    return render_template("ui_logical_channels.html", channels=channels, handler=handler)

@app.route("/ui/logical-channels/form", methods=["GET", "POST"])
@app.route("/ui/logical-channels/form/<string:logical_channel_id>", methods=["GET", "POST"])
def ui_logical_channel_form(logical_channel_id: str | None = None):
    """Handles both adding and editing a logical channel and its mappings."""

    if request.method == "GET":
        channel = {}
        if logical_channel_id:
            channel = handler.get_logical_channel_by_id(logical_channel_id)
            if not channel:
                flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "error")
                return redirect(url_for('ui_logical_channels_list'))

        all_services = handler.get_all_discovered_source_services_for_ui()
        
        filter_query = channel.get('display_name', '').lower().strip()
        
        suggested_services = []
        if filter_query:
            for service in all_services:
                name_to_check = service.get('original_tvg_name', '').lower()
                extinf_name_to_check = service.get('original_display_name_extinf', '').lower()

                if filter_query in name_to_check or filter_query in extinf_name_to_check:
                    suggested_services.append(service)
        
        current_mappings = []
        if logical_channel_id:
            current_mappings = handler.get_mappings_for_logical_channel(logical_channel_id)

        action_url = url_for('ui_logical_channel_form', logical_channel_id=logical_channel_id) if logical_channel_id else url_for('ui_logical_channel_form')

        return render_template("ui_logical_channel_form.html", 
                               channel=channel,
                               all_services=all_services,
                               suggested_services=suggested_services,
                               current_mappings=current_mappings,
                               action_url=action_url)

    if request.method == "POST":
        
        lc_data = {
            "display_name": request.form.get("display_name", "").strip(),
            "channel_num": request.form.get("channel_num", "").strip(),
            "group_title": request.form.get("group_title", "Uncategorized").strip(),
            "tvg_id": request.form.get("tvg_id", "").strip(),
            "tvg_logo": request.form.get("tvg_logo", "").strip()
        }

        if not lc_data['display_name'] or not lc_data['channel_num']:
            flash("Display Name and Channel Number are required.", "error")
            return render_template("ui_logical_channel_form.html", channel=lc_data)

        if logical_channel_id:  # This is an UPDATE of an existing channel
            lc_data['logical_channel_id'] = logical_channel_id
            if not handler.update_logical_channel(logical_channel_id, lc_data):
                flash("Error updating channel details.", "error")
                return redirect(url_for('ui_logical_channel_form', logical_channel_id=logical_channel_id))

            mappings_from_form = []
            
            # Getlist to get all submitted values for these names in order
            service_ids = request.form.getlist('mapping_service_id')
            priorities = request.form.getlist('mapping_priority')

            # Zip the lists together to process them as pairs
            for service_id_str, priority_str in zip(service_ids, priorities):
                if not service_id_str:
                    continue

                try:
                    mappings_from_form.append({
                        'source_service_id': service_id_str,
                        'priority': int(priority_str)
                    })
                except (ValueError, TypeError):
                    flash(f"Skipping a mapping row with invalid data (service_id: '{service_id_str}').", "warning")
                    continue
            
            handler.update_mappings_for_logical_channel(logical_channel_id, mappings_from_form)

            flash(f"Channel '{lc_data['display_name']}' and its mappings were updated successfully.", "success")
            handler.reload_handler_config()
            return redirect(url_for('ui_logical_channel_form', logical_channel_id=logical_channel_id))
        
        else: 
            new_id = handler.add_logical_channel(lc_data)
            if new_id:
                flash(f"Channel '{lc_data['display_name']}' added. You can now map its sources.", "success")
                handler.reload_handler_config()
                return redirect(url_for('ui_logical_channel_form', logical_channel_id=new_id))
            else:
                flash("Error adding channel. A channel with that number may already exist.", "error")
                return render_template("ui_logical_channel_form.html", channel=lc_data)

    return redirect(url_for('ui_logical_channels_list'))


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
            if handler.add_provider(alias, url, max_streams):
                flash(f"Provider '{alias}' added successfully.", "success")
                new_provider_data = {
                    "alias": alias,
                    "url": url,
                    "max_concurrent_streams": max_streams,
                    "active_streams": 0 
                }
                response = Response(render_template("_provider_row.html", provider=new_provider_data, is_new=True))
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
        # Render the edit form for HTMX swap
        provider_display = {
            "alias": alias,
            "url": provider.get("url", ""),
            "max_concurrent_streams": provider.get("max_concurrent_streams", 1),
            "active_streams": handler.active_streams_per_provider.get(alias, 0)
        }
        return render_template("_provider_edit_form.html", provider=provider_display)
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


@app.route("/ui/channels/suggest", methods=["GET"])
def ui_channel_suggest() -> str:
    """Provides channel suggestions based on user input for HTMX active search."""
    query = request.args.get('display_name', '')
    if len(query) < 2:
        return ""
    suggestions = handler.search_predefined_channels(query)
    return render_template("_channel_suggestions.html", suggestions=suggestions)


@app.route("/ui/channels/select-suggestion")
def ui_channel_select_suggestion() -> str:
    """Returns a pre-filled form partial when a user selects a suggestion."""
    num = request.args.get('num', '')
    name = request.args.get('name', '')
    group = request.args.get('group', 'Uncategorized')
    
    prefilled_data = {
        'display_name': name,
        'channel_num': num,
        'group_title': group,
        'tvg_logo': ''
    }

    form_html = render_template("_logical_channel_form_fields.html", channel=prefilled_data)
    clear_suggestions_html = '<div id="suggestion-box" hx-swap-oob="true"></div>'

    return form_html + clear_suggestions_html


@app.route("/ui/logical-channels/delete/<string:logical_channel_id>", methods=["POST"])
def ui_logical_channel_delete(logical_channel_id: str) -> Response:
    """Handles the deletion of a logical channel and its mappings."""
    channel = handler.get_logical_channel_by_id(logical_channel_id)
    if channel:
        if handler.delete_logical_channel(logical_channel_id):
            flash(f"Logical Channel '{channel['display_name']}' deleted.", "success")
            handler.reload_handler_config()
        else:
            flash(f"Error deleting logical channel '{channel['display_name']}'.", "error")
    else:
        flash(f"Logical Channel with ID '{logical_channel_id}' not found.", "warning")
    return redirect(url_for('ui_logical_channels_list'))


@app.route("/ui/logical-channels/new-mapping-row")
def ui_get_new_mapping_row() -> str:
    """Returns an HTML partial for a new, empty mapping row for the UI."""
    logical_channel_id = request.args.get('logical_channel_id', "")
    current_channel = {}
    if logical_channel_id:
        current_channel = handler.get_logical_channel_by_id(logical_channel_id)
    
    all_services = handler.get_all_discovered_source_services_for_ui()
    filter_query = current_channel.get('display_name', '').lower().strip()
    
    suggested_services = []
    if filter_query:
        for service in all_services:
            name_to_check = service.get('original_tvg_name', '').lower()
            extinf_name_to_check = service.get('original_display_name_extinf', '').lower()
            if filter_query in name_to_check or filter_query in extinf_name_to_check:
                suggested_services.append(service)
    
    row_idx_for_names = int(time.time() * 1000)
    
    return render_template('_mapping_row.html',
                           current_row_index=row_idx_for_names,
                           mapping=None,
                           all_services=all_services,
                           suggested_services=suggested_services,
                           channel=current_channel)


@app.route("/ui/remove-mapping-row-placeholder", methods=["DELETE"])
def ui_remove_mapping_row_placeholder() -> tuple[str, int]:
    """Returns an empty response for HTMX to remove a mapping row."""
    return "", 200


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=True, use_reloader=False)