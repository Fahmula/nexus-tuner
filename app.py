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
from datetime import datetime, UTC

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
    hls_manager = HLSStreamManager(
        config,
        handler.check_provider_capacity,
        handler.increment_stream_count_for_provider,
        handler.decrement_stream_count_for_provider
    )
    GhostSessionMonitor(config, handler, hls_manager)
except ValueError as e:
    # A fatal error during startup should be logged and cause an exit.
    print(f"FATAL: Could not initialize application due to a configuration error: {e}")
    exit(1)
except Exception as e:
    print(f"FATAL: An unexpected error occurred during application startup: {e}")
    exit(1)


@app.context_processor
def inject_now() -> dict[str, datetime]:
    """Injects the current UTC time into all templates for display purposes."""
    return {'now': datetime.now(UTC)}


# --- Core Streaming and Playlist Endpoints ---

@app.route('/hls/<string:lc_user_id>/playlist.m3u8')
def serve_hls_playlist(lc_user_id: str) -> Response:
    """
    Serves the HLS playlist for a channel.
    
    This is the primary entry point for a client starting a stream. It triggers
    the HLSStreamManager to start the FFmpeg process if it's not already running.
    It then waits for the playlist file to become available before serving it.
    """
    hls_manager.record_hls_access(lc_user_id)

    with hls_manager.hls_process_lock:
        is_running = lc_user_id in hls_manager.hls_ffmpeg_processes and \
                     hls_manager.hls_ffmpeg_processes[lc_user_id]['process'].poll() is None

    if not is_running:
        sources_to_try = handler.get_sources_for_client_facing_channel(lc_user_id)
        if not sources_to_try:
            abort(404, f"Logical channel '{lc_user_id}' not found or has no sources.")

        started_successfully = False
        for source in sources_to_try:
            if hls_manager.ensure_stream_is_active(
                lc_user_id=lc_user_id,
                actual_url=source['actual_stream_url'],
                provider_alias=source['provider_alias']
            ):
                started_successfully = True
                break
            config.log_message(f"Failed to start '{lc_user_id}' from provider '{source['provider_alias']}'. Trying next source.", level="WARN")

        if not started_successfully:
            abort(503, f"Could not start HLS stream for '{lc_user_id}' with any available source.")

    playlist_path = hls_manager.get_hls_playlist_path(lc_user_id)
    if not playlist_path:
        abort(500, "Internal error: HLS playlist path not found after activation.")

    start_wait = time.monotonic()
    while time.monotonic() - start_wait < config.ffmpeg_start_timeout:
        with hls_manager.hls_process_lock:
            if lc_user_id not in hls_manager.hls_ffmpeg_processes or hls_manager.hls_ffmpeg_processes[lc_user_id]['process'].poll() is not None:
                config.log_message(f"FFmpeg for '{lc_user_id}' terminated while waiting for playlist.", level="ERROR")
                hls_manager.stop_hls_ffmpeg_process(lc_user_id)
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


@app.route('/hls/<string:lc_user_id>/<path:segment_filename>')
def serve_hls_segment(lc_user_id: str, segment_filename: str) -> Response:
    """Serves an HLS video segment (.ts file)."""
    if not segment_filename.endswith(".ts") or ".." in segment_filename:
        abort(400, "Invalid segment filename.")
    
    segment_path = hls_manager.get_hls_segment_path(lc_user_id, segment_filename)
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


@app.route("/ui/logical-channels/add", methods=["GET", "POST"])
def ui_logical_channel_add() -> Response | str:
    """Handles the creation of a new logical channel."""
    if request.method == "POST":
        user_id = request.form.get("user_defined_id", "").strip().lower().replace(" ", "-")
        display_name = request.form.get("display_name", "").strip()

        if not user_id or not display_name:
            flash("User Defined ID and Display Name are required.", "error")
        else:
            lc_data = {
                "user_defined_id": user_id, "display_name": display_name,
                "group_title": request.form.get("group_title", "Uncategorized").strip(),
                "tvg_id": request.form.get("tvg_id", "").strip(),
                "tvg_logo": request.form.get("tvg_logo", "").strip(),
                "channel_num": request.form.get("channel_num", "").strip(),
            }
            try:
                if handler.add_logical_channel(lc_data):
                    flash(f"Logical Channel '{display_name}' added. Now map its sources.", "success")
                    handler.reload_handler_config()
                    return redirect(url_for('ui_logical_channel_edit', lc_user_id=user_id))
                else:
                    flash("Error saving logical channel.", "error")
            except ValueError as e:
                flash(str(e), "error")

    return render_template("ui_logical_channel_form.html",
                           action_url=url_for('ui_logical_channel_add'),
                           channel={},
                           form_action_label="Add Logical Channel")


@app.route("/ui/logical-channels/edit/<string:lc_user_id>", methods=["GET", "POST"])
def ui_logical_channel_edit(lc_user_id: str) -> Response | str:
    """Handles editing a logical channel's details and its source mappings."""
    channel = handler.get_logical_channel_by_user_id(lc_user_id)
    if not channel:
        flash(f"Logical Channel with ID '{lc_user_id}' not found.", "error")
        return redirect(url_for('ui_logical_channels_list'))

    if request.method == "POST":
        action = request.form.get("form_action")
        if action == "update_details":
            updated_lc_data = {
                "display_name": request.form.get("display_name", "").strip(),
                "group_title": request.form.get("group_title", "").strip(),
                "tvg_id": request.form.get("tvg_id", "").strip(),
                "tvg_logo": request.form.get("tvg_logo", "").strip(),
                "channel_num": request.form.get("channel_num", "").strip()
            }
            if handler.update_logical_channel(lc_user_id, updated_lc_data):
                flash(f"Logical Channel '{updated_lc_data['display_name']}' details updated.", "success")
                handler.reload_handler_config()
            else:
                flash("Error updating logical channel details.", "error")

        elif action == "update_mappings":
            new_mappings = []
            for key in request.form:
                if key.startswith("mapping_service_id_"):
                    suffix = key.split("mapping_service_id_")[-1]
                    service_id = request.form.get(key)
                    priority_str = request.form.get(f"mapping_priority_{suffix}")
                    if service_id and priority_str:
                        try:
                            priority = int(priority_str)
                            new_mappings.append({"source_service_id": service_id, "priority": priority})
                        except (ValueError, TypeError):
                            flash(f"Invalid priority for service '{service_id}'. Must be a whole number.", "error")
            
            new_mappings.sort(key=lambda x: x.get("priority", 0))
            if handler.update_mappings_for_logical_channel(lc_user_id, new_mappings):
                flash("Channel source mappings updated.", "success")
                handler.reload_handler_config()
            else:
                flash("Error updating channel source mappings.", "error")

        return redirect(url_for('ui_logical_channel_edit', lc_user_id=lc_user_id))

    all_services = handler.get_all_discovered_source_services_for_ui()
    current_mappings = handler.get_mappings_for_logical_channel(lc_user_id)
    return render_template("ui_logical_channel_form.html",
                           action_url=url_for('ui_logical_channel_edit', lc_user_id=lc_user_id),
                           channel=channel,
                           all_services=all_services,
                           current_mappings=current_mappings,
                           form_action_label="Update Channel")


@app.route("/ui/logical-channels/delete/<string:lc_user_id>", methods=["POST"])
def ui_logical_channel_delete(lc_user_id: str) -> Response:
    """Handles the deletion of a logical channel and its mappings."""
    channel = handler.get_logical_channel_by_user_id(lc_user_id)
    if channel:
        if handler.delete_logical_channel(lc_user_id):
            flash(f"Logical Channel '{channel['display_name']}' deleted.", "success")
            handler.reload_handler_config()
        else:
            flash(f"Error deleting logical channel '{channel['display_name']}'.", "error")
    else:
        flash(f"Logical Channel with ID '{lc_user_id}' not found.", "warning")
    return redirect(url_for('ui_logical_channels_list'))


@app.route("/ui/source-services")
def ui_source_services_list() -> str:
    """Renders a filterable list of all discovered source services."""
    services = handler.get_all_discovered_source_services_for_ui()
    providers = sorted(list(set(s['provider_alias'] for s in services)))

    filter_provider = request.args.get('provider_alias', '')
    filter_name = request.args.get('name_filter', '').lower()

    if filter_provider:
        services = [s for s in services if s['provider_alias'] == filter_provider]
    if filter_name:
        services = [s for s in services if filter_name in s.get('original_tvg_name', '').lower() or filter_name in s.get('original_display_name_extinf', '').lower()]

    return render_template("ui_source_services.html",
                           services=services,
                           providers=providers,
                           current_provider=filter_provider,
                           current_name_filter=filter_name)


@app.route("/ui/providers", methods=["GET", "POST"])
def ui_providers_manage() -> Response | str:
    """Handles viewing and updating provider configurations."""
    if request.method == "POST":
        new_providers_config = {"source_m3u_providers": {}}
        idx = 0
        while True:
            alias = request.form.get(f"provider_alias_{idx}")
            if alias is None:
                break

            url = request.form.get(f"provider_url_{idx}", "").strip()
            max_streams_str = request.form.get(f"provider_max_streams_{idx}", "1")

            try:
                max_streams = int(max_streams_str)
                if not url or max_streams < 0:
                    flash(f"Invalid data for provider '{alias}'. URL must not be empty and streams must be non-negative.", "error")
                    idx += 1
                    continue
            except (ValueError, TypeError):
                flash(f"Max streams for '{alias}' must be a whole number.", "error")
                idx += 1
                continue

            new_providers_config["source_m3u_providers"][alias.strip()] = {
                "url": url,
                "max_concurrent_streams": max_streams,
            }
            idx += 1

        if handler.save_providers_for_ui(new_providers_config):
            flash("Providers configuration updated.", "success")
            handler.reload_handler_config()
        else:
            flash("Error saving providers configuration.", "error")
        return redirect(url_for('ui_providers_manage'))

    all_providers = handler.get_all_providers_for_ui()
    providers_display = [
        {
            "alias": alias,
            "url": details.get("url", ""),
            "max_concurrent_streams": details.get("max_concurrent_streams", 1),
            "active_streams": handler.active_streams_per_provider.get(alias, 0)
        } for alias, details in sorted(all_providers.items())
    ]
    return render_template("ui_providers.html", providers=providers_display)


# --- HTMX Partial Routes ---

@app.route("/ui/logical-channels/new-mapping-row")
def ui_get_new_mapping_row() -> str:
    """Returns an HTML partial for a new, empty mapping row for the UI."""
    channel_id = request.args.get('channel_id')
    current_channel = handler.get_logical_channel_by_user_id(channel_id)
    all_services = handler.get_all_discovered_source_services_for_ui()
    row_idx_for_names = int(time.time() * 1000) # Pseudo-unique index for field names
    return render_template('_mapping_row.html',
                           current_row_index=row_idx_for_names,
                           mapping=None, 
                           all_services=all_services,
                           channel=current_channel)


@app.route("/ui/remove-mapping-row-placeholder", methods=["DELETE"])
def ui_remove_mapping_row_placeholder() -> tuple[str, int]:
    """Returns an empty response for HTMX to remove a mapping row."""
    return "", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=True, use_reloader=False)