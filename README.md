# NexusStream

![Latest Release](https://img.shields.io/github/v/release/Fahmula/nexus-stream?style=for-the-badge&logo=github)
![License](https://img.shields.io/github/license/Fahmula/nexus-stream?style=for-the-badge)

NexusStream is a smart, self-hosted proxy for your IPTV providers. It aggregates multiple M3U sources, provides a stable, unified playlist for your clients (like Plex, Jellyfin, or Emby), and adds powerful features like automatic stream failover, concurrent stream limiting, and ghost session cleanup.

## Key Features

-   **M3U Aggregation:** Combine multiple IPTV provider M3U files into a single, unified channel lineup.
-   **Automatic Failover:** Map multiple source streams to a single logical channel with priority. If the highest priority stream fails, NexusStream automatically switches to the next one.
-   **Concurrent Stream Limiting:** Set a maximum number of concurrent streams allowed per provider to avoid service interruptions.
-   **On-the-Fly HLS Transcoding:** All streams are processed via FFmpeg to produce a standardized HLS format, improving client compatibility.
-   **Ghost Session Cleanup:** Integrates with Emby & Jellyfin to detect and terminate "ghost" streams—FFmpeg processes that are still running after a client has disconnected improperly.
-   **Web-Based UI:** An intuitive web interface to manage providers, logical channels, and source mappings.

## Getting Started

NexusStream is designed to be run as a Docker container.

### Prerequisites

-   Git
-   Docker & Docker Compose

### 1. Clone the Repository

```bash
git clone https://github.com/Fahmula/nexus-stream.git
cd nexus-stream
```

### 2. Create the Configuration File
Copy the example environment file and update variables.
```bash
cp env.example .env
```

### 3. Run with Docker Compose
```bash
docker-compose up -d
```

The application will now be running and accessible.
### 4. Initial Setup in the UI
-   Open a web browser and navigate to your BASE_URL (e.g., http://your-server-ip).
-   Navigate to the Providers page and add the URLs for your M3U playlists.
-   Go to the Logical Channels page to create your channels.
-   Edit each logical channel to map the discovered source streams to it, setting priorities for failover.

## Usage
Once configured, your unified, smart M3U playlist is available at:
http://your-server-ip/playlist.m3u
Use this URL in your client applications (Plex, Emby, Jellyfin etc.).

## Development
Contributions are welcome! This project follows the Conventional Commits specification for all commit messages. This standard is enforced to automate versioning and changelog generation.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
