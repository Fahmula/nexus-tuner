# NexusStream

[![Latest Release](https://img.shields.io/github/v/release/Fahmula/nexus-stream?style=for-the-badge&logo=github)](https://github.com/Fahmula/nexus-stream/releases/latest)
[![License](https://img.shields.io/github/license/Fahmula/nexus-stream?style=for-the-badge)](https://github.com/Fahmula/nexus-stream/blob/main/LICENSE)

NexusStream is a smart, self-hosted IPTV proxy that empowers you to aggregate multiple M3U sources into a single, stable, and feature-rich playlist for your clients (like Plex, Jellyfin, or Emby). It solves common IPTV pain points by introducing powerful features like reliable parallel stream initiation, provider-level concurrent stream limiting, and intelligent session management, all controlled through an intuitive web interface.

## Key Features

*   **M3U Aggregation:** Combine multiple IPTV provider playlists into one unified and consistent channel lineup.
*   **Reliable & Fast Stream Startup:** When a channel is requested, NexusStream attempts to start multiple mapped sources in parallel (up to the provider's limit). It then instantly selects the best healthy stream based on quality and priority, leading to faster and more reliable channel starts.
*   **Concurrent Stream Limiting:** Set a maximum number of simultaneous streams allowed for each provider to prevent account locks or service interruptions.
*   **Web-Based UI:** A modern, user-friendly interface to manage providers, create logical channels, map sources, and view application logs.
*   **On-the-Fly HLS Remux:** All streams are processed via FFmpeg to produce a standardized HLS format, maximizing compatibility across a wide range of client devices and applications.
*   **Ghost Session Cleanup:** Integrates with Emby and Jellyfin to detect and terminate "ghost" streams—FFmpeg processes that are still running after a client has disconnected improperly, freeing up valuable provider slots.

## Getting Started

This project is designed to be run as a Docker container, which is the recommended method for deployment.

### Prerequisites

*   [Git](https://git-scm.com/)
*   [Docker](https://www.docker.com/products/docker-desktop/)
*   [Docker Compose](https://docs.docker.com/compose/install/)

### Installation

1.  **Clone the Repository**

    Clone the NexusStream repository to your local machine.

    ```bash
    git clone https://github.com/Fahmula/nexus-stream.git
    cd nexus-stream
    ```

2.  **Create Configuration Directory**

    The application stores all its data and configuration in a dedicated directory.

    ```bash
    mkdir -p config
    ```

3.  **Create Environment File**

    Copy the example environment file into your new `config` directory. This file controls the core settings of the application.

    ```bash
    cp env.example config/.env
    ```

4.  **Configure Environment Variables**

    Open `config/.env` with a text editor and customize the variables. **Crucially**, you must set `NEXUS_URL`.

    | Variable                              | Description                                                                                             | Example                               |
    | :------------------------------------ | :------------------------------------------------------------------------------------------------------ | :------------------------------------ |
    | **`NEXUS_URL`** (Required)            | The full public URL of your NexusStream instance, including the port. Used to build playlist URLs.        | `http://192.168.1.100:4040`             |
    | `NEXUS_PORT`                          | The internal port the application listens on. Default is `4040`.                                        | `4040`                                |
    | `NEXUS_LOG_LEVEL`                     | Sets the logging verbosity. Options: `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default is `INFO`.              | `DEBUG`                               |
    | `NEXUS_GHOST_SESSION_CHECK_INTERVAL`  | Interval in seconds to check media servers for ghost sessions. Default is `60`.                         | `120`                                 |
    | `NEXUS_FFMPEG_HLS_INACTIVITY_TIMEOUT` | Seconds of inactivity before an HLS stream is automatically stopped. Default is `900`.                    | `900`                                 |
    | `NEXUS_EMBY_URL`                      | (Optional) URL for your Emby server to enable ghost session cleanup.                                      | `http://emby.local:8096`              |
    | `NEXUS_EMBY_API_KEY`                  | (Optional) API key for your Emby server.                                                                | `your_emby_api_key`                   |
    | `NEXUS_JELLYFIN_URL`                  | (Optional) URL for your Jellyfin server to enable ghost session cleanup.                                  | `http://jellyfin.local:8096`          |
    | `NEXUS_JELLYFIN_API_KEY`              | (Optional) API key for your Jellyfin server.                                                            | `your_jellyfin_api_key`               |

5.  **Launch the Application**

    Run the application using Docker Compose. This will build the image and start the container in the background.

    ```bash
    docker-compose up -d
    ```

6.  **Initial UI Setup**

    *   Open a web browser and navigate to the `NEXUS_URL` you configured (e.g., `http://192.168.1.100:4040`).
    *   Navigate to the **Providers** page and add your M3U source providers.
    *   Navigate to the **Logical Channels** page to create your desired channel lineup.
    *   For each logical channel, click **Edit** to map the discovered source streams to it, setting priorities for failover.

## Usage

Once you have configured your providers and logical channels, your unified and intelligent M3U playlist is available at:

**`http://<your-nexus-url>/playlist.m3u`**

Use this single URL in your client applications (Plex, Emby, Jellyfin, TiviMate, etc.) to access all your channels.

## Development

Contributions are welcome! This project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for all commit messages to ensure a clear and automated versioning and changelog process.

## License

This project is licensed under the MIT License.