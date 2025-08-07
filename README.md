# NexusTuner

[![Latest Release](https://img.shields.io/github/v/release/Fahmula/nexus-tuner?style=for-the-badge&logo=github)](https://github.com/Fahmula/nexus-tuner/releases/latest)
[![License](https://img.shields.io/github/license/Fahmula/nexus-tuner?style=for-the-badge)](https://github.com/Fahmula/nexus-tuner/blob/main/LICENSE)

`NexusTuner` is a smart, self-hosted IPTV proxy that aggregates multiple M3U sources into a single, stable, and feature-rich playlist for your clients.

> [!TIP]
> **Key Features:**
> * **Web-Based UI:** A modern, user-friendly UI to manage providers, create logical channels, map sources, and view application logs. Mapping channels has never been easier with automatic suggestions and prefilled data to make the setup process frictionless. Additionally, sources mapped to other logical channels will be marked as `In Use` or `Duplicated` if it's also mapped to the current channel.
> * **M3U Aggregation:** Combine multiple IPTV provider playlists into one unified and consistent channel lineup. Each provider can be configured with their maximum concurrent streams, which `NexusTuner` will manage intelligently.
> * **`In-Browser Previews:`** Easily preview streams directly in your browser while configuring channels or from the `Sources` page. No more guesswork and fiddling with external players to check stream region and language.
> * **`Automatic Quality Monitoring:`** `NexusTuner` continuously monitors the health of each source along with its resolution, bitrate, and framerate. No more endless tinkering to find the best source, just select them all and `NexusTuner` automatically chooses the highest quality and most reliable stream from your configured sources when a channel is requested.
> * **`Fast & Reliable Streams:`** When a channel is requested, `NexusTuner` starts multiple mapped sources in parallel (respecting each provider's limit). It then quickly selects the best healthy stream based on priority and quality, leading to faster, more reliable, and more consistent channel startup times. Additionally, it will automatically switch to another source if the current one fails, ensuring uninterrupted viewing.
> * **`Dead Source Detection:`** Automatically detects dead mapped sources (e.g. stream url changed) and intelligently tries to remap them based on their tvg data. If it's not possible, channels with dead sources will be marked in the UI and logs with a convenient button to remove them.
> * **`On-the-Fly Remux:`** All streams are processed via FFmpeg to produce an HLS or MPEGTS format, maximizing compatibility across a wide range of client devices and applications. Each stream type also supports sharing the same FFmpeg process for multiple simultaneous connections automatically, reducing resource usage and provider slot usage.
> * **HDHomeRun Server:** `NexusTuner` can act as an HDHomeRun server, allowing you to use your IPTV channels with compatible clients that support HDHomeRun such as Plex, Emby, Jellyfin, and more.
> * **Ghost Session Cleanup:** Integrates with Emby and Jellyfin to detect and terminate "ghost" streams, FFmpeg processes that are still running after a client has disconnected improperly, freeing up valuable provider slots.

<img width="1920" height="911" alt="NexusTuner Dashboard" src="public/screenshots/dashboard.png"/>

<img width="1920" height="911" alt="NexusTuner Logical Channels" src="public/screenshots/logical_channels.png"/>

<img width="1920" height="911" alt="NexusTuner Edit Logical Channel Preview" src="public/screenshots/logical_channels_edit_preview.png"/>

## Getting Started

> [!NOTE]
> This application can be run using docker or directly using python.

### Install `NexusTuner`

#### With Docker

Use the compose file and instructions provided here: [docker-compose.yml](docker-compose.yml).

#### Without Docker

For non-docker users, install python 3.13 or later and run the following commands:

```bash
git clone https://github.com/Fahmula/nexus-tuner.git
cd nexus-tuner
python3.13 -m venv venv

source venv/bin/activate  # Linux/macOS
venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

> [!IMPORTANT]
> You'll need to install FFmpeg separately on your system, you'll be able to configure the path to
> FFmpeg in the `.env` file later. FFprobe will be inferred from the FFmpeg path.

### Configuring `NexusTuner`

You can set the environment variables on your system, in the docker compose file, or in the `.env` file.
> [!WARNING]
> Values from your system or the docker compose file will override the `.env` file.

#### Create the Environment File

For docker users, your config directory will be your mount to `/config`.

For non-docker users, create a directory anywhere on your system to use as your config directory.

Copy the example environment file at [env.example](env.example) to `config/.env`.

#### Edit the Environment File

There are two baseline options you need to set:

1. **`NEXUS_URL`**: The base URL where your `NexusTuner` instance will be accessible.
2. **`NEXUS_PORT`**: The port `NexusTuner` will listen on

Non-docker users will also need to set two more options:

3. **`NEXUS_CONFIG_DIR`**: The directory where `NexusTuner` will store its configuration files.
4. **`NEXUS_FFMPEG_PATH`**: The path to the FFmpeg executable on your system.

Here are all the available environment variables:

| Variable                              | Description                                                                                             | Example                               |
| :------------------------------------ | :------------------------------------------------------------------------------------------------------ | :------------------------------------ |
| **`NEXUS_CONFIG_DIR`** (Required)     | The directory where `NexusTuner` will store its configuration files.                                    | `/path/to/config/folder/nexus-tuner`  |
| **`NEXUS_FFMPEG_PATH`** (Required)    | The path to the FFmpeg executable on your system. Used for processing streams.                          | `/usr/bin/ffmpeg`                     |
| **`NEXUS_URL`** (Required)            | The full URL of your `NexusTuner` instance, without the port. Used for UI and building playlist URLs.   | `http://192.168.1.100`                |
| `NEXUS_PORT`                          | The internal port the application listens on.                                                           | `4040`                                |
| `NEXUS_FFMPEG_INACTIVITY_TIMEOUT`     | Seconds of inactivity before a stream is stopped. Inactive streams will be pruned earlier when needed.  | `900`                                 |
| `NEXUS_FFMPEG_LOGS_RETENTION_SECONDS` | How long to keep FFmpeg logs in seconds.                                                                | `86400`                               |
| `NEXUS_LOG_LEVEL`                     | Sets the logging verbosity. Options: `DEBUG`, `INFO`, `WARNING`, `ERROR`.                               | `INFO`                                |
| `NEXUS_LOG_BACKUP_COUNT`              | Number of days of log backups to keep.                                                                  | `7`                                   |
| `NEXUS_BACKUP_COUNT`                  | Number of days of config backups to keep. Each backup is `<1MB` in size.                                | `30`                                  |
| `NEXUS_GHOST_SESSION_CHECK_INTERVAL`  | Interval in seconds to check Jellyfin/Emby servers for ghost sessions.                                  | `60`                                  |
| `NEXUS_EMBY_URL`                      | (Optional) URL for your Emby server to enable ghost session cleanup.                                    | `http://emby.local:8096`              |
| `NEXUS_EMBY_API_KEY`                  | (Optional) API key for your Emby server.                                                                | `your_emby_api_key`                   |
| `NEXUS_JELLYFIN_URL`                  | (Optional) URL for your Jellyfin server to enable ghost session cleanup.                                | `http://jellyfin.local:8096`          |
| `NEXUS_JELLYFIN_API_KEY`              | (Optional) API key for your Jellyfin server.                                                            | `your_jellyfin_api_key`               |

### Run `NexusTuner`

#### With Docker

Run the application using the docker-compose.yml, this will build the image and start the container in the background:

```bash
docker compose up -d
```

#### Without Docker

> [!IMPORTANT]
> Ensure you are in the directory you cloned earlier that contains `app.py`.
> You do not need to activate the virtual environment for running the application.

Run the following command, replacing `NEXUS_PORT` and `NEXUS_CONFIG_DIR` with the values you set in your `.env` file:

```bash
venv/bin/uvicorn app:app --workers 1 --log-level warning --host 0.0.0.0 --port NEXUS_PORT --env-file "NEXUS_CONFIG_DIR/.env"
```

Schedule `NexusTuner` to run on startup using your system's service manager (e.g. systemd, launchd, Task Scheduler).

### Configuring Channels

> [!TIP]
> On your IPTV service, select all the channels that correspond to the regions and languages you want to include in your unified playlist.
> You will map specific sources to your configured logical channels in `NexusTuner`, perform only the bare minimum filtering on your IPTV
> service (e.g. only US channels) for the best experience.

* Open a web browser and navigate to the `NEXUS_URL` and `NEXUS_PORT` you configured (e.g. `http://192.168.1.100:4040`).
* Navigate to the **Providers** page and add your M3U source providers.
  * Return to the Dashboard and click `Full Reload` to discover the sources from your configured providers.
* Navigate to the **Logical Channels** page to create your desired channel lineup.
  * As you type in the channel name, `NexusTuner` will display suggestions to prefill the channel data.
  * You can map as many sources as you like. You can also preview each source to ensure they are correct.
  * If a source is mapped to another logical channel, it will be marked as `In Use` or `Duplicated` if it's also mapped to the current channel.
  * You can optionally trigger an early run of `Quality Monitor` for this channel only if you'd like to start using it immediately.

### Configuring Your Clients

> [!NOTE]
> `NexusTuner` supports unlimited simultaneous streams for the same channel, even if your client does not support it natively. Also, if a source dies during viewing, it will automatically failover to the next best source.

The best way to use `NexusTuner` is through the `HDHomeRun` interface, which is supported by many clients including Plex, Emby, and Jellyfin.
`NexusTuner` should automatically be detected by your client applications if they are on the same network. Otherwise, follow the instructions
for your specific client application to add an `HDHomeRun` tuner by specifying the `NEXUS_URL` and `NEXUS_PORT`.

`NexusTuner` also provides a standard M3U playlist that can be used in any client application that supports it. For an application
like VLC, you can use the M3U playlist URL: **`<NEXUS_URL>:<NEXUS_PORT>/playlist.m3u`**

## Development

Contributions are welcome! This project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for all commit messages to ensure a clear and automated versioning and changelog process.

## License

This project is licensed under the MIT License.
