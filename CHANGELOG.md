# [3.0.0-rc.1](https://github.com/Fahmula/nexus-tuner/compare/v2.0.0...v3.0.0-rc.1) (2025-08-06)


### Bug Fixes

* **async:** Gracefully terminate ffprobe on task preemption ([c3ae8db](https://github.com/Fahmula/nexus-tuner/commit/c3ae8dbc5211f27b7a28411265034a2aeb392502))
* **async:** prevent deadlocks from RLock to Lock transition ([f36b62a](https://github.com/Fahmula/nexus-tuner/commit/f36b62a65d35e11a3c293f247f4859a4244427af))
* **async:** properly run tasks in background ([cb103ff](https://github.com/Fahmula/nexus-tuner/commit/cb103ffa4231f17d387c9dffb71acfe7e15cdd4b))
* **ci:** Ensure build job uses latest code for Docker imageThe build-and-push job was checking out the original commit thattriggered the workflow, not the new commit with the version bumpcreated by the preceding release job.This caused the Docker image to be built with a stale version file.This fix explicitly checks out the latest branch `ref` to get thecorrect, updated code. ([615ac5a](https://github.com/Fahmula/nexus-tuner/commit/615ac5ae9252f4cd6c91e49666e889892360461d))
* **cleanup:** add timestamps to files to prevent race conditions ([a87e595](https://github.com/Fahmula/nexus-tuner/commit/a87e5955433c417a8af8bab6ac2aedf79d4a4043))
* **create_stream:** gracefully handle stream validation cancellation ([acc82a3](https://github.com/Fahmula/nexus-tuner/commit/acc82a378e72a073db46c073fbc79a0a6830595e))
* **hdhomerun:** use NexusStream version ([dda805a](https://github.com/Fahmula/nexus-tuner/commit/dda805a908031cf806e0a7db920713dfcd7f97f1))
* **mpegts-stream:** ensure proper cleanup and update last access on client disconnect ([a616954](https://github.com/Fahmula/nexus-tuner/commit/a6169545f54aac03350c2b04a4c50da9be3a79da))
* **mpegts-stream:** use async context manager for stream process lock ([6a4d96e](https://github.com/Fahmula/nexus-tuner/commit/6a4d96eb58d4b7431a46db4c6b34729dc9f35e04))
* **mpegts:** don't timeout mpegts stream ([4d93833](https://github.com/Fahmula/nexus-tuner/commit/4d9383371841f018453393d1087edee0420de66f))
* **refactor:** duplicate lines ([144d89f](https://github.com/Fahmula/nexus-tuner/commit/144d89fe00387247f92871c63c303a5ebda21150))
* **refactor:** fix issues from refactor ([9b952be](https://github.com/Fahmula/nexus-tuner/commit/9b952be9cf9b64145e3e56df64c0538cef9cb838))
* Resolve race condition in provider slot management ([d23cdd7](https://github.com/Fahmula/nexus-tuner/commit/d23cdd7c5741c6aed020d92b6e1cca27f0e78aea))
* **server:** set worker count to 1 to ensure shared state ([624096e](https://github.com/Fahmula/nexus-tuner/commit/624096ee28886c7478eb8f01e94ac3b1839fe6cd))
* **session_monitor:** exclude preview HLS sessions from ghost session monitoring ([fdd3e7d](https://github.com/Fahmula/nexus-tuner/commit/fdd3e7d46d63f17e861cddef7d41777529d6a27a))
* **stream:** add hls key to active keys if process already process already exists and healthy. ([8bffeb8](https://github.com/Fahmula/nexus-tuner/commit/8bffeb8ab6da264630dfed9987ba21d3c559878d))
* **stream:** Address `ffmpeg_processes` dictionary modification issue ([24ccbde](https://github.com/Fahmula/nexus-tuner/commit/24ccbde49b8ca25baac49348cc72316e84f5db48))
* **stream:** allow dynamic HLS segment path retrieval ([027d704](https://github.com/Fahmula/nexus-tuner/commit/027d7042978ee2d98f07a78a04c574da6cc0da07))
* **stream:** create and close streams faster ([6cee145](https://github.com/Fahmula/nexus-tuner/commit/6cee1456807b5e54b056b8d49bae61f26a4baba2))
* **stream:** ensure resource slots are released when stopping HLS streams ([2f6dc7e](https://github.com/Fahmula/nexus-tuner/commit/2f6dc7e8161ed5d739941165f1b293cc29336352))
* **stream:** fix bugs in create_stream from async refactor ([7c1bfc9](https://github.com/Fahmula/nexus-tuner/commit/7c1bfc98baae984dd426d40f7a3e8a3987e7b4c6))
* **stream:** Prevent AttributeError on FFmpeg process termination ([3df4a7b](https://github.com/Fahmula/nexus-tuner/commit/3df4a7ba8b78b5d0cdac20102212dc8fc513d62a))
* **stream:** use event token for read blocking ([3325137](https://github.com/Fahmula/nexus-tuner/commit/33251372858ba04ce6e019a60b8a63a96c472fad))
* **stream:** use video_key for logging ([cf4c9b7](https://github.com/Fahmula/nexus-tuner/commit/cf4c9b77df3af5635f3e5e558ce09e8efeab3ee0))
* **typing:** singleton objects ([160d9cb](https://github.com/Fahmula/nexus-tuner/commit/160d9cbb71ff9641ed651b338879798f9c3c47f3))
* **ui:** footer stays at the bottom of the page ([c4e9bf0](https://github.com/Fahmula/nexus-tuner/commit/c4e9bf0462055f2ec33c1ee46f90f8510dce1330))
* **ui:** now showing spinners for long running buttons ([9d2c1e0](https://github.com/Fahmula/nexus-tuner/commit/9d2c1e07b109cd2eb1d4d9dfdc6f97b898627417))
* **ui:** remove gap above active streams ([ee32f40](https://github.com/Fahmula/nexus-tuner/commit/ee32f40d745c973da5674f9e79be720c6ca325de))
* **ui:** reset row edit on cancel ([83eb44d](https://github.com/Fahmula/nexus-tuner/commit/83eb44d72e7d03d2dcc3105ccb30d4763dfb3e27))
* **ui:** tooltips now disappear when no longer hovering ([5e76a3e](https://github.com/Fahmula/nexus-tuner/commit/5e76a3ee1308a1244637e66db84b478dcf2ecac9))
* **version:** include version in Dockerfile ([eab8362](https://github.com/Fahmula/nexus-tuner/commit/eab8362ca3eaf6b92dd4bd55876d4539e6533774))


### Features

* add MPEG-TS streaming and HDHomeRun emulation ([3b34cfa](https://github.com/Fahmula/nexus-tuner/commit/3b34cfa11f4a5f6d48194d24a5cc92b0d676ccd0))
* **api:** add ping endpoint ([72aeb62](https://github.com/Fahmula/nexus-tuner/commit/72aeb627cbc77a7e5612d5e638aade59bb37900a))
* **app:** initialize Quart application with async components and routes for HLS streaming ([be618c8](https://github.com/Fahmula/nexus-tuner/commit/be618c840d231d63bd2ef4d6a477e0c0add96618))
* **config:** add ability to backup config ([b8a8ef5](https://github.com/Fahmula/nexus-tuner/commit/b8a8ef5751332d01f5bed37fd50a0aeb9d6a086c))
* **config:** implement asynchronous configuration management and logging ([8a44056](https://github.com/Fahmula/nexus-tuner/commit/8a44056baf34f5431d5e370e8782d8f784df2ad7))
* **create-stream:** add support for generalized video stream types ([ff4f614](https://github.com/Fahmula/nexus-tuner/commit/ff4f6141c1cb893ae4988de87969db50b690abc7))
* Display application version in footer ([0b0854c](https://github.com/Fahmula/nexus-tuner/commit/0b0854c9e5e1a478f6f037b4863aa5f253b9a737))
* **handler:** implement asynchronous ChannelHandler for improved concurrency and provider management ([675d002](https://github.com/Fahmula/nexus-tuner/commit/675d002a458fbdef586751ecde0398b4d20bc7b4))
* **logging:** add labels and colors to logging messages ([6b9a676](https://github.com/Fahmula/nexus-tuner/commit/6b9a676921a36194be9bdb38abc479c3963f48ef))
* **monitor:** implement asynchronous QualityMonitor for non-blocking stream quality analysis ([af60c7b](https://github.com/Fahmula/nexus-tuner/commit/af60c7b300adf71e2709864d4d596c870f34f493))
* **monitor:** implement GhostSessionMonitor for asynchronous monitoring of ghost HLS streams ([1cde9be](https://github.com/Fahmula/nexus-tuner/commit/1cde9befa86862516dd18a96e76f260c179f838f))
* **scheduler:** add scheduler for tasks ([a6c00fc](https://github.com/Fahmula/nexus-tuner/commit/a6c00fc4a4350eebfb5a97be101fafa037a0893f))
* **slots:** implement asyncio-native ProviderSlots class for concurrent slot management ([7dd9024](https://github.com/Fahmula/nexus-tuner/commit/7dd90242453c0d48c2b216e0e1652b65b6235122))
* **sources:** automatically update mappings if stream url changes ([a866f32](https://github.com/Fahmula/nexus-tuner/commit/a866f3247edc0035ffbbb7c3fe51d9a679ac1d6c))
* **stream:** enhance _process_results method for improved stream selection and resource cleanup ([0e61b6d](https://github.com/Fahmula/nexus-tuner/commit/0e61b6df63322c065966a5ce1331d3ce5b6c5f0d))
* **stream:** enhance async stream creation with improved error handling and non-blocking operations ([58eefa4](https://github.com/Fahmula/nexus-tuner/commit/58eefa4241b8b24eb7b056948374b9d4fee5ebb4))
* **stream:** enhance non-blocking file operations with aiofiles and aioshutil for improved async performance ([c32cd71](https://github.com/Fahmula/nexus-tuner/commit/c32cd71b04838957d5bb899505624e20e6f542e8))
* **stream:** implement asynchronous HLS stream creation with FFmpeg and improved resource management ([8a50f10](https://github.com/Fahmula/nexus-tuner/commit/8a50f10c9a239ab2a5bbe763c19b376564f047c1))
* **stream:** recreate mpegts streams on failures with existing connection ([e351cc1](https://github.com/Fahmula/nexus-tuner/commit/e351cc1f6f0467f6aa58fe84990fbb4c4f348199))
* **stream:** refactor create_hls_ffmpeg_command and CreateHLSStream for improved async handling and task management ([dc53bfc](https://github.com/Fahmula/nexus-tuner/commit/dc53bfc46cb01be16ac0073b1d61094063121384))
* **stream:** refactor CreateHLSStream and ProviderSlots for improved async handling and accurate slot management ([1b26fc9](https://github.com/Fahmula/nexus-tuner/commit/1b26fc9b56ef8580f42e24db70031d9326765ac6))
* **stream:** refactor HLSStreamManager for asynchronous FFmpeg process management and cleanup ([5092b9c](https://github.com/Fahmula/nexus-tuner/commit/5092b9c11a256632f8ccbf900f0669afff3a3b6c))
* **stream:** support multiple connections to a mpegts stream ([0c00e34](https://github.com/Fahmula/nexus-tuner/commit/0c00e34e916e48458e66c0997198f7cc1ca70068))
* switch from gunicorn to uvicorn for async app execution and update requirements ([02c064a](https://github.com/Fahmula/nexus-tuner/commit/02c064a7fa0ebabd07066e5a2db3230d309f8599))
* **ui:** add analyze button for logical channel ([5d73dbd](https://github.com/Fahmula/nexus-tuner/commit/5d73dbd97164c759a03f1999137216488a30a12a))
* update NEXUS_STREAM_VERSION to read from VERSION file ([a5333ae](https://github.com/Fahmula/nexus-tuner/commit/a5333aecaaa9c1478b9d75e8362ba2e52fa7087a))

# [2.0.0](https://github.com/Fahmula/nexus-tuner/compare/v1.11.0...v2.0.0) (2025-07-20)


### Bug Fixes

* **mpegts:** update last_access on connection close ([#36](https://github.com/Fahmula/nexus-tuner/issues/36)) ([5ad8bfa](https://github.com/Fahmula/nexus-tuner/commit/5ad8bfadd70c06a66f847e38b0f589184b067f56))


### Features

* **ci:** enable manual release workflow ([#24](https://github.com/Fahmula/nexus-tuner/issues/24)) ([ba3115e](https://github.com/Fahmula/nexus-tuner/commit/ba3115e88f39eebd1f93cc8d0982acfa2f9eaf00))
* **ci:** Enhance release workflow with dynamic config and dry run ([#33](https://github.com/Fahmula/nexus-tuner/issues/33)) ([8cbffd7](https://github.com/Fahmula/nexus-tuner/commit/8cbffd756196e7b01a2add09e2c00a7ba8256a98))
* **hdhomerun:** add hdhomerun server support ([#34](https://github.com/Fahmula/nexus-tuner/issues/34)) ([8eee656](https://github.com/Fahmula/nexus-tuner/commit/8eee656cfb36622c9bbbfd28ec9a157f11dd2f1e))
* **quality_monitor:** automatically probe stream if metrics ([#26](https://github.com/Fahmula/nexus-tuner/issues/26)) ([e2bfea5](https://github.com/Fahmula/nexus-tuner/commit/e2bfea57204cefaf1c6d35ae8151f7bfb8e2415e))

# [2.0.0-rc.2](https://github.com/Fahmula/nexus-tuner/compare/v2.0.0-rc.1...v2.0.0-rc.2) (2025-06-28)


### Bug Fixes

* **mpegts:** update last_access on connection close ([#36](https://github.com/Fahmula/nexus-tuner/issues/36)) ([636bd8e](https://github.com/Fahmula/nexus-tuner/commit/636bd8e85639275955badb5d8486301176162aa7))

# [2.0.0-rc.1](https://github.com/Fahmula/nexus-tuner/compare/v1.11.0...v2.0.0-rc.1) (2025-06-26)


### Features

* **ci:** enable manual release workflow ([#24](https://github.com/Fahmula/nexus-tuner/issues/24)) ([ba3115e](https://github.com/Fahmula/nexus-tuner/commit/ba3115e88f39eebd1f93cc8d0982acfa2f9eaf00))
* **ci:** Enhance release workflow with dynamic config and dry run ([#33](https://github.com/Fahmula/nexus-tuner/issues/33)) ([8cbffd7](https://github.com/Fahmula/nexus-tuner/commit/8cbffd756196e7b01a2add09e2c00a7ba8256a98))
* **ci:** Streamline Docker image tagging and labeling ([ef2798c](https://github.com/Fahmula/nexus-tuner/commit/ef2798c14b1a3f04794370bc48dd213bd398a86f))
* **hdhomerun:** add hdhomerun server support ([#34](https://github.com/Fahmula/nexus-tuner/issues/34)) ([8eee656](https://github.com/Fahmula/nexus-tuner/commit/8eee656cfb36622c9bbbfd28ec9a157f11dd2f1e))
* **quality_monitor:** automatically probe stream if metrics ([#26](https://github.com/Fahmula/nexus-tuner/issues/26)) ([e2bfea5](https://github.com/Fahmula/nexus-tuner/commit/e2bfea57204cefaf1c6d35ae8151f7bfb8e2415e))

# [2.0.0-rc.1](https://github.com/Fahmula/nexus-tuner/compare/v1.11.0...v2.0.0-rc.1) (2025-06-26)


### Features

* **ci:** enable manual release workflow ([#24](https://github.com/Fahmula/nexus-tuner/issues/24)) ([ba3115e](https://github.com/Fahmula/nexus-tuner/commit/ba3115e88f39eebd1f93cc8d0982acfa2f9eaf00))
* **ci:** Enhance release workflow with dynamic config and dry run ([#33](https://github.com/Fahmula/nexus-tuner/issues/33)) ([8cbffd7](https://github.com/Fahmula/nexus-tuner/commit/8cbffd756196e7b01a2add09e2c00a7ba8256a98))
* **hdhomerun:** add hdhomerun server support ([#34](https://github.com/Fahmula/nexus-tuner/issues/34)) ([8eee656](https://github.com/Fahmula/nexus-tuner/commit/8eee656cfb36622c9bbbfd28ec9a157f11dd2f1e))
* **quality_monitor:** automatically probe stream if metrics ([#26](https://github.com/Fahmula/nexus-tuner/issues/26)) ([e2bfea5](https://github.com/Fahmula/nexus-tuner/commit/e2bfea57204cefaf1c6d35ae8151f7bfb8e2415e))

# [1.11.0](https://github.com/Fahmula/nexus-tuner/compare/v1.10.1...v1.11.0) (2025-06-16)


### Features

* **channel_list:** create channel_list.json if not exists ([#23](https://github.com/Fahmula/nexus-tuner/issues/23)) ([0320b3e](https://github.com/Fahmula/nexus-tuner/commit/0320b3e7f31da61a292dc2547d5a57b45ae823f2))

## [1.10.1](https://github.com/Fahmula/nexus-tuner/compare/v1.10.0...v1.10.1) (2025-06-16)


### Bug Fixes

* **handler:** readd get_all_logical_channels_for_ui ([#22](https://github.com/Fahmula/nexus-tuner/issues/22)) ([4a91351](https://github.com/Fahmula/nexus-tuner/commit/4a91351ca36924e27f0e2be41ce07f8a6c90f94c))

# [1.10.0](https://github.com/Fahmula/nexus-tuner/compare/v1.9.1...v1.10.0) (2025-06-16)


### Features

* **providers:** Implement full CRUD UI and logic ([#19](https://github.com/Fahmula/nexus-tuner/issues/19)) ([0da614d](https://github.com/Fahmula/nexus-tuner/commit/0da614da2a05c39104d73886de44cf4d8a932314))

## [1.9.1](https://github.com/Fahmula/nexus-tuner/compare/v1.9.0...v1.9.1) (2025-06-15)


### Bug Fixes

* **hls:** Prevent stream startup race condition with atomic lock ([#17](https://github.com/Fahmula/nexus-tuner/issues/17)) ([8523ec1](https://github.com/Fahmula/nexus-tuner/commit/8523ec139f64090e0fcff760b66cb5d1e190fcd6))

# [1.9.0](https://github.com/Fahmula/nexus-tuner/compare/v1.8.0...v1.9.0) (2025-06-15)


### Features

* Implement assisted logical channel creation workflow ([#16](https://github.com/Fahmula/nexus-tuner/issues/16)) ([bc303a0](https://github.com/Fahmula/nexus-tuner/commit/bc303a0a2042f4367645909a018e6fcede16c01c))

# [1.8.0](https://github.com/Fahmula/nexus-tuner/compare/v1.7.1...v1.8.0) (2025-06-13)


### Features

* add pagination and limit controls to Source Services ([#14](https://github.com/Fahmula/nexus-tuner/issues/14)) ([c9260d5](https://github.com/Fahmula/nexus-tuner/commit/c9260d5220cd8f6fdae1a7c06c928aae0ef17024))

## [1.7.1](https://github.com/Fahmula/nexus-tuner/compare/v1.7.0...v1.7.1) (2025-06-12)


### Bug Fixes

* Restore correct docstring for ui_source_services_list ([#13](https://github.com/Fahmula/nexus-tuner/issues/13)) ([211d660](https://github.com/Fahmula/nexus-tuner/commit/211d6609eb945a8db14bcb5ec98f027a0e8aca4f))

# [1.7.0](https://github.com/Fahmula/nexus-tuner/compare/v1.6.1...v1.7.0) (2025-06-12)


### Features

* Remove Wishlist functionality ([#12](https://github.com/Fahmula/nexus-tuner/issues/12)) ([a4634fc](https://github.com/Fahmula/nexus-tuner/commit/a4634fc3c6de19ff2aa27cbbe4fccfe2509ee3ca))

## [1.6.1](https://github.com/Fahmula/nexus-tuner/compare/v1.6.0...v1.6.1) (2025-06-12)


### Bug Fixes

* **ui:** Prevent loss of subsequent mappings on update ([#10](https://github.com/Fahmula/nexus-tuner/issues/10)) ([b28f4a2](https://github.com/Fahmula/nexus-tuner/commit/b28f4a2b17506f6abeff61cb3e6fc8d996804828))

# [1.6.0](https://github.com/Fahmula/nexus-tuner/compare/v1.5.0...v1.6.0) (2025-06-12)


### Features

* **ui:** implement log viewer modal ([#9](https://github.com/Fahmula/nexus-tuner/issues/9)) ([2f8f87c](https://github.com/Fahmula/nexus-tuner/commit/2f8f87c6e957159e27af06068de480ab47100621))

# [1.5.0](https://github.com/Fahmula/nexus-tuner/compare/v1.4.0...v1.5.0) (2025-06-12)


### Features

* **ui:** aggregate provider status into a single total ([#8](https://github.com/Fahmula/nexus-tuner/issues/8)) ([cae132f](https://github.com/Fahmula/nexus-tuner/commit/cae132f06658535f3442c5e2c5fa567924ff1b6f))

# [1.4.0](https://github.com/Fahmula/nexus-tuner/compare/v1.3.0...v1.4.0) (2025-06-12)


### Features

* **ui:** implement provider status bar ([07c0f8a](https://github.com/Fahmula/nexus-tuner/commit/07c0f8a18286d0dee277ca53e008ea0d768ac871))

# [1.3.0](https://github.com/Fahmula/nexus-tuner/compare/v1.2.0...v1.3.0) (2025-06-12)


### Features

* **ui:** Implement dark mode theme switcher ([#6](https://github.com/Fahmula/nexus-tuner/issues/6)) ([00816b0](https://github.com/Fahmula/nexus-tuner/commit/00816b03ac1a08ba2a1e157c6d16857c01ebb545))

# [1.2.0](https://github.com/Fahmula/nexus-tuner/compare/v1.1.1...v1.2.0) (2025-06-12)


### Features

* **providers:** update UI for managing providers ([#5](https://github.com/Fahmula/nexus-tuner/issues/5)) ([9c939aa](https://github.com/Fahmula/nexus-tuner/commit/9c939aa64bc6ad4d85385eac8f17bd4a22f6e211))

## [1.1.1](https://github.com/Fahmula/nexus-tuner/compare/v1.1.0...v1.1.1) (2025-06-11)


### Bug Fixes

* **logging:** append to ffmpeg logs and reduce retention to 24h ([#4](https://github.com/Fahmula/nexus-tuner/issues/4)) ([63b47ed](https://github.com/Fahmula/nexus-tuner/commit/63b47edf19a7b2d4fd4aafdf2d1c8d7d9668ec99))

# [1.1.0](https://github.com/Fahmula/nexus-tuner/compare/v1.0.0...v1.1.0) (2025-06-11)


### Features

* **wishlist:** add UI for managing wishlist ([#3](https://github.com/Fahmula/nexus-tuner/issues/3)) ([50a641f](https://github.com/Fahmula/nexus-tuner/commit/50a641f696e2c023b4bb4f8d60889be094e8fc74))

# 1.0.0 (2025-06-11)


### Features

* initial release ([efa7eb6](https://github.com/Fahmula/nexus-tuner/commit/efa7eb68a8ee76f616b3584273d3fa5fc3924a61))
