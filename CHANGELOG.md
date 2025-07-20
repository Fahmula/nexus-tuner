# [2.0.0](https://github.com/Fahmula/nexus-stream/compare/v1.11.0...v2.0.0) (2025-07-20)


### Bug Fixes

* **mpegts:** update last_access on connection close ([#36](https://github.com/Fahmula/nexus-stream/issues/36)) ([5ad8bfa](https://github.com/Fahmula/nexus-stream/commit/5ad8bfadd70c06a66f847e38b0f589184b067f56))


### Features

* **ci:** enable manual release workflow ([#24](https://github.com/Fahmula/nexus-stream/issues/24)) ([ba3115e](https://github.com/Fahmula/nexus-stream/commit/ba3115e88f39eebd1f93cc8d0982acfa2f9eaf00))
* **ci:** Enhance release workflow with dynamic config and dry run ([#33](https://github.com/Fahmula/nexus-stream/issues/33)) ([8cbffd7](https://github.com/Fahmula/nexus-stream/commit/8cbffd756196e7b01a2add09e2c00a7ba8256a98))
* **hdhomerun:** add hdhomerun server support ([#34](https://github.com/Fahmula/nexus-stream/issues/34)) ([8eee656](https://github.com/Fahmula/nexus-stream/commit/8eee656cfb36622c9bbbfd28ec9a157f11dd2f1e))
* **quality_monitor:** automatically probe stream if metrics ([#26](https://github.com/Fahmula/nexus-stream/issues/26)) ([e2bfea5](https://github.com/Fahmula/nexus-stream/commit/e2bfea57204cefaf1c6d35ae8151f7bfb8e2415e))

# [2.0.0-rc.2](https://github.com/Fahmula/nexus-stream/compare/v2.0.0-rc.1...v2.0.0-rc.2) (2025-06-28)


### Bug Fixes

* **mpegts:** update last_access on connection close ([#36](https://github.com/Fahmula/nexus-stream/issues/36)) ([636bd8e](https://github.com/Fahmula/nexus-stream/commit/636bd8e85639275955badb5d8486301176162aa7))

# [2.0.0-rc.1](https://github.com/Fahmula/nexus-stream/compare/v1.11.0...v2.0.0-rc.1) (2025-06-26)


### Features

* **ci:** enable manual release workflow ([#24](https://github.com/Fahmula/nexus-stream/issues/24)) ([ba3115e](https://github.com/Fahmula/nexus-stream/commit/ba3115e88f39eebd1f93cc8d0982acfa2f9eaf00))
* **ci:** Enhance release workflow with dynamic config and dry run ([#33](https://github.com/Fahmula/nexus-stream/issues/33)) ([8cbffd7](https://github.com/Fahmula/nexus-stream/commit/8cbffd756196e7b01a2add09e2c00a7ba8256a98))
* **ci:** Streamline Docker image tagging and labeling ([ef2798c](https://github.com/Fahmula/nexus-stream/commit/ef2798c14b1a3f04794370bc48dd213bd398a86f))
* **hdhomerun:** add hdhomerun server support ([#34](https://github.com/Fahmula/nexus-stream/issues/34)) ([8eee656](https://github.com/Fahmula/nexus-stream/commit/8eee656cfb36622c9bbbfd28ec9a157f11dd2f1e))
* **quality_monitor:** automatically probe stream if metrics ([#26](https://github.com/Fahmula/nexus-stream/issues/26)) ([e2bfea5](https://github.com/Fahmula/nexus-stream/commit/e2bfea57204cefaf1c6d35ae8151f7bfb8e2415e))

# [2.0.0-rc.1](https://github.com/Fahmula/nexus-stream/compare/v1.11.0...v2.0.0-rc.1) (2025-06-26)


### Features

* **ci:** enable manual release workflow ([#24](https://github.com/Fahmula/nexus-stream/issues/24)) ([ba3115e](https://github.com/Fahmula/nexus-stream/commit/ba3115e88f39eebd1f93cc8d0982acfa2f9eaf00))
* **ci:** Enhance release workflow with dynamic config and dry run ([#33](https://github.com/Fahmula/nexus-stream/issues/33)) ([8cbffd7](https://github.com/Fahmula/nexus-stream/commit/8cbffd756196e7b01a2add09e2c00a7ba8256a98))
* **hdhomerun:** add hdhomerun server support ([#34](https://github.com/Fahmula/nexus-stream/issues/34)) ([8eee656](https://github.com/Fahmula/nexus-stream/commit/8eee656cfb36622c9bbbfd28ec9a157f11dd2f1e))
* **quality_monitor:** automatically probe stream if metrics ([#26](https://github.com/Fahmula/nexus-stream/issues/26)) ([e2bfea5](https://github.com/Fahmula/nexus-stream/commit/e2bfea57204cefaf1c6d35ae8151f7bfb8e2415e))

# [1.11.0](https://github.com/Fahmula/nexus-stream/compare/v1.10.1...v1.11.0) (2025-06-16)


### Features

* **channel_list:** create channel_list.json if not exists ([#23](https://github.com/Fahmula/nexus-stream/issues/23)) ([0320b3e](https://github.com/Fahmula/nexus-stream/commit/0320b3e7f31da61a292dc2547d5a57b45ae823f2))

## [1.10.1](https://github.com/Fahmula/nexus-stream/compare/v1.10.0...v1.10.1) (2025-06-16)


### Bug Fixes

* **handler:** readd get_all_logical_channels_for_ui ([#22](https://github.com/Fahmula/nexus-stream/issues/22)) ([4a91351](https://github.com/Fahmula/nexus-stream/commit/4a91351ca36924e27f0e2be41ce07f8a6c90f94c))

# [1.10.0](https://github.com/Fahmula/nexus-stream/compare/v1.9.1...v1.10.0) (2025-06-16)


### Features

* **providers:** Implement full CRUD UI and logic ([#19](https://github.com/Fahmula/nexus-stream/issues/19)) ([0da614d](https://github.com/Fahmula/nexus-stream/commit/0da614da2a05c39104d73886de44cf4d8a932314))

## [1.9.1](https://github.com/Fahmula/nexus-stream/compare/v1.9.0...v1.9.1) (2025-06-15)


### Bug Fixes

* **hls:** Prevent stream startup race condition with atomic lock ([#17](https://github.com/Fahmula/nexus-stream/issues/17)) ([8523ec1](https://github.com/Fahmula/nexus-stream/commit/8523ec139f64090e0fcff760b66cb5d1e190fcd6))

# [1.9.0](https://github.com/Fahmula/nexus-stream/compare/v1.8.0...v1.9.0) (2025-06-15)


### Features

* Implement assisted logical channel creation workflow ([#16](https://github.com/Fahmula/nexus-stream/issues/16)) ([bc303a0](https://github.com/Fahmula/nexus-stream/commit/bc303a0a2042f4367645909a018e6fcede16c01c))

# [1.8.0](https://github.com/Fahmula/nexus-stream/compare/v1.7.1...v1.8.0) (2025-06-13)


### Features

* add pagination and limit controls to Source Services ([#14](https://github.com/Fahmula/nexus-stream/issues/14)) ([c9260d5](https://github.com/Fahmula/nexus-stream/commit/c9260d5220cd8f6fdae1a7c06c928aae0ef17024))

## [1.7.1](https://github.com/Fahmula/nexus-stream/compare/v1.7.0...v1.7.1) (2025-06-12)


### Bug Fixes

* Restore correct docstring for ui_source_services_list ([#13](https://github.com/Fahmula/nexus-stream/issues/13)) ([211d660](https://github.com/Fahmula/nexus-stream/commit/211d6609eb945a8db14bcb5ec98f027a0e8aca4f))

# [1.7.0](https://github.com/Fahmula/nexus-stream/compare/v1.6.1...v1.7.0) (2025-06-12)


### Features

* Remove Wishlist functionality ([#12](https://github.com/Fahmula/nexus-stream/issues/12)) ([a4634fc](https://github.com/Fahmula/nexus-stream/commit/a4634fc3c6de19ff2aa27cbbe4fccfe2509ee3ca))

## [1.6.1](https://github.com/Fahmula/nexus-stream/compare/v1.6.0...v1.6.1) (2025-06-12)


### Bug Fixes

* **ui:** Prevent loss of subsequent mappings on update ([#10](https://github.com/Fahmula/nexus-stream/issues/10)) ([b28f4a2](https://github.com/Fahmula/nexus-stream/commit/b28f4a2b17506f6abeff61cb3e6fc8d996804828))

# [1.6.0](https://github.com/Fahmula/nexus-stream/compare/v1.5.0...v1.6.0) (2025-06-12)


### Features

* **ui:** implement log viewer modal ([#9](https://github.com/Fahmula/nexus-stream/issues/9)) ([2f8f87c](https://github.com/Fahmula/nexus-stream/commit/2f8f87c6e957159e27af06068de480ab47100621))

# [1.5.0](https://github.com/Fahmula/nexus-stream/compare/v1.4.0...v1.5.0) (2025-06-12)


### Features

* **ui:** aggregate provider status into a single total ([#8](https://github.com/Fahmula/nexus-stream/issues/8)) ([cae132f](https://github.com/Fahmula/nexus-stream/commit/cae132f06658535f3442c5e2c5fa567924ff1b6f))

# [1.4.0](https://github.com/Fahmula/nexus-stream/compare/v1.3.0...v1.4.0) (2025-06-12)


### Features

* **ui:** implement provider status bar ([07c0f8a](https://github.com/Fahmula/nexus-stream/commit/07c0f8a18286d0dee277ca53e008ea0d768ac871))

# [1.3.0](https://github.com/Fahmula/nexus-stream/compare/v1.2.0...v1.3.0) (2025-06-12)


### Features

* **ui:** Implement dark mode theme switcher ([#6](https://github.com/Fahmula/nexus-stream/issues/6)) ([00816b0](https://github.com/Fahmula/nexus-stream/commit/00816b03ac1a08ba2a1e157c6d16857c01ebb545))

# [1.2.0](https://github.com/Fahmula/nexus-stream/compare/v1.1.1...v1.2.0) (2025-06-12)


### Features

* **providers:** update UI for managing providers ([#5](https://github.com/Fahmula/nexus-stream/issues/5)) ([9c939aa](https://github.com/Fahmula/nexus-stream/commit/9c939aa64bc6ad4d85385eac8f17bd4a22f6e211))

## [1.1.1](https://github.com/Fahmula/nexus-stream/compare/v1.1.0...v1.1.1) (2025-06-11)


### Bug Fixes

* **logging:** append to ffmpeg logs and reduce retention to 24h ([#4](https://github.com/Fahmula/nexus-stream/issues/4)) ([63b47ed](https://github.com/Fahmula/nexus-stream/commit/63b47edf19a7b2d4fd4aafdf2d1c8d7d9668ec99))

# [1.1.0](https://github.com/Fahmula/nexus-stream/compare/v1.0.0...v1.1.0) (2025-06-11)


### Features

* **wishlist:** add UI for managing wishlist ([#3](https://github.com/Fahmula/nexus-stream/issues/3)) ([50a641f](https://github.com/Fahmula/nexus-stream/commit/50a641f696e2c023b4bb4f8d60889be094e8fc74))

# 1.0.0 (2025-06-11)


### Features

* initial release ([efa7eb6](https://github.com/Fahmula/nexus-stream/commit/efa7eb68a8ee76f616b3584273d3fa5fc3924a61))
