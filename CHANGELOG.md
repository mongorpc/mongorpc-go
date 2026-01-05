# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-01-05

### Added
- **Admin SDK**: Full implementation of `AdminClient` for privileged operations.
  - `CreateIndex`, `DropCollection`, `CreateCollection`.
- **Change Streams**: Implemented `OnQuerySnapshot` for real-time query updates.
- **Bulk Operations**: Added `BulkWrite` support.
- **Real-time**: Robust `OnSnapshot` and `OnQuerySnapshot` support with `updateLookup` logic.

### Changed
- **API**: Refactored `Client` to standard `Connect` pattern.
- **Dependencies**: Updated to latest generated protos from `mongorpc`.

### Fixed
- **Sync Issues**: Fixed data coherence in real-time listeners.
