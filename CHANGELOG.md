# Changelog

## [1.1.0] - 2026-01-15
### Added
- Parallel extraction with ThreadPoolExecutor
- HS code enrichment transformer (70+ chapters)
- ISO country code normalization

### Changed
- PostgreSQL loader refactored for ON CONFLICT upsert support
- Batch size optimization: 86K records/sec sustained throughput

## [1.0.0] - 2025-08-01
### Added
- Initial release: Pipeline with extract-transform-load stages
- CSV and database extractors
- Validation transformer with configurable rules
- Structured logging via structlog
