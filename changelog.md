# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] - 2023-11-15

### Added

- Auto detect thread numbers based on the CPU numbers

## [1.3.0] - 2023-11-10

### Added

- Restrict the number in threadpool to leave more CPU to single queries

### Changed

- Modify some queries to multi-thread version

## [1.2.0] - 2023-11-07

## Added

- Add a global output buffer to keep the order of the output

## [1.1.0] - 2023-11-02

## Added

- Add a global order mutex to keep the order of different queries

## Changed

- Copy table will lock the whole db

## [1.0.2] - 2023-11-01

### Fixed

- Fix a mistake for QUIT

## [1.0.1] - 2023-10-31

### Changed

- Use a FIFO shared mutex for tables to ensure the order of queries

### Fixed

- Fix a segmentation fault when submitting tasks to thread pool

# [1.0.0] - 2023-10-30

### Added

- Use thread pool to perform multi-threading queries
- Add locks to all tables and the database

## [0.6.2] - 2023-10-29

### Changed

- Change implementation of DELETE query

## [0.6.1] - 2023-10-29

### Changed

- Slightly change output style

### Fixed

- Fix mistake result in ADD/SUB query

## [0.6.0] - 2023-10-29

### Added

- Implement ADD/SUB query

## [0.5.1] - 2023-10-27

### Fixed

- Fix bugs in DELETE query

## [0.5.0] - 2023-10-27

### Added

- Implement SWAP query

## [0.4.0] - 2023-10-25

### Added

- Implement COUNT query
- Implement MIN/MAX query
- Implement SUM query

## [0.3.0] - 2023-10-25

### Added

- Implement SELECT query
- Implement DELETE query
- Implement DUPLICATE query

## [0.2.0] - 2023-10-21

### Added

- Implement COPYTABLE query
- Implement TRUNCATE query

## [0.1.0] - 2023-10-20

## Fixed

- Fix compile error to initial the software to be compilable
