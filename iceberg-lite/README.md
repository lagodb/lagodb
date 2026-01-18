# iceberg-lite

`iceberg-lite` is a library based on [iceberg-rust](https://github.com/apache/iceberg-rust), tailored for synchronous execution environments and flexible storage backends.

## Overview

This library is a fork of [apache/iceberg-rust](https://github.com/apache/iceberg-rust) with several key architectural changes to meet the specific requirements of integrating with synchronous systems like PostgreSQL.

## Key Features & Modifications

- **Synchronous Execution Model**: The original asynchronous model in `iceberg-rust` has been converted to a fully synchronous execution model. This transition eliminates the need for an async runtime (like `tokio`), making it ideal for integration into PostgreSQL's process-based, synchronous architecture.
- **Custom FileIO Implementation**: `iceberg-lite` introduces support for custom `FileIO` implementations. This abstraction allows for flexible storage backends, enabling the library to work with various storage layers according to specific needs.

## Why iceberg-lite?

- **PostgreSQL Integration**: By providing a synchronous API, `iceberg-lite` allows for seamless integration into PostgreSQL extensions without the complexity of managing asynchronous tasks and runtimes.
- **Flexibility**: The ability to implement custom storage layers means `iceberg-lite` can be adapted to a wide range of environments and storage technologies.

## License

This project is derived from [apache/iceberg-rust](https://github.com/apache/iceberg-rust) and is licensed under the same terms as the original project.
