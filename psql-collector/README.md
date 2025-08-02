# Collector Service API Documentation

This document provides detailed descriptions of the RPC methods available in the `Collector` service. The `Collector` service is designed to manage and optimize PostgreSQL database instances by collecting various metrics and applying configuration settings.

## Methods

### `CollectKnobs`

- **Description**: Collects current configuration settings, known as "knobs", from a PostgreSQL database instance.
- **Request**: `CollectKnobsRequest` - May include identifiers or filters to specify which knobs to collect.
- **Response**: `CollectKnobsResponse` - Contains the list of collected knobs and their current settings.

### `CollectInternalMetrics`

- **Description**: Gathers internal metrics from the PostgreSQL database. These metrics are typically derived from internal database statistics which can indicate the performance and health of the database.
- **Request**: `CollectInternalMetricsRequest` - May include parameters to specify the scope or type of internal metrics to collect.
- **Response**: `CollectInternalMetricsResponse` - Includes detailed metrics such as disk usage, query execution times, and other performance indicators.

### `CollectExternalMetrics`

- **Description**: Retrieves metrics from external sources that may impact or reflect the database performance. This could include operating system metrics, network statistics, or metrics from related applications.
- **Request**: `CollectExternalMetricsRequest` - Parameters to define which external metrics are needed.
- **Response**: `CollectExternalMetricsResponse` - Contains the external metrics data collected.

### `InitLoad`

- **Description**: Initializes and loads the required resources or configurations for the Collector service. This method is typically called at startup or when a new database instance needs to be monitored.
- **Request**: `InitLoadRequest` - May include configurations or parameters necessary for initialization.
- **Response**: `InitLoadResponse` - Provides the status of the initialization process, including success or failure information.

### `SetKnobs`

- **Description**: Applies specified configuration parameters or "knobs" to the PostgreSQL database. This is used to adjust settings based on performance analysis or operational requirements.
- **Request**: `SetKnobsRequest` - Contains the knobs and their desired settings to be applied to the database.
- **Response**: `SetKnobsResponse` - Returns the result of the operation, indicating whether the settings were successfully applied.

## Usage

To interact with the `Collector` service, clients must send a request to the server with the appropriate request type. The server processes the request and returns a response containing the requested data or a status of the operation.

For more detailed usage examples and configuration settings, please refer to the specific client and server documentation.

