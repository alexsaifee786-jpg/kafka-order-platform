# Legacy Kafka failure-testing helpers

These helpers belong to the earlier JSON / `orders.created-dlt` phase. They are retained for historical reference and are **not the current Avro verification workflow**.

Use the root [How to Run / Local Setup](../README.md#how-to-run--local-setup) and [Retry, Error Handling, DLT & Replay](../README.md#retry-error-handling-dlt--replay) sections for the current implementation.

## Compatibility gaps

| Helper | Legacy assumption | Current implementation |
|---|---|---|
| `test-failed-order.ps1` | Caller supplies `orderId`; product 999 always triggers an intentional listener failure | The API generates the order ID and returns it in HTTP 202 text. Missing inventory causes failure; product 999 is not a special listener switch. |
| `read-dlt.ps1` | Reads `orders.created-dlt` as console text and filters JSON by order ID | Current source is `orders.created.avro`; DLT is `orders.created.avro-dlt`, with Avro payloads for valid business-failure records. |
| `dlt-offsets.ps1` | Queries offsets for `orders.created-dlt` | Current DLT offsets must be queried for `orders.created.avro-dlt`. |

The old script's “about five seconds” message is not an end-to-end guarantee: publication, serialization, scheduling, processing, and recovery add time.

The `.cmd` wrappers still launch these legacy PowerShell scripts. Updating this documentation does not migrate the helpers.

## Current failure-verification approach

1. In a local test environment, choose a positive product ID that has no inventory row.
2. Submit the current API request with `productId`, `quantity`, and `amount`, and retain the returned generated order ID.
3. Inspect application logs for the retryable failure and DLT recovery.
4. Inspect the Avro DLT through the current Java inspection consumer and Schema Registry.

An existing product with insufficient stock produces an inventory rejection result; it is not the same technical-failure scenario.

The Java replay runner remains disabled by default and contains hard-coded record selection. Treat it as a controlled proof-of-concept; do not enable it for a routine setup check.
