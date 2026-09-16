# Local Kafka failure testing

Run these commands from the repository root in PowerShell. Docker Desktop, the
three Kafka containers, `order-service`, and `inventory-service` must be running.

## Send an order that intentionally fails inventory processing

```powershell
scripts\test-failed-order 2030
```

`productId` defaults to `999`, which triggers the intentional failure in
`InventoryEventConsumer`. The consumer makes one initial attempt and two retries
before publishing the record to `orders.created-dlt`.

## Read one order from the DLT

Wait about five seconds for retries to finish, then run:

```powershell
scripts\read-dlt 2030
```

Omit `-OrderId` to display all DLT records:

```powershell
scripts\read-dlt
```

## Show DLT end offsets

```powershell
scripts\dlt-offsets
```

For a newly failed record, one partition's end offset increases by one.

The `.cmd` launchers use the repository's PowerShell scripts without requiring a
machine-wide PowerShell execution-policy change.
