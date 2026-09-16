[CmdletBinding()]
param(
    [long]$OrderId,
    [int]$TimeoutMilliseconds = 15000
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker command was not found. Start Docker Desktop and try again."
}

$dockerArguments = @(
    "run", "--rm", "--network", "host", "apache/kafka:4.3.1",
    "/opt/kafka/bin/kafka-console-consumer.sh",
    "--bootstrap-server", "localhost:9092",
    "--topic", "orders.created-dlt",
    "--from-beginning",
    "--timeout-ms", $TimeoutMilliseconds,
    "--formatter-property", "print.key=true",
    "--formatter-property", "print.partition=true",
    "--formatter-property", "print.offset=true"
)

# Windows PowerShell can promote native stderr to an error record when the global
# preference is Stop. Kafka writes its startup notice and normal timeout there.
$previousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
$records = @(& docker @dockerArguments 2>$null)
$ErrorActionPreference = $previousErrorActionPreference

if ($PSBoundParameters.ContainsKey("OrderId")) {
    $records = @($records | Select-String -SimpleMatch "`"orderId`":$OrderId")
}

if ($records.Count -eq 0) {
    if ($PSBoundParameters.ContainsKey("OrderId")) {
        Write-Host "No DLT record found for orderId=$OrderId."
    } else {
        Write-Host "No records found in orders.created-dlt."
    }
    exit 0
}

$records
