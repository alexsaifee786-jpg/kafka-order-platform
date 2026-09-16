[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker command was not found. Start Docker Desktop and try again."
}

Write-Host "orders.created-dlt end offsets:"
docker exec kop-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh `
    --bootstrap-server kop-kafka-1:19092 `
    --topic orders.created-dlt

if ($LASTEXITCODE -ne 0) {
    throw "Could not read DLT offsets. Check that the Kafka containers are running."
}
