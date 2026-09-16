[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [long]$OrderId,

    [long]$ProductId = 999,
    [int]$Quantity = 1,
    [decimal]$Amount = 500.00,
    [string]$OrderServiceUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"

$payload = [ordered]@{
    orderId = $OrderId
    productId = $ProductId
    quantity = $Quantity
    amount = $Amount
} | ConvertTo-Json -Compress

Write-Host "Publishing failure-test order: $payload"

$response = Invoke-RestMethod `
    -Uri "$OrderServiceUrl/api/orders" `
    -Method Post `
    -ContentType "application/json" `
    -Body $payload

Write-Host $response
Write-Host "Inventory will try once, retry twice, then publish to the DLT."
Write-Host "After about 5 seconds, run: scripts\read-dlt $OrderId"
