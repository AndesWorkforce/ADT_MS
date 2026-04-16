<#
.SYNOPSIS
  Vacía una tabla en ClickHouse vía HTTP (puerto 8123), sin DBeaver.

.DESCRIPTION
  Por defecto usa TRUNCATE TABLE. Si -UseDelete, usa ALTER TABLE ... DELETE WHERE 1=1 (mutación asíncrona).

.PARAMETER ClickHouseHost
  Host de ClickHouse (ej. 72.61.129.234 o localhost)

.PARAMETER Port
  Puerto HTTP (default 8123)

.PARAMETER User / Password
  Credenciales ClickHouse

.PARAMETER Database / Table
  Base y tabla (ej. pulse_analytics, events_raw)

.EXAMPLE
  .\clickhouse-clear-table.ps1 -ClickHouseHost "127.0.0.1" -User "admin" -Password "admin123" -Database "pulse_analytics" -Table "events_raw"

.EXAMPLE
  .\clickhouse-clear-table.ps1 -ClickHouseHost "127.0.0.1" -User "admin" -Password "admin123" -Database "pulse_analytics" -Table "events_raw" -UseDelete
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$ClickHouseHost,

  [int]$Port = 8123,

  [Parameter(Mandatory = $true)]
  [string]$User,

  [Parameter(Mandatory = $true)]
  [string]$Password,

  [Parameter(Mandatory = $true)]
  [string]$Database,

  [Parameter(Mandatory = $true)]
  [string]$Table,

  [switch]$UseDelete
)

$ErrorActionPreference = "Stop"

if ($UseDelete) {
  $sql = "ALTER TABLE $Database.$Table DELETE WHERE 1 = 1"
} else {
  $sql = "TRUNCATE TABLE IF EXISTS $Database.$Table"
}

$uri = "http://${ClickHouseHost}:${Port}/"
$pair = "${User}:${Password}"
$bytes = [System.Text.Encoding]::ASCII.GetBytes($pair)
$b64 = [System.Convert]::ToBase64String($bytes)
$headers = @{
  Authorization = "Basic $b64"
}

Write-Host "Ejecutando en ClickHouse HTTP:" -ForegroundColor Cyan
Write-Host "  $sql" -ForegroundColor Gray
Write-Host ""

try {
  $body = [System.Text.Encoding]::UTF8.GetBytes($sql)
  $response = Invoke-WebRequest -Uri $uri -Method Post -Headers $headers -Body $body -ContentType "text/plain; charset=utf-8" -UseBasicParsing
  Write-Host "OK (HTTP $($response.StatusCode))" -ForegroundColor Green
  if ($response.Content) {
    Write-Host $response.Content
  }
} catch {
  Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
  if ($_.Exception.Response) {
    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
    $reader.BaseStream.Position = 0
    $reader.DiscardBufferedData()
    $errBody = $reader.ReadToEnd()
    if ($errBody) { Write-Host $errBody -ForegroundColor Red }
  }
  exit 1
}
