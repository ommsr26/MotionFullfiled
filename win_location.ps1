Add-Type -AssemblyName System.Device
$watcher = New-Object System.Device.Location.GeoCoordinateWatcher([System.Device.Location.GeoPositionAccuracy]::High)
$watcher.Start()
$timeoutSec = 8
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($watcher.Status -ne [System.Device.Location.GeoPositionStatus]::Ready -and $sw.Elapsed.TotalSeconds -lt $timeoutSec) {
    Start-Sleep -Milliseconds 150
}
$pos = $watcher.Position.Location
$watcher.Stop()
$watcher.Dispose()

if ($pos -and !$pos.IsUnknown) {
    [PSCustomObject]@{
        success = $true
        latitude = [math]::Round($pos.Latitude, 6)
        longitude = [math]::Round($pos.Longitude, 6)
        altitude = $pos.Altitude
        accuracy = $pos.HorizontalAccuracy
        source = "WindowsLocationService"
    } | ConvertTo-Json
} else {
    [PSCustomObject]@{
        success = $false
        error = "Location not ready"
    } | ConvertTo-Json
}