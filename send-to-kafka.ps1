param()

$inbox = "inbox"
$sent  = "inbox\sent"

if (-not (Test-Path $sent)) { New-Item -ItemType Directory -Path $sent | Out-Null }

$files = Get-ChildItem "$inbox\*.json" -ErrorAction SilentlyContinue

if ($files.Count -eq 0) {
    Write-Host "No JSON files found in inbox\. Drop .json files there and try again."
    exit 0
}

Write-Host ""
Write-Host "=== Send JSON files from inbox\ to Kafka ==="
Write-Host ""

$count = 0
foreach ($file in $files) {
    Write-Host "Processing: $($file.Name)"

    try {
        $raw    = Get-Content $file.FullName -Raw
        $parsed = $raw | ConvertFrom-Json

        # Infer the correct Kafka topic from header.type
        $type  = $parsed.header.type
        $topic = if ($type -eq "order") { "nosql-orders" } else { "nosql-replication" }

        Write-Host "  -> topic: $topic"

        # Minify to a single line — kafka-console-producer sends one message per line
        $minified = $parsed | ConvertTo-Json -Compress -Depth 20

        # Publish to Kafka via docker exec
        $minified | docker exec -i replication-kafka kafka-console-producer `
            --bootstrap-server localhost:9092 `
            --topic $topic

        if ($LASTEXITCODE -eq 0) {
            Write-Host "  Sent OK."
            Move-Item $file.FullName "$sent\$($file.Name)" -Force
            $count++
        } else {
            Write-Host "  ERROR: docker exec returned non-zero exit code. Is the app running?"
        }
    }
    catch {
        Write-Host "  ERROR: $_"
    }

    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "$count file(s) sent. Processed files moved to inbox\sent\"
Write-Host ""
Write-Host "View results:"
Write-Host "  docker exec -it replication-mysql mysql -uroot -ppassword replication_db -e `"SELECT * FROM db_invoices;`""
Write-Host "  docker exec -it replication-mysql mysql -uroot -ppassword replication_db -e `"SELECT * FROM db_audit_log;`""
Write-Host "  docker exec -it replication-mysql mysql -uroot -ppassword replication_db -e `"SELECT * FROM orders;`""
Write-Host "  docker exec -it replication-mysql mysql -uroot -ppassword replication_db -e `"SELECT * FROM order_items;`""
Write-Host ""
