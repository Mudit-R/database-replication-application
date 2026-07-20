@echo off
setlocal enabledelayedexpansion

echo.
echo === Replication Engine - Full App ===
echo.
echo Starting Spring Boot with LIVE Kafka listener...
echo Kafka topics: nosql-replication, nosql-orders
echo.
echo Drop JSON files into the inbox\ folder, then run send-to-kafka.bat
echo to publish them. The app will process them in real time.
echo.
echo Press Ctrl+C to stop the app.
echo.

"C:\Users\mohit\maven\apache-maven-3.9.6\bin\mvn.cmd" spring-boot:run
