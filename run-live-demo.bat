@echo off
echo.
echo === Starting MySQL ^& Kafka Containers ===
docker-compose up -d

echo.
echo === Running Replication Engine Live Demo ===
echo.

mvn spring-boot:run "-Dspring-boot.run.profiles=live-demo"

echo.
pause
