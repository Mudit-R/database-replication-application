@echo off
echo Sending 15 test messages to Kafka topic: nosql-replication
echo.

for %%f in (src\main\resources\samples\*.json) do (
    echo --- Sending: %%~nf ---
    type "%%f" | docker exec -i replication-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic nosql-replication
    echo Sent.
    timeout /t 1 /nobreak >nul
)

echo.
echo All 15 messages sent.
echo Check the app logs at: logs\replication-engine.log
echo Check the database:    docker exec -it replication-mysql mysql -uroot -ppassword replication_db -e "SELECT * FROM db_invoices;"
