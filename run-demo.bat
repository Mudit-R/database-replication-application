@echo off
echo.
echo === Replication Engine Demo ===
echo.

set JAVA="C:\gaming\code\bin\java.exe"
set JAVAC="C:\gaming\code\bin\javac.exe"

set M2=C:\Users\mohit\.m2\repository

set CP=target\classes
set CP=%CP%;%M2%\com\fasterxml\jackson\core\jackson-databind\2.15.4\jackson-databind-2.15.4.jar
set CP=%CP%;%M2%\com\fasterxml\jackson\core\jackson-core\2.15.4\jackson-core-2.15.4.jar
set CP=%CP%;%M2%\com\fasterxml\jackson\core\jackson-annotations\2.15.4\jackson-annotations-2.15.4.jar
set CP=%CP%;%M2%\org\yaml\snakeyaml\2.2\snakeyaml-2.2.jar
set CP=%CP%;%M2%\org\apache\logging\log4j\log4j-api\2.23.1\log4j-api-2.23.1.jar
set CP=%CP%;%M2%\org\apache\logging\log4j\log4j-core\2.23.1\log4j-core-2.23.1.jar

echo [1/3] Compiling...
if exist target\classes rmdir /s /q target\classes
mkdir target\classes
%JAVAC% -cp "%CP%" -d "target\classes" ^
    src\main\java\com\replication\app\SqlBuilder.java ^
    src\main\java\com\replication\demo\DemoRunner.java
if errorlevel 1 (
    echo ERROR: Compilation failed.
    pause
    exit /b 1
)

echo [2/3] Copying resources...
copy /y src\main\resources\application.yml target\classes\ >nul
copy /y src\main\resources\sample-message.json target\classes\ >nul
copy /y src\main\resources\log4j2.xml target\classes\ >nul

echo [3/3] Running demo...
echo.
%JAVA% -cp "%CP%" com.replication.demo.DemoRunner

echo.
pause
