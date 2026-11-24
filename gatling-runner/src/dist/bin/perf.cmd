@echo off
rem perf - run YAML performance test plans (no Maven needed).   perf help
rem   PERF_HEAP        load generator heap (default 2g)
rem   PERF_JAVA_OPTS   extra JVM options
rem   JAVA_HOME        Java 17+ (otherwise java from PATH)
setlocal
set "PERF_HOME=%~dp0.."
if "%PERF_HEAP%"=="" set "PERF_HEAP=2g"
set "JAVA=java"
if not "%JAVA_HOME%"=="" set "JAVA=%JAVA_HOME%\bin\java"

"%JAVA%" -Xmx%PERF_HEAP% --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED %PERF_JAVA_OPTS% ^
  "-Dlogback.configurationFile=%PERF_HOME%\conf\logback.xml" ^
  -cp "%PERF_HOME%\lib\*;%PERF_HOME%\drivers\*" ^
  org.codelabuk.perf.runner.Perf %*
exit /b %ERRORLEVEL%
