@echo on

pip install delvewheel wheel .\tools\ci\pg_config_vcpkg_stub\

REM A specific version cannot be easily chosen.
REM https://github.com/microsoft/vcpkg/discussions/25622
vcpkg install libpq:x64-windows-release

for /f "delims=" %%i in ('pg_config --bindir') do set "LIBPQ_BINDIR=%%i"
if not exist "%LIBPQ_BINDIR%\libpq.dll" (
    echo ERROR: libpq.dll not found in %LIBPQ_BINDIR%
    exit /b 1
)

if defined GITHUB_PATH (
    >> "%GITHUB_PATH%" echo %LIBPQ_BINDIR%
    for /f "delims=" %%i in ('pg_config --libdir') do >> "%GITHUB_PATH%" echo %%i
)
