@echo on

uv tool install --force delvewheel

REM A specific version cannot be easily chosen.
REM https://github.com/microsoft/vcpkg/discussions/25622
vcpkg install libpq:x64-windows-release
uv tool install --force .\tools\ci\pg_config_vcpkg_stub\
