@echo off
rem Network Monitor - Windows launcher.
rem If Python is missing, a box offers to install it, then Network Monitor starts.
setlocal
title Network Monitor
cd /d "%~dp0"
set "APP=Network Monitor"

call :findpy
if defined PY goto havepy
set "BOX_MSG=%APP% needs Python to run.|Python is free and takes a minute or two to install.||Would you like me to install it now and then start %APP%?"
call :ask
if errorlevel 1 goto :eof
call :installpy
call :findpy
if not defined PY goto pyfail

:havepy
%PY% -c "import psutil" >nul 2>nul
if not errorlevel 1 goto run
set "BOX_MSG=%APP% needs a free add-on for Python called psutil.||Would you like me to install it now?"
call :ask
if errorlevel 1 goto :eof
start "Installing add-ons for %APP%" /wait %PY% -m pip install --user psutil
%PY% -c "import psutil" >nul 2>nul
if not errorlevel 1 goto run
set "BOX_MSG=The add-on didn't install, so %APP% can't start.||Check the internet connection and try again."
call :tell
goto :eof

:run
%PY% "network monitor2.py" %*
echo.
echo  %APP% has stopped.
pause
goto :eof

rem ---------------------------------------------------------------
:findpy
rem Real Python only - skips the Microsoft Store placeholder that just opens the Store.
set "PY="
py -3 -c "1" >nul 2>nul && set "PY=py -3"
if defined PY exit /b 0
python -c "1" >nul 2>nul && set "PY=python"
if defined PY exit /b 0
for /d %%D in ("%LOCALAPPDATA%\Programs\Python\Python3*") do if exist "%%D\python.exe" set "PY="%%D\python.exe""
if defined PY exit /b 0
for /d %%D in ("%ProgramFiles%\Python3*") do if exist "%%D\python.exe" set "PY="%%D\python.exe""
exit /b 0

:ask
rem Shows BOX_MSG in a Yes/No box on top of other windows. Sets errorlevel 0 for Yes, 1 for No.
powershell -NoProfile -ExecutionPolicy Bypass -Command "Add-Type -AssemblyName System.Windows.Forms; $f = New-Object System.Windows.Forms.Form; $f.TopMost = $true; $m = $env:BOX_MSG -replace '\|', [Environment]::NewLine; $r = [System.Windows.Forms.MessageBox]::Show($f, $m, $env:APP, 'YesNo', 'Question'); if ($r -eq 'Yes') { exit 0 } else { exit 1 }"
exit /b %errorlevel%

:tell
rem Shows BOX_MSG in an OK box.
powershell -NoProfile -ExecutionPolicy Bypass -Command "Add-Type -AssemblyName System.Windows.Forms; $f = New-Object System.Windows.Forms.Form; $f.TopMost = $true; $m = $env:BOX_MSG -replace '\|', [Environment]::NewLine; [void][System.Windows.Forms.MessageBox]::Show($f, $m, $env:APP, 'OK', 'Information')"
exit /b 0

:installpy
where winget >nul 2>nul
if errorlevel 1 goto browser
start "Installing Python for %APP% - this takes a minute or two" /wait winget install -e --id Python.Python.3.12 --scope user --silent --accept-package-agreements --accept-source-agreements
exit /b 0

:browser
start "" "https://www.python.org/downloads/"
set "BOX_MSG=Your browser is opening the Python download page.||Download and run the installer, and tick 'Add python.exe to PATH' on the first screen.||When it's done, open %APP% again."
call :tell
exit /b 1

:pyfail
set "BOX_MSG=Python didn't finish installing, so %APP% can't start yet.||Your browser will open the Python download page. Install it, tick 'Add python.exe to PATH', then open %APP% again."
call :tell
start "" "https://www.python.org/downloads/"
goto :eof
