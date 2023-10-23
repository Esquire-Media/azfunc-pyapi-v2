@echo off
REM Script: PurgeGitFile.bat
REM Description: This batch file bypasses PowerShell's execution policy to execute the provided PowerShell script.
REM Usage: .tools/PurgeGitFile.bat --FilePath "path\to\your\file.txt"

powershell -ExecutionPolicy Bypass -NoProfile -File ".\PurgeGitFile.ps1" %*

pause
