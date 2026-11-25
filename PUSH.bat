@echo off
setlocal
cd /d "%~dp0"

git --version >nul 2>&1
if errorlevel 1 goto missing_git

set "COT_REMOTE=https://github.com/Hhiieeuu1706/COT.git"
set "COT_ACTUAL_REMOTE="
set "COT_BRANCH="
for /f "delims=" %%R in ('git remote get-url --push origin 2^>nul') do set "COT_ACTUAL_REMOTE=%%R"
if not "%COT_ACTUAL_REMOTE%"=="%COT_REMOTE%" goto wrong_repo

for /f "delims=" %%B in ('git symbolic-ref --quiet --short HEAD 2^>nul') do set "COT_BRANCH=%%B"
if not "%COT_BRANCH%"=="main" goto wrong_repo

if /i "%~1"=="--check" goto check

git fetch origin main
if errorlevel 1 goto failed
git merge-base --is-ancestor origin/main HEAD
if errorlevel 1 goto behind

git add -A
if errorlevel 1 goto failed
git diff --cached --quiet
if errorlevel 2 goto failed
if errorlevel 1 (
    git var GIT_AUTHOR_IDENT >nul 2>&1
    if errorlevel 1 goto missing_identity
    git var GIT_COMMITTER_IDENT >nul 2>&1
    if errorlevel 1 goto missing_identity
    git diff --cached --stat
    if errorlevel 1 goto failed
    git commit -m "Update COT"
    if errorlevel 1 goto failed
)

git push origin HEAD:refs/heads/main
if errorlevel 1 goto failed
echo COT is up to date on GitHub.
pause
exit /b 0

:check
git status --short --branch
exit /b %errorlevel%

:missing_git
echo Git is required. Install Git for Windows and run this file again.
goto failed

:missing_identity
echo Set your Git identity in this folder and run this file again:
echo   git config user.name "Your name"
echo   git config user.email "Your GitHub email"
goto failed

:wrong_repo
echo This script requires branch main and push remote %COT_REMOTE%.
goto failed

:behind
echo GitHub main contains changes missing locally. Integrate origin/main before pushing.
goto failed

:failed
echo COT was not pushed. Check the message above.
pause
exit /b 1
