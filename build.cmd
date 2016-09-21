@ECHO OFF

SETLOCAL

SET CACHED_NUGET=%LocalAppData%\NuGet\NuGet.exe
SET SOLUTION_PATH="%~dp0ElasticDatabaseTools.sln"
SET MSBUILD12_TOOLS_PATH="%ProgramFiles(x86)%\MSBuild\12.0\bin\MSBuild.exe"
SET BUILD_TOOLS_PATH=%MSBUILD14_TOOLS_PATH%

IF NOT EXIST %MSBUILD12_TOOLS_PATH% (
  echo Could not find MSBuild 12.  Please install build tools ^(See above^)
  exit /b 1
) else (
  set BUILD_TOOLS_PATH=%MSBUILD12_TOOLS_PATH%
)

IF EXIST %CACHED_NUGET% goto restore
echo Downloading latest version of NuGet.exe...
IF NOT EXIST %LocalAppData%\NuGet md %LocalAppData%\NuGet
@powershell -NoProfile -ExecutionPolicy unrestricted -Command "$ProgressPreference = 'SilentlyContinue'; Invoke-WebRequest 'https://www.nuget.org/nuget.exe' -OutFile '%CACHED_NUGET%'"

:restore
IF NOT EXIST src\packages md src\packages
%CACHED_NUGET% restore %SOLUTION_PATH%
dotnet restore

%BUILD_TOOLS_PATH% %SOLUTION_PATH% /nologo /m /v:m /flp:verbosity=normal %*