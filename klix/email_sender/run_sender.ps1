# run_sender.ps1 — Klix Email Sender launcher (final+)
# - Single-instance lock (Global\KlixEmailSenderMutex)
# - 5-min default wait window so early runs still send
# - Rotates modes/ratio/limits via args
# - Logs to logs/sender_YYYY-MM-DD.log (append)
# - ntfy push via $env:NTFY_TOPIC
# - Supports one-shot friendlies via -SendFriendliesNow
# - Optional min/max delay passthrough for testing

[CmdletBinding()]
param(
  [ValidateSet('mixed','friendlies','cold')]
  [string]$Mode = 'mixed',
  [string]$Ratio = '60:40',
  [int]$Limit = 0,
  [int]$DripPerRun = 1,
  [int]$AllowWaitSecs = 300,   # <-- default 5-minute window
  [int]$MinDelay = -1,         # pass through to sender.py for testing
  [int]$MaxDelay = -1,         # pass through to sender.py for testing
  [switch]$VerboseSender,
  [switch]$SendFriendliesNow,
  [switch]$AsciiLogs            # avoids unicode arrows/dashes in PS/ntfy
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8

# ------------------ Paths ------------------
$WorkDir = 'C:\Users\ohkod\OneDrive\Desktop\email_sender'
$Python  = 'C:\Users\ohkod\AppData\Local\Programs\Python\Python312\python.exe'
$Script  = 'sender.py'
if (-not (Test-Path $Python)) { $Python = 'python' }

# ------------------ Env Vars ----------------
$env:SENDER_TZ          = 'America/Toronto'
$env:GOOGLE_SHEETS_CRED = 'C:\Users\ohkod\OneDrive\Desktop\leadfinder\sa.json'
$env:PYTHONIOENCODING   = 'utf-8'
$env:NTFY_TOPIC         = $env:NTFY_TOPIC  # set globally if you want; keeps your value if already set
if ($AsciiLogs) { $env:USE_ASCII_LOGS = '1' } else { Remove-Item Env:USE_ASCII_LOGS -ErrorAction SilentlyContinue }

# Make wait window active by default (also pass as flag below)
$env:ALLOW_WAIT_SECS = [string]$AllowWaitSecs

if ($SendFriendliesNow) {
  $env:SEND_FRIENDLIES_NOW = '1'
} else {
  Remove-Item Env:SEND_FRIENDLIES_NOW -ErrorAction SilentlyContinue
}

# ------------------ Logging -----------------
Set-Location $WorkDir
$LogDir  = Join-Path $WorkDir 'logs'
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile = Join-Path $LogDir ("sender_" + (Get-Date -Format "yyyy-MM-dd") + ".log")
if (!(Test-Path $LogFile)) { New-Item -ItemType File -Path $LogFile | Out-Null }

# ------------------ ntfy (optional) --------
$Topic = $env:NTFY_TOPIC
function Send-Ntfy {
  param([string]$Message, [string]$Tags = '')
  if ([string]::IsNullOrWhiteSpace($Topic)) { return }
  try {
    $uri  = "https://ntfy.sh/$Topic"
    $curl = Join-Path $env:SystemRoot 'System32\curl.exe'
    if (!(Test-Path $curl)) { return }
    if ($Tags) {
      & $curl -s -H "Title: Klix Sender" -H "Tags: $Tags" -d $Message $uri | Out-Null
    } else {
      & $curl -s -H "Title: Klix Sender" -d $Message $uri | Out-Null
    }
  } catch { }
}

# ------------------ Helpers -----------------
function New-RunGuid { [Guid]::NewGuid().ToString("N") }

function Get-RunLines {
  param([string]$Path, [string]$StartMarker, [string]$EndMarker)
  if (!(Test-Path $Path)) { return @() }
  $raw = Get-Content $Path -Raw -ErrorAction SilentlyContinue
  if (-not $raw) { return @() }
  $pattern = [regex]::Escape($StartMarker) + '(?<core>[\s\S]*?)' + [regex]::Escape($EndMarker)
  $m = [regex]::Match($raw, $pattern)
  if ($m.Success) { return ($m.Groups['core'].Value -split "`r?`n") } else { return @() }
}

function Summarize-Section {
  param([string[]]$Lines)
  $summary = [ordered]@{
    cold     = 0
    followup = 0
    friendly = 0
    skipped  = 0
    errors   = 0
    doneLine = $null
  }
  foreach ($l in $Lines) {
    if     ($l -match '^\[SENT\].* COLD ')     { $summary.cold++ }
    elseif ($l -match '^\[SENT\].* FOLLOWUP ') { $summary.followup++ }
    elseif ($l -match '^\[SENT\].* FRIENDLY ') { $summary.friendly++ }
    elseif ($l -match '^\[SKIP\]')             { $summary.skipped++ }
    elseif ($l -match '^\[ERROR\]')            { $summary.errors++ }
    elseif ($l -match '^\[DONE')               { $summary.doneLine = $l }
  }
  return $summary
}

# ------------------ Single-instance lock ----
$mutexName = 'Global\KlixEmailSenderMutex'
$mutex     = New-Object System.Threading.Mutex($false, $mutexName)
$haveLock  = $false

try {
  $haveLock = $mutex.WaitOne(0)
  if (-not $haveLock) {
    $msg = "[SKIP] overlap on $env:COMPUTERNAME at $(Get-Date -Format s)"
    Write-Host $msg
    Send-Ntfy $msg 'warning'
    exit 0
  }

  # --- small random jitter (3–45s) to desync concurrent tasks ---
  Start-Sleep -Seconds (Get-Random -Minimum 3 -Maximum 45)

  # ---------------- Args build -------------
  $argsList = @($Script, '--mode', $Mode, '--ratio', $Ratio, '--drip-per-run', "$DripPerRun", '--allow-wait-secs', "$AllowWaitSecs")
  if ($Limit -gt 0)      { $argsList += @('--limit', "$Limit") }
  if ($MinDelay -ge 0)   { $argsList += @('--min-delay', "$MinDelay") }
  if ($MaxDelay -ge 0)   { $argsList += @('--max-delay', ([Math]::Max($MaxDelay, $MinDelay))) }
  if ($VerboseSender)    { $argsList += @('--verbose') }

  # ---------------- Markers ----------------
  $runId   = New-RunGuid
  $startAt = Get-Date
  $startMk = "=== RUN-START $runId $(Get-Date -Format s) ==="
  Add-Content -Path $LogFile -Value $startMk

  # ---------------- Notify start -----------
  $startMsg = "[START] $env:COMPUTERNAME mode=$Mode ratio=$Ratio drip=$DripPerRun limit=$Limit wait=${AllowWaitSecs}s $($startAt.ToString('s'))"
  Send-Ntfy $startMsg 'play'

  # ---------------- Run + log --------------
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $exitCode = 0
  try {
    & $Python @argsList 2>&1 | Tee-Object -FilePath $LogFile -Append
    $exitCode = $LASTEXITCODE
  } catch {
    ($_ | Out-String) | Tee-Object -FilePath $LogFile -Append | Out-Null
    $exitCode = 1
  }
  $sw.Stop()

  $endMk = "=== RUN-END   $runId $(Get-Date -Format s) ==="
  Add-Content -Path $LogFile -Value $endMk

  # -------------- Summarize section --------
  $section = Get-RunLines -Path $LogFile -StartMarker $startMk -EndMarker $endMk
  $sum     = Summarize-Section -Lines $section

  $sentTotal = $sum.cold + $sum.followup + $sum.friendly
  $doneLine  = if ($sum.doneLine) { $sum.doneLine } else { "[DONE:$Mode] sent=$sentTotal skipped=$($sum.skipped) errors=$($sum.errors)" }
  $durStr    = ('{0:n1}s' -f $sw.Elapsed.TotalSeconds)
  $tag       = if ($exitCode -eq 0) { 'white_check_mark' } else { 'warning' }

  $endMsg = "$doneLine host=$env:COMPUTERNAME exit=$exitCode dur=$durStr  breakdown: cold=$($sum.cold) fu=$($sum.followup) fr=$($sum.friendly) skip=$($sum.skipped) err=$($sum.errors)"
  Write-Host $endMsg
  Send-Ntfy $endMsg $tag

  # -------------- Compact JSON -------------
  $compact = @{
    run_id     = $runId
    mode       = $Mode
    ratio      = $Ratio
    drip       = $DripPerRun
    limit      = $Limit
    wait_secs  = $AllowWaitSecs
    started_at = $startAt.ToString("s")
    duration_s = [math]::Round($sw.Elapsed.TotalSeconds,1)
    exit_code  = $exitCode
    counts     = @{
      cold     = $sum.cold
      followup = $sum.followup
      friendly = $sum.friendly
      skipped  = $sum.skipped
      errors   = $sum.errors
      sent     = $sentTotal
    }
  } | ConvertTo-Json -Depth 4

  Set-Content -Path (Join-Path $LogDir 'last_run.json') -Value $compact -Encoding UTF8
  exit $exitCode

} finally {
  if ($haveLock) { $mutex.ReleaseMutex() | Out-Null }
  $mutex.Dispose()
}
