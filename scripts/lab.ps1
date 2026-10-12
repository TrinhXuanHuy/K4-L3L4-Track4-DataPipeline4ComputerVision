# PowerShell wrapper for lab commands (equivalent to scripts/lab.sh)
param(
    [Parameter(Position=0)]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$RemainingArgs
)

$PY = "/home/airflow/lab-venv/bin/python"

switch ($Command) {
    "step" {
        docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli step @RemainingArgs
    }
    "exp" {
        docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli exp @RemainingArgs
    }
    "verify" {
        docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli verify @RemainingArgs
    }
    "test" {
        docker compose exec -T -w /opt/lab airflow $PY -m unittest discover -s tests
    }
    "run" {
        docker compose exec -T -w /opt/lab airflow scripts/run_pipeline.sh @RemainingArgs
    }
    "cli" {
        docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli @RemainingArgs
    }
    "dbt" {
        docker compose exec -T -w /opt/dbt airflow /home/airflow/lab-venv/bin/dbt @RemainingArgs --no-use-colors
    }
    "shell" {
        docker compose exec airflow bash
    }
    Default {
        Write-Host "Lenh ngan cho bai lab:"
        Write-Host "  .\scripts\lab.ps1 step <1..5|all> [--reset]"
        Write-Host "  .\scripts\lab.ps1 exp <idempotency|break-gate|leakage|threshold>"
        Write-Host "  .\scripts\lab.ps1 verify [--module N]"
        Write-Host "  .\scripts\lab.ps1 test"
        Write-Host "  .\scripts\lab.ps1 run [--reset]"
        Write-Host "  .\scripts\lab.ps1 cli <lenh> [tham so]"
        Write-Host "  .\scripts\lab.ps1 dbt <lenh> [tham so]"
        Write-Host "  .\scripts\lab.ps1 shell"
    }
}

