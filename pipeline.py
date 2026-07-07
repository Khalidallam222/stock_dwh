import subprocess
import sys
from datetime import date

print(f"{'='*55}")
print(f"  Stock Market DWH Pipeline — {date.today().isoformat()}")
print(f"{'='*55}")

scripts = [
    ('extract.py',   'Extract: Alpha Vantage API → S3 raw'),
    ('transform.py', 'Transform: S3 raw → S3 processed'),
    ('load.py',      'Load: S3 processed → RDS'),
]

for script, description in scripts:
    print(f"\n[{description}]")
    print('-' * 45)
    result = subprocess.run([sys.executable, script], capture_output=False)

    if result.returncode != 0:
        print(f"\n❌ {script} failed. Pipeline aborted.")
        sys.exit(1)

print(f"\n{'='*55}")
print("  🎉 Pipeline completed successfully.")
print(f"{'='*55}")
