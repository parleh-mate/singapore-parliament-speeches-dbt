import os
import subprocess


def run_dbt(_request):
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Put dbt artifacts somewhere writable (Cloud Functions/Run)
    os.environ.setdefault("DBT_TARGET_PATH", "/tmp/dbt_target")
    os.environ.setdefault("DBT_LOG_PATH", "/tmp/dbt_logs")

    # Make dbt find profiles.yml reliably (assumes profiles.yml is in the same dir as main.py)
    os.environ["DBT_PROFILES_DIR"] = base_dir

    # Force the Cloud target
    cmd = ["dbt", "--debug", "build", "--target", "cloud_adc"]

    proc = subprocess.run(
        cmd,
        cwd=base_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    print(proc.stdout)

    out_tail = (proc.stdout or "")[-20000:]  # keep response size reasonable

    if proc.returncode != 0:
        return (
            f"dbt build failed (exit={proc.returncode})\n\n{out_tail}\n",
            500,
            {"Content-Type": "text/plain"},
        )

    return (f"dbt build succeeded\n\n{out_tail}\n", 200, {"Content-Type": "text/plain"})
