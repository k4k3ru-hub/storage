"""Run Go integration tests against a disposable MySQL on a random loopback port."""

import os
from pathlib import Path
import subprocess
import time
import uuid


def main():
    name = "oms-store-check-" + uuid.uuid4().hex[:12]
    created = False
    try:
        subprocess.run(
            ["docker", "run", "--detach", "--name", name,
             "--publish", "127.0.0.1::3306", "--tmpfs", "/var/lib/mysql",
             "-e", "MYSQL_ALLOW_EMPTY_PASSWORD=yes", "-e", "MYSQL_ROOT_HOST=%",
             "-e", "MYSQL_DATABASE=oms_review", "mysql:8.4"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        created = True
        deadline = time.monotonic() + 50
        while True:
            ping = subprocess.run(
                ["docker", "exec", name, "mysql", "--host=127.0.0.1",
                 "--user=root", "oms_review", "-e", "SELECT 1"],
                capture_output=True, timeout=5,
            )
            if ping.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("temporary MySQL startup timed out")
            time.sleep(0.5)
        address = subprocess.run(
            ["docker", "port", name, "3306/tcp"], check=True,
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        env = dict(os.environ, GOWORK="off",
                   K4K3RU_OMS_TEST_DSN=f"root@tcp({address})/oms_review?parseTime=true&loc=UTC")
        subprocess.run(
            ["go", "test", "-race", "-count=1", "-v", "./..."],
            cwd=Path(__file__).parent, env=env, check=True, timeout=180,
        )
    finally:
        if created:
            subprocess.run(["docker", "rm", "--force", name], check=True,
                           capture_output=True, timeout=30)
            print("Temporary MySQL removed; existing databases unchanged.")


if __name__ == "__main__":
    main()
