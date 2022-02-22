import time
from multiprocessing import Process

import pytest

from application.factory import create_app

HOST = "0.0.0.0"
PORT = 9000
BASE_URL = f"http://{HOST}:{PORT}"


def run_server():
    app = create_app("config.TestConfig")
    app.run(host=HOST, port=PORT, debug=False)


@pytest.fixture(scope="session")
def server_process():
    proc = Process(target=run_server, args=(), daemon=True)
    proc.start()
    time.sleep(10)
    yield proc
    proc.kill()
