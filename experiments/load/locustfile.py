from __future__ import annotations

from locust import HttpUser, between, task


class StandUser(HttpUser):
    wait_time = between(0.0, 0.1)

    @task(3)
    def submit_async(self) -> None:
        self.client.post("/jobs", json={"data": {"n": 1}}, name="POST /jobs (async)")

    @task(1)
    def submit_sync(self) -> None:
        self.client.post("/jobs/sync", json={"data": {"n": 1}}, name="POST /jobs/sync (sync)")
