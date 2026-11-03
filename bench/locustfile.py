"""Closed-loop load against the gateway.

Each user sends the next request only after the previous response.
Achieved RPS is about the user count divided by latency. It is not an
open-loop arrival rate.

The client timeout is 10 seconds. The gateway deadline is 3 seconds, so
a 504 is the gateway's answer.
"""

from collections import Counter

from locust import HttpUser, constant, events, task

statuses: Counter[str] = Counter()


class ProcessUser(HttpUser):
    wait_time = constant(0)

    @task
    def process(self) -> None:
        with self.client.post(
            "/v1/process",
            json={"payload": {"order_id": "1001"}},
            timeout=10,
            name="/v1/process",
            catch_response=True,
        ) as response:
            code = response.status_code
            statuses[str(code)] += 1
            if code < 500:
                response.success()
            else:
                response.failure(str(code))


@events.quitting.add_listener
def _report(environment, **_kwargs) -> None:
    print("STATUS_COUNTS", dict(statuses), flush=True)
