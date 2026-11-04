"""Closed-loop load against the gateway, with idempotency called out.

Each user sends the next request only after the previous response.
The client timeout is 10 seconds. The gateway deadline is 3 seconds.

Select a workload with --scenario. A new process uses a new run id, so
keys from an earlier run are not reused and do not become accidental hits.

  unique      one new Idempotency-Key per request. This is the provider path.
  repeat      a shared pool of keys. The first use is a miss. Later uses
              must replay the stored response.
  mixed       --duplicate-ratio of requests draw from the pool. The rest
              are unique. The default ratio is 0.3.
  concurrent  every user shares one key, at the same time.
  mismatch    each user sends order 1001, then order 1002, on one key.
              The stored body is replayed. The second payload is not compared.
  legacy      no Idempotency-Key and no X-Client-Id. This is the old bench:
              every user is one client, and nothing is cached.
  pressure    one new key per request, on purpose. Nothing is cached.
              A 200 is counted under the provider named in the body,
              so A, B, and C are separate rows.

Profiles, closed loop, host http://127.0.0.1:8000 from the machine that
published port 8000:

  baseline   -u 20  -r 2   -t 30s  --scenario unique
  normal     -u 100 -r 10  -t 30s  --scenario unique
  stress     -u 300 -r 20  -t 30s  --scenario unique
  spike      -u 500 -r 100 -t 30s  --scenario unique

Add --scenario repeat, mixed, concurrent, or mismatch to the same -u/-r/-t.
mixed accepts --duplicate-ratio 0.3. repeat accepts --key-pool 4.
"""

from __future__ import annotations

import threading
import uuid
from collections import Counter
from random import Random

from locust import HttpUser, constant, events, task

RUN = uuid.uuid4().hex[:12]
LOCK = threading.Lock()
SEEN: dict[str, dict] = {}
COUNTS: Counter[str] = Counter()
LATENCY_MS: Counter[str] = Counter()
LATENCY_N: Counter[str] = Counter()

_PROVIDER_FIELDS = ("id", "status", "provider", "echo", "request_id")
_EXPECTED_429 = frozenset({"rate_limited", "admission_rejected"})
_POOL_SIZE_DEFAULT = 4


@events.init_command_line_parser.add_listener
def _arguments(parser) -> None:
    parser.add_argument(
        "--scenario",
        choices=["unique", "repeat", "mixed", "concurrent", "mismatch", "legacy", "pressure"],
        default="unique",
        include_in_web_ui=True,
        help="Which idempotency workload to run.",
    )
    parser.add_argument(
        "--duplicate-ratio",
        type=float,
        default=0.3,
        include_in_web_ui=True,
        help="Fraction of mixed-scenario requests that reuse a pool key.",
    )
    parser.add_argument(
        "--key-pool",
        type=int,
        default=_POOL_SIZE_DEFAULT,
        include_in_web_ui=True,
        help="How many distinct keys the repeat and mixed scenarios share.",
    )
    parser.add_argument(
        "--order-id",
        default="1001",
        include_in_web_ui=True,
        help="payload.order_id for every scenario except the second mismatch call.",
    )


def _remember(label: str, elapsed_ms: float) -> None:
    COUNTS[label] += 1
    LATENCY_MS[label] += elapsed_ms
    LATENCY_N[label] += 1


def _body_of(response) -> object:
    try:
        return response.json()
    except ValueError:
        return None


def _error_of(body: object) -> str | None:
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return body["error"]
    return None


def _valid_provider(body: object) -> bool:
    return isinstance(body, dict) and all(field in body for field in _PROVIDER_FIELDS)


def _classify(scenario: str, key: str | None, status: int, body: object) -> tuple[str, str | None]:
    """Return a stable label and, when the response breaks the contract, a failure string.

    A cache hit replays the first terminal response, including a stored 5xx.
    409 is never stored: it means the first call is still in flight.
    """
    error = _error_of(body)
    # Inbound limiting runs before the idempotency read. A 429 here is not a cache miss.
    if status == 429 and error in _EXPECTED_429:
        return f"rate-limit-{error}", None

    if status == 409 and error == "idempotency_in_progress":
        with LOCK:
            already = key is not None and key in SEEN
        if scenario in {"unique", "pressure"}:
            return "unexpected-409", "409 on a key this run used only once"
        if already:
            # The 409 was sent while the first call was still open. This
            # response can be handled after the 200 is recorded.
            COUNTS["in-progress-late"] += 1
        return "in-progress", None

    if key is None:
        return _fresh(status, body)

    with LOCK:
        previous = SEEN.get(key)
        if previous is None:
            SEEN[key] = {"status": status, "body": body, "ids": set()}
            txn = body.get("id") if isinstance(body, dict) else None
            if isinstance(txn, str):
                SEEN[key]["ids"].add(txn)
            label, problem = _fresh(status, body)
            return f"first-{label}", problem

        same = status == previous["status"] and body == previous["body"]
        txn = body.get("id") if isinstance(body, dict) else None
        extra = isinstance(txn, str) and previous["ids"] and txn not in previous["ids"]
        if isinstance(txn, str):
            previous["ids"].add(txn)

    if extra:
        return "extra-provider-call", "same Idempotency-Key returned a second provider id"
    if same:
        code = "cache-hit" if status < 400 else f"cache-hit-{status}"
        return code, None
    return "cache-diverged", f"repeat of {key} returned a different {status} body"


def _provider_name(body: object) -> str | None:
    if isinstance(body, dict) and body.get("provider") in {"a", "b", "c"}:
        return str(body["provider"])
    return None


def _fresh(status: int, body: object) -> tuple[str, str | None]:
    error = _error_of(body)
    provider = _provider_name(body)
    if status == 200 and _valid_provider(body):
        return f"provider-{provider or 'unknown'}-200", None
    if status >= 500 and provider is not None:
        return f"provider-{provider}-{status}", f"{status} from provider {provider}"
    if status == 429 and error in _EXPECTED_429:
        return f"rate-limit-{error}", None
    if status == 429:
        return "provider-429", None
    if status >= 500:
        name = error or "no-error-field"
        return f"gateway-{status}-{name}", f"{status} {name}"
    if isinstance(body, dict):
        return f"unexpected-{status}", f"unexpected status {status}"
    return f"invalid-body-{status}", f"status {status} body is not JSON"


def _note_retry_after(status: int, response, label: str) -> str:
    if status == 429 and response.headers.get("Retry-After") is None and response.headers.get("retry-after") is None:
        COUNTS["429-missing-retry-after"] += 1
        return label
    return label


@events.quitting.add_listener
def _report(environment, **_kwargs) -> None:
    scenario = getattr(environment.parsed_options, "scenario", "unique")
    print(f"SCENARIO {scenario} run={RUN}", flush=True)
    for label, count in sorted(COUNTS.items()):
        seen = LATENCY_N[label]
        avg = (LATENCY_MS[label] / seen) if seen else 0
        print(f"CLASS {label} count={count} avg_ms={avg:.1f}", flush=True)
    with LOCK:
        extra = [
            (key, sorted(record["ids"]))
            for key, record in SEEN.items()
            if len(record["ids"]) > 1
        ]
    if extra:
        print(f"EXTRA_PROVIDER_IDS keys={len(extra)} sample={extra[:3]}", flush=True)
    else:
        print("EXTRA_PROVIDER_IDS keys=0", flush=True)
    for name in ("a", "b", "c"):
        answered = sum(count for label, count in COUNTS.items() if f"provider-{name}-200" in label)
        print(f"PROVIDER_200 {name}={answered}", flush=True)


class GatewayUser(HttpUser):
    wait_time = constant(0)
    _users = 0

    def on_start(self) -> None:
        options = self.environment.parsed_options
        self.scenario = options.scenario
        self.order_id = options.order_id
        self.duplicate_ratio = min(max(options.duplicate_ratio, 0.0), 1.0)
        pool = max(options.key_pool, 1)
        self.pool = [f"{RUN}-pool-{index}" for index in range(pool)]
        self.rng = Random()
        self.mismatch_step = 0
        with LOCK:
            GatewayUser._users += 1
            number = GatewayUser._users
        self.number = number
        if self.scenario == "legacy":
            self.client_id = None
        else:
            self.client_id = f"bench{RUN}u{number}"
        self.concurrent_key = f"{RUN}-concurrent"
        self.mismatch_key = f"{RUN}-mis-{number}"

    @task
    def process(self) -> None:
        key, payload, intent = self._choose()
        headers = {"X-Request-ID": uuid.uuid4().hex}
        if self.client_id is not None:
            headers["X-Client-Id"] = self.client_id
        if key is not None:
            headers["Idempotency-Key"] = key
        with self.client.post(
            "/v1/process",
            json=payload,
            headers=headers,
            timeout=10,
            name="/v1/process",
            catch_response=True,
        ) as response:
            elapsed_ms = response.elapsed.total_seconds() * 1000
            body = _body_of(response)
            label, problem = _classify(self.scenario, key, response.status_code, body)
            label = _note_retry_after(response.status_code, response, label)
            if (
                intent == "mismatch"
                and response.status_code == 200
                and label.startswith("cache-hit")
                and isinstance(body, dict)
            ):
                echo = body.get("echo")
                if echo != {"order_id": "1001"}:
                    problem = "mismatch replay did not keep the first payload"
                    label = "mismatch-wrong-body"
                else:
                    COUNTS["mismatch-kept-first-payload"] += 1
            _remember(label, elapsed_ms)
            response.request_meta["name"] = f"/v1/process [{label}]"
            if problem is None:
                response.success()
            else:
                response.failure(problem)

    def _choose(self) -> tuple[str | None, dict, str]:
        payload = {"payload": {"order_id": self.order_id}}
        if self.scenario == "legacy":
            return None, payload, "legacy"
        if self.scenario in {"unique", "pressure"}:
            return f"{RUN}-u-{uuid.uuid4().hex}", payload, self.scenario
        if self.scenario == "repeat":
            return self.rng.choice(self.pool), payload, "repeat"
        if self.scenario == "mixed":
            if self.rng.random() < self.duplicate_ratio:
                return self.rng.choice(self.pool), payload, "duplicate"
            return f"{RUN}-u-{uuid.uuid4().hex}", payload, "unique"
        if self.scenario == "concurrent":
            return self.concurrent_key, payload, "concurrent"
        self.mismatch_step += 1
        if self.mismatch_step == 1:
            return self.mismatch_key, {"payload": {"order_id": "1001"}}, "seed"
        return self.mismatch_key, {"payload": {"order_id": "1002"}}, "mismatch"
