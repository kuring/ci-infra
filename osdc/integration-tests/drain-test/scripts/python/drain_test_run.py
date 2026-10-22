"""Exercise real drain/resume recipes with canary jobs on meta-staging-aws-uw1.

Cleanup follows the existing slow-test pattern: finally, atexit and SIGINT/
SIGTERM handlers, plus an always() same-job recovery step in CI. Recovery is
still subject to GitHub cancellation deadlines. CI defers exception recovery to
that step and leaves canary leftovers for the next run; local runs recover inline.
A runner-local intent file and process token allow that step to stop orphaned
drain writers before restoring. SIGKILL of the whole job or runner loss
cannot execute this process's cleanup; no independent recovery agent is used.
"""

from __future__ import annotations

import argparse
import atexit
import contextlib
import json
import logging
import math
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from pathlib import Path
from uuid import uuid4

from drain_test_checks import (
    CLUSTER,
    advertised_capacity,
    listener_pods,
    probe_jobs,
    probe_scale_set,
    require_limits,
    require_queued,
    require_success,
    runner_limits,
)

# Use the shared integration test helpers, matching the other orchestrators.
_INTEG_SCRIPTS = Path(__file__).resolve().parents[3] / "scripts" / "python"
if str(_INTEG_SCRIPTS) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_INTEG_SCRIPTS))

from run import CANARY_REPO, load_cluster_config, resolve  # noqa: E402

log = logging.getLogger("osdc-drain-test")
PROC_ROOT = Path("/proc")
DRAIN_OWNER_ENV = "OSDC_DRAIN_OWNER_TOKEN"
PR_TITLE_PREFIX = "[NO REVIEW][NO MERGE] OSDC Drain Test"
UPSTREAM = Path(__file__).resolve().parents[4]
RUN_TIMEOUT = 45 * 60
CLEANUP_TIMEOUT = 10 * 60
RESTORE_TIMEOUT = 3 * 60
_deadline: ContextVar[float | None] = ContextVar("drain_test_deadline", default=None)


def remaining(timeout: float) -> float:
    """Clamp a blocking operation to the current absolute deadline."""
    deadline = _deadline.get()
    if deadline is None:
        return timeout
    available = deadline - time.monotonic()
    if available <= 0:
        raise TimeoutError("Controller deadline exceeded")
    return min(timeout, available)


@contextlib.contextmanager
def deadline_scope(deadline: float):
    """Nested waits may shorten a budget, but must never extend it."""
    parent = _deadline.get()
    deadline = min(parent, deadline) if parent is not None else deadline
    token = _deadline.set(deadline)
    try:
        remaining(float("inf"))
        yield
        remaining(float("inf"))
    finally:
        _deadline.reset(token)


def generate_workflow(run_key: str) -> str:
    """Generate uniquely identifiable canary probes pinned to the uw1 runner group."""
    if not run_key or not run_key.isalnum():
        raise ValueError("Run key must be nonempty and alphanumeric")
    template = Path(__file__).resolve().parents[2] / "workflows" / "drain-test.yaml.tpl"
    return template.read_text().replace("{{RUN_KEY}}", run_key)


def command(args: list[str], *, timeout: float = 30, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """Bound calls and kill owned descendants before a timed-out recipe can write late."""
    timeout = remaining(timeout)
    payload = kwargs.pop("input", None)
    proc = subprocess.Popen(
        args,
        stdin=subprocess.PIPE if payload is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        **kwargs,
    )
    try:
        stdout, stderr = proc.communicate(input=payload, timeout=remaining(timeout))
    except BaseException:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        # Teardown uses the same budget, including when it is already exhausted.
        deadline = _deadline.get()
        grace = min(2, max(0, deadline - time.monotonic())) if deadline is not None else 2
        try:
            proc.communicate(timeout=grace)
        except subprocess.TimeoutExpired:
            proc.stdout.close()
            proc.stderr.close()
            proc.poll()
        raise
    result = subprocess.CompletedProcess(args, proc.returncode, stdout, stderr)
    if check:
        result.check_returncode()
    remaining(float("inf"))
    return result


def api(
    endpoint: str,
    payload: dict | None = None,
    method: str = "GET",
    *,
    repository: str = CANARY_REPO,
    timeout: float = 30,
) -> dict | list:
    args = ["gh", "api", f"repos/{repository}/{endpoint}".rstrip("/"), "--method", method]
    if payload is not None:
        args.extend(["--input", "-"])
    output = command(args, input=json.dumps(payload) if payload is not None else None, timeout=timeout).stdout
    return json.loads(output) if output.strip() else {}


def kubectl(*args: str) -> str:
    return command(["kubectl", "--request-timeout=15s", *args]).stdout


def process_group_running(pgid: int) -> bool:
    """Check all group members, ignoring zombies that cannot execute patches."""
    processes = command(["ps", "-eo", "pgid=,stat="], timeout=2).stdout
    return any(
        int(group) == pgid and not state.startswith("Z")
        for group, state in (line.split() for line in processes.splitlines())
    )


def wait_for(description: str, check, timeout: float, poll: float = 10) -> None:
    """Retry convergence/API failures, but let definitive test failures escape."""
    deadline = time.monotonic() + timeout
    last_error = "condition not reached"
    try:
        with deadline_scope(deadline):
            while True:
                remaining(float("inf"))
                try:
                    passed = check()
                    remaining(float("inf"))
                    if passed:
                        log.info("PASS: %s", description)
                        return
                except (ValueError, subprocess.SubprocessError) as exc:
                    last_error = str(exc)
                time.sleep(remaining(poll))
    except TimeoutError as exc:
        raise TimeoutError(f"{description}: deadline exceeded ({last_error})") from exc


class DrainTest:
    def __init__(self, args: argparse.Namespace, run_key: str):
        self.args = args
        self.cluster = CLUSTER
        self.group = CLUSTER
        self.repository = CANARY_REPO
        self.run_key = run_key
        self.branch = f"osdc-drain-test-{CLUSTER}"
        self.workflow_name = f"OSDC Drain Test {run_key}"
        self.pr: int | None = None
        self.run_id: int | None = None
        self.branch_created = False
        self.draining = False
        self.drain: subprocess.Popen | None = None
        self.desired: dict[str, int] = {}
        self.probe_ids: dict[str, set[int]] = {}
        self.assigned_phases: set[str] = set()
        self.wakeup_counts: dict[str, int] = {}
        self.cleanup_deadline: float | None = None
        self.recovery_attempts = 0
        self.recovery_file = getattr(args, "recovery_file", None)
        self.defer_recovery = getattr(args, "defer_recovery", False)
        self.skip_canary_cleanup = False
        self.env = {
            **os.environ,
            "OSDC_ROOT": str(args.root_dir),
            "OSDC_UPSTREAM": str(args.upstream_dir),
            "CLUSTERS_YAML": str(args.clusters_yaml),
            # Taints can make capacity zero even when maxRunners=0 is broken.
            "OSDC_TAINT_NODES": "no",
        }

    def record_recovery(self) -> dict[str, str]:
        """Persist intent before launching any drain writer; never tag the controller."""
        if self.recovery_file is None:
            return {}
        if not PROC_ROOT.is_dir():
            raise RuntimeError("Runner-local recovery requires Linux /proc")
        token = uuid4().hex
        # Exclusive creation prevents overwriting an earlier writer's identity.
        with self.recovery_file.open("x") as state:
            json.dump({"cluster": self.cluster, "token": token}, state)
            state.flush()
            os.fsync(state.fileno())
        return {DRAIN_OWNER_ENV: token}

    def recorded_drain_groups(self, token: str) -> set[int]:
        """Match inherited ownership tokens, including descendants of an exited leader."""
        groups = set()
        for process in PROC_ROOT.glob("[0-9]*"):
            remaining(float("inf"))
            try:
                if process.stat().st_uid != os.getuid():
                    continue
                fields = (process / "stat").read_text().rsplit(")", 1)[1].split()
                if fields[0] == "Z":
                    continue
                environment = (process / "environ").read_bytes().split(b"\0")
                if f"{DRAIN_OWNER_ENV}={token}".encode() not in environment:
                    continue
                group = int(fields[2])
                if group <= 1 or group == os.getpgrp():
                    raise RuntimeError("Refusing to stop a drain writer in an unsafe process group")
                groups.add(group)
            except (FileNotFoundError, ProcessLookupError):
                # Processes may exit between enumeration and inspection.
                continue
        return groups

    def stop_recorded_drain(self) -> None:
        """Fence old step writers before a new restore-only process changes limits."""
        if self.recovery_file is None:
            return
        if not PROC_ROOT.is_dir():
            raise RuntimeError("Runner-local recovery requires Linux /proc")
        state = json.loads(self.recovery_file.read_text())
        token = state.get("token", "")
        if (
            state.get("cluster") != self.cluster
            or not isinstance(token, str)
            or len(token) != 32
            or any(c not in "0123456789abcdef" for c in token)
        ):
            raise ValueError("Invalid runner-local drain recovery state")
        for sig in (signal.SIGTERM, signal.SIGKILL):
            for group in self.recorded_drain_groups(token):
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(group, sig)
            try:
                wait_for("recorded drain writers stopped", lambda: not self.recorded_drain_groups(token), 2, poll=0.05)
                return
            except TimeoutError:
                if sig == signal.SIGKILL:
                    raise

    def api(self, endpoint, payload=None, method="GET", **kwargs):
        return api(endpoint, payload, method, **kwargs)

    def kube(self, *args):
        return kubectl(*args)

    def workflow(self):
        return generate_workflow(self.run_key)

    def check_probe(self, items):
        return probe_scale_set(items, self.desired)

    def recipe(self, name: str, timeout: float = 120) -> None:
        result = command(["just", name, self.cluster], cwd=self.args.root_dir, env=self.env, timeout=timeout)
        log.info("%s: %s", name, result.stdout)

    def scale_sets(self) -> list[dict]:
        return json.loads(self.kube("get", "autoscalingrunnersets", "-n", "arc-runners", "-o", "json"))["items"]

    def prepare(self) -> None:
        cfg = load_cluster_config(self.args.clusters_yaml, CLUSTER)
        # Match the runner generator's whole-block defaults fallback.
        arc_config = resolve(cfg, "arc-runners", {})
        if not isinstance(arc_config, dict):
            raise ValueError("Expected arc-runners configuration to be a mapping")
        group = arc_config.get("runner_group")
        if group != CLUSTER:
            raise ValueError(f"Expected runner group {CLUSTER}, got {group!r}")
        self.recipe("kubeconfig")
        script = self.args.upstream_dir / "modules/arc-runners/scripts/python/runner_max_map.py"
        self.desired = json.loads(
            command(["uv", "run", str(script), CLUSTER], cwd=self.args.root_dir, env=self.env).stdout
        )

    def capacities(self, expected: set[str] | None = None) -> dict[str, float]:
        """Scrape all configured listeners, or only the requested scale sets."""
        pods = json.loads(
            self.kube(
                "get",
                "pods",
                "-n",
                "arc-systems",
                "-l",
                "app.kubernetes.io/component=runner-scale-set-listener",
                "-o",
                "json",
            )
        )["items"]
        listeners = listener_pods(pods, set(self.desired) if expected is None else expected)
        # Resolve the deployed fork's ERS metric label from its owning listener.
        objects = json.loads(self.kube("get", "autoscalinglisteners", "-n", "arc-systems", "-o", "json"))["items"]
        metric_names = {
            obj["metadata"]["name"]: {obj["spec"]["autoscalingRunnerSetName"], obj["spec"]["ephemeralRunnerSetName"]}
            for obj in objects
        }

        deadline = _deadline.get()

        def scrape(item):
            name, (pod, port) = item
            path = f"/api/v1/namespaces/arc-systems/pods/{pod}:{port}/proxy/metrics"
            allowed = metric_names.get(pod, {name})
            if name not in allowed:
                raise ValueError(f"Listener {pod} does not belong to scale set {name}")
            # Executor threads do not inherit ContextVars. Capture this phase's
            # deadline so outstanding reads cannot acquire a fresh cleanup budget.
            with deadline_scope(deadline) if deadline is not None else contextlib.nullcontext():
                return name, advertised_capacity(self.kube("get", "--raw", path), name, metric_names=allowed)

        pool = ThreadPoolExecutor(max_workers=8)
        try:
            return dict(
                pool.map(scrape, listeners.items(), timeout=remaining(float("inf")) if deadline is not None else None)
            )
        finally:
            # Cancellation must reach cluster cleanup without waiting for scrapes.
            pool.shutdown(wait=False, cancel_futures=True)

    def create_pr(self) -> None:
        """Replace workflows only on the cluster's temporary canary branch, never main."""
        base = self.api("git/ref/heads/main")["object"]["sha"]
        base_tree = self.api(f"git/commits/{base}")["tree"]["sha"]
        workflows = self.api(
            "git/trees",
            {
                "tree": [
                    {
                        "path": "drain-test.yaml",
                        "mode": "100644",
                        "type": "blob",
                        "content": self.workflow(),
                    }
                ]
            },
            "POST",
        )["sha"]
        tree = self.api(
            "git/trees",
            {
                "base_tree": base_tree,
                "tree": [{"path": ".github/workflows", "mode": "040000", "type": "tree", "sha": workflows}],
            },
            "POST",
        )["sha"]
        commit = self.api("git/commits", {"message": self.workflow_name, "tree": tree, "parents": [base]}, "POST")[
            "sha"
        ]
        # Register intent before the write: a timed-out API call may have succeeded.
        self.branch_created = True
        self.api("git/refs", {"ref": f"refs/heads/{self.branch}", "sha": commit}, "POST")
        self.pr = self.api(
            "pulls",
            {
                "title": f"{PR_TITLE_PREFIX} {self.run_key}",
                "head": self.branch,
                "base": "main",
                "body": "Temporary OSDC drain/undrain probes. Do not merge.",
            },
            "POST",
        )["number"]
        log.info("Canary PR: https://github.com/%s/pull/%s", self.repository, self.pr)

    def runs(self) -> list[dict]:
        return [
            run
            for run in self.api(f"actions/runs?event=pull_request&branch={self.branch}&per_page=100")["workflow_runs"]
            if run["name"] == self.workflow_name
        ]

    def find_run(self) -> bool:
        runs = self.runs()
        if not runs:
            return False
        if len(runs) != 1:
            raise RuntimeError(f"Expected one canary run, got {len(runs)}")
        self.run_id = runs[0]["id"]
        log.info("Original canary run: https://github.com/%s/actions/runs/%s", self.repository, self.run_id)
        return True

    def jobs(self) -> list[dict]:
        return self.api(f"actions/runs/{self.run_id}/jobs?per_page=100")["jobs"]

    def probes(self, phase: str, count: int) -> list[dict]:
        """Pin the original jobs; wakeup runs must never replace the acceptance probes."""
        jobs = probe_jobs(self.jobs(), phase, count)
        ids = {job.get("id") for job in jobs}
        if len(ids) != count or any(type(job_id) is not int or job_id <= 0 for job_id in ids):
            raise RuntimeError(f"Invalid {phase} probe job IDs: {ids}")
        expected = self.probe_ids.setdefault(phase, ids)
        if ids != expected:
            raise RuntimeError(f"Original {phase} probe jobs changed: {expected} -> {ids}")
        if any(job.get("runner_id") for job in jobs):
            self.assigned_phases.add(phase)
        return jobs

    def wakeup(self, phase: str, attempt: int, deadline: float) -> None:
        """New same-scale-set traffic works around ARC #4715 without rerunning old jobs."""
        if self.draining or not self.branch_created or self.pr is None or self.run_id is None:
            raise RuntimeError("Waking probes requires an active canary outside drain")

        def request(endpoint, payload=None, method="GET"):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("ARC #4715 wakeup deadline exceeded")
            return self.api(endpoint, payload, method, timeout=min(30, remaining))

        head = request(f"git/ref/heads/{self.branch}")["object"]["sha"]
        tree = request(f"git/commits/{head}")["tree"]["sha"]
        tree = request(
            "git/trees",
            {
                "base_tree": tree,
                "tree": [
                    {
                        "path": ".osdc-drain-wakeup",
                        "mode": "100644",
                        "type": "blob",
                        "content": f"{self.run_key}:{phase}:{attempt}\n",
                    }
                ],
            },
            "POST",
        )["sha"]
        commit = request(
            "git/commits",
            {"message": f"ARC #4715 wakeup {phase} {attempt}", "tree": tree, "parents": [head]},
            "POST",
        )["sha"]
        request(f"git/refs/heads/{self.branch}", {"sha": commit, "force": False}, "PATCH")

    def wait_with_wakeup(self, description: str, phase: str, check, timeout: float) -> None:
        """Bound workaround traffic by both count and the original phase deadline."""
        deadline = time.monotonic() + timeout
        next_wakeup = 0.0
        self.wakeup_counts[phase] = 0

        def poll():
            nonlocal next_wakeup
            if self.draining:
                raise RuntimeError("ARC #4715 wakeup is forbidden during drain")
            if check():
                return True
            if phase in self.assigned_phases or self.wakeup_counts[phase] >= 8:
                return False
            if time.monotonic() < next_wakeup or time.monotonic() >= deadline:
                return False
            items = self.scale_sets()
            require_limits(runner_limits(items), self.desired)
            target = self.check_probe(items)
            if self.capacities().get(target, 0) <= 0:
                raise ValueError(f"Waiting for positive advertised capacity on {target}")
            # Record intent before writes: a timed-out ref update may have succeeded.
            self.wakeup_counts[phase] += 1
            next_wakeup = time.monotonic() + 15
            log.info("ARC #4715 workaround: %s wakeup %d/8", phase, self.wakeup_counts[phase])
            self.wakeup(phase, self.wakeup_counts[phase], deadline)
            return False

        wait_for(description, poll, timeout, poll=5)
        log.info("ARC #4715 workaround: %s completed with %d wakeup attempt(s)", phase, self.wakeup_counts[phase])

    def inflight_ready(self) -> bool:
        jobs = self.probes("inflight", 1)
        job = jobs[0]
        if job["status"] == "completed":
            raise RuntimeError("In-flight probe finished before drain")
        if job.get("runner_id") and job.get("runner_group_name") != self.group:
            raise RuntimeError("In-flight probe was assigned to a different runner group")
        return bool(job.get("runner_id")) and any(
            step["name"] == "Wait for drain" and step["status"] == "in_progress" for step in job.get("steps", [])
        )

    def drained(self, *, strict: bool = False, probe_only: bool = False) -> bool:
        """Allow initial convergence, but reject observed regressions immediately."""
        items = self.scale_sets()
        limits = runner_limits(items)
        try:
            require_limits(limits, dict.fromkeys(self.desired, 0))
        except ValueError as exc:
            if strict:
                raise RuntimeError(str(exc)) from exc
            raise
        values = self.capacities({self.check_probe(items)}) if probe_only else self.capacities()
        if any(values.values()):
            error = RuntimeError if strict else ValueError
            raise error(f"Drained listeners still advertise capacity: {values}")
        return True

    def completed(self, phase: str, count: int) -> bool:
        jobs = self.probes(phase, count)
        if any(job["status"] == "completed" and job.get("conclusion") != "success" for job in jobs):
            raise RuntimeError(f"{phase} probe failed: {jobs}")
        if not all(job["status"] == "completed" for job in jobs):
            return False
        try:
            require_success(jobs, self.group)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        return True

    def held_ready(self) -> bool:
        jobs = self.probes("held", 3)
        # Do not retry an invariant failure: even a brief assignment is a failure.
        try:
            require_queued(jobs)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        return True

    def observe_held(self) -> None:
        deadline = time.monotonic() + self.args.observe_seconds
        first = True
        final = False

        def check():
            nonlocal final
            self.held_ready()
            final = time.monotonic() >= deadline
            # Keep every limit check, but sweep all listeners only at the
            # boundaries. Between them, scrape the scale set used by the probes.
            self.drained(strict=True, probe_only=not (first or final))
            return True

        while True:
            # Retry unavailable evidence, never a confirmed invariant failure.
            # Require a successful final check even if retries outlast the window.
            wait_for("held jobs queued and drain still in effect", check, timeout=30, poll=5)
            if final:
                return
            first = False
            left = deadline - time.monotonic()
            if left > 0:
                time.sleep(remaining(min(10, left)))

    def resume(self) -> None:
        """Restore configured limits, not a potentially already-drained snapshot."""
        self.recipe("resume-runners")
        require_limits(runner_limits(self.scale_sets()), self.desired)
        nodes = json.loads(self.kube("get", "nodes", "-l", "workload-type=github-runner", "-o", "json"))["items"]
        if any(t["key"] == "deploy.osdc.io/refresh-pending" for node in nodes for t in node["spec"].get("taints", [])):
            raise ValueError("Runner nodes still have the refresh-pending taint after resume")
        self.draining = False

    def cancel_run(self, run: dict) -> None:
        """Allow a run to finish between listing and cancellation."""
        try:
            self.api(f"actions/runs/{run['id']}/cancel", method="POST")
        except subprocess.SubprocessError:
            if self.api(f"actions/runs/{run['id']}")["status"] != "completed":
                raise

    def cleanup_stale_canary(self) -> None:
        """Reclaim the test's fixed branch, including refs left without a PR."""
        page = 1
        while True:
            runs = self.api(f"actions/runs?event=pull_request&branch={self.branch}&per_page=100&page={page}")[
                "workflow_runs"
            ]
            for run in runs:
                if run["status"] != "completed":
                    self.cancel_run(run)
            if len(runs) < 100:
                break
            page += 1
        pulls = self.api(f"pulls?head={self.repository.split('/')[0]}:{self.branch}&state=open&per_page=100")
        for pr in pulls:
            self.api(f"pulls/{pr['number']}", {"state": "closed"}, "PATCH")
        # matching-refs returns an empty list for a missing branch; API errors
        # still propagate. Match exactly so similarly named branches are safe.
        ref = f"refs/heads/{self.branch}"
        refs = self.api(f"git/matching-refs/heads/{self.branch}")
        if any(item["ref"] == ref for item in refs):
            self.api(f"git/refs/heads/{self.branch}", method="DELETE")

    def run(self) -> None:
        self.prepare()
        self.cleanup_stale_canary()
        items = self.scale_sets()
        require_limits(runner_limits(items), self.desired)
        self.check_probe(items)
        # Establish metric availability before mutating anything.
        wait_for("listener metrics available", lambda: bool(self.capacities()), self.args.drain_timeout)
        self.create_pr()
        wait_for("canary workflow created", self.find_run, 120)
        self.wait_with_wakeup(
            f"in-flight job executing its wait step on {self.cluster}",
            "inflight",
            self.inflight_ready,
            self.args.start_timeout,
        )
        drain_owner = self.record_recovery()
        self.draining = True
        # Let controller phase deadlines bound convergence and probe teardown.
        self.drain = subprocess.Popen(
            ["just", "drain-runners", self.cluster],
            cwd=self.args.root_dir,
            env={
                **self.env,
                **drain_owner,
                "OSDC_DRAIN_TIMEOUT_SECS": str(math.ceil(remaining(self.args.run_timeout))),
            },
            start_new_session=True,
        )
        wait_for("zero runner limits and advertised capacity", self.drained, self.args.drain_timeout)
        self.api(f"pulls/{self.pr}", {"body": f"OSDC_DRAIN_RELEASE_{self.run_key}"}, "PATCH")
        wait_for("in-flight job succeeds after drain", lambda: self.completed("inflight", 1), 180)
        if self.drain.wait(timeout=remaining(180)) != 0:
            raise RuntimeError("drain-runners failed")
        # Reap remaining group members before observation/resume, and retire the
        # group ID promptly rather than retaining it through the completion wait.
        self.stop_drain()
        wait_for("held jobs queued", self.held_ready, 120)
        self.observe_held()
        self.resume()
        self.wait_with_wakeup(
            "held jobs succeed after undrain", "held", lambda: self.completed("held", 3), self.args.finish_timeout
        )

    def stop_drain(self) -> None:
        """Stop all owned recipe children before restoring; avoid late zero patches."""
        if self.drain is None:
            return
        # start_new_session=True makes the original PID the process group ID.
        # The group can outlive its leader, so poll()/wait() alone are insufficient.
        pgid = self.drain.pid
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGTERM)
        try:
            wait_for("drain process group stopped", lambda: not process_group_running(pgid), 2, poll=0.05)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
            wait_for("drain process group killed", lambda: not process_group_running(pgid), 2, poll=0.05)
        self.drain.wait(timeout=remaining(2))
        self.drain = None

    def cleanup(self) -> bool:
        """Share one cleanup deadline and retry count with the atexit fallback."""
        if self.cleanup_deadline is None:
            self.cleanup_deadline = time.monotonic() + remaining(self.args.cleanup_timeout)
        if self.draining:
            log.warning("Recovery command: cd %s && just resume-runners %s", self.args.root_dir, self.cluster)
        try:
            with deadline_scope(self.cleanup_deadline):
                return self._cleanup()
        except TimeoutError:
            log.exception("Cleanup deadline exceeded")
            if self.draining:
                log.error("RECOVERY REQUIRED: cd %s && just resume-runners %s", self.args.root_dir, self.cluster)
            return False

    def _cleanup(self) -> bool:
        ok = True
        try:
            self.stop_drain()
            if self.recovery_file is not None and self.recovery_file.exists():
                self.stop_recorded_drain()
        except (OSError, subprocess.SubprocessError):
            log.exception("Could not stop drain process; refusing to race it with resume")
            return False
        if self.defer_recovery and (self.draining or self.skip_canary_cleanup):
            log.warning("Deferring cluster recovery to the workflow restore step; leaving canary cleanup for next run")
            return True
        # Local runs restore first. GitHub API failure must never prevent recovery.
        if self.draining:
            while self.recovery_attempts < 3:
                try:
                    remaining(float("inf"))
                    self.recovery_attempts += 1
                    self.resume()
                    break
                except TimeoutError:
                    raise
                except Exception:
                    log.exception("Recovery attempt %d/3 failed", self.recovery_attempts)
            if self.draining:
                log.error("RECOVERY REQUIRED: cd %s && just resume-runners %s", self.args.root_dir, self.cluster)
                ok = False
        if self.branch_created:
            try:
                for run in self.runs():
                    if run["status"] != "completed":
                        self.cancel_run(run)
                # Resolve by head branch even if PR creation timed out before returning its number.
                pulls = json.loads(
                    command(
                        [
                            "gh",
                            "api",
                            f"repos/{self.repository}/pulls?head={self.repository.split('/')[0]}:{self.branch}&state=open",
                        ]
                    ).stdout
                )
                for pr in pulls:
                    self.api(f"pulls/{pr['number']}", {"state": "closed"}, "PATCH")
                self.api(f"git/refs/heads/{self.branch}", method="DELETE")
                self.branch_created = False
            except Exception:
                log.exception("Canary cleanup failed for %s", self.branch)
                ok = False
        return ok


def positive(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster-id", choices=[CLUSTER], required=True)
    parser.add_argument("--root-dir", type=Path, default=UPSTREAM)
    parser.add_argument("--upstream-dir", type=Path, default=UPSTREAM)
    parser.add_argument("--clusters-yaml", type=Path, default=UPSTREAM / "clusters.yaml")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print probe workflow without API calls or cluster access")
    mode.add_argument(
        "--restore-only", action="store_true", help="Resume and verify configured runner limits and taints"
    )
    mode.add_argument(
        "--defer-recovery",
        action="store_true",
        help="CI only: leave exception recovery to the subsequent workflow step; skip canary cleanup on failure",
    )
    parser.add_argument(
        "--recovery-file",
        type=Path,
        default=os.environ.get("OSDC_DRAIN_RECOVERY_FILE"),
        help="Linux runner-local intent file shared with the subsequent restore-only step",
    )
    parser.add_argument("--run-timeout", type=positive, default=RUN_TIMEOUT, help="Main flow budget in seconds")
    parser.add_argument("--cleanup-timeout", type=positive, default=CLEANUP_TIMEOUT, help="Cleanup budget in seconds")
    parser.add_argument(
        "--restore-timeout", type=positive, default=RESTORE_TIMEOUT, help="Restore-only total budget in seconds"
    )
    parser.add_argument("--start-timeout", type=positive, default=900, help="Queue/start deadline in seconds")
    parser.add_argument("--finish-timeout", type=positive, default=900, help="Queue/completion deadline in seconds")
    parser.add_argument("--drain-timeout", type=positive, default=300, help="Drain convergence deadline in seconds")
    parser.add_argument("--observe-seconds", type=positive, default=60)
    args = parser.parse_args()
    if args.defer_recovery and args.recovery_file is None:
        parser.error("--defer-recovery requires --recovery-file or OSDC_DRAIN_RECOVERY_FILE")
    return args


def on_signal(signum: int, frame: object) -> None:
    raise SystemExit(128 + signum)


def run_controller(test: DrainTest, *, restore_only: bool = False) -> None:
    previous = {sig: signal.signal(sig, on_signal) for sig in (signal.SIGINT, signal.SIGTERM)}
    atexit.register(test.cleanup)
    passed = False
    restore_deadline = time.monotonic() + test.args.restore_timeout if restore_only else None
    if restore_only:
        test.cleanup_deadline = restore_deadline
    try:
        if restore_only:
            with deadline_scope(restore_deadline):
                test.stop_recorded_drain()
                test.prepare()
                test.draining = True
                # Count the initial attempt against cleanup/atexit's shared limit.
                test.recovery_attempts += 1
                test.resume()
        else:
            with deadline_scope(time.monotonic() + test.args.run_timeout):
                test.run()
        passed = True
    finally:
        # Failed/cancelled CI tests must reach the restore step without GitHub API cleanup.
        if test.defer_recovery:
            test.skip_canary_cleanup = not passed
        # A second signal must not interrupt this process's recovery attempt.
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        cleaned = test.cleanup()
        if cleaned:
            atexit.unregister(test.cleanup)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        if passed and not cleaned:
            raise RuntimeError("Test passed but cleanup failed")


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    key = uuid4().hex
    if args.dry_run:
        print(generate_workflow(key))
        return
    run_controller(DrainTest(args, key), restore_only=args.restore_only)


if __name__ == "__main__":
    main()
