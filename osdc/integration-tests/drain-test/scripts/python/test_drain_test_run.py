"""Offline controller tests with mocked services and local process-cleanup regression checks."""

import argparse
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from unittest.mock import Mock, call

import drain_test_run as module
import pytest
import yaml
from drain_test_checks import CAPACITY_METRIC, CLUSTER, PROBE_LABEL, SCALE_SET_LABEL


@pytest.fixture
def controller(tmp_path, monkeypatch):
    config = tmp_path / "clusters.yaml"
    config.write_text(f"clusters:\n  {CLUSTER}:\n    arc-runners:\n      runner_group: {CLUSTER}\n")
    args = argparse.Namespace(
        cluster_id=CLUSTER,
        root_dir=tmp_path,
        upstream_dir=tmp_path,
        clusters_yaml=config,
        dry_run=False,
        restore_only=False,
        defer_recovery=False,
        recovery_file=None,
        run_timeout=module.RUN_TIMEOUT,
        cleanup_timeout=module.CLEANUP_TIMEOUT,
        restore_timeout=module.RESTORE_TIMEOUT,
        start_timeout=1800,
        finish_timeout=1800,
        drain_timeout=600,
        observe_seconds=60,
    )
    test = module.DrainTest(args, "abc123")
    monkeypatch.setattr(test, "cleanup_stale_canary", Mock())
    test.desired = {"cpu": 8}
    return test


def ars(limit=8):
    return [{"metadata": {"name": "cpu"}, "spec": {"maxRunners": limit, "runnerScaleSetName": PROBE_LABEL}}]


def job(phase="inflight", status="completed", conclusion="success", group=CLUSTER, job_id=1):
    return {
        "id": job_id,
        "name": f"drain-{phase}",
        "status": status,
        "conclusion": conclusion,
        "runner_group_name": group,
    }


def test_command_and_api_transport(monkeypatch):
    proc = Mock(returncode=0)
    proc.communicate.return_value = ('{"ok":true}', "")
    run = Mock(return_value=proc)
    monkeypatch.setattr(module.subprocess, "Popen", run)
    assert module.api("pulls/3", {"body": "release"}, "PATCH") == {"ok": True}
    args, kwargs = run.call_args
    assert args[0] == ["gh", "api", f"repos/{module.CANARY_REPO}/pulls/3", "--method", "PATCH", "--input", "-"]
    assert json.loads(proc.communicate.call_args.kwargs["input"]) == {"body": "release"}
    assert proc.communicate.call_args.kwargs["timeout"] == 30
    assert kwargs["start_new_session"] is True
    assert module.kubectl("get", "nodes") == '{"ok":true}'
    assert run.call_args.args[0][:2] == ["kubectl", "--request-timeout=15s"]
    proc.communicate.return_value = ("", "")
    assert module.api("actions/runs/1/cancel", method="POST") == {}
    proc.returncode = 1
    result = module.command(["false"], check=False)
    assert result.returncode == 1
    assert result.stdout == result.stderr == ""
    assert "check" not in run.call_args.kwargs
    with pytest.raises(subprocess.CalledProcessError):
        module.command(["false"])


def test_wait_convergence_and_deadline(clock):
    check = Mock(side_effect=[ValueError("not zero"), False, True])
    module.wait_for("drain", check, timeout=5, poll=1)
    assert clock[0] == 2
    with pytest.raises(TimeoutError, match=r"deadline exceeded.*API unavailable"):
        module.wait_for("queued", Mock(side_effect=subprocess.SubprocessError("API unavailable")), timeout=1)
    assert clock[0] == 3
    with pytest.raises(RuntimeError, match="escaped"):
        module.wait_for("queued", Mock(side_effect=RuntimeError("escaped")), timeout=1)


@pytest.mark.parametrize(
    ("cluster_config", "defaults"),
    [
        pytest.param({"arc-runners": {"runner_group": CLUSTER}}, {}, id="cluster-block"),
        pytest.param(
            {"arc-runners": {"runner_group": CLUSTER}},
            {"arc-runners": {"runner_group": "other"}},
            id="cluster-block-overrides-defaults",
        ),
        pytest.param({}, {"arc-runners": {"runner_group": CLUSTER}}, id="defaults-block"),
        pytest.param({"arc-runners": None}, {"arc-runners": {"runner_group": CLUSTER}}, id="null-block-fallback"),
    ],
)
def test_prepare_and_recipe(controller, monkeypatch, cluster_config, defaults):
    controller.args.clusters_yaml.write_text(
        yaml.safe_dump({"clusters": {CLUSTER: cluster_config}, "defaults": defaults})
    )
    command = Mock(return_value=subprocess.CompletedProcess([], 0, '{"cpu":8}', ""))
    monkeypatch.setattr(module, "command", command)
    controller.prepare()
    assert controller.desired == {"cpu": 8}
    assert command.call_args_list[0].args[0] == ["just", "kubeconfig", CLUSTER]
    assert command.call_args_list[1].args[0][-1] == CLUSTER
    assert command.call_args.kwargs["env"]["CLUSTERS_YAML"] == str(controller.args.clusters_yaml)


@pytest.mark.parametrize(
    "arc_config",
    [
        pytest.param({"runner_group": "other"}, id="wrong-group"),
        pytest.param({}, id="empty-block-does-not-inherit-group"),
        pytest.param({"github_config_url": "https://github.com/pytorch"}, id="partial-block-does-not-inherit-group"),
    ],
)
def test_wrong_group_preflight_is_read_only(controller, monkeypatch, arc_config):
    controller.args.clusters_yaml.write_text(
        yaml.safe_dump(
            {
                "defaults": {"arc-runners": {"runner_group": CLUSTER}},
                "clusters": {CLUSTER: {"arc-runners": arc_config}},
            }
        )
    )
    command = Mock()
    monkeypatch.setattr(module, "command", command)
    with pytest.raises(ValueError, match="Expected runner group"):
        controller.prepare()
    command.assert_not_called()


def test_invalid_arc_config_preflight_is_read_only(controller, monkeypatch):
    controller.args.clusters_yaml.write_text(yaml.safe_dump({"clusters": {CLUSTER: {"arc-runners": []}}}))
    command = Mock()
    monkeypatch.setattr(module, "command", command)
    with pytest.raises(ValueError, match="Expected arc-runners configuration to be a mapping"):
        controller.prepare()
    command.assert_not_called()


def test_unknown_cluster_preflight_is_read_only(controller, monkeypatch, caplog):
    controller.args.clusters_yaml.write_text(yaml.safe_dump({"clusters": {}}))
    command = Mock()
    monkeypatch.setattr(module, "command", command)
    with pytest.raises(SystemExit) as exc:
        controller.prepare()
    assert exc.value.code == 1
    assert f"Cluster '{CLUSTER}' not found" in caplog.text
    command.assert_not_called()


@pytest.mark.parametrize("bounded", [False, True])
def test_scale_sets_and_live_metrics(controller, monkeypatch, bounded):
    pod = {
        "metadata": {"name": "listener", "labels": {SCALE_SET_LABEL: "cpu"}},
        "spec": {"containers": [{"ports": [{"name": "metrics", "containerPort": 8080}]}]},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }
    linked = {
        "metadata": {"name": "listener"},
        "spec": {"autoscalingRunnerSetName": "cpu", "ephemeralRunnerSetName": "cpu-ers"},
    }
    responses = iter(
        [
            json.dumps({"items": ars()}),
            json.dumps({"items": [pod]}),
            json.dumps({"items": [linked]}),
            f'{CAPACITY_METRIC}{{name="cpu-ers"}} 0\n',
        ]
    )

    def read(*args):
        if "--raw" in args:
            assert module._deadline.get() == deadline
        return next(responses)

    deadline = time.monotonic() + 5 if bounded else None
    kube = Mock(side_effect=read)
    monkeypatch.setattr(module, "kubectl", kube)
    assert controller.scale_sets() == ars()
    with module.deadline_scope(deadline) if bounded else contextlib.nullcontext():
        assert controller.capacities() == {"cpu": 0}
    assert kube.call_args.args == ("get", "--raw", "/api/v1/namespaces/arc-systems/pods/listener:8080/proxy/metrics")


@pytest.mark.parametrize("expected", [None, {"cpu"}, set()])
def test_capacities_scrapes_only_selected_listeners(controller, monkeypatch, expected):
    controller.desired = {"cpu": 8, **{f"other-{i}": 0 for i in range(48)}}
    pods = [
        {
            "metadata": {"name": f"{name}-listener", "labels": {SCALE_SET_LABEL: name}},
            "spec": {"containers": [{"ports": [{"name": "metrics", "containerPort": 8080}]}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
        for name in controller.desired
    ]
    linked = [
        {
            "metadata": {"name": f"{name}-listener"},
            "spec": {"autoscalingRunnerSetName": name, "ephemeralRunnerSetName": f"{name}-ers"},
        }
        for name in controller.desired
    ]
    metrics = {
        f"/api/v1/namespaces/arc-systems/pods/{name}-listener:8080/proxy/metrics": f'{CAPACITY_METRIC}{{name="{name}-ers"}} 0\n'
        for name in controller.desired
    }

    def read(*args):
        if args[1] == "pods":
            return json.dumps({"items": pods})
        if args[1] == "autoscalinglisteners":
            return json.dumps({"items": linked})
        return metrics[args[2]]

    kube = Mock(side_effect=read)
    monkeypatch.setattr(controller, "kube", kube)
    if expected == set():
        with pytest.raises(ValueError, match="Missing ready listeners"):
            controller.capacities(expected)
        assert kube.call_count == 1
    else:
        names = set(controller.desired) if expected is None else expected
        assert controller.capacities(expected) == dict.fromkeys(names, 0)
        assert kube.call_count == 2 + len(names)
        assert {call.args[2] for call in kube.call_args_list if "--raw" in call.args} == {
            f"/api/v1/namespaces/arc-systems/pods/{name}-listener:8080/proxy/metrics" for name in names
        }


def test_canary_pr_tree_and_branch(controller, monkeypatch):
    next_run = module.DrainTest(controller.args, "def456")
    assert controller.branch == next_run.branch == f"osdc-drain-test-{CLUSTER}"
    assert controller.workflow_name != next_run.workflow_name
    assert "OSDC_DRAIN_RELEASE_abc123" in controller.workflow()
    assert "OSDC_DRAIN_RELEASE_def456" in next_run.workflow()
    api = Mock(
        side_effect=[
            {"object": {"sha": "base"}},
            {"tree": {"sha": "base-tree"}},
            {"sha": "workflows"},
            {"sha": "tree"},
            {"sha": "commit"},
            {},
            {"number": 4},
        ]
    )
    monkeypatch.setattr(module, "api", api)
    controller.create_pr()
    assert controller.pr == 4
    assert controller.branch_created
    assert api.call_args_list[3].args[1] == {
        "base_tree": "base-tree",
        "tree": [{"path": ".github/workflows", "mode": "040000", "type": "tree", "sha": "workflows"}],
    }
    ref = api.call_args_list[5].args[1]
    assert ref == {"ref": f"refs/heads/{controller.branch}", "sha": "commit"}
    assert ref["ref"] != "refs/heads/main"
    assert api.call_args_list[6].args[1]["title"] == f"{module.PR_TITLE_PREFIX} abc123"
    assert "OSDC_DRAIN_RELEASE_abc123" not in api.call_args_list[6].args[1]["body"]


def test_ref_timeout_retains_cleanup_intent(controller, monkeypatch):
    api = Mock(
        side_effect=[
            {"object": {"sha": "base"}},
            {"tree": {"sha": "base-tree"}},
            {"sha": "workflows"},
            {"sha": "tree"},
            {"sha": "commit"},
            subprocess.TimeoutExpired("gh", 30),
        ]
    )
    monkeypatch.setattr(module, "api", api)
    with pytest.raises(subprocess.TimeoutExpired):
        controller.create_pr()
    assert controller.branch_created


def test_startup_removes_orphan_branch_without_pr_before_creating_canary(controller, monkeypatch):
    test = module.DrainTest(controller.args, "next123")
    test.desired = controller.desired
    branch_exists = True
    events = []

    def api(endpoint, payload=None, method="GET"):
        nonlocal branch_exists
        if endpoint.startswith("actions/runs?"):
            return {"workflow_runs": []}
        if endpoint.startswith("pulls?"):
            return []  # The previous process died before creating its PR.
        if endpoint.startswith("git/matching-refs/"):
            return [{"ref": f"refs/heads/{test.branch}"}] if branch_exists else []
        if endpoint == f"git/refs/heads/{test.branch}" and method == "DELETE":
            branch_exists = False
            events.append("delete-orphan")
            return {}
        if endpoint == "git/ref/heads/main":
            return {"object": {"sha": "base"}}
        if endpoint == "git/commits/base":
            return {"tree": {"sha": "base-tree"}}
        if endpoint in {"git/trees", "git/commits"}:
            return {"sha": "new-object"}
        if endpoint == "git/refs":
            if branch_exists:
                raise subprocess.CalledProcessError(1, "gh api", stderr="Reference already exists (HTTP 422)")
            branch_exists = True
            events.append("create-branch")
            return {}
        assert endpoint == "pulls"
        assert method == "POST"
        events.append("create-pr")
        return {"number": 4}

    monkeypatch.setattr(test, "api", api)
    monkeypatch.setattr(test, "prepare", Mock())
    monkeypatch.setattr(test, "scale_sets", lambda: ars())
    monkeypatch.setattr(test, "capacities", lambda: {"cpu": 0})
    monkeypatch.setattr(test, "find_run", Mock(side_effect=RuntimeError("new canary created")))
    with pytest.raises(RuntimeError, match="new canary created"):
        test.run()
    assert events == ["delete-orphan", "create-branch", "create-pr"]
    assert test.pr == 4


@pytest.mark.parametrize("branch_exists", [False, True])
@pytest.mark.parametrize("pr_exists", [False, True])
def test_startup_cleanup_cancels_runs_then_closes_pr_then_deletes_only_exact_ref(
    controller, monkeypatch, branch_exists, pr_exists
):
    ref = f"refs/heads/{controller.branch}"
    refs = [{"ref": f"{ref}-other"}, *([{"ref": ref}] if branch_exists else [])]
    responses = [
        {
            "workflow_runs": [
                {"id": 1, "status": "queued"},
                {"id": 2, "status": "in_progress"},
                {"id": 3, "status": "completed"},
            ]
        },
        {},
        {},
        [{"number": 4}] if pr_exists else [],
        *([{}] if pr_exists else []),
        refs,
        *([{}] if branch_exists else []),
    ]
    api = Mock(side_effect=responses)
    monkeypatch.setattr(controller, "api", api)
    module.DrainTest.cleanup_stale_canary(controller)
    assert api.call_args_list == [
        call(f"actions/runs?event=pull_request&branch={controller.branch}&per_page=100&page=1"),
        call("actions/runs/1/cancel", method="POST"),
        call("actions/runs/2/cancel", method="POST"),
        call(f"pulls?head=pytorch:{controller.branch}&state=open&per_page=100"),
        *([call("pulls/4", {"state": "closed"}, "PATCH")] if pr_exists else []),
        call(f"git/matching-refs/heads/{controller.branch}"),
        *([call(f"git/refs/heads/{controller.branch}", method="DELETE")] if branch_exists else []),
    ]
    assert not controller.branch_created  # Startup never claims a new branch.


def test_startup_cleanup_finds_active_runs_beyond_first_page(controller, monkeypatch):
    api = Mock(
        side_effect=[
            {"workflow_runs": [{"id": i, "status": "completed"} for i in range(100)]},
            {"workflow_runs": [{"id": 101, "status": "queued"}]},
            {},
            [],
            [],
        ]
    )
    monkeypatch.setattr(controller, "api", api)
    module.DrainTest.cleanup_stale_canary(controller)
    assert api.call_args_list[:3] == [
        call(f"actions/runs?event=pull_request&branch={controller.branch}&per_page=100&page=1"),
        call(f"actions/runs?event=pull_request&branch={controller.branch}&per_page=100&page=2"),
        call("actions/runs/101/cancel", method="POST"),
    ]


@pytest.mark.parametrize("failure", ["list-runs", "cancel-run", "list-prs", "close-pr", "list-refs", "delete-ref"])
def test_startup_cleanup_failure_stops_before_preflight_or_canary_creation(controller, monkeypatch, failure):
    monkeypatch.delattr(controller, "cleanup_stale_canary")
    prepare, sets, create = Mock(), Mock(), Mock()
    monkeypatch.setattr(controller, "prepare", prepare)
    monkeypatch.setattr(controller, "scale_sets", sets)
    monkeypatch.setattr(controller, "create_pr", create)

    def api(endpoint, payload=None, method="GET"):
        if endpoint.startswith("actions/runs?"):
            operation, result = "list-runs", {"workflow_runs": [{"id": 1, "status": "queued"}]}
        elif endpoint == "actions/runs/1/cancel":
            operation, result = "cancel-run", {}
        elif endpoint == "actions/runs/1":
            return {"status": "in_progress"}  # Cancellation really failed.
        elif endpoint.startswith("pulls?"):
            operation, result = "list-prs", [{"number": 4}]
        elif endpoint == "pulls/4":
            operation, result = "close-pr", {}
        elif endpoint.startswith("git/matching-refs/"):
            operation, result = "list-refs", [{"ref": f"refs/heads/{controller.branch}"}]
        else:
            assert endpoint == f"git/refs/heads/{controller.branch}"
            assert method == "DELETE"
            operation, result = "delete-ref", {}
        if operation == failure:
            raise subprocess.CalledProcessError(1, "gh api", stderr=operation)
        return result

    monkeypatch.setattr(controller, "api", api)
    with pytest.raises(subprocess.CalledProcessError) as exc:
        controller.run()
    assert exc.value.stderr == failure
    prepare.assert_called_once()
    sets.assert_not_called()
    create.assert_not_called()
    assert not controller.draining
    assert not controller.branch_created


def test_canary_run_lookup_and_job_api(controller, monkeypatch):
    api = Mock(
        return_value={"workflow_runs": [{"id": 99, "name": controller.workflow_name}, {"id": 100, "name": "unrelated"}]}
    )
    monkeypatch.setattr(module, "api", api)
    assert controller.find_run()
    assert controller.run_id == 99
    api.return_value = {"jobs": [job()]}
    assert controller.jobs() == [job()]
    assert api.call_args.args[0] == "actions/runs/99/jobs?per_page=100"
    api.return_value = {"workflow_runs": []}
    assert not controller.find_run()
    api.return_value = {"workflow_runs": [{"name": controller.workflow_name}] * 2}
    with pytest.raises(RuntimeError, match="Expected one canary run"):
        controller.find_run()


def test_wait_for_actual_inflight_step(controller, monkeypatch):
    running = job(status="in_progress")
    jobs = Mock(return_value=[running])
    monkeypatch.setattr(controller, "jobs", jobs)
    assert not controller.inflight_ready()
    running["steps"] = [{"name": "Wait for drain", "status": "in_progress"}]
    assert not controller.inflight_ready()
    running["runner_id"] = 3
    assert controller.inflight_ready()
    running["runner_group_name"] = "other"
    with pytest.raises(RuntimeError, match="different runner group"):
        controller.inflight_ready()
    jobs.return_value = [job()]
    with pytest.raises(RuntimeError, match="finished before drain"):
        controller.inflight_ready()


@pytest.mark.parametrize("strict", [False, True])
def test_drained_requires_config_and_capacity(controller, monkeypatch, strict):
    sets = Mock(return_value=ars(0))
    caps = Mock(return_value={"cpu": 0})
    monkeypatch.setattr(controller, "scale_sets", sets)
    monkeypatch.setattr(controller, "capacities", caps)
    assert controller.drained(strict=strict)
    error = RuntimeError if strict else ValueError
    caps.return_value = {"cpu": 2147483647}
    with pytest.raises(error, match="still advertise capacity"):
        controller.drained(strict=strict)
    sets.return_value = ars(8)
    with pytest.raises(error, match="Runner limits differ"):
        controller.drained(strict=strict)


@pytest.mark.parametrize("source", ["limits", "capacity"])
def test_initial_drain_waits_for_convergence(controller, monkeypatch, clock, source):
    sets = Mock(side_effect=[ars(8), ars(0)]) if source == "limits" else Mock(return_value=ars(0))
    caps = Mock(side_effect=[{"cpu": 8}, {"cpu": 0}]) if source == "capacity" else Mock(return_value={"cpu": 0})
    monkeypatch.setattr(controller, "scale_sets", sets)
    monkeypatch.setattr(controller, "capacities", caps)
    module.wait_for("initial drain", controller.drained, timeout=30, poll=5)
    assert clock[0] == 5


def test_completion_and_held_job_invariants(controller, monkeypatch):
    jobs = Mock(return_value=[job(status="queued")])
    monkeypatch.setattr(controller, "jobs", jobs)
    assert not controller.completed("inflight", 1)
    jobs.return_value = [job()]
    assert controller.completed("inflight", 1)
    jobs.return_value = [job(conclusion="failure")]
    with pytest.raises(RuntimeError, match="probe failed"):
        controller.completed("inflight", 1)
    jobs.return_value = [job(group="other")]
    with pytest.raises(RuntimeError, match="did not succeed"):
        controller.completed("inflight", 1)
    jobs.return_value = [job("held", status="queued", job_id=i) for i in range(1, 4)]
    assert controller.held_ready()
    jobs.return_value = [job("held", job_id=i) for i in range(1, 4)]
    with pytest.raises(RuntimeError, match="escaped"):
        controller.held_ready()


def test_observation_window_checks_every_poll(controller, monkeypatch, clock):
    held = Mock(return_value=True)
    drained = Mock(return_value=True)
    monkeypatch.setattr(controller, "held_ready", held)
    monkeypatch.setattr(controller, "drained", drained)
    controller.observe_held()
    assert clock[0] == 60
    assert held.call_count == drained.call_count == 7
    assert drained.call_args_list == [
        call(strict=True, probe_only=False),
        *[call(strict=True, probe_only=True)] * 5,
        call(strict=True, probe_only=False),
    ]


@pytest.fixture
def observing_controller(controller, monkeypatch):
    monkeypatch.setattr(
        controller, "jobs", Mock(return_value=[job("held", status="queued", job_id=i) for i in range(1, 4)])
    )
    monkeypatch.setattr(controller, "scale_sets", Mock(return_value=ars(0)))
    monkeypatch.setattr(controller, "capacities", Mock(return_value={"cpu": 0}))
    return controller


@pytest.mark.parametrize("failure", ["limits", "capacity", "assigned", "completed"])
def test_observation_rejects_intermediate_regressions(observing_controller, clock, failure):
    test = observing_controller
    test.desired["other"] = 8

    def scale_sets():
        other_limit = 1 if failure == "limits" and clock[0] >= 10 else 0
        return [*ars(0), {"metadata": {"name": "other"}, "spec": {"maxRunners": other_limit}}]

    def capacities(expected=None):
        assert expected == ({"cpu"} if clock[0] else None)
        value = 1 if failure == "capacity" and clock[0] >= 10 else 0
        return {"cpu": value} if expected is not None else {"cpu": value, "other": 0}

    def jobs():
        items = [job("held", status="queued", job_id=i) for i in range(1, 4)]
        if clock[0] >= 10:
            if failure == "assigned":
                items[0]["runner_id"] = 123
            elif failure == "completed":
                items[0]["status"] = "completed"
        return items

    test.scale_sets.side_effect = scale_sets
    test.capacities.side_effect = capacities
    test.jobs.side_effect = jobs
    message = {"limits": "Runner limits differ", "capacity": "still advertise capacity"}.get(failure, "Jobs escaped")
    with pytest.raises(RuntimeError, match=message):
        test.observe_held()
    assert clock[0] == 10
    assert test.jobs.call_count == 2


def test_observation_final_sweep_checks_other_listener(observing_controller, clock):
    test = observing_controller
    test.desired["other"] = 8
    test.scale_sets.return_value = [*ars(0), {"metadata": {"name": "other"}, "spec": {"maxRunners": 0}}]

    def capacities(expected=None):
        if expected is not None:
            assert expected == {"cpu"}
            return {"cpu": 0}
        return {"cpu": 0, "other": int(clock[0] >= 60)}

    test.capacities.side_effect = capacities
    with pytest.raises(RuntimeError, match="still advertise capacity"):
        test.observe_held()
    assert clock[0] == 60
    assert test.scale_sets.call_count == 7
    assert test.capacities.call_args_list == [call(), *[call({"cpu"})] * 5, call()]


@pytest.mark.parametrize("slow_check", [0, 50])
def test_observation_requires_full_sweep_after_slow_check(observing_controller, clock, slow_check):
    test = observing_controller
    test.args.observe_seconds = 5 if slow_check == 0 else 60
    sweeps = []

    def capacities(expected=None):
        sweeps.append((clock[0], expected))
        if clock[0] == slow_check:
            clock[0] = test.args.observe_seconds + 1
        return {"cpu": 0}

    test.capacities.side_effect = capacities
    test.observe_held()
    assert sweeps[-2:] == [(slow_check, None if slow_check == 0 else {"cpu"}), (test.args.observe_seconds + 1, None)]
    assert test.jobs.call_count == test.scale_sets.call_count == len(sweeps)


def test_observation_retry_crossing_deadline_uses_full_sweep(observing_controller, clock):
    test = observing_controller

    def capacities(expected=None):
        if 50 <= clock[0] < 60:
            assert expected == {"cpu"}
            raise ValueError("Missing ready listeners")
        if clock[0] >= 60:
            assert expected is None
        return {"cpu": 0}

    test.capacities.side_effect = capacities
    test.observe_held()
    assert clock[0] == 60
    assert test.capacities.call_args_list[-3:] == [call({"cpu"}), call({"cpu"}), call()]
    assert test.jobs.call_count == test.scale_sets.call_count == test.capacities.call_count


@pytest.mark.parametrize(
    ("source", "failure"),
    [
        ("jobs", subprocess.CalledProcessError(1, ["gh", "api"])),
        ("scale_sets", subprocess.CalledProcessError(1, ["kubectl", "get"])),
        ("capacities", subprocess.TimeoutExpired("kubectl", 30)),
        ("capacities", ValueError("Missing ready listeners")),
        ("capacities", ValueError("Missing or invalid gha_capacity_advertised_max_runners")),
        ("scale_sets", ValueError("Invalid JSON snapshot")),
    ],
)
def test_observation_retries_transient_reads(observing_controller, clock, source, failure):
    read = getattr(observing_controller, source)

    def snapshot(*args):
        if clock[0] == 0:
            raise failure
        return read.return_value

    read.side_effect = snapshot
    observing_controller.observe_held()
    assert read.call_count > 2
    assert clock[0] == 60


@pytest.mark.parametrize("snapshot", [[], ars(-1)])
def test_observation_retries_unavailable_runner_snapshot(observing_controller, clock, snapshot):
    sets = observing_controller.scale_sets
    sets.side_effect = lambda: snapshot if clock[0] < 5 else ars(0)
    observing_controller.observe_held()
    assert clock[0] == 60
    assert sets.call_count == 8


@pytest.mark.parametrize("failure", ["assigned", "completed", "limits", "capacity"])
def test_observation_fails_immediately_on_regression(observing_controller, clock, failure):
    test = observing_controller
    if failure in ("assigned", "completed"):
        jobs = test.jobs.return_value
        if failure == "assigned":
            jobs[0]["runner_id"] = 123
        else:
            jobs[0]["status"] = "completed"
        message = "Jobs escaped"
    elif failure == "limits":
        test.scale_sets.return_value = ars(8)
        message = "Runner limits differ"
    else:
        test.capacities.return_value = {"cpu": 1}
        message = "still advertise capacity"
    with pytest.raises(RuntimeError, match=message):
        test.observe_held()
    assert clock[0] == 0
    test.jobs.assert_called_once()


def test_observation_rechecks_jobs_after_transient_cluster_failure(observing_controller, clock):
    test = observing_controller
    test.capacities.side_effect = ValueError("Missing ready listeners")
    queued = test.jobs.return_value
    escaped = [{**item, "runner_id": 123} for item in queued]
    test.jobs.side_effect = [queued, escaped]
    with pytest.raises(RuntimeError, match="Jobs escaped"):
        test.observe_held()
    assert clock[0] == 5
    assert test.jobs.call_count == 2
    test.capacities.assert_called_once()


@pytest.mark.parametrize("recovers", [False, True])
def test_observation_requires_successful_final_check(observing_controller, clock, recovers):
    test = observing_controller

    def capacities(expected=None):
        if clock[0] >= 60 and (not recovers or clock[0] < 65):
            raise ValueError("Missing ready listeners")
        return {"cpu": 0}

    test.capacities.side_effect = capacities
    if recovers:
        test.observe_held()
        assert clock[0] == 65
    else:
        with pytest.raises(TimeoutError, match=r"drain still in effect: deadline exceeded.*Missing ready listeners"):
            test.observe_held()
        assert clock[0] == 90


def test_resume_verifies_limits_and_taints(controller, monkeypatch):
    controller.draining = True
    recipe = Mock()
    monkeypatch.setattr(controller, "recipe", recipe)
    sets = Mock(return_value=ars(8))
    monkeypatch.setattr(controller, "scale_sets", sets)
    kube = Mock(return_value='{"items":[]}')
    monkeypatch.setattr(module, "kubectl", kube)
    controller.resume()
    recipe.assert_called_once_with("resume-runners")
    assert not controller.draining
    controller.draining = True
    sets.return_value = ars(0)
    with pytest.raises(ValueError, match="Runner limits differ"):
        controller.resume()
    assert controller.draining
    sets.return_value = ars(8)
    kube.return_value = json.dumps({"items": [{"spec": {"taints": [{"key": "deploy.osdc.io/refresh-pending"}]}}]})
    with pytest.raises(ValueError, match="still have"):
        controller.resume()


@pytest.mark.parametrize("handoff", [False, True])
@pytest.mark.parametrize(("elapsed", "recipe_budget"), [(0, "2700"), (123.25, "2577"), (2699.75, "1")])
def test_controller_orders_drain_release_observe_and_resume(
    controller, monkeypatch, clock, elapsed, recipe_budget, handoff
):
    events = []
    if handoff:
        proc_root = controller.args.root_dir / "proc"
        proc_root.mkdir()
        monkeypatch.setattr(module, "PROC_ROOT", proc_root)
        controller.recovery_file = controller.args.root_dir / "intent.json"
    cleanup = Mock(side_effect=lambda: events.append("cleanup_stale_canary"))
    monkeypatch.setattr(controller, "cleanup_stale_canary", cleanup)
    for name in ("prepare", "create_pr", "stop_drain", "observe_held"):
        monkeypatch.setattr(controller, name, lambda name=name: events.append(name))
    monkeypatch.setattr(controller, "scale_sets", lambda: ars())
    monkeypatch.setattr(controller, "capacities", lambda: {"cpu": 0})
    for name in ("find_run", "inflight_ready", "held_ready", "drained"):
        monkeypatch.setattr(controller, name, lambda name=name: events.append(name) or True)
    monkeypatch.setattr(controller, "completed", lambda phase, count: events.append(f"complete-{phase}") or True)
    monkeypatch.setattr(controller, "resume", lambda: events.append("resume"))
    monkeypatch.setattr(module, "wait_for", lambda description, check, timeout: check())
    api = Mock(side_effect=lambda *a: events.append("release") or {})
    monkeypatch.setattr(module, "api", api)
    proc = Mock()
    proc.wait.return_value = 0

    def launch(*args, **kwargs):
        if handoff:
            state = json.loads(controller.recovery_file.read_text())
            assert kwargs["env"][module.DRAIN_OWNER_ENV] == state["token"]
        return proc

    popen = Mock(side_effect=launch)
    monkeypatch.setattr(module.subprocess, "Popen", popen)
    monkeypatch.setattr(controller, "wait_with_wakeup", lambda description, phase, check, timeout: check())
    with module.deadline_scope(controller.args.run_timeout):
        clock[0] = elapsed
        controller.run()
    assert events == [
        "prepare",
        "cleanup_stale_canary",
        "create_pr",
        "find_run",
        "inflight_ready",
        "drained",
        "release",
        "complete-inflight",
        "stop_drain",
        "held_ready",
        "observe_held",
        "resume",
        "complete-held",
    ]
    cleanup.assert_called_once_with()
    assert controller.draining
    popen.assert_called_once()
    assert popen.call_args.args[0] == ["just", "drain-runners", CLUSTER]
    assert popen.call_args.kwargs["start_new_session"] is True
    assert popen.call_args.kwargs["env"]["OSDC_TAINT_NODES"] == "no"
    assert popen.call_args.kwargs["env"]["OSDC_DRAIN_TIMEOUT_SECS"] == recipe_budget
    assert api.call_args.args[1] == {"body": "OSDC_DRAIN_RELEASE_abc123"}


@pytest.mark.parametrize("pods_remain", [False, True])
def test_drain_recipe_covers_slow_probe_teardown_but_still_bounds_pod_wait(controller, monkeypatch, clock, pods_remain):
    controller.args.drain_timeout = 300
    launched = []
    released = []
    for name in ("create_pr", "find_run", "wait_with_wakeup", "stop_drain", "held_ready", "observe_held", "resume"):
        monkeypatch.setattr(controller, name, Mock(return_value=True))
    monkeypatch.setattr(controller, "prepare", lambda: module.time.sleep(100))
    monkeypatch.setattr(controller, "scale_sets", lambda: ars(0 if launched else 8))
    monkeypatch.setattr(
        controller, "capacities", lambda: {"cpu": 0 if launched and clock[0] >= launched[0] + 280 else 8}
    )
    monkeypatch.setattr(controller, "api", lambda *args: released.append(clock[0]))
    monkeypatch.setattr(
        controller,
        "probes",
        lambda phase, count: [job(status="completed" if clock[0] >= released[0] + 40 else "in_progress")],
    )

    def launch(*args, **kwargs):
        launched.append(clock[0])
        recipe_deadline = clock[0] + int(kwargs["env"]["OSDC_DRAIN_TIMEOUT_SECS"])
        # Model a runner pod disappearing after the probe succeeds. A recipe
        # with the old 300-second budget has already exited unsuccessfully.
        pod_gone = float("inf") if pods_remain else clock[0] + 340

        def wait(timeout):
            finished = min(recipe_deadline, pod_gone)
            if finished > clock[0] + timeout:
                module.time.sleep(timeout)
                raise subprocess.TimeoutExpired("just drain-runners", timeout)
            clock[0] = max(clock[0], finished)
            return int(recipe_deadline < pod_gone)

        return Mock(wait=Mock(side_effect=wait))

    monkeypatch.setattr(module.subprocess, "Popen", launch)
    with module.deadline_scope(controller.args.run_timeout):
        if pods_remain:
            with pytest.raises(subprocess.TimeoutExpired):
                controller.run()
        else:
            controller.run()
    assert launched == [100]
    assert released == [380]
    assert clock[0] == (600 if pods_remain else 440)
    assert controller.resume.call_count == (0 if pods_remain else 1)


def test_nonzero_recipe_exit_is_failure(controller, monkeypatch):
    for name in ("prepare", "create_pr"):
        monkeypatch.setattr(controller, name, Mock())
    monkeypatch.setattr(controller, "scale_sets", lambda: ars())
    monkeypatch.setattr(controller, "capacities", Mock())
    monkeypatch.setattr(module, "wait_for", Mock())
    monkeypatch.setattr(module, "api", Mock())
    proc = Mock()
    proc.wait.return_value = 1
    monkeypatch.setattr(module.subprocess, "Popen", Mock(return_value=proc))
    with pytest.raises(RuntimeError, match="drain-runners failed"):
        controller.run()
    assert controller.draining


def test_drain_cannot_pass_without_zero_limits(controller, monkeypatch, clock):
    for name in ("prepare", "create_pr", "find_run", "inflight_ready"):
        monkeypatch.setattr(controller, name, Mock(return_value=True))
    monkeypatch.setattr(controller, "scale_sets", lambda: ars(8))
    monkeypatch.setattr(controller, "capacities", Mock(return_value={"cpu": 0}))
    popen = Mock(return_value=Mock(returncode=0))
    monkeypatch.setattr(module.subprocess, "Popen", popen)
    actual_wait = module.wait_for

    def wait(description, check, timeout, **kwargs):
        if description.startswith("zero runner"):
            return actual_wait(description, check, 1)
        return check()

    monkeypatch.setattr(module, "wait_for", wait)
    with pytest.raises(TimeoutError, match="Runner limits differ"):
        controller.run()
    popen.assert_called_once()


@pytest.mark.parametrize(
    ("snapshot", "expected"),
    [
        ("", False),
        ("123 S\n456 R\n", True),
        ("123 Z+\n456 S\n", False),
        ("123 Z\n123 R+\n", True),
        ("456 S\n", False),
    ],
)
def test_process_group_running_ignores_other_groups_and_zombies(monkeypatch, snapshot, expected):
    command = Mock(return_value=subprocess.CompletedProcess([], 0, snapshot, ""))
    monkeypatch.setattr(module, "command", command)
    assert module.process_group_running(123) is expected
    command.assert_called_once_with(["ps", "-eo", "pgid=,stat="], timeout=2)


@pytest.mark.parametrize("force_kill", [False, True])
def test_stop_drain_kills_owned_process_group(controller, monkeypatch, clock, force_kill):
    proc = Mock(pid=123)
    controller.drain = proc
    kill = Mock()
    monkeypatch.setattr(module.os, "killpg", kill)
    running = Mock(side_effect=lambda pgid: force_kill and clock[0] < 2)
    monkeypatch.setattr(module, "process_group_running", running)
    controller.stop_drain()
    assert kill.call_args_list[0] == call(123, signal.SIGTERM)
    assert kill.call_count == (2 if force_kill else 1)
    if force_kill:
        assert kill.call_args_list[-1] == call(123, signal.SIGKILL)
    proc.wait.assert_called_once_with(timeout=2)
    assert controller.drain is None
    controller.stop_drain()
    assert kill.call_count == (2 if force_kill else 1)


def test_stop_drain_exit_race(controller, monkeypatch):
    proc = Mock(pid=123)
    controller.drain = proc
    monkeypatch.setattr(module.os, "killpg", Mock(side_effect=ProcessLookupError))
    monkeypatch.setattr(module, "process_group_running", Mock(return_value=False))
    controller.stop_drain()
    proc.wait.assert_called_once_with(timeout=2)


def test_cleanup_refuses_resume_if_process_group_survives(controller, monkeypatch, clock):
    controller.drain = Mock(pid=123)
    controller.draining = True
    kill = Mock()
    resume = Mock()
    monkeypatch.setattr(module.os, "killpg", kill)
    monkeypatch.setattr(controller, "resume", resume)
    monkeypatch.setattr(module, "process_group_running", Mock(return_value=True))
    assert not controller.cleanup()
    assert kill.call_args_list == [call(123, signal.SIGTERM), call(123, signal.SIGKILL)]
    resume.assert_not_called()
    assert controller.draining
    assert controller.drain is not None


@pytest.mark.parametrize("leader_exited", [False, True])
def test_stop_drain_terminates_real_descendant_after_leader_exit(controller, leader_exited):
    # Only local owned processes: the child ignores TERM and must receive KILL.
    child = (
        "import os, signal, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print(os.getpid(), flush=True); time.sleep(30)"
    )
    parent = (
        "import subprocess, sys; "
        "p = subprocess.Popen([sys.executable, '-c', sys.argv[1]], stdout=subprocess.PIPE, text=True); "
        "print(p.stdout.readline().strip(), flush=True); "
        "sys.exit(0) if sys.argv[2] == 'exit' else p.wait()"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", parent, child, "exit" if leader_exited else "wait"],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    controller.drain = proc
    try:
        child_pid = int(proc.stdout.readline())
        assert child_pid != proc.pid
        if leader_exited:
            proc.wait(timeout=5)
        assert module.process_group_running(proc.pid)
        controller.stop_drain()
        assert not module.process_group_running(proc.pid)
        assert proc.poll() is not None
        assert controller.drain is None
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)
        proc.stdout.close()


def test_cleanup_restores_before_github_and_is_idempotent(controller, monkeypatch):
    events = []
    controller.draining = controller.branch_created = True

    def resume():
        events.append("resume")
        controller.draining = False

    monkeypatch.setattr(controller, "resume", resume)
    monkeypatch.setattr(controller, "runs", lambda: [{"id": 1, "status": "queued"}, {"id": 2, "status": "completed"}])
    api = Mock(side_effect=lambda *a, **kw: events.append(a[0]) or {})
    monkeypatch.setattr(module, "api", api)
    monkeypatch.setattr(module, "command", Mock(return_value=subprocess.CompletedProcess([], 0, '[{"number":3}]', "")))
    assert controller.cleanup()
    assert events == ["resume", "actions/runs/1/cancel", "pulls/3", f"git/refs/heads/{controller.branch}"]
    assert not controller.branch_created
    assert controller.cleanup()
    assert len(events) == 4


def test_cleanup_retries_restore_and_keeps_failed_state(controller, monkeypatch, caplog):
    controller.draining = True
    resume = Mock(side_effect=ValueError("not restored"))
    monkeypatch.setattr(controller, "resume", resume)
    assert not controller.cleanup()
    assert resume.call_count == 3
    assert controller.draining
    assert "RECOVERY REQUIRED" in caplog.text
    assert not controller.cleanup()  # atexit cannot start another batch of retries.
    assert resume.call_count == 3


def test_cleanup_recovers_after_transient_error(controller, monkeypatch):
    controller.draining = True
    resume = Mock()
    monkeypatch.setattr(controller, "resume", resume)

    def recovered():
        controller.draining = False

    attempt = 0

    def transient_then_recovered():
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            raise ValueError("transient")
        recovered()

    resume.side_effect = transient_then_recovered
    assert controller.cleanup()
    assert attempt == 2


def test_cleanup_errors_do_not_hide_restore(controller, monkeypatch):
    controller.draining = controller.branch_created = True

    def resume():
        controller.draining = False

    monkeypatch.setattr(controller, "resume", resume)
    monkeypatch.setattr(controller, "runs", Mock(side_effect=subprocess.SubprocessError("GitHub unavailable")))
    assert not controller.cleanup()
    assert not controller.draining
    assert controller.branch_created
    monkeypatch.setattr(controller, "stop_drain", Mock(side_effect=OSError("cannot stop")))
    assert not controller.cleanup()


def test_positive_and_cli_defaults(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["test", "--cluster-id", CLUSTER])
    args = module.parse_args()
    assert args.start_timeout == args.finish_timeout == 900
    assert args.drain_timeout == 300
    assert args.run_timeout == 2700
    assert args.cleanup_timeout == 600
    assert args.restore_timeout == 180
    assert not args.defer_recovery
    assert args.root_dir == module.UPSTREAM
    assert module.positive("3") == 3
    with pytest.raises(argparse.ArgumentTypeError, match="positive"):
        module.positive("0")


@pytest.mark.parametrize(
    "extra",
    [
        ["--cluster-id", "meta-prod-aws-ue1"],
        ["--cluster-id", CLUSTER, "--start-timeout", "0"],
        ["--cluster-id", CLUSTER, "--run-timeout", "0"],
        ["--cluster-id", CLUSTER, "--cleanup-timeout", "0"],
        ["--cluster-id", CLUSTER, "--restore-timeout", "0"],
        ["--cluster-id", CLUSTER, "--dry-run", "--restore-only"],
        ["--cluster-id", CLUSTER, "--defer-recovery"],
        ["--cluster-id", CLUSTER, "--defer-recovery", "--restore-only"],
        ["--cluster-id", CLUSTER, "--defer-recovery", "--dry-run"],
    ],
)
def test_cli_rejects_unsafe_cluster_and_modes(monkeypatch, extra):
    monkeypatch.setattr(sys, "argv", ["test", *extra])
    with pytest.raises(SystemExit):
        module.parse_args()


def test_signal_exits_with_original_status():
    with pytest.raises(SystemExit) as exc:
        module.on_signal(signal.SIGTERM, None)
    assert exc.value.code == 128 + signal.SIGTERM


def test_dry_run_has_no_external_effects(controller, monkeypatch, capsys):
    controller.args.dry_run = True
    monkeypatch.setattr(module, "parse_args", lambda: controller.args)
    constructor = Mock()
    monkeypatch.setattr(module, "DrainTest", constructor)
    monkeypatch.setattr(module, "command", Mock(side_effect=AssertionError("external command")))
    module.main()
    assert "OSDC Drain Test" in capsys.readouterr().out
    constructor.assert_not_called()


@pytest.mark.parametrize("mode", ["run", "restore", "failure", "signal", "cleanup-failure"])
def test_main_always_cleans_up_and_restores_signal_handlers(controller, monkeypatch, mode):
    controller.args.restore_only = mode == "restore"
    monkeypatch.setattr(module, "parse_args", lambda: controller.args)
    monkeypatch.setattr(module, "DrainTest", lambda *a: controller)
    for name in ("prepare", "resume", "run"):
        monkeypatch.setattr(controller, name, Mock())
    if mode == "failure":
        controller.run.side_effect = RuntimeError("test failed")
    if mode == "signal":
        controller.run.side_effect = SystemExit(130)
    cleanup = Mock(return_value=mode != "cleanup-failure")
    monkeypatch.setattr(controller, "cleanup", cleanup)
    signals = Mock(return_value="previous")
    monkeypatch.setattr(module.signal, "signal", signals)
    register = Mock()
    unregister = Mock()
    monkeypatch.setattr(module.atexit, "register", register)
    monkeypatch.setattr(module.atexit, "unregister", unregister)
    if mode in {"failure", "cleanup-failure"}:
        with pytest.raises(RuntimeError, match=r"test failed|cleanup failed"):
            module.main()
    elif mode == "signal":
        with pytest.raises(SystemExit) as exc:
            module.main()
        assert exc.value.code == 130
    else:
        module.main()
    cleanup.assert_called_once()
    register.assert_called_once_with(cleanup)
    assert unregister.call_count == (0 if mode == "cleanup-failure" else 1)
    assert call(signal.SIGTERM, signal.SIG_IGN) in signals.call_args_list
    assert call(signal.SIGTERM, "previous") in signals.call_args_list
    if mode == "restore":
        controller.prepare.assert_called_once()
        controller.resume.assert_called_once()


@pytest.fixture
def clock(monkeypatch):
    elapsed = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))
    return elapsed


def test_nested_deadlines_only_shorten_and_reset(clock):
    with module.deadline_scope(10):
        assert module.remaining(30) == 10
        clock[0] = 2
        with module.deadline_scope(50):
            assert module.remaining(30) == 8
        with module.deadline_scope(4):
            assert module.remaining(30) == 2
        assert module.remaining(30) == 8
    assert module.remaining(30) == 30
    with pytest.raises(TimeoutError, match="deadline exceeded"), module.deadline_scope(1):
        pytest.fail("Expired scope must not execute")
    assert module.remaining(30) == 30


def test_wait_rejects_success_returned_after_deadline(clock):
    def slow_check():
        clock[0] += 6
        return True

    with pytest.raises(TimeoutError, match="late success: deadline exceeded"):
        module.wait_for("late success", slow_check, 5)


def test_wait_clamps_api_call_to_both_phase_and_total_budget(monkeypatch, clock):
    proc = Mock(returncode=0)
    calls = []

    def communicate(**kwargs):
        calls.append(kwargs["timeout"])
        clock[0] += kwargs["timeout"]
        if len(calls) == 1:
            raise subprocess.TimeoutExpired("gh", kwargs["timeout"])
        return "", ""

    proc.communicate.side_effect = communicate
    monkeypatch.setattr(module.subprocess, "Popen", Mock(return_value=proc))
    kill = Mock()
    monkeypatch.setattr(module.os, "killpg", kill)
    with pytest.raises(TimeoutError, match="deadline exceeded"), module.deadline_scope(7):
        module.wait_for("API", lambda: module.api("actions/runs"), 20)
    assert calls == [7, 0]  # Teardown cannot extend the seven-second budget.
    kill.assert_called_once_with(proc.pid, signal.SIGKILL)


def test_command_closes_pipes_when_group_cannot_drain(monkeypatch):
    proc = Mock()
    proc.communicate.side_effect = subprocess.TimeoutExpired("recipe", 1)
    monkeypatch.setattr(module.subprocess, "Popen", Mock(return_value=proc))
    monkeypatch.setattr(module.os, "killpg", Mock(side_effect=ProcessLookupError))
    with pytest.raises(subprocess.TimeoutExpired):
        module.command(["recipe"], timeout=1)
    proc.stdout.close.assert_called_once()
    proc.stderr.close.assert_called_once()


def test_command_timeout_kills_real_recipe_descendants(tmp_path):
    marker = tmp_path / "child-pgid"
    child = "import os, pathlib, sys, time; pathlib.Path(sys.argv[1]).write_text(str(os.getpgrp())); time.sleep(30)"
    parent = "import subprocess, sys; subprocess.run([sys.executable, '-c', sys.argv[1], sys.argv[2]], check=True)"
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            module.command([sys.executable, "-c", parent, child, str(marker)], timeout=1)
        pgid = int(marker.read_text())
        module.wait_for("recipe descendants stopped", lambda: not module.process_group_running(pgid), 2, poll=0.05)
    finally:
        if marker.exists():
            with contextlib.suppress(ProcessLookupError):
                os.killpg(int(marker.read_text()), signal.SIGKILL)


def test_observation_sleep_cannot_extend_main_budget(observing_controller, clock):
    with pytest.raises(TimeoutError, match="deadline exceeded"), module.deadline_scope(7):
        observing_controller.observe_held()
    assert clock[0] == 7


@pytest.fixture
def controller_runtime(monkeypatch):
    # Failure tests must not register real exit callbacks that can contact staging.
    monkeypatch.setattr(module.atexit, "register", Mock())
    monkeypatch.setattr(module.atexit, "unregister", Mock())
    monkeypatch.setattr(module.signal, "signal", Mock(return_value=signal.SIG_DFL))


def test_main_timeout_still_gets_independent_cleanup_budget(controller, monkeypatch, clock, controller_runtime):
    controller.args.run_timeout = 5
    controller.args.cleanup_timeout = 10

    def run():
        controller.draining = True
        module.wait_for("main flow", lambda: False, 100)

    def resume():
        assert module.remaining(100) == 10
        clock[0] += 2
        controller.draining = False

    monkeypatch.setattr(controller, "run", run)
    monkeypatch.setattr(controller, "resume", resume)
    with pytest.raises(TimeoutError, match="main flow"):
        module.run_controller(controller)
    assert clock[0] == 7
    assert controller.cleanup_deadline == 15
    assert not controller.draining
    assert module.remaining(100) == 100


@pytest.mark.parametrize(
    ("command_seconds", "run_budget", "expected_timeouts"),
    [(2, 5, [5, 3, 1]), (31, 50, [30])],
)
def test_startup_cleanup_commands_share_main_budget(
    controller, monkeypatch, clock, controller_runtime, command_seconds, run_budget, expected_timeouts
):
    controller.args.run_timeout = run_budget
    monkeypatch.delattr(controller, "cleanup_stale_canary")
    prepare = Mock()
    create = Mock()
    scale_sets = Mock()
    monkeypatch.setattr(controller, "prepare", prepare)
    monkeypatch.setattr(controller, "create_pr", create)
    monkeypatch.setattr(controller, "scale_sets", scale_sets)
    default_executor = Mock(side_effect=AssertionError("Shared executor must not bypass the deadline"))
    monkeypatch.setattr("phases.run_cmd", default_executor)
    kill = Mock()
    monkeypatch.setattr(module.os, "killpg", kill)
    timeouts = []
    processes = []

    def popen(args, **kwargs):
        assert args[0] == "gh"
        assert kwargs["start_new_session"] is True
        proc = Mock(returncode=0)
        processes.append(proc)
        timed_out = False

        def communicate(**options):
            nonlocal timed_out
            if timed_out:  # Reaping the killed command must not start another wait.
                assert options["timeout"] <= 2
                return "", ""
            timeout = options["timeout"]
            timeouts.append(timeout)
            clock[0] += min(command_seconds, timeout)
            if command_seconds > timeout:
                timed_out = True
                raise subprocess.TimeoutExpired(args, timeout)
            if "actions/runs?" in args[2]:
                return json.dumps({"workflow_runs": [{"id": 10, "status": "queued"}]}), ""
            return "", ""

        proc.communicate.side_effect = communicate
        return proc

    monkeypatch.setattr(module.subprocess, "Popen", popen)
    with pytest.raises(subprocess.TimeoutExpired):
        module.run_controller(controller)
    assert timeouts == expected_timeouts
    assert clock[0] == min(run_budget, 30)
    assert controller.cleanup_deadline == clock[0] + controller.args.cleanup_timeout
    assert module.remaining(100) == 100
    kill.assert_called_once_with(processes[-1].pid, signal.SIGKILL)
    prepare.assert_called_once()
    default_executor.assert_not_called()
    scale_sets.assert_not_called()
    create.assert_not_called()
    assert not controller.draining
    assert controller.drain is None


def test_cleanup_and_atexit_share_deadline(controller, monkeypatch, clock, caplog):
    controller.args.cleanup_timeout = 5
    controller.draining = True
    attempts = []

    def resume():
        attempts.append(module.remaining(4))
        clock[0] += attempts[-1]
        raise ValueError("still drained")

    monkeypatch.setattr(controller, "resume", resume)
    assert not controller.cleanup()
    assert attempts == [4, 1]
    assert clock[0] == controller.cleanup_deadline == 5
    assert not controller.cleanup()
    assert attempts == [4, 1]
    assert "RECOVERY REQUIRED" in caplog.text


def test_restore_only_three_slow_attempts_share_total_budget(controller, monkeypatch, clock, controller_runtime):
    controller.args.restore_timeout = 900  # Explicit larger budget to exercise all three slow attempts.
    timeouts = []
    proc = Mock(returncode=0)

    def communicate(**kwargs):
        timeouts.append(kwargs["timeout"])
        clock[0] += kwargs["timeout"]
        return "", ""

    proc.communicate.side_effect = communicate
    monkeypatch.setattr(module.subprocess, "Popen", Mock(return_value=proc))

    def prepare():
        module.command(["prepare"], timeout=120)
        module.command(["map"], timeout=30)

    def resume():
        module.command(["resume"], timeout=120)
        module.command(["limits"], timeout=30)
        module.command(["taints"], timeout=30)
        raise ValueError("not restored")

    monkeypatch.setattr(controller, "prepare", prepare)
    monkeypatch.setattr(controller, "resume", resume)
    with pytest.raises(ValueError, match="not restored"):
        module.run_controller(controller, restore_only=True)
    assert clock[0] == 690
    assert controller.cleanup_deadline == 900
    assert controller.recovery_attempts == 3
    assert not controller.cleanup()  # atexit cannot acquire three new attempts.
    assert clock[0] == 690
    assert timeouts == [120, 30] + [120, 30, 30] * 3


def test_restore_deadline_includes_prepare_and_retries(controller, monkeypatch, clock, controller_runtime):
    controller.args.restore_timeout = 8
    monkeypatch.setattr(controller, "prepare", lambda: clock.__setitem__(0, 2))
    attempts = []

    def resume():
        attempts.append(module.remaining(3))
        clock[0] += attempts[-1]
        raise ValueError("not restored")

    monkeypatch.setattr(controller, "resume", resume)
    with pytest.raises(ValueError, match="not restored"):
        module.run_controller(controller, restore_only=True)
    assert attempts == [3, 3]
    assert clock[0] == controller.cleanup_deadline == 8
    assert not controller.cleanup()
    assert attempts == [3, 3]


def test_wakeup_changes_only_owned_branch_and_preserves_original_run(controller, monkeypatch, clock):
    controller.pr, controller.run_id, controller.branch_created = 4, 99, True
    api = Mock(
        side_effect=[
            {"object": {"sha": "head"}},
            {"tree": {"sha": "base-tree"}},
            {"sha": "wakeup-tree"},
            {"sha": "wakeup-commit"},
            {},
        ]
    )
    monkeypatch.setattr(controller, "api", api)
    controller.wakeup("held", 2, 10)
    assert api.call_args_list[0] == call(f"git/ref/heads/{controller.branch}", None, "GET", timeout=10)
    tree = api.call_args_list[2].args[1]
    assert tree["base_tree"] == "base-tree"
    assert tree["tree"] == [
        {"path": ".osdc-drain-wakeup", "mode": "100644", "type": "blob", "content": "abc123:held:2\n"}
    ]
    assert api.call_args_list[3].args[1]["parents"] == ["head"]
    assert api.call_args_list[4] == call(
        f"git/refs/heads/{controller.branch}", {"sha": "wakeup-commit", "force": False}, "PATCH", timeout=10
    )
    assert controller.run_id == 99
    assert controller.pr == 4


@pytest.mark.parametrize("invalid", ["draining", "branch", "pr", "run"])
def test_wakeup_refuses_drain_or_missing_canary(controller, monkeypatch, invalid):
    controller.pr, controller.run_id, controller.branch_created = 4, 99, True
    if invalid == "draining":
        controller.draining = True
    elif invalid == "branch":
        controller.branch_created = False
    elif invalid == "pr":
        controller.pr = None
    else:
        controller.run_id = None
    api = Mock()
    monkeypatch.setattr(controller, "api", api)
    with pytest.raises(RuntimeError, match="active canary outside drain"):
        controller.wakeup("held", 1, 10)
    api.assert_not_called()


def test_wakeup_api_calls_share_phase_deadline(controller, monkeypatch, clock):
    controller.pr, controller.run_id, controller.branch_created = 4, 99, True

    def api(*args, **kwargs):
        remaining = 7 - clock[0]
        assert kwargs["timeout"] == min(30, remaining)
        clock[0] += 5
        return {"object": {"sha": "head"}, "tree": {"sha": "base-tree"}}

    request = Mock(side_effect=api)
    monkeypatch.setattr(controller, "api", request)
    with pytest.raises(TimeoutError, match="wakeup deadline exceeded"):
        controller.wakeup("held", 1, 7)
    assert request.call_count == 2  # Deadline expires before any write.


@pytest.fixture
def waking_controller(controller, monkeypatch, clock):
    monkeypatch.setattr(controller, "scale_sets", lambda: ars())
    monkeypatch.setattr(controller, "capacities", lambda: {"cpu": 3})
    monkeypatch.setattr(controller, "wakeup", Mock())
    return controller


def test_wakeup_retries_until_original_assignment_then_waits_for_completion(waking_controller, clock):
    test = waking_controller

    def check():
        if clock[0] >= 35:
            test.assigned_phases.add("held")
        return clock[0] >= 60

    test.wait_with_wakeup("held succeeds", "held", check, 100)
    assert test.wakeup.call_args_list == [call("held", n, 100) for n in (1, 2, 3)]
    assert clock[0] == 60


def test_wakeup_count_is_bounded_without_extending_deadline(waking_controller, clock):
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        waking_controller.wait_with_wakeup("held succeeds", "held", lambda: False, 130)
    assert waking_controller.wakeup.call_count == 8
    assert clock[0] == 130


def test_wakeup_waits_for_target_capacity_and_preserves_regional_zeros(waking_controller, monkeypatch, clock):
    test = waking_controller
    test.desired["excluded"] = 0
    monkeypatch.setattr(
        test,
        "scale_sets",
        lambda: [*ars(), {"metadata": {"name": "excluded"}, "spec": {"maxRunners": 0, "runnerScaleSetName": "gpu"}}],
    )
    monkeypatch.setattr(test, "capacities", lambda: {"cpu": 0 if clock[0] < 15 else 3, "excluded": 0})
    test.wait_with_wakeup("inflight starts", "inflight", lambda: clock[0] >= 20, 100)
    test.wakeup.assert_called_once_with("inflight", 1, 100)


@pytest.mark.parametrize("broken", ["limits", "metrics", "target-capacity"])
def test_wakeup_requires_restored_limits_and_capacity(waking_controller, monkeypatch, clock, broken):
    test = waking_controller
    if broken == "limits":
        monkeypatch.setattr(test, "scale_sets", lambda: ars(0))
    elif broken == "metrics":
        monkeypatch.setattr(test, "capacities", Mock(side_effect=ValueError("missing metric")))
    else:
        monkeypatch.setattr(test, "capacities", lambda: {"cpu": 0, "another": 3})
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        test.wait_with_wakeup("inflight starts", "inflight", lambda: False, 20)
    test.wakeup.assert_not_called()


def test_failed_wakeup_write_is_bounded_and_cannot_reset_deadline(waking_controller, clock):
    waking_controller.wakeup.side_effect = subprocess.TimeoutExpired("gh", 1)
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        waking_controller.wait_with_wakeup("inflight starts", "inflight", lambda: False, 20)
    assert waking_controller.wakeup.call_count == 2
    assert waking_controller.wakeup_counts["inflight"] == 2
    assert clock[0] == 20


def test_workaround_never_wakes_during_drain(waking_controller):
    waking_controller.draining = True
    with pytest.raises(RuntimeError, match="forbidden during drain"):
        waking_controller.wait_with_wakeup("held succeeds", "held", lambda: False, 20)
    waking_controller.wakeup.assert_not_called()


def test_workaround_propagates_original_failure_without_waking(waking_controller):
    with pytest.raises(RuntimeError, match="wrong group"):
        waking_controller.wait_with_wakeup("held succeeds", "held", Mock(side_effect=RuntimeError("wrong group")), 20)
    waking_controller.wakeup.assert_not_called()


@pytest.mark.parametrize("invalid", [None, True, 0, -1])
def test_original_job_ids_must_be_valid(controller, monkeypatch, invalid):
    monkeypatch.setattr(controller, "jobs", lambda: [job(job_id=invalid)])
    with pytest.raises(RuntimeError, match="Invalid inflight probe job IDs"):
        controller.inflight_ready()


def test_original_jobs_cannot_be_replaced_by_successful_new_jobs(controller, monkeypatch):
    jobs = Mock(return_value=[job(status="queued")])
    monkeypatch.setattr(controller, "jobs", jobs)
    assert not controller.completed("inflight", 1)
    jobs.return_value = [job(job_id=2)]
    with pytest.raises(RuntimeError, match="Original inflight probe jobs changed"):
        controller.completed("inflight", 1)


def test_duplicate_held_job_ids_fail(controller, monkeypatch):
    monkeypatch.setattr(controller, "jobs", lambda: [job("held", status="queued")] * 3)
    with pytest.raises(RuntimeError, match="Invalid held probe job IDs"):
        controller.held_ready()


@pytest.mark.parametrize("finished", [True, False])
def test_cleanup_handles_wakeup_completion_race(controller, monkeypatch, finished):
    controller.branch_created = True
    monkeypatch.setattr(controller, "runs", lambda: [{"id": 99, "status": "in_progress"}])
    api = Mock(
        side_effect=[subprocess.CalledProcessError(1, "gh"), {"status": "completed" if finished else "in_progress"}, {}]
    )
    monkeypatch.setattr(controller, "api", api)
    monkeypatch.setattr(module, "command", Mock(return_value=subprocess.CompletedProcess([], 0, "[]", "")))
    assert controller.cleanup() is finished
    assert controller.branch_created is not finished
    if finished:
        assert api.call_args_list[-1] == call(f"git/refs/heads/{controller.branch}", method="DELETE")
    else:
        assert api.call_count == 2


def test_restore_only_and_cleanup_never_wake_probes(controller, monkeypatch):
    controller.draining = True
    monkeypatch.setattr(controller, "prepare", Mock())
    monkeypatch.setattr(controller, "recipe", Mock())
    monkeypatch.setattr(controller, "scale_sets", lambda: ars())
    monkeypatch.setattr(controller, "kube", lambda *a: '{"items":[]}')
    wake = Mock(side_effect=AssertionError("restore must not create wakeup jobs"))
    monkeypatch.setattr(controller, "wakeup", wake)
    module.run_controller(controller, restore_only=True)
    assert not controller.draining
    wake.assert_not_called()


@pytest.fixture
def recovery_state(controller, tmp_path, monkeypatch):
    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(module, "PROC_ROOT", proc)
    controller.recovery_file = tmp_path / "recovery.json"
    owner = controller.record_recovery()
    return proc, owner[module.DRAIN_OWNER_ENV]


def test_recovery_intent_precedes_writer_and_does_not_tag_controller(controller, recovery_state):
    _, token = recovery_state
    assert json.loads(controller.recovery_file.read_text()) == {"cluster": CLUSTER, "token": token}
    assert module.DRAIN_OWNER_ENV not in controller.env
    with pytest.raises(FileExistsError):
        controller.record_recovery()
    assert json.loads(controller.recovery_file.read_text())["token"] == token


def test_runner_local_recovery_requires_proc_before_mutation(controller, tmp_path, monkeypatch):
    controller.recovery_file = tmp_path / "intent"
    monkeypatch.setattr(module, "PROC_ROOT", tmp_path / "missing-proc")
    with pytest.raises(RuntimeError, match="Linux /proc"):
        controller.record_recovery()
    assert not controller.recovery_file.exists()
    with pytest.raises(RuntimeError, match="Linux /proc"):
        controller.stop_recorded_drain()


def write_proc(root, pid, group, token, state="S"):
    process = root / str(pid)
    process.mkdir()
    (process / "stat").write_text(f"{pid} (name with ) spaces) {state} 1 {group} 0")
    (process / "environ").write_bytes(f"PATH=/bin\0{module.DRAIN_OWNER_ENV}={token}\0".encode())
    return process


def test_recovery_finds_orphan_descendants_not_unrelated_reused_pids(controller, recovery_state):
    proc, token = recovery_state
    write_proc(proc, 123, 123, "unrelated")
    write_proc(proc, 456, 123, token)  # leader exited; descendant retains ownership.
    write_proc(proc, 789, 789, token, state="Z")
    (proc / "999").mkdir()  # disappeared before inspection
    assert controller.recorded_drain_groups(token) == {123}
    (proc / "456" / "environ").write_bytes(b"PATH=/bin\0")
    assert controller.recorded_drain_groups(token) == set()


def test_recovery_does_not_inspect_other_users(controller, recovery_state, monkeypatch):
    proc, token = recovery_state
    write_proc(proc, 123, 123, token)
    monkeypatch.setattr(module.os, "getuid", lambda: -1)
    assert controller.recorded_drain_groups(token) == set()


@pytest.mark.parametrize("group", [1, 0, "own"])
def test_recovery_refuses_unsafe_process_group(controller, recovery_state, group):
    proc, token = recovery_state
    write_proc(proc, 123, os.getpgrp() if group == "own" else group, token)
    with pytest.raises(RuntimeError, match="unsafe process group"):
        controller.recorded_drain_groups(token)


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"cluster": "production", "token": "a" * 32},
        {"cluster": CLUSTER, "token": "z" * 32},
        {"cluster": CLUSTER, "token": None},
    ],
)
def test_recovery_rejects_invalid_intent_without_signalling(controller, recovery_state, monkeypatch, state):
    controller.recovery_file.write_text(json.dumps(state))
    kill = Mock()
    monkeypatch.setattr(module.os, "killpg", kill)
    with pytest.raises(ValueError, match="Invalid runner-local"):
        controller.stop_recorded_drain()
    kill.assert_not_called()


@pytest.mark.parametrize("survives_term", [False, True])
def test_recovery_stops_owned_writers_before_restore(
    controller, recovery_state, monkeypatch, clock, controller_runtime, survives_term
):
    proc, token = recovery_state
    process = write_proc(proc, 456, 123, token)
    signals = []

    def kill(group, sig):
        assert group == 123
        signals.append(sig)
        if sig == signal.SIGKILL or not survives_term:
            (process / "environ").write_bytes(b"")

    monkeypatch.setattr(module.os, "killpg", kill)
    monkeypatch.setattr(controller, "prepare", Mock())

    def resume():
        assert not controller.recorded_drain_groups(token)
        controller.draining = False

    monkeypatch.setattr(controller, "resume", resume)
    module.run_controller(controller, restore_only=True)
    assert signals == ([signal.SIGTERM, signal.SIGKILL] if survives_term else [signal.SIGTERM])
    assert controller.recovery_attempts == 1


def test_recovery_refuses_to_resume_when_writer_cannot_be_stopped(
    controller, recovery_state, monkeypatch, clock, controller_runtime
):
    proc, token = recovery_state
    write_proc(proc, 456, 123, token)
    monkeypatch.setattr(module.os, "killpg", Mock())
    resume = Mock()
    monkeypatch.setattr(controller, "resume", resume)
    with pytest.raises(TimeoutError, match="recorded drain writers stopped"):
        module.run_controller(controller, restore_only=True)
    resume.assert_not_called()
    assert clock[0] < controller.args.restore_timeout


def test_recovery_process_disappears_before_signal(controller, recovery_state, monkeypatch):
    groups = Mock(side_effect=[{123}, set()])
    monkeypatch.setattr(controller, "recorded_drain_groups", groups)
    monkeypatch.setattr(module.os, "killpg", Mock(side_effect=ProcessLookupError))
    controller.stop_recorded_drain()


def test_recovery_file_cli_environment(monkeypatch, tmp_path):
    state = tmp_path / "recovery.json"
    monkeypatch.setenv("OSDC_DRAIN_RECOVERY_FILE", str(state))
    monkeypatch.setattr(sys, "argv", ["test", "--cluster-id", CLUSTER, "--restore-only"])
    assert module.parse_args().recovery_file == state


def test_workflow_cancellation_stops_test_and_attempts_recovery_under_lock():
    root = module.UPSTREAM.parent
    jobs = yaml.safe_load((root / ".github/workflows/_osdc-slow-tests.yml").read_text())["jobs"]
    drain = jobs["drain-test"]
    assert "always()" not in drain["if"]
    assert "!cancelled()" in drain["if"]
    assert "!failure()" in drain["if"]
    assert set(drain["needs"]) == {"load-test", "compactor", "janitor"}
    assert drain["concurrency"] == {"group": "osdc-tofu-${{ inputs.cluster }}", "cancel-in-progress": False}
    steps = drain["steps"]
    for step in steps[:4]:
        assert "success() && !cancelled()" in step["if"]
    restore = next(step for step in steps if step.get("id") == "restore")
    assert "always()" in restore["if"]
    assert "cancelled()" not in restore["if"]
    assert restore["timeout-minutes"] == 4
    assert "--restore-timeout 180" in restore["run"]
    assert '[[ -f "$OSDC_DRAIN_RECOVERY_FILE" ]]' in restore["run"]
    test = next(step for step in steps if step.get("id") == "drain")
    assert "--defer-recovery" in test["run"]
    assert "--defer-recovery" not in restore["run"]
    assert restore["env"]["OSDC_DRAIN_RECOVERY_FILE"] == test["env"]["OSDC_DRAIN_RECOVERY_FILE"]
    caller = yaml.safe_load((root / ".github/workflows/osdc-pre-merge.yml").read_text())["jobs"]["slow-tests"]
    assert "always()" not in caller["if"]
    assert "needs.deploy-fast.result == 'success'" in caller["if"]


def test_cleanup_fences_writer_when_launch_interrupted_before_handle_saved(controller, recovery_state, monkeypatch):
    controller.draining = True
    controller.drain = None
    events = []
    monkeypatch.setattr(controller, "stop_recorded_drain", lambda: events.append("fenced"))

    def resume():
        assert events == ["fenced"]
        controller.draining = False
        events.append("restored")

    monkeypatch.setattr(controller, "resume", resume)
    assert controller.cleanup()
    assert events == ["fenced", "restored"]


@pytest.mark.parametrize("failure", [RuntimeError("test failed"), SystemExit(130), TimeoutError("test timed out")])
def test_ci_failure_stops_writers_and_defers_recovery_without_canary_api(
    controller, recovery_state, monkeypatch, controller_runtime, failure
):
    controller.defer_recovery = True
    controller.draining = controller.branch_created = True
    events = []

    def run():
        raise failure

    monkeypatch.setattr(controller, "run", run)
    monkeypatch.setattr(controller, "stop_drain", lambda: events.append("stop"))
    monkeypatch.setattr(controller, "stop_recorded_drain", lambda: events.append("fence"))
    resume, api, runs = Mock(), Mock(), Mock()
    monkeypatch.setattr(controller, "resume", resume)
    monkeypatch.setattr(controller, "api", api)
    monkeypatch.setattr(controller, "runs", runs)
    with pytest.raises(type(failure)) as exc:
        module.run_controller(controller)
    assert exc.value is failure
    assert events == ["stop", "fence"]
    resume.assert_not_called()
    api.assert_not_called()
    runs.assert_not_called()
    assert controller.draining
    assert controller.branch_created
    assert controller.recovery_file.exists()
    module.atexit.unregister.assert_called_once_with(controller.cleanup)


def test_ci_success_cleans_canary_in_test_step(controller, monkeypatch, controller_runtime):
    controller.defer_recovery = True
    controller.branch_created = True
    monkeypatch.setattr(controller, "run", Mock())  # successful run already resumed the cluster
    resume = Mock()
    monkeypatch.setattr(controller, "resume", resume)
    monkeypatch.setattr(controller, "runs", lambda: [{"id": 1, "status": "queued"}])
    api = Mock(return_value={})
    monkeypatch.setattr(module, "api", api)
    monkeypatch.setattr(module, "command", Mock(return_value=subprocess.CompletedProcess([], 0, '[{"number":3}]', "")))
    module.run_controller(controller)
    resume.assert_not_called()
    assert not controller.branch_created
    assert not controller.skip_canary_cleanup
    assert api.call_args_list == [
        call("actions/runs/1/cancel", None, "POST"),
        call("pulls/3", {"state": "closed"}, "PATCH"),
        call(f"git/refs/heads/{controller.branch}", None, "DELETE"),
    ]


def test_ci_deferral_refuses_to_race_surviving_writers(controller, recovery_state, monkeypatch, clock):
    controller.defer_recovery = True
    controller.draining = controller.branch_created = True
    monkeypatch.setattr(controller, "stop_recorded_drain", Mock(side_effect=OSError("writer survives")))
    assert not controller.cleanup()
    assert controller.draining
    assert controller.branch_created


def test_ci_defer_recovery_cli_requires_handoff_and_preserves_local_default(monkeypatch, tmp_path):
    state = tmp_path / "intent.json"
    monkeypatch.setattr(
        sys, "argv", ["test", "--cluster-id", CLUSTER, "--defer-recovery", "--recovery-file", str(state)]
    )
    args = module.parse_args()
    assert args.defer_recovery
    assert args.recovery_file == state
    assert not args.restore_only
