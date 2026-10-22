"""Offline tests for the drain-test canary workflow template."""

import http.client
import io
import json
import time
import urllib.error
import urllib.request
from unittest.mock import Mock

import drain_test_run as module
import pytest
import yaml
from drain_test_checks import CLUSTER, PROBE_LABEL


def test_group_pinning_and_release_gate():
    content = module.generate_workflow("abc123")
    workflow = yaml.safe_load(content)
    assert workflow["name"] == "OSDC Drain Test abc123"
    assert workflow[True]["pull_request"]["types"] == ["opened", "synchronize"]
    # Editing the PR body releases the probe; it must not create another run.
    for job in workflow["jobs"].values():
        assert job["runs-on"] == {"group": CLUSTER, "labels": [PROBE_LABEL]}
        assert int(job["timeout-minutes"]) > 0
        assert "head.repo.full_name == github.repository" in job["if"]
    inflight = workflow["jobs"]["drain-inflight"]
    assert inflight["steps"][0]["env"]["RELEASE_MARKER"] == "OSDC_DRAIN_RELEASE_abc123"
    held = workflow["jobs"]["drain-held"]
    assert held["needs"] == "drain-inflight"
    assert "action == 'opened'" in held["if"]
    assert "action == 'opened'" in inflight["if"]
    wakeup = workflow["jobs"]["drain-wakeup"]
    assert "action == 'synchronize'" in wakeup["if"]
    assert "needs" not in wakeup
    assert len(held["strategy"]["matrix"]["probe"]) == 3
    assert "time.monotonic()" in inflight["steps"][0]["run"]


@pytest.mark.parametrize("key", ["", "../bad"])
def test_invalid_keys(key):
    with pytest.raises(ValueError, match="Run key"):
        module.generate_workflow(key)


@pytest.mark.parametrize("transient", [None, "network", 503, "incomplete", "timeout", "reset"])
def test_probe_wait_code_releases_and_retries(monkeypatch, transient):
    workflow = yaml.safe_load(module.generate_workflow("abc123"))
    step = workflow["jobs"]["drain-inflight"]["steps"][0]
    monkeypatch.setenv("PR_URL", "https://api.github.com/repos/pytorch/pytorch-canary/pulls/1")
    monkeypatch.setenv("GH_TOKEN", "test")
    monkeypatch.setenv("RELEASE_MARKER", "OSDC_DRAIN_RELEASE_abc123")
    responses = [io.StringIO(json.dumps({"body": "OSDC_DRAIN_RELEASE_abc123"}))]
    if transient == "network":
        responses.insert(0, urllib.error.URLError("offline"))
    elif transient in ("incomplete", "timeout", "reset"):
        error = {
            "incomplete": http.client.IncompleteRead(b'{"body":', 20),
            "timeout": TimeoutError("Response body timed out"),
            "reset": ConnectionResetError("Connection reset while reading body"),
        }[transient]

        class InterruptedResponse(io.StringIO):
            def read(self, *args):
                raise error

        responses.insert(0, InterruptedResponse())
    elif transient is not None:
        responses.insert(0, urllib.error.HTTPError("https://api.github.com", transient, "retry", {}, None))
    read = Mock(side_effect=responses)
    monkeypatch.setattr(urllib.request, "urlopen", read)
    monkeypatch.setattr(time, "monotonic", Mock(return_value=0))
    monkeypatch.setattr(time, "sleep", Mock())
    # Execute only our local template, with its external effects mocked.
    exec(compile(step["run"].split("\n", 1)[1].rsplit("\nPY", 1)[0], "<probe>", "exec"), {})  # noqa: S102
    assert read.call_count == (1 if transient is None else 2)
    assert read.call_args.kwargs["timeout"] == 10


def test_probe_wait_does_not_retry_auth_failures(monkeypatch):
    workflow = yaml.safe_load(module.generate_workflow("abc123"))
    step = workflow["jobs"]["drain-inflight"]["steps"][0]
    monkeypatch.setenv("PR_URL", "https://api.github.com/repos/pytorch/pytorch-canary/pulls/1")
    monkeypatch.setenv("GH_TOKEN", "test")
    monkeypatch.setattr(time, "monotonic", Mock(return_value=0))
    sleep = Mock()
    monkeypatch.setattr(time, "sleep", sleep)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        Mock(side_effect=urllib.error.HTTPError("https://api.github.com", 403, "forbidden", {}, None)),
    )
    with pytest.raises(urllib.error.HTTPError, match="403"):
        exec(compile(step["run"].split("\n", 1)[1].rsplit("\nPY", 1)[0], "<probe>", "exec"), {})  # noqa: S102
    sleep.assert_not_called()


def test_probe_wait_code_has_its_own_deadline(monkeypatch):
    workflow = yaml.safe_load(module.generate_workflow("abc123"))
    code = workflow["jobs"]["drain-inflight"]["steps"][0]["run"].split("\n", 1)[1].rsplit("\nPY", 1)[0]
    monkeypatch.setattr(time, "monotonic", Mock(side_effect=[0, 2401]))
    with pytest.raises(SystemExit, match="deadline"):
        exec(compile(code, "<probe>", "exec"), {})  # noqa: S102 -- local template, mocked clock/network
