"""Generate uniquely identifiable canary probes pinned to the uw1 runner group."""

import json
from pathlib import Path

from drain_test_checks import CLUSTER, PROBE_LABEL


def generate_workflow(run_key: str, *, runner_group: str | None = CLUSTER, runner_label: str = PROBE_LABEL) -> str:
    if not run_key or not run_key.isalnum():
        raise ValueError("Run key must be nonempty and alphanumeric")
    template = Path(__file__).resolve().parents[2] / "workflows" / "drain-test.yaml.tpl"
    content = template.read_text().replace("{{RUN_KEY}}", run_key)
    group = f"      group: {json.dumps(runner_group)}\n" if runner_group is not None else ""
    return content.replace(f"      group: {CLUSTER}\n", group).replace(
        f"labels: [{PROBE_LABEL}]", f"labels: [{json.dumps(runner_label)}]"
    )
