"""Validates a suite YAML file and loads it as a new suites version.

Every run creates a brand-new version — there's no dedup or diffing.
Running this twice with unchanged content just makes two versions with
identical tasks, which is fine: "suite version increments on any
change" (F3) means the loader's job is to record what was loaded, not
to guess whether it was worth recording.

Run with: uv run python suite_loader.py suites/v1.yaml
"""

import argparse
import hashlib
from typing import Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, model_validator

from watchdog.db.models import Suite, Task
from watchdog.db.session import get_session
from watchdog.git_info import git_sha

CATEGORIES = Literal["structured_extraction", "factual_recall", "instruction_following", "reasoning"]
SCORING_METHODS = Literal["exact", "regex", "json_schema", "numeric_tolerance", "graded"]


class TaskDef(BaseModel):
    id: str
    prompt: str
    category: CATEGORIES
    scoring_method: SCORING_METHODS
    expected: str | None = None
    rubric: str | None = None

    @model_validator(mode="after")
    def check_expected_or_rubric(self) -> "TaskDef":
        if self.scoring_method == "graded":
            if not self.rubric:
                raise ValueError(f"task '{self.id}': scoring_method=graded requires 'rubric'")
        elif self.expected is None:
            raise ValueError(f"task '{self.id}': scoring_method={self.scoring_method} requires 'expected'")
        return self


class SuiteDef(BaseModel):
    tasks: list[TaskDef]

    @model_validator(mode="after")
    def check_unique_ids(self) -> "SuiteDef":
        ids = [t.id for t in self.tasks]
        duplicates = {i for i in ids if ids.count(i) > 1}
        if duplicates:
            raise ValueError(f"duplicate task ids: {sorted(duplicates)}")
        return self


def load_suite(path: str) -> None:
    with open(path) as f:
        raw = yaml.safe_load(f)
    suite_def = SuiteDef.model_validate(raw)

    session = get_session()
    next_version = (session.query(Suite.version).order_by(Suite.version.desc()).first() or (0,))[0] + 1

    suite = Suite(version=next_version, git_sha=git_sha())
    session.add(suite)
    session.flush()

    deterministic_count = 0
    for task_def in suite_def.tasks:
        prompt_hash = hashlib.sha256(task_def.prompt.encode()).hexdigest()
        session.add(
            Task(
                suite_id=suite.id,
                external_id=task_def.id,
                prompt=task_def.prompt,
                category=task_def.category,
                scoring_method=task_def.scoring_method,
                expected=task_def.expected,
                rubric=task_def.rubric,
                prompt_hash=prompt_hash,
            )
        )
        if task_def.scoring_method != "graded":
            deterministic_count += 1

    session.commit()

    total = len(suite_def.tasks)
    print(f"loaded suite version {next_version}: {total} tasks "
          f"({deterministic_count} deterministic, {total - deterministic_count} graded)")
    session.close()


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="path to a suite YAML file, e.g. suites/v1.yaml")
    args = parser.parse_args()
    load_suite(args.path)
