"""Module providing Module docstring is missing capabilities."""

from __future__ import annotations

from typing import Protocol, TypedDict

from contextunity.core.narrowing import as_str
from contextunity.core.parsing import json_loads
from contextunity.core.types import is_json_dict
from pydantic import BaseModel

__all__ = ["MatchResult", "MatchEvaluator"]


class GroundTruthCase(TypedDict):
    query: str
    sku: str


class MatchAgent(Protocol):
    def __call__(self, query: str) -> str: ...


class MatchResult(BaseModel):
    """
    Result of a single match evaluation case.

    Attributes:
        query: The input search query used.
        expected_sku: The ground truth SKU.
        predicted_sku: The SKU returned by the agent.
        is_correct: Boolean flag for match accuracy.
    """

    query: str
    expected_sku: str
    predicted_sku: str
    is_correct: bool


class MatchEvaluator:
    """
    Framework for evaluating Matcher Agent accuracy against a Golden Set.
    """

    ground_truth: list[GroundTruthCase]

    def __init__(self, ground_truth_path: str):
        """
        Initialize the evaluator.

        Args:
            ground_truth_path (str): Path to JSONL file containing test cases.
        """
        self.ground_truth = self._load_ground_truth(ground_truth_path)

    def _load_ground_truth(self, path: str) -> list[GroundTruthCase]:
        """Load JSONL ground truth file.

        Args:
            path (str): The filesystem path.

        Returns:
            list[GroundTruthCase]: Parsed evaluation cases.
        """
        data: list[GroundTruthCase] = []
        with open(path, "r") as f:
            for line in f:
                parsed = json_loads(line)
                if not is_json_dict(parsed):
                    continue
                data.append(
                    {
                        "query": as_str(parsed.get("query")),
                        "sku": as_str(parsed.get("sku")),
                    }
                )
        return data

    def evaluate(self, agent_func: MatchAgent) -> dict[str, float]:
        """
        Run evaluation against the provided agent function.

        Args:
            agent_func (MatchAgent): Function taking a query str and returning a SKU str.

        Returns:
            dict[str, float]: Metrics including accuracy, total cases, and correct matches.
        """
        results: list[MatchResult] = []
        correct = 0
        total = len(self.ground_truth)

        for case in self.ground_truth:
            query = case["query"]
            expected = case["sku"]

            prediction = agent_func(query)

            is_correct = prediction == expected
            if is_correct:
                correct += 1

            results.append(
                MatchResult(
                    query=query,
                    expected_sku=expected,
                    predicted_sku=prediction,
                    is_correct=is_correct,
                )
            )

        accuracy = correct / total if total > 0 else 0.0
        return {"accuracy": accuracy, "total_cases": total, "correct_matches": correct}
