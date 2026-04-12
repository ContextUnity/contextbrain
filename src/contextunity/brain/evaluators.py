import json
from typing import Dict, List

from pydantic import BaseModel

__all__ = ["MatchResult", "MatchEvaluator"]


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

    def __init__(self, ground_truth_path: str):
        """
        Initialize the evaluator.

        Args:
            ground_truth_path (str): Path to JSONL file containing test cases.
        """
        self.ground_truth = self._load_ground_truth(ground_truth_path)

    def _load_ground_truth(self, path: str) -> List[Dict]:
        """Load JSONL ground truth file."""
        data = []
        with open(path, "r") as f:
            for line in f:
                data.append(json.loads(line))
        return data

    def evaluate(self, agent_func) -> Dict[str, float]:
        """
        Run evaluation against the provided agent function.

        Args:
            agent_func (callable): Function taking a query str and returning a SKU str.

        Returns:
            Dict[str, float]: Metrics including accuracy, total cases, and correct matches.
        """
        results = []
        correct = 0
        total = len(self.ground_truth)

        for case in self.ground_truth:
            query = case["query"]
            expected = case["sku"]

            # Call the agent
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
