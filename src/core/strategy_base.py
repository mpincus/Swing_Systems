"""
Strategy interface and grading helper. Each strategy module should expose a
`strategy` object implementing Strategy.generate.
"""
from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class Strategy(ABC):
    name: str
    urls: List[str]

    @abstractmethod
    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        """
        Return a DataFrame of signals with required columns:
        Date, Ticker, Strategy, Setup, Side, EntryTrigger, Stop, Target, R, Grade, GradeBasis, Reason
        """

    def grade_from_r(self, r_value: float) -> str:
        if r_value >= 1.75:
            return "A+"
        if r_value >= 1.5:
            return "A"
        if r_value >= 1.25:
            return "B+"
        return ""
