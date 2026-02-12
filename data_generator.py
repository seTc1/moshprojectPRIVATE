"""
Utility functions for generating synthetic admissions lists.

This module contains helpers to construct randomised admissions data
for testing and demonstration purposes. The generated data respects
the required column names expected by ``DatabaseManager``.
"""

from __future__ import annotations

import random
from typing import Dict, List, Tuple

import pandas as pd


def generate_random_applications(
    num_applicants: int,
    id_start: int = 1,
    consent_rate: float = 0.7,
    priority_dist: List[int] | None = None,
) -> pd.DataFrame:
    """
    Generate a random admissions list DataFrame.

    Parameters
    ----------
    num_applicants : int
        Number of application records to generate.
    id_start : int, optional
        Starting applicant ID. IDs will be assigned sequentially
        increasing from this number.
    consent_rate : float, optional
        Probability that an applicant has provided consent.
    priority_dist : list[int], optional
        Weights for priority 1..4; list of length 4. If not provided
        then a uniform distribution over 1..4 is used.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with columns ``ID``, ``Consent``, ``Priority``,
        ``Physics``, ``Russian``, ``Math``, ``Achievements`` and ``Total``.
    """
    if priority_dist is None:
        priority_dist = [1, 1, 1, 1]
    priorities = [1, 2, 3, 4]
    rows = []
    for idx in range(num_applicants):
        applicant_id = id_start + idx
        consent = 1 if random.random() < consent_rate else 0
        priority = random.choices(priorities, weights=priority_dist, k=1)[0]
        physics = random.randint(40, 100)
        russian = random.randint(40, 100)
        math = random.randint(40, 100)
        achievements = random.randint(0, 10)
        total = physics + russian + math + achievements
        rows.append({
            'ID': applicant_id,
            'Consent': consent,
            'Priority': priority,
            'Physics': physics,
            'Russian': russian,
            'Math': math,
            'Achievements': achievements,
            'Total': total,
        })
    df = pd.DataFrame(rows)
    return df


def generate_campaign_lists(
    programmes: Dict[str, int],
    days: List[str],
    counts: Dict[str, Dict[str, int]],
    id_start: int = 1,
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """
    Generate a set of admissions lists for multiple programmes and days.

    Parameters
    ----------
    programmes : dict[str, int]
        Mapping of programme names to seat counts. Only keys matter for
        generation; seats are ignored here.
    days : list[str]
        Identifiers for each day (e.g. ``['01.08', '02.08']``).
    counts : dict[str, dict[str, int]]
        Mapping of programmes to a mapping from day to number of
        applications. For example ``counts['PM']['01.08'] = 60``.
    id_start : int, optional
        Starting applicant ID. IDs will increment across all lists.

    Returns
    -------
    dict[(str, str), pandas.DataFrame]
        A dictionary keyed by ``(programme, day)`` whose values are
        dataframes suitable for ingestion via
        ``DatabaseManager.load_list_from_dataframe``.
    """
    results: Dict[Tuple[str, str], pd.DataFrame] = {}
    next_id = id_start
    for day in days:
        for prog in programmes.keys():
            num_applicants = counts.get(prog, {}).get(day, 0)
            df = generate_random_applications(num_applicants, id_start=next_id)
            # Advance the id counter so IDs remain unique across lists
            next_id += num_applicants
            results[(prog, day)] = df
    return results