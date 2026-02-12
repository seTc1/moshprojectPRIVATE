"""
Unit tests for the admissions database and cascade algorithm.

These tests exercise the core logic of the ``DatabaseManager``:

* Ingestion of admissions lists from pandas DataFrames and their
  correct insertion, update and deletion within the SQLite database.
* Calculation of passing scores for programmes with a limited number
  of seats using the cascade algorithm where applicants may have
  multiple programme preferences and only one admission per applicant
  is allowed.

pytest is required to run these tests.
"""

import pandas as pd

from admission_project.database import DatabaseManager


def make_df(records):
    """Helper to build a DataFrame with proper columns from a list of dicts."""
    df = pd.DataFrame(records)
    # Ensure total column is correct if not provided
    if 'Total' not in df.columns:
        df['Total'] = df[['Physics', 'Russian', 'Math', 'Achievements']].sum(axis=1)
    return df[['ID', 'Consent', 'Priority', 'Physics', 'Russian', 'Math', 'Achievements', 'Total']]


def test_update_applications_add_update_remove():
    """Ensure that loading a list replaces old applications correctly."""
    db = DatabaseManager(':memory:')
    db.add_program('TestProg', 10)
    # Initial list has three applicants
    data1 = [
        {'ID': 1, 'Consent': 1, 'Priority': 1, 'Physics': 80, 'Russian': 80, 'Math': 80, 'Achievements': 5},
        {'ID': 2, 'Consent': 1, 'Priority': 2, 'Physics': 70, 'Russian': 70, 'Math': 70, 'Achievements': 0},
        {'ID': 3, 'Consent': 0, 'Priority': 3, 'Physics': 60, 'Russian': 60, 'Math': 60, 'Achievements': 0},
    ]
    df1 = make_df(data1)
    db.load_list_from_dataframe('TestProg', '01.08', df1)
    # Ensure all three are present
    apps = db.get_applications('TestProg', '01.08')
    assert len(apps) == 3
    assert set(apps['ID']) == {1, 2, 3}
    # Second list removes applicant 2 and adds applicant 4, updates applicant 1
    data2 = [
        {'ID': 1, 'Consent': 1, 'Priority': 1, 'Physics': 90, 'Russian': 90, 'Math': 90, 'Achievements': 10},
        {'ID': 3, 'Consent': 0, 'Priority': 3, 'Physics': 60, 'Russian': 60, 'Math': 60, 'Achievements': 0},
        {'ID': 4, 'Consent': 1, 'Priority': 2, 'Physics': 75, 'Russian': 75, 'Math': 75, 'Achievements': 5},
    ]
    df2 = make_df(data2)
    db.load_list_from_dataframe('TestProg', '01.08', df2)
    apps = db.get_applications('TestProg', '01.08')
    # We should now have applicants 1, 3 and 4
    assert set(apps['ID']) == {1, 3, 4}
    # Applicant 2 should have been removed
    assert 2 not in apps['ID'].values
    # Applicant 1's scores should reflect the updated values
    rec1 = apps[apps['ID'] == 1].iloc[0]
    assert rec1['Physics'] == 90
    assert rec1['Achievements'] == 10


def test_passing_score_cascade_algorithm():
    """Test the cascade allocation of seats across priorities."""
    db = DatabaseManager(':memory:')
    # Two programmes with limited seats
    db.add_program('ProgA', 2)
    db.add_program('ProgB', 1)
    # Create a synthetic day with applicants applying to both programmes
    # Applicant 1: high total, priority 1 for ProgA and 2 for ProgB
    # Applicant 2: mid total, priority 1 for ProgB and 2 for ProgA
    # Applicant 3: low total, priority 2 for ProgA and 1 for ProgB
    records = []
    # Applicant 1: consent for both
    records.append({'ID': 1, 'Consent': 1, 'Priority': 1, 'Physics': 95, 'Russian': 95, 'Math': 95, 'Achievements': 5})
    records.append({'ID': 1, 'Consent': 1, 'Priority': 2, 'Physics': 95, 'Russian': 95, 'Math': 95, 'Achievements': 5})
    # Applicant 2
    records.append({'ID': 2, 'Consent': 1, 'Priority': 2, 'Physics': 90, 'Russian': 90, 'Math': 90, 'Achievements': 5})
    records.append({'ID': 2, 'Consent': 1, 'Priority': 1, 'Physics': 90, 'Russian': 90, 'Math': 90, 'Achievements': 5})
    # Applicant 3
    records.append({'ID': 3, 'Consent': 1, 'Priority': 2, 'Physics': 85, 'Russian': 85, 'Math': 85, 'Achievements': 5})
    records.append({'ID': 3, 'Consent': 1, 'Priority': 1, 'Physics': 85, 'Russian': 85, 'Math': 85, 'Achievements': 5})
    # Convert to two separate dataframes for ProgA and ProgB on the same day
    # ProgA applications (priority: applicant 1->1, applicant 2->2, applicant 3->2)
    df_a = make_df([
        records[0],  # applicant 1 priority 1
        records[2],  # applicant 2 priority 2
        records[4],  # applicant 3 priority 2
    ])
    # ProgB applications (priority: applicant 2->1, applicant 3->1, applicant 1->2)
    df_b = make_df([
        records[3],  # applicant 2 priority 1
        records[5],  # applicant 3 priority 1
        records[1],  # applicant 1 priority 2
    ])
    # Load lists
    db.load_list_from_dataframe('ProgA', '01.08', df_a)
    db.load_list_from_dataframe('ProgB', '01.08', df_b)
    # Compute passing scores
    scores = db.compute_passing_scores('01.08')
    # Programme A has 2 seats. Applicant 1 has highest priority and
    # highest total, so gets admitted first. Applicant 2 receives a
    # place on ProgB (their top priority), therefore the remaining
    # seat on ProgA should be filled by applicant 3.
    assert scores['ProgA'][1] == [1, 3]
    # Passing score for ProgA should be the total score of applicant 3
    pass_a = records[4]['Physics'] + records[4]['Russian'] + records[4]['Math'] + records[4]['Achievements']
    assert scores['ProgA'][0] == pass_a
    # Programme B has 1 seat. Applicant 2 has priority 1 and higher
    # total than applicant 3, so applicant 2 should be admitted.
    assert scores['ProgB'][1] == [2]