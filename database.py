"""
Database management layer for the admissions analysis application.

This module defines the ``DatabaseManager`` class which encapsulates
all interactions with a SQLite database. The database stores
information about study programmes, applicants and their applications.

Applications capture an applicant's intent to enrol in a particular
programme on a specific day of the admissions campaign along with
their scores, priority and consent to enrol. The database schema is
simple but flexible enough to support multiple programmes per
applicant and multiple snapshots (days) of the admissions lists.

Functions are provided for creating the necessary tables, inserting
programmes, loading lists from pandas DataFrames, querying data and
computing passing scores using the cascade algorithm described in the
case study. Passing scores are calculated per programme taking into
account seat quotas, applicant priorities and consent flags.
"""

from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple, Dict, Union

import pandas as pd


class DatabaseManager:
    """A high-level interface around a SQLite database storing admissions data."""

    def __init__(self, db_path: str) -> None:
        """
        Initialise the database connection and ensure all tables exist.

        Parameters
        ----------
        db_path : str
            Path to the SQLite database file. Use ``':memory:'`` to
            create a transient in-memory database (useful for testing).
        """
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self) -> None:
        """Create the tables if they do not already exist."""
        c = self.conn.cursor()
        # Programmes table stores the name and number of budget seats
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS programs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                seats INTEGER
            )
            """
        )
        # Applicants table stores unique applicant identifiers
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS applicants (
                id INTEGER PRIMARY KEY
            )
            """
        )
        # Applications table stores each applicant's application to a
        # particular programme on a particular day along with their
        # scores, priority and consent. The unique constraint on
        # (applicant_id, program_id, day) prevents duplicate entries for
        # the same applicant/program/day combination.
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                applicant_id INTEGER,
                program_id INTEGER,
                day TEXT,
                consent INTEGER,
                priority INTEGER,
                physics INTEGER,
                russian INTEGER,
                math INTEGER,
                achievements INTEGER,
                total INTEGER,
                FOREIGN KEY(applicant_id) REFERENCES applicants(id),
                FOREIGN KEY(program_id) REFERENCES programs(id),
                UNIQUE(applicant_id, program_id, day)
            )
            """
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Programme management
    # ------------------------------------------------------------------
    def add_program(self, name: str, seats: int) -> None:
        """Insert a new study programme.

        If the programme already exists, the call is silently ignored.

        Parameters
        ----------
        name : str
            Name of the study programme.
        seats : int
            Number of budget places available on the programme.
        """
        c = self.conn.cursor()
        c.execute("INSERT OR IGNORE INTO programs(name, seats) VALUES(?, ?)", (name, seats))
        self.conn.commit()

    def get_program_id(self, name: str) -> Optional[int]:
        """Return the primary key of a programme given its name.

        Parameters
        ----------
        name : str
            Name of the programme.

        Returns
        -------
        Optional[int]
            The programme ID if found, otherwise ``None``.
        """
        c = self.conn.cursor()
        c.execute("SELECT id FROM programs WHERE name=?", (name,))
        row = c.fetchone()
        return row[0] if row else None

    def get_programs(self) -> List[Tuple[str, int]]:
        """Return a list of all programmes along with their seat counts."""
        df = pd.read_sql_query("SELECT name, seats FROM programs", self.conn)
        return list(df.itertuples(index=False, name=None))

    # ------------------------------------------------------------------
    # Loading and updating lists
    # ------------------------------------------------------------------
    def load_list_from_dataframe(self, program_name: str, day: str, df: pd.DataFrame) -> None:
        """
        Import an admissions list into the database, updating existing
        records as necessary.

        The incoming dataframe must contain the following columns with
        exactly these names (case sensitive):

        - ``ID``: applicant identifier (integer)
        - ``Consent``: whether the applicant has provided consent to
          enrol (1/0 or True/False)
        - ``Priority``: priority of this programme in the applicant's
          preference list (1–4, where 1 is highest)
        - ``Physics``: exam score in Physics/ICT (integer)
        - ``Russian``: exam score in Russian language (integer)
        - ``Math``: exam score in Mathematics (integer)
        - ``Achievements``: individual achievements score (integer)
        - ``Total``: total score (integer), usually the sum of the
          previous scores

        Any existing applications in the database for the same
        programme and day will be updated or removed. Applicants not
        present in the incoming list will be deleted for that
        programme/day. New applicants will be inserted.

        Parameters
        ----------
        program_name : str
            Name of the programme this list relates to.
        day : str
            Day identifier (e.g. ``'01.08'``) representing when the
            list was generated.
        df : pandas.DataFrame
            Tabular list of applications for the programme/day.
        """
        program_id = self.get_program_id(program_name)
        if not program_id:
            raise ValueError(f"Program '{program_name}' not found in the database. Did you forget to call add_program()?")

        # Ensure the applicants table contains all incoming applicant IDs
        c = self.conn.cursor()
        applicant_ids = df['ID'].astype(int).unique().tolist()
        for aid in applicant_ids:
            c.execute("INSERT OR IGNORE INTO applicants(id) VALUES(?)", (aid,))
        self.conn.commit()

        # Identify existing applications for this programme and day
        c.execute("SELECT applicant_id FROM applications WHERE program_id=? AND day=?", (program_id, day))
        existing = {row[0] for row in c.fetchall()}
        incoming = set(applicant_ids)

        # Remove applications that are no longer present
        to_delete = existing - incoming
        if to_delete:
            c.executemany(
                "DELETE FROM applications WHERE program_id=? AND day=? AND applicant_id=?",
                [(program_id, day, aid) for aid in to_delete],
            )

        # Insert or update each incoming record
        for _, row in df.iterrows():
            aid = int(row['ID'])
            consent = int(row['Consent'])
            priority = int(row['Priority'])
            physics = int(row['Physics'])
            russian = int(row['Russian'])
            math = int(row['Math'])
            achievements = int(row['Achievements'])
            total = int(row['Total'])
            # Check if record already exists
            c.execute(
                "SELECT id FROM applications WHERE program_id=? AND day=? AND applicant_id=?",
                (program_id, day, aid),
            )
            existing_app = c.fetchone()
            if existing_app:
                # Update the existing application
                c.execute(
                    """
                    UPDATE applications
                    SET consent=?, priority=?, physics=?, russian=?, math=?, achievements=?, total=?
                    WHERE id=?
                    """,
                    (consent, priority, physics, russian, math, achievements, total, existing_app[0]),
                )
            else:
                # Insert a new application
                c.execute(
                    """
                    INSERT INTO applications(
                        applicant_id, program_id, day, consent, priority,
                        physics, russian, math, achievements, total
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        aid,
                        program_id,
                        day,
                        consent,
                        priority,
                        physics,
                        russian,
                        math,
                        achievements,
                        total,
                    ),
                )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def get_applications(self, program_name: Optional[str] = None, day: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve applications from the database.

        Parameters
        ----------
        program_name : Optional[str]
            Name of the programme to filter by. If ``None``, all
            programmes are returned.
        day : Optional[str]
            Day identifier to filter by. If ``None``, all days are
            returned.

        Returns
        -------
        pandas.DataFrame
            A dataframe containing the requested applications sorted
            descending by total score.
        """
        query = (
            """
            SELECT applications.applicant_id AS ID,
                   programs.name AS Program,
                   applications.day AS Day,
                   applications.consent AS Consent,
                   applications.priority AS Priority,
                   applications.physics AS Physics,
                   applications.russian AS Russian,
                   applications.math AS Math,
                   applications.achievements AS Achievements,
                   applications.total AS Total
            FROM applications
            JOIN programs ON applications.program_id = programs.id
            """
        )
        conditions: List[str] = []
        params: List[Union[str, int]] = []
        if program_name:
            conditions.append("programs.name = ?")
            params.append(program_name)
        if day:
            conditions.append("applications.day = ?")
            params.append(day)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY applications.total DESC"
        df = pd.read_sql_query(query, self.conn, params=params)
        return df

    # ------------------------------------------------------------------
    # Passing score calculation
    # ------------------------------------------------------------------
    def compute_passing_scores(self, day: str) -> Dict[str, Tuple[Union[int, str], List[int]]]:
        """
        Compute passing scores and lists of admitted applicants for each programme.

        The algorithm follows a cascade approach over applicant priorities.
        Applicants with consent=1 are considered. For each priority level
        from 1 to 4, candidates for a programme who have that priority
        and are not already admitted to a higher priority programme are
        sorted descending by total score and offered places until the
        programme's seat limit is filled. Lower priority applicants are
        considered only if seats remain after higher priority
        applicants have been allocated.

        Parameters
        ----------
        day : str
            Day identifier for which to compute passing scores.

        Returns
        -------
        Dict[str, Tuple[Union[int, str], List[int]]]
            A mapping from programme name to a tuple consisting of
            either the passing score (integer) or the string
            ``'НЕДОБОР'`` when insufficient applicants exist, and a
            list of admitted applicant IDs sorted descending by total
            score.
        """
        # Fetch programmes with seat counts
        c = self.conn.cursor()
        c.execute("SELECT id, name, seats FROM programs")
        programmes = c.fetchall()
        # Prepare data structures
        prog_info: Dict[int, Dict[str, Union[str, int, List[Tuple[int, int]]]]] = {}
        for pid, pname, seats in programmes:
            prog_info[pid] = {
                'name': pname,
                'seats': seats,
                'admitted': [],  # list of (applicant_id, total)
            }
        # Retrieve all consented applications for the day ordered by
        # priority then by total score descending
        c.execute(
            """
            SELECT applicant_id, program_id, priority, total
            FROM applications
            WHERE day = ? AND consent = 1
            ORDER BY priority ASC, total DESC
            """,
            (day,),
        )
        rows = c.fetchall()
        admitted_global: set[int] = set()  # applicants admitted to any programme
        for applicant_id, program_id, priority, total in rows:
            info = prog_info.get(program_id)
            if info is None:
                continue  # unknown programme, skip
            # Skip if programme is already full
            if len(info['admitted']) >= info['seats']:
                continue
            # Skip if applicant already admitted to a higher priority programme
            if applicant_id in admitted_global:
                continue
            # Admit the applicant
            info['admitted'].append((applicant_id, total))
            admitted_global.add(applicant_id)
        # Construct result mapping
        result: Dict[str, Tuple[Union[int, str], List[int]]] = {}
        for pid, info in prog_info.items():
            admitted_list = sorted(info['admitted'], key=lambda x: (-x[1], x[0]))
            # Determine passing score
            if len(admitted_list) >= info['seats'] and info['seats'] > 0:
                passing_score = admitted_list[info['seats'] - 1][1]
            else:
                passing_score = "НЕДОБОР"
            result[info['name']] = (
                passing_score,
                [aid for (aid, _) in admitted_list],
            )
        return result