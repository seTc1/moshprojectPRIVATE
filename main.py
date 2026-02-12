"""
Entry point for the admissions analysis application.

This script sets up a SQLite database, populates it with synthetic
admissions data and launches the graphical user interface. If executed
as a module (``python -m admission_project.main``) or directly
(``python main.py``), the application will generate sample data
matching the approximate scale described in the case study and then
start the GUI. For real use cases the database could be preloaded
from actual CSV files instead.
"""

from __future__ import annotations

import sys

from database import DatabaseManager
from data_generator import generate_campaign_lists
from gui import run_gui


def initialise_database(db: DatabaseManager) -> None:
    """Populate the database with programmes and synthetic lists."""
    # Define programmes and their seat quotas
    programmes = {
        'ПМ': 40,   # Applied Mathematics
        'ИВТ': 50,  # Informatics and Computer Technology
        'ИТСС': 30, # Infocommunication Technologies and Systems
        'ИБ': 20,   # Information Security
    }
    # Insert programmes
    for name, seats in programmes.items():
        db.add_program(name, seats)
    # Define days
    days = ['01.08', '02.08', '03.08', '04.08']
    # Define counts of applicants for each programme/day. These
    # roughly follow the scale outlined in the case study but do not
    # attempt to satisfy the intricate intersection constraints.
    counts = {
        'ПМ': {'01.08': 60, '02.08': 380, '03.08': 100, '04.08': 120},
        'ИВТ': {'01.08': 100, '02.08': 370, '03.08': 110, '04.08': 140},
        'ИТСС': {'01.08': 50, '02.08': 350, '03.08': 90, '04.08': 110},
        'ИБ': {'01.08': 70, '02.08': 260, '03.08': 80, '04.08': 100},
    }
    lists = generate_campaign_lists(programmes, days, counts, id_start=1)
    # Load synthetic lists into the database
    for (prog, day), df in lists.items():
        db.load_list_from_dataframe(prog, day, df)


def main() -> None:
    # Create or open the database
    db_path = 'admission.db'
    db = DatabaseManager(db_path)
    # If the programmes table is empty assume the DB is new and
    # initialisation is required
    if not db.get_programs():
        initialise_database(db)
    # Launch the GUI
    run_gui(db_path)


if __name__ == '__main__':
    main()