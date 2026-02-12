"""
Graphical user interface for the admissions analysis application.

This module defines a PyQt‑based GUI exposing the core functionality
of the admissions system to end users. The interface allows users to
select a programme and day, view the corresponding admissions list,
load new lists from CSV files, compute passing scores and generate
comprehensive PDF reports.

The GUI is implemented using the QtWidgets module from either
``PyQt5`` or ``PyQt6``. If neither library is available, the
application will raise an ImportError at import time. Sorting and
filtering of the tabular data is provided via Qt's model/view
framework.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

try:
    # Try PyQt5 first
    from PyQt5.QtWidgets import (
        QApplication,
        QMainWindow,
        QWidget,
        QVBoxLayout,
        QHBoxLayout,
        QPushButton,
        QTableView,
        QComboBox,
        QFileDialog,
        QLabel,
        QMessageBox,
    )
    from PyQt5.QtGui import QStandardItemModel, QStandardItem
    from PyQt5.QtCore import Qt
except ImportError:
    try:
        # Fall back to PyQt6
        from PyQt6.QtWidgets import (
            QApplication,
            QMainWindow,
            QWidget,
            QVBoxLayout,
            QHBoxLayout,
            QPushButton,
            QTableView,
            QComboBox,
            QFileDialog,
            QLabel,
            QMessageBox,
        )
        from PyQt6.QtGui import QStandardItemModel, QStandardItem
        from PyQt6.QtCore import Qt
    except ImportError as exc:
        raise ImportError(
            "Neither PyQt5 nor PyQt6 could be imported. Please install one of them to use the GUI."
        ) from exc

import pandas as pd

from database import DatabaseManager
from report import ReportGenerator


class MainWindow(QMainWindow):
    """Top‑level window that hosts the admissions analysis GUI."""

    def __init__(self, db: DatabaseManager) -> None:
        super().__init__()
        self.setWindowTitle("Admissions Analysis")
        self.db = db
        self.reporter = ReportGenerator(db)
        # Main container widget
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout()
        central.setLayout(layout)

        # Controls: programme and day selectors
        ctrl_layout = QHBoxLayout()
        layout.addLayout(ctrl_layout)

        ctrl_layout.addWidget(QLabel("Programme:"))
        self.program_combo = QComboBox()
        self._populate_programmes()
        self.program_combo.currentIndexChanged.connect(self.refresh_table)
        ctrl_layout.addWidget(self.program_combo)

        ctrl_layout.addWidget(QLabel("Day:"))
        self.day_combo = QComboBox()
        self._populate_days()
        self.day_combo.currentIndexChanged.connect(self.refresh_table)
        ctrl_layout.addWidget(self.day_combo)

        # Action buttons
        self.load_button = QPushButton("Load CSV…")
        self.load_button.clicked.connect(self.load_csv)
        ctrl_layout.addWidget(self.load_button)

        self.compute_button = QPushButton("Compute Passing Scores")
        self.compute_button.clicked.connect(self.compute_scores)
        ctrl_layout.addWidget(self.compute_button)

        self.report_button = QPushButton("Generate Report…")
        self.report_button.clicked.connect(self.generate_report)
        ctrl_layout.addWidget(self.report_button)

        # Table view for applications
        self.table_view = QTableView()
        layout.addWidget(self.table_view)
        self.model: Optional[QStandardItemModel] = None
        self.refresh_table()

    # ------------------------------------------------------------------
    # Combo box population helpers
    # ------------------------------------------------------------------
    def _populate_programmes(self) -> None:
        """Populate the programme selector with the list of programmes."""
        self.program_combo.clear()
        programmes = self.db.get_programs()
        for name, _ in programmes:
            self.program_combo.addItem(name)

    def _populate_days(self) -> None:
        """Populate the day selector with the distinct days available."""
        self.day_combo.clear()
        c = self.db.conn.cursor()
        c.execute("SELECT DISTINCT day FROM applications ORDER BY day")
        days = [row[0] for row in c.fetchall()]
        for d in days:
            self.day_combo.addItem(d)

    # ------------------------------------------------------------------
    # Table population
    # ------------------------------------------------------------------
    def refresh_table(self) -> None:
        """Refresh the table view based on the current selections."""
        program = self.program_combo.currentText()
        day = self.day_combo.currentText()
        if not program or not day:
            self.table_view.setModel(None)
            return
        df = self.db.get_applications(program, day)
        model = QStandardItemModel(df.shape[0], df.shape[1])
        model.setHorizontalHeaderLabels(df.columns.tolist())
        for row_idx in range(df.shape[0]):
            for col_idx in range(df.shape[1]):
                value = df.iat[row_idx, col_idx]
                item = QStandardItem(str(value))
                # Align numeric columns to the right for readability
                if isinstance(value, (int, float)):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
                model.setItem(row_idx, col_idx, item)
        self.model = model
        self.table_view.setModel(model)
        self.table_view.resizeColumnsToContents()

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------
    def load_csv(self) -> None:
        """Prompt the user for a CSV file and import it into the database."""
        # Determine which programme and day are selected
        programme = self.program_combo.currentText()
        day = self.day_combo.currentText()
        if not programme or not day:
            QMessageBox.warning(self, "Missing selection", "Please select both a programme and a day before loading data.")
            return
        path, _ = QFileDialog.getOpenFileName(self, "Select CSV file", str(Path.home()), "CSV files (*.csv)")
        if not path:
            return
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            QMessageBox.critical(self, "Error", f"Failed to read CSV file:\n{exc}")
            return
        # Validate columns
        required_cols = {'ID', 'Consent', 'Priority', 'Physics', 'Russian', 'Math', 'Achievements', 'Total'}
        if not required_cols.issubset(df.columns):
            QMessageBox.critical(
                self,
                "Invalid file",
                f"CSV must contain the following columns: {', '.join(sorted(required_cols))}",
            )
            return
        try:
            self.db.load_list_from_dataframe(programme, day, df)
        except Exception as exc:
            QMessageBox.critical(self, "Error", f"Failed to import list:\n{exc}")
            return
        # Refresh day combo in case this is a new day
        self._populate_days()
        self.refresh_table()
        QMessageBox.information(self, "Success", "List imported successfully.")

    def compute_scores(self) -> None:
        """Compute and display passing scores for the selected day."""
        day = self.day_combo.currentText()
        if not day:
            QMessageBox.warning(self, "Missing selection", "Please select a day to compute passing scores.")
            return
        scores = self.db.compute_passing_scores(day)
        # Build message string
        lines = []
        for prog in sorted(scores.keys()):
            score, admitted = scores[prog]
            lines.append(f"{prog}: {score} (admitted {len(admitted)})")
        QMessageBox.information(self, f"Passing scores for {day}", "\n".join(lines))

    def generate_report(self) -> None:
        """Generate a PDF report for the selected day."""
        day = self.day_combo.currentText()
        if not day:
            QMessageBox.warning(self, "Missing selection", "Please select a day to generate a report.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Save report as…", f"report_{day}.pdf", "PDF files (*.pdf)")
        if not path:
            return
        try:
            self.reporter.generate(path, day)
        except Exception as exc:
            QMessageBox.critical(self, "Error", f"Failed to generate report:\n{exc}")
            return
        QMessageBox.information(self, "Report generated", f"Report saved to {path}")


def run_gui(db_path: str) -> None:
    """Convenience function to run the GUI given a database path."""
    app = QApplication(sys.argv)
    db = DatabaseManager(db_path)
    window = MainWindow(db)
    window.resize(800, 600)
    window.show()
    sys.exit(app.exec())