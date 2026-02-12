"""
PDF report generation utilities for the admissions analysis application.

This module defines the ``ReportGenerator`` class which produces
human‑readable PDF reports summarising passing scores and admission
statistics. Reports include the current date/time, passing scores for
each programme on a chosen day, lists of admitted applicants,
dynamics of passing scores over the campaign, and a statistical
summary table.

The ``fpdf2`` library is used to construct the PDF document and
``matplotlib`` to plot the passing score dynamics which is embedded
as an image in the report.
"""

from __future__ import annotations

import datetime
import os
import tempfile
from typing import Dict, List, Tuple, Union

from fpdf import FPDF
import matplotlib.pyplot as plt

from database import DatabaseManager


class ReportGenerator:
    """Generate PDF reports summarising admission statistics."""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def _create_passing_scores_plot(self, output_path: str) -> None:
        """Create a line chart of passing scores across all days.

        The plot is saved to ``output_path`` as a PNG file. Programmes
        with insufficient applicants (``'НЕДОБОР'``) are plotted at zero
        to keep the graph consistent.
        """
        # Collect distinct days in chronological order
        c = self.db.conn.cursor()
        c.execute("SELECT DISTINCT day FROM applications ORDER BY day")
        days = [row[0] for row in c.fetchall()]
        if not days:
            # No data present; create an empty figure
            fig, ax = plt.subplots()
            ax.set_title("No admissions data available")
            fig.savefig(output_path)
            plt.close(fig)
            return
        # Get programme names
        programme_names = [name for name, _ in self.db.get_programs()]
        # Compute passing scores for each day
        scores_by_day: Dict[str, Dict[str, Tuple[Union[int, str], List[int]]]] = {}
        for day in days:
            scores_by_day[day] = self.db.compute_passing_scores(day)
        # Generate plot
        fig, ax = plt.subplots()
        for pname in programme_names:
            y_vals: List[Union[int, float]] = []
            for d in days:
                score, _ = scores_by_day[d].get(pname, ("НЕДОБОР", []))
                # Plot 0 for недобор so that the line can still be drawn
                y_vals.append(score if isinstance(score, (int, float)) else 0)
            ax.plot(days, y_vals, marker='o', label=pname)
        ax.set_xlabel('Day of campaign')
        ax.set_ylabel('Passing score')
        ax.set_title('Passing score dynamics')
        ax.legend()
        fig.tight_layout()
        fig.savefig(output_path)
        plt.close(fig)

    def generate(self, output_path: str, day: str) -> None:
        """
        Generate a comprehensive admissions report for a specific day.

        Parameters
        ----------
        output_path : str
            Path to the PDF file to write.
        day : str
            Day identifier for which the report is generated (e.g.
            ``'04.08'``).
        """
        # Prepare a temporary file for the passing score chart
        temp_plot_path = os.path.join(tempfile.gettempdir(), 'passing_scores_plot.png')
        self._create_passing_scores_plot(temp_plot_path)

        # Compute passing scores for the requested day
        passing_scores = self.db.compute_passing_scores(day)
        programme_names = [name for name, _ in self.db.get_programs()]

        # Create PDF
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)

        # First page: summary
        pdf.add_page()
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(0, 10, f'Admission Report for {day}', ln=True, align='C')
        pdf.ln(4)
        pdf.set_font('Arial', '', 12)
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pdf.cell(0, 8, f'Report generated: {now_str}', ln=True)
        pdf.ln(2)
        # Passing scores table
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 8, f'Passing scores on {day}:', ln=True)
        pdf.set_font('Arial', '', 12)
        for pname in programme_names:
            score, _ = passing_scores[pname]
            pdf.cell(0, 6, f'{pname}: {score}', ln=True)
        pdf.ln(4)
        # Insert passing score dynamics chart
        if os.path.exists(temp_plot_path):
            # Fit graph to page width with margin
            page_width = pdf.w - 2 * pdf.l_margin
            pdf.image(temp_plot_path, x=pdf.l_margin, w=page_width)
        pdf.ln(2)
        pdf.add_page()
        # Second page: admitted lists
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, f'Admitted applicants on {day}', ln=True)
        pdf.ln(4)
        pdf.set_font('Arial', 'B', 12)
        for pname in programme_names:
            score, admitted_ids = passing_scores[pname]
            pdf.cell(0, 8, f'{pname} (admitted: {len(admitted_ids)})', ln=True)
            pdf.set_font('Arial', '', 10)
            if admitted_ids:
                # Join IDs into a comma separated string; wrap at reasonable length
                id_str = ', '.join(str(aid) for aid in admitted_ids)
                pdf.multi_cell(0, 5, id_str)
            else:
                pdf.multi_cell(0, 5, 'No admitted applicants')
            pdf.ln(2)
            pdf.set_font('Arial', 'B', 12)
        pdf.add_page()
        # Third page: statistics table
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, f'Admissions statistics on {day}', ln=True)
        pdf.ln(4)
        # Build the statistics table header
        headers = [
            'Program', 'Total', 'Seats', 'P1', 'P2', 'P3', 'P4',
            'AdmP1', 'AdmP2', 'AdmP3', 'AdmP4'
        ]
        # Determine column widths based on page width
        table_width = pdf.w - 2 * pdf.l_margin
        col_width = table_width / len(headers)
        pdf.set_font('Arial', 'B', 10)
        for h in headers:
            pdf.cell(col_width, 6, h, border=1, align='C')
        pdf.ln()
        pdf.set_font('Arial', '', 10)
        for pname in programme_names:
            df = self.db.get_applications(pname, day)
            total_count = len(df)
            seats = next(seats for name, seats in self.db.get_programs() if name == pname)
            # Count applications per priority
            counts = []
            for p in [1, 2, 3, 4]:
                counts.append(len(df[df['Priority'] == p]))
            # Count admitted per priority
            _, admitted_ids = passing_scores[pname]
            admitted_df = df[df['ID'].isin(admitted_ids)]
            admitted_counts = []
            for p in [1, 2, 3, 4]:
                admitted_counts.append(len(admitted_df[admitted_df['Priority'] == p]))
            row = [
                pname,
                str(total_count),
                str(seats),
                *(str(c) for c in counts),
                *(str(a) for a in admitted_counts),
            ]
            for cell in row:
                pdf.cell(col_width, 6, cell, border=1, align='C')
            pdf.ln()
        # Write the PDF to file
        pdf.output(output_path)