"""DISPLAY — CakeLytics Owner Dashboard backed by the warehouse."""

from __future__ import annotations

import os
import logging
import tkinter as tk
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from tkinter import messagebox, ttk

import customtkinter as ctk
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from tkcalendar import DateEntry

# The managed Windows environment blocks SQLAlchemy's optional C extensions.
os.environ["DISABLE_SQLALCHEMY_CEXT"] = "1"
from sqlalchemy import create_engine, text
from config.warehouse_db import get_warehouse_engine

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PH_TZ = timezone(timedelta(hours=8), "Asia/Manila")
log = logging.getLogger("display.dashboard")

# --- WARM ESPRESSO NIGHT PALETTE (Night-Friendly & Clean) ---
BG_DARK = "#181412"        # Very Deep Warm Charcoal Background
CARD_BG = "#261F1C"        # Rich Dark Mocha Card Background
SIDEBAR_BG = "#110D0B"     # Deepest Dark Espresso Sidebar
BORDER_COLOR = "#3D322E"   # Subtle Warm Border
ACCENT_BLUE = "#3B82F6"    # Soft Cobalt Blue
HOVER_BLUE = "#2563EB"
TEXT_MAIN = "#FAF7F5"      # Soft Cream White (Non-glaring)
TEXT_MUTED = "#A89F91"     # Warm Muted Beige Gray

COLOR_SALES = "#38BDF8"    # Sky Blue
COLOR_EXPENSES = "#F87171" # Soft Red
COLOR_PROFIT = "#34D399"   # Soft Mint Green

PRESETS = [
    "Today", "Yesterday", "Last 7 Days", "Last 14 Days", "Last 30 Days",
    "This Month", "Last Month", "This Year", "Custom Range",
]


def fetch_summary_by_date(start_date: str, end_date: str) -> pd.DataFrame:
    """Read committed warehouse summaries; never write or recompute measures."""
    engine = get_warehouse_engine(readonly=True)
    try:
        with engine.connect() as connection:
            frame = pd.read_sql(
                text(
                    "SELECT d.full_date, f.total_sales, f.total_expenses, f.profit "
                    "FROM public.fact_daily_summary f "
                    "JOIN public.dim_date d ON d.date_key=f.date_key "
                    "WHERE d.full_date >= :start_date AND d.full_date <= :end_date "
                    "ORDER BY d.full_date ASC"
                ),
                connection,
                params={"start_date": start_date, "end_date": end_date},
            )
    finally:
        engine.dispose()
    if not frame.empty:
        frame["full_date"] = pd.to_datetime(frame["full_date"])
        for column in ("total_sales", "total_expenses", "profit"):
            frame[column] = pd.to_numeric(frame[column])
    return frame


class Dashboard(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.title("CakeLytics Executive Dashboard")
        self.geometry("1200x800")
        self.minsize(980, 680)
        self.configure(fg_color=BG_DARK)
        ctk.set_appearance_mode("dark")

        self.df_current = pd.DataFrame()
        self._last_hover_index = None

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        
        self._build_sidebar()
        self._build_main_area()
        self._on_preset_change("Last 14 Days")

    def _build_sidebar(self) -> None:
        self.sidebar = ctk.CTkFrame(self, width=280, corner_radius=0, fg_color=SIDEBAR_BG)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_columnconfigure(0, weight=1)
        self.sidebar.grid_rowconfigure(11, weight=1)

        ctk.CTkLabel(
            self.sidebar, text="🍰 CakeLytics", text_color=TEXT_MAIN,
            font=ctk.CTkFont(size=24, weight="bold"),
        ).grid(row=0, column=0, padx=20, pady=(25, 2), sticky="w")
        
        ctk.CTkLabel(
            self.sidebar, text="Executive Analytics Engine", text_color=TEXT_MUTED,
            font=ctk.CTkFont(size=12),
        ).grid(row=1, column=0, padx=20, pady=(0, 20), sticky="w")
        
        ctk.CTkFrame(self.sidebar, height=1, fg_color=BORDER_COLOR).grid(
            row=2, column=0, padx=20, pady=(0, 20), sticky="ew"
        )
        
        ctk.CTkLabel(
            self.sidebar, text="QUICK PRESETS", text_color=TEXT_MUTED,
            font=ctk.CTkFont(size=11, weight="bold"),
        ).grid(row=3, column=0, padx=20, pady=(0, 5), sticky="w")
        
        self.preset_var = ctk.StringVar(value="Last 14 Days")
        self.preset_menu = ctk.CTkOptionMenu(
            self.sidebar, values=PRESETS, variable=self.preset_var,
            command=self._on_preset_change, fg_color=CARD_BG,
            button_color=BORDER_COLOR, button_hover_color="#4F413B",
            text_color=TEXT_MAIN, dropdown_fg_color=CARD_BG,
        )
        self.preset_menu.grid(row=4, column=0, padx=20, pady=(0, 25), sticky="ew")

        ctk.CTkLabel(
            self.sidebar, text="CALENDAR SELECTOR", text_color=TEXT_MUTED,
            font=ctk.CTkFont(size=11, weight="bold"),
        ).grid(row=5, column=0, padx=20, pady=(0, 8), sticky="w")
        
        ctk.CTkLabel(self.sidebar, text="Start Date 📅", text_color=TEXT_MAIN).grid(
            row=6, column=0, padx=20, pady=(2, 2), sticky="w"
        )
        self.start_date_picker = self._date_picker(self.sidebar)
        self.start_date_picker.grid(row=7, column=0, padx=20, pady=(0, 15), sticky="ew")
        
        ctk.CTkLabel(self.sidebar, text="End Date 📅", text_color=TEXT_MAIN).grid(
            row=8, column=0, padx=20, pady=(2, 2), sticky="w"
        )
        self.end_date_picker = self._date_picker(self.sidebar)
        self.end_date_picker.grid(row=9, column=0, padx=20, pady=(0, 25), sticky="ew")
        
        ctk.CTkButton(
            self.sidebar, text="🔍 Apply Filters", command=self._apply_filters,
            corner_radius=8, fg_color=ACCENT_BLUE, hover_color=HOVER_BLUE,
            text_color="#FFFFFF", font=ctk.CTkFont(weight="bold", size=13), height=38,
        ).grid(row=10, column=0, padx=20, pady=10, sticky="ew")

    def _date_picker(self, parent) -> DateEntry:
        return DateEntry(
            parent,
            date_pattern="yyyy-mm-dd", background=CARD_BG, foreground=TEXT_MAIN,
            headersbackground=SIDEBAR_BG, headersforeground=TEXT_MUTED,
            selectbackground=ACCENT_BLUE, selectforeground="#FFFFFF",
            normalbackground=CARD_BG, normalforeground=TEXT_MAIN,
            bordercolor=BORDER_COLOR, font=("Segoe UI", 10),
        )

    def _on_preset_change(self, choice: str) -> None:
        today = datetime.now(PH_TZ).date()
        if choice == "Today":
            start = end = today
        elif choice == "Yesterday":
            start = end = today - timedelta(days=1)
        elif choice == "Last 7 Days":
            start, end = today - timedelta(days=7), today
        elif choice == "Last 14 Days":
            start, end = today - timedelta(days=14), today
        elif choice == "Last 30 Days":
            start, end = today - timedelta(days=30), today
        elif choice == "This Month":
            start, end = today.replace(day=1), today
        elif choice == "Last Month":
            end = today.replace(day=1) - timedelta(days=1)
            start = end.replace(day=1)
        elif choice == "This Year":
            start, end = today.replace(month=1, day=1), today
        else:
            return
        
        self.start_date_picker.set_date(start)
        self.end_date_picker.set_date(end)
        self._apply_filters()

    def _build_main_area(self) -> None:
        self.main_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.main_frame.grid(row=0, column=1, sticky="nsew", padx=25, pady=25)
        
        self.main_frame.grid_rowconfigure(0, weight=0)
        self.main_frame.grid_rowconfigure(1, weight=3)
        self.main_frame.grid_rowconfigure(2, weight=2)
        self.main_frame.grid_columnconfigure(0, weight=1)
        
        self._build_kpi_cards()
        self._build_chart()
        self._build_table()

    def _build_kpi_cards(self) -> None:
        container = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        container.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        container.grid_columnconfigure((0, 1, 2), weight=1, uniform="kpi")
        
        cards = (
            ("TOTAL SALES", COLOR_SALES, "sales"),
            ("TOTAL EXPENSES", COLOR_EXPENSES, "expenses"),
            ("NET PROFIT", COLOR_PROFIT, "profit"),
        )
        
        for column, (title, color, key) in enumerate(cards):
            card = ctk.CTkFrame(container, fg_color=CARD_BG, corner_radius=12,
                                border_width=1, border_color=BORDER_COLOR)
            
            padx_tuple = (0, 8) if column == 0 else ((8, 0) if column == 2 else (8, 8))
            card.grid(row=0, column=column, padx=padx_tuple, sticky="nsew")
            
            ctk.CTkLabel(card, text=title, text_color=TEXT_MUTED,
                         font=ctk.CTkFont(size=11, weight="bold")).pack(anchor="w", padx=15, pady=(15, 0))
            
            label = ctk.CTkLabel(card, text="₱0.00", text_color=color,
                                 font=ctk.CTkFont(size=22, weight="bold"))
            label.pack(anchor="w", padx=15, pady=(2, 15))
            setattr(self, f"kpi_{key}", label)

    def _build_chart(self) -> None:
        self.chart_frame = ctk.CTkFrame(self.main_frame, corner_radius=12, fg_color=CARD_BG,
                                        border_width=1, border_color=BORDER_COLOR)
        self.chart_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 20))
        
        self.figure = Figure(figsize=(6, 3), dpi=100, facecolor=CARD_BG)
        self.ax = self.figure.add_subplot(111)
        self.ax.set_facecolor(CARD_BG)
        self.annot = self.ax.annotate(
            "", xy=(0, 0), xytext=(15, 15), textcoords="offset points",
            bbox=dict(boxstyle="round,pad=0.5", fc=BG_DARK, ec=ACCENT_BLUE, lw=1.5),
            color=TEXT_MAIN, fontsize=9, fontweight="bold",
        )
        self.annot.set_visible(False)
        self.canvas = FigureCanvasTkAgg(self.figure, master=self.chart_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=15)
        self.canvas.mpl_connect("motion_notify_event", self._hover_tooltip)

    def _build_table(self) -> None:
        self.table_frame = ctk.CTkFrame(self.main_frame, corner_radius=12, fg_color=CARD_BG,
                                        border_width=1, border_color=BORDER_COLOR)
        self.table_frame.grid(row=2, column=0, sticky="nsew")
        
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=CARD_BG, foreground=TEXT_MAIN, rowheight=32,
                        fieldbackground=CARD_BG, borderwidth=0, font=("Segoe UI", 10))
        style.configure("Treeview.Heading", background=BG_DARK, foreground=TEXT_MUTED,
                        relief="flat", font=("Segoe UI", 10, "bold"), padding=6)
        style.map("Treeview", background=[("selected", ACCENT_BLUE)], foreground=[("selected", "#FFFFFF")])
        style.map("Treeview.Heading", background=[("active", BORDER_COLOR)])
        
        columns = ("date", "sales", "expenses", "profit")
        self.tree = ttk.Treeview(self.table_frame, columns=columns, show="headings")
        
        for column, title in zip(columns, ("DATE", "TOTAL SALES", "TOTAL EXPENSES", "NET PROFIT")):
            self.tree.heading(column, text=title)
            self.tree.column(column, anchor="center", width=150)
            
        scrollbar = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side="left", fill="both", expand=True, padx=(15, 0), pady=15)
        scrollbar.pack(side="right", fill="y", padx=(0, 15), pady=15)

    def _apply_filters(self) -> None:
        start = self.start_date_picker.get_date()
        end = self.end_date_picker.get_date()
        if start > end:
            messagebox.showerror("Invalid date range", "Start date must be on or before end date.")
            return
        self._load_data(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    def _hover_tooltip(self, event) -> None:
        if event.inaxes == self.ax and not self.df_current.empty and event.xdata is not None:
            index = int(round(event.xdata))
            if 0 <= index < len(self.df_current):
                if self._last_hover_index == index and self.annot.get_visible():
                    return
                    
                self._last_hover_index = index
                row = self.df_current.iloc[index]
                date_text = row["full_date"].strftime("%Y-%m-%d")
                tooltip = (
                    f"📅 {date_text}\nSales: ₱{row['total_sales']:,.2f}"
                    f"\nExp: ₱{row['total_expenses']:,.2f}\nProfit: ₱{row['profit']:,.2f}"
                )
                y_value = max(row["total_sales"], row["total_expenses"], row["profit"])
                y_offset = -75 if y_value > self.ax.get_ylim()[1] * 0.65 else 15
                x_offset = -110 if index > len(self.df_current) * 0.75 else 15
                
                self.annot.xy = (index, y_value)
                self.annot.set_text(tooltip)
                self.annot.set_position((x_offset, y_offset))
                self.annot.set_visible(True)
                self.canvas.draw_idle()
                return
                
        if self.annot.get_visible():
            self.annot.set_visible(False)
            self._last_hover_index = None
            self.canvas.draw_idle()

    def _load_data(self, start_date: str, end_date: str) -> None:
        try:
            frame = fetch_summary_by_date(start_date, end_date)
            self.df_current = frame
        except Exception as exc:
            log.exception("Failed to load warehouse data")
            messagebox.showerror("Database Error", str(exc))
            return

        for item in self.tree.get_children():
            self.tree.delete(item)
            
        self.ax.clear()
        self.annot = self.ax.annotate(
            "", xy=(0, 0), xytext=(15, 15), textcoords="offset points",
            bbox=dict(boxstyle="round,pad=0.5", fc=BG_DARK, ec=ACCENT_BLUE, lw=1.5),
            color=TEXT_MAIN, fontsize=9, fontweight="bold",
        )
        self.annot.set_visible(False)
        self._last_hover_index = None

        if frame.empty:
            self._update_kpis(0, 0, 0)
            self.ax.set_facecolor(CARD_BG)
            self.ax.text(0.5, 0.5, "Walang records sa napiling timeframe.", color=TEXT_MUTED,
                         ha="center", va="center", fontsize=12, transform=self.ax.transAxes)
            self.canvas.draw()
            return

        totals = (frame["total_sales"].sum(), frame["total_expenses"].sum(), frame["profit"].sum())
        self._update_kpis(*totals)
        
        for _, row in frame.iterrows():
            self.tree.insert("", "end", values=(
                row["full_date"].strftime("%Y-%m-%d"), f"₱{row['total_sales']:,.2f}",
                f"₱{row['total_expenses']:,.2f}", f"₱{row['profit']:,.2f}",
            ))

        indices = list(range(len(frame)))
        for column, color, label in (
            ("total_sales", COLOR_SALES, "Sales"),
            ("total_expenses", COLOR_EXPENSES, "Expenses"),
            ("profit", COLOR_PROFIT, "Profit"),
        ):
            self.ax.plot(indices, frame[column], marker="o", color=color,
                         linewidth=2, markersize=4, label=label)
            if column != "profit":
                self.ax.fill_between(indices, frame[column], color=color, alpha=0.15)

        min_value = min(0.0, float(frame["profit"].min()))
        max_value = max(float(frame["total_sales"].max()), float(frame["total_expenses"].max()),
                        float(frame["profit"].max()), 100.0)
                        
        self.ax.set_ylim(bottom=min_value * 1.25 if min_value < 0 else 0, top=max_value * 1.25)
        step = max(1, len(frame) // 8)
        tick_positions = indices[::step]
        self.ax.set_xticks(tick_positions)
        self.ax.set_xticklabels(
            [frame["full_date"].iloc[index].strftime("%b %d") for index in tick_positions],
            rotation=20, ha="right",
        )
        self.ax.tick_params(colors=TEXT_MUTED, labelsize=9)
        self.ax.grid(color=BORDER_COLOR, linestyle="--", linewidth=0.5, alpha=0.7)
        
        for spine in self.ax.spines.values():
            spine.set_color(BORDER_COLOR)
            spine.set_linewidth(0.5)
            
        self.ax.legend(facecolor=CARD_BG, edgecolor=BORDER_COLOR, labelcolor=TEXT_MAIN,
                       framealpha=0.9, fontsize=9)
        self.figure.tight_layout()
        self.canvas.draw()

    def _update_kpis(self, sales, expenses, profit) -> None:
        self.kpi_sales.configure(text=f"₱{Decimal(str(sales)):,.2f}")
        self.kpi_expenses.configure(text=f"₱{Decimal(str(expenses)):,.2f}")
        self.kpi_profit.configure(text=f"₱{Decimal(str(profit)):,.2f}")


if __name__ == "__main__":
    app = Dashboard()
    app.mainloop()