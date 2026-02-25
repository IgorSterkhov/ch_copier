#!/usr/bin/env python3
"""ClickHouse Migration Tool — GUI for migrating tables between ClickHouse instances."""

import os
import re
import subprocess
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from typing import Optional

import clickhouse_connect
from dotenv import load_dotenv

CHECKED = "\u2611"
UNCHECKED = "\u2610"
WINDOW_TITLE = "ClickHouse Migration Tool"
WINDOW_SIZE = "1400x900"


class CHMigrateApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(WINDOW_TITLE)
        self.root.geometry(WINDOW_SIZE)

        self.source_client: Optional[clickhouse_connect.driver.Client] = None
        self.dest_client: Optional[clickhouse_connect.driver.Client] = None
        self.docker_container_name: Optional[str] = None

        self.selected_tables: set[tuple[str, str]] = set()
        self.table_ddls: dict[tuple[str, str], str] = {}
        self.table_columns: dict[tuple[str, str], list[dict]] = {}
        # map treeview item id -> (database, table)
        self.tree_item_map: dict[str, tuple[str, str]] = {}

        self._build_gui()

    # ── GUI Construction ─────────────────────────────────────────────

    def _build_gui(self):
        main = ttk.Frame(self.root, padding=5)
        main.pack(fill=tk.BOTH, expand=True)

        self._build_connection_bar(main)

        paned = ttk.PanedWindow(main, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        left = ttk.Frame(paned)
        right = ttk.Frame(paned)
        paned.add(left, weight=1)
        paned.add(right, weight=2)

        self._build_left_panel(left)
        self._build_right_panel(right)

    def _build_connection_bar(self, parent):
        bar = ttk.Frame(parent)
        bar.pack(fill=tk.X)

        # Source
        src_frame = ttk.LabelFrame(bar, text="Source", padding=5)
        src_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        self.btn_connect_src = ttk.Button(src_frame, text="Подключиться",
                                          command=self._connect_source)
        self.btn_connect_src.pack(side=tk.LEFT)
        self.lbl_src_status = ttk.Label(src_frame, text="Не подключён", foreground="red")
        self.lbl_src_status.pack(side=tk.LEFT, padx=10)

        # Destination
        dst_frame = ttk.LabelFrame(bar, text="Destination", padding=5)
        dst_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.btn_connect_dst = ttk.Button(dst_frame, text="Подключиться",
                                          command=self._connect_destination)
        self.btn_connect_dst.pack(side=tk.LEFT)

        self.btn_docker = ttk.Button(dst_frame, text="Docker CH",
                                     command=self._show_docker_dialog)
        self.btn_docker.pack(side=tk.LEFT, padx=(5, 0))

        self.btn_docker_stop = ttk.Button(dst_frame, text="Остановить Docker",
                                          command=self._stop_docker_ch, state=tk.DISABLED)
        self.btn_docker_stop.pack(side=tk.LEFT, padx=(5, 0))

        self.lbl_dst_status = ttk.Label(dst_frame, text="Не подключён", foreground="red")
        self.lbl_dst_status.pack(side=tk.LEFT, padx=10)

    def _build_left_panel(self, parent):
        # Schema tree
        tree_frame = ttk.LabelFrame(parent, text="Схема Source", padding=5)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        tree_scroll_y = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree_scroll_x = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)

        self.schema_tree = ttk.Treeview(
            tree_frame, columns=("type", "engine"), show="tree headings",
            yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set,
        )
        self.schema_tree.heading("#0", text="Объект")
        self.schema_tree.heading("type", text="Тип")
        self.schema_tree.heading("engine", text="Engine")
        self.schema_tree.column("#0", width=200, minwidth=120)
        self.schema_tree.column("type", width=80, minwidth=60)
        self.schema_tree.column("engine", width=150, minwidth=80)

        tree_scroll_y.config(command=self.schema_tree.yview)
        tree_scroll_x.config(command=self.schema_tree.xview)

        self.schema_tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        tree_scroll_x.grid(row=1, column=0, sticky="ew")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        self.schema_tree.bind("<ButtonRelease-1>", self._on_tree_click)

        # DDL Preview
        ddl_frame = ttk.LabelFrame(parent, text="DDL Preview", padding=5)
        ddl_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        self.ddl_text = tk.Text(ddl_frame, wrap=tk.WORD, height=10)
        ddl_scroll = ttk.Scrollbar(ddl_frame, orient=tk.VERTICAL, command=self.ddl_text.yview)
        self.ddl_text.config(yscrollcommand=ddl_scroll.set)

        self.ddl_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ddl_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(btn_frame, text="Копировать DDL",
                   command=lambda: self._copy_to_clipboard(self.ddl_text.get("1.0", tk.END))
                   ).pack(side=tk.LEFT)

    def _build_right_panel(self, parent):
        # Filters
        filter_frame = ttk.LabelFrame(parent, text="Фильтры запроса", padding=5)
        filter_frame.pack(fill=tk.X)

        ttk.Label(filter_frame, text="Колонка даты:").grid(row=0, column=0, sticky="w")
        self.date_column_combo = ttk.Combobox(filter_frame, width=25, state="readonly")
        self.date_column_combo.grid(row=0, column=1, padx=5)

        ttk.Label(filter_frame, text="Дата от (YYYY-MM-DD):").grid(row=0, column=2, sticky="w", padx=(10, 0))
        self.date_entry = ttk.Entry(filter_frame, width=15)
        self.date_entry.grid(row=0, column=3, padx=5)

        ttk.Label(filter_frame, text="LIMIT:").grid(row=0, column=4, sticky="w", padx=(10, 0))
        self.limit_entry = ttk.Entry(filter_frame, width=10)
        self.limit_entry.grid(row=0, column=5, padx=5)

        # SELECT SQL
        sql_frame = ttk.LabelFrame(parent, text="SELECT SQL (редактируемый)", padding=5)
        sql_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        self.sql_text = tk.Text(sql_frame, wrap=tk.WORD, height=8)
        sql_scroll = ttk.Scrollbar(sql_frame, orient=tk.VERTICAL, command=self.sql_text.yview)
        self.sql_text.config(yscrollcommand=sql_scroll.set)
        self.sql_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sql_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        sql_btn_frame = ttk.Frame(parent)
        sql_btn_frame.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(sql_btn_frame, text="Копировать SQL",
                   command=lambda: self._copy_to_clipboard(self.sql_text.get("1.0", tk.END))
                   ).pack(side=tk.LEFT)
        ttk.Button(sql_btn_frame, text="Сгенерировать SELECT",
                   command=self._generate_select_sql).pack(side=tk.LEFT, padx=5)

        # Migration DDL
        ddl_mig_frame = ttk.LabelFrame(parent, text="DDL для миграции (редактируемый)", padding=5)
        ddl_mig_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        self.ddl_mig_text = tk.Text(ddl_mig_frame, wrap=tk.WORD, height=8)
        ddl_mig_scroll = ttk.Scrollbar(ddl_mig_frame, orient=tk.VERTICAL,
                                       command=self.ddl_mig_text.yview)
        self.ddl_mig_text.config(yscrollcommand=ddl_mig_scroll.set)
        self.ddl_mig_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ddl_mig_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        ddl_mig_btn_frame = ttk.Frame(parent)
        ddl_mig_btn_frame.pack(fill=tk.X, pady=(3, 0))
        ttk.Button(ddl_mig_btn_frame, text="Копировать DDL",
                   command=lambda: self._copy_to_clipboard(self.ddl_mig_text.get("1.0", tk.END))
                   ).pack(side=tk.LEFT)

        # Action buttons
        action_frame = ttk.Frame(parent)
        action_frame.pack(fill=tk.X, pady=(5, 0))

        self.btn_gen_ddl = ttk.Button(action_frame, text="Генерировать DDL",
                                      command=self._generate_ddl)
        self.btn_gen_ddl.pack(side=tk.LEFT, padx=(0, 5))

        self.btn_create_ddl = ttk.Button(action_frame, text="Создать DDL на Destination",
                                         command=self._create_ddl_on_destination)
        self.btn_create_ddl.pack(side=tk.LEFT, padx=(0, 5))

        self.btn_migrate = ttk.Button(action_frame, text="Мигрировать данные",
                                      command=self._migrate_data)
        self.btn_migrate.pack(side=tk.LEFT)

        # Log
        log_frame = ttk.LabelFrame(parent, text="Лог", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        self.log_text = tk.Text(log_frame, wrap=tk.WORD, height=8, state=tk.DISABLED)
        log_scroll = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.config(yscrollcommand=log_scroll.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    # ── Connection Management ────────────────────────────────────────

    def _make_client(self, prefix: str):
        host = os.getenv(f"{prefix}_HOST", "localhost")
        port = int(os.getenv(f"{prefix}_PORT", "8123"))
        user = os.getenv(f"{prefix}_USER", "default")
        password = os.getenv(f"{prefix}_PASS", "")
        database = os.getenv(f"{prefix}_DB", "default")
        secure = os.getenv(f"{prefix}_SECURE", "False").lower() in ("true", "1", "yes")
        ca_cert = os.getenv(f"{prefix}_CA_CERT", "")

        kwargs = dict(
            host=host, port=port, username=user,
            password=password, database=database,
        )
        if secure:
            kwargs["secure"] = True
        if ca_cert and os.path.isfile(ca_cert):
            kwargs["ca_cert"] = ca_cert
        elif secure:
            kwargs["verify"] = False

        return clickhouse_connect.get_client(**kwargs)

    def _connect_source(self):
        def _do():
            try:
                self.source_client = self._make_client("SOURCE")
                ver = self.source_client.server_version
                self.root.after(0, lambda: self.lbl_src_status.config(
                    text=f"Подключён ({ver})", foreground="green"))
                self._log(f"Source подключён: {os.getenv('SOURCE_HOST')}:{os.getenv('SOURCE_PORT')}")
                self.root.after(0, self._load_schema_tree)
            except Exception as e:
                self.root.after(0, lambda: self.lbl_src_status.config(
                    text="Ошибка", foreground="red"))
                self._log(f"Ошибка подключения Source: {e}", "ERROR")

        threading.Thread(target=_do, daemon=True).start()

    def _connect_destination(self):
        def _do():
            try:
                self.dest_client = self._make_client("DESTINATION")
                ver = self.dest_client.server_version
                self.root.after(0, lambda: self.lbl_dst_status.config(
                    text=f"Подключён ({ver})", foreground="green"))
                self._log(f"Destination подключён: {os.getenv('DESTINATION_HOST')}:{os.getenv('DESTINATION_PORT')}")
            except Exception as e:
                self.root.after(0, lambda: self.lbl_dst_status.config(
                    text="Ошибка", foreground="red"))
                self._log(f"Ошибка подключения Destination: {e}", "ERROR")

        threading.Thread(target=_do, daemon=True).start()

    # ── Docker ClickHouse ────────────────────────────────────────────

    def _show_docker_dialog(self):
        dlg = tk.Toplevel(self.root)
        dlg.title("Запуск ClickHouse в Docker")
        dlg.geometry("400x220")
        dlg.resizable(False, False)
        dlg.transient(self.root)
        dlg.grab_set()

        frame = ttk.Frame(dlg, padding=15)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Имя контейнера:").grid(row=0, column=0, sticky="w", pady=3)
        name_var = tk.StringVar(value="ch_migrate_dest")
        ttk.Entry(frame, textvariable=name_var, width=30).grid(row=0, column=1, pady=3)

        ttk.Label(frame, text="HTTP порт (локальный):").grid(row=1, column=0, sticky="w", pady=3)
        port_var = tk.StringVar(value="18123")
        ttk.Entry(frame, textvariable=port_var, width=30).grid(row=1, column=1, pady=3)

        ttk.Label(frame, text="Образ:").grid(row=2, column=0, sticky="w", pady=3)
        image_var = tk.StringVar(value="clickhouse/clickhouse-server:latest")
        ttk.Entry(frame, textvariable=image_var, width=30).grid(row=2, column=1, pady=3)

        ttk.Label(frame, text="Пароль (default):").grid(row=3, column=0, sticky="w", pady=3)
        pass_var = tk.StringVar(value="")
        ttk.Entry(frame, textvariable=pass_var, width=30).grid(row=3, column=1, pady=3)

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=(15, 0))

        def on_start():
            dlg.destroy()
            self._start_docker_ch(
                name=name_var.get().strip(),
                port=port_var.get().strip(),
                image=image_var.get().strip(),
                password=pass_var.get(),
            )

        ttk.Button(btn_frame, text="Запустить", command=on_start).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Отмена", command=dlg.destroy).pack(side=tk.LEFT, padx=5)

    def _start_docker_ch(self, name: str, port: str, image: str, password: str):
        def _do():
            try:
                # Check docker is available
                r = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
                if r.returncode != 0:
                    self._log("Docker недоступен. Проверьте установку.", "ERROR")
                    return

                self._log(f"Запуск контейнера {name} ({image}) на порту {port}...")

                # Remove old container with same name if exists
                subprocess.run(["docker", "rm", "-f", name],
                               capture_output=True, timeout=15)

                # Build docker run command
                cmd = [
                    "docker", "run", "-d",
                    "--name", name,
                    "-p", f"{port}:8123",
                ]
                if password:
                    cmd += ["-e", f"CLICKHOUSE_PASSWORD={password}"]
                cmd.append(image)

                r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if r.returncode != 0:
                    self._log(f"Ошибка запуска контейнера: {r.stderr.strip()}", "ERROR")
                    return

                self.docker_container_name = name
                self._log("Контейнер запущен, ожидание готовности ClickHouse...")

                # Wait for CH to be ready (up to 30 seconds)
                int_port = int(port)
                client = None
                for attempt in range(30):
                    time.sleep(1)
                    try:
                        kwargs = dict(host="localhost", port=int_port,
                                      username="default", password=password)
                        client = clickhouse_connect.get_client(**kwargs)
                        client.command("SELECT 1")
                        break
                    except Exception:
                        client = None
                        continue

                if client is None:
                    self._log("ClickHouse в контейнере не отвечает после 30 сек", "ERROR")
                    return

                self.dest_client = client
                ver = self.dest_client.server_version
                self.root.after(0, lambda: self.lbl_dst_status.config(
                    text=f"Docker ({ver})", foreground="green"))
                self.root.after(0, lambda: self.btn_docker_stop.config(state=tk.NORMAL))
                self._log(f"Destination подключён: Docker контейнер {name} (localhost:{port})")

            except Exception as e:
                self._log(f"Ошибка Docker: {e}", "ERROR")

        threading.Thread(target=_do, daemon=True).start()

    def _stop_docker_ch(self):
        if not self.docker_container_name:
            return

        name = self.docker_container_name

        def _do():
            try:
                subprocess.run(["docker", "stop", name], capture_output=True, timeout=30)
                subprocess.run(["docker", "rm", name], capture_output=True, timeout=15)
                self.docker_container_name = None
                self.dest_client = None
                self.root.after(0, lambda: self.lbl_dst_status.config(
                    text="Не подключён", foreground="red"))
                self.root.after(0, lambda: self.btn_docker_stop.config(state=tk.DISABLED))
                self._log(f"Docker контейнер {name} остановлен и удалён")
            except Exception as e:
                self._log(f"Ошибка остановки Docker: {e}", "ERROR")

        threading.Thread(target=_do, daemon=True).start()

    # ── Schema Tree ──────────────────────────────────────────────────

    def _load_schema_tree(self):
        self.schema_tree.delete(*self.schema_tree.get_children())
        self.tree_item_map.clear()
        self.selected_tables.clear()

        try:
            databases = self.source_client.query(
                "SELECT name FROM system.databases ORDER BY name"
            ).result_rows

            for (db_name,) in databases:
                db_node = self.schema_tree.insert(
                    "", "end", text=db_name, values=("database", ""), open=False
                )
                tables = self.source_client.query(
                    "SELECT name, engine, "
                    "multiIf(engine LIKE '%View%', 'view', "
                    "engine LIKE '%Dictionary%', 'dictionary', 'table') AS type "
                    "FROM system.tables WHERE database = %(db)s ORDER BY name",
                    parameters={"db": db_name},
                ).result_rows

                for tbl_name, engine, tbl_type in tables:
                    item_id = self.schema_tree.insert(
                        db_node, "end",
                        text=f"{UNCHECKED} {tbl_name}",
                        values=(tbl_type, engine),
                    )
                    self.tree_item_map[item_id] = (db_name, tbl_name)

            self._log(f"Загружено {len(databases)} баз данных")
        except Exception as e:
            self._log(f"Ошибка загрузки схемы: {e}", "ERROR")

    def _on_tree_click(self, event):
        item = self.schema_tree.focus()
        if not item:
            return

        # If it is a table-level node — toggle checkbox & show DDL
        if item in self.tree_item_map:
            db, table = self.tree_item_map[item]
            text = self.schema_tree.item(item, "text")

            if text.startswith(CHECKED):
                new_text = UNCHECKED + text[1:]
                self.selected_tables.discard((db, table))
            else:
                new_text = CHECKED + text[1:]
                self.selected_tables.add((db, table))
            self.schema_tree.item(item, text=new_text)

            self._show_ddl_preview(db, table)
            self._update_date_columns()

    def _show_ddl_preview(self, database: str, table: str):
        key = (database, table)
        if key not in self.table_ddls:
            try:
                tbl_type = None
                for item_id, (d, t) in self.tree_item_map.items():
                    if d == database and t == table:
                        tbl_type = self.schema_tree.item(item_id, "values")[0]
                        break

                if tbl_type == "dictionary":
                    ddl = self.source_client.command(
                        f"SHOW CREATE DICTIONARY `{database}`.`{table}`"
                    )
                else:
                    ddl = self.source_client.command(
                        f"SHOW CREATE TABLE `{database}`.`{table}`"
                    )
                self.table_ddls[key] = ddl
            except Exception as e:
                self.table_ddls[key] = f"-- Error: {e}"

        self.ddl_text.config(state=tk.NORMAL)
        self.ddl_text.delete("1.0", tk.END)
        self.ddl_text.insert("1.0", self.table_ddls[key])

    def _get_columns(self, database: str, table: str) -> list[dict]:
        key = (database, table)
        if key not in self.table_columns:
            try:
                rows = self.source_client.query(
                    "SELECT name, type FROM system.columns "
                    "WHERE database = %(db)s AND table = %(tbl)s ORDER BY position",
                    parameters={"db": database, "tbl": table},
                ).result_rows
                self.table_columns[key] = [{"name": r[0], "type": r[1]} for r in rows]
            except Exception:
                self.table_columns[key] = []
        return self.table_columns[key]

    def _update_date_columns(self):
        date_cols: set[str] = set()
        for db, table in self.selected_tables:
            for col in self._get_columns(db, table):
                if any(t in col["type"] for t in ("Date", "DateTime")):
                    date_cols.add(col["name"])
        self.date_column_combo["values"] = sorted(date_cols)
        if date_cols and not self.date_column_combo.get():
            self.date_column_combo.current(0)

    # ── SQL Generation ───────────────────────────────────────────────

    def _generate_select_sql(self):
        if not self.selected_tables:
            self._log("Нет выбранных таблиц", "WARN")
            return

        date_col = self.date_column_combo.get().strip()
        date_val = self.date_entry.get().strip()
        limit_val = self.limit_entry.get().strip()

        sqls = []
        for db, table in sorted(self.selected_tables):
            sql = f"SELECT * FROM `{db}`.`{table}`"

            where_parts = []
            if date_col and date_val:
                where_parts.append(f"`{date_col}` >= '{date_val}'")
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
            if limit_val and limit_val.isdigit():
                sql += f" LIMIT {limit_val}"

            sqls.append(sql + ";")

        self.sql_text.delete("1.0", tk.END)
        self.sql_text.insert("1.0", "\n\n".join(sqls))
        self._log(f"Сгенерирован SELECT для {len(sqls)} таблиц")

    # ── Engine Cleaning ──────────────────────────────────────────────

    @staticmethod
    def _clean_replicated_engine(ddl: str) -> str:
        """Remove parenthesized args from Replicated*MergeTree engines.

        Uses balanced-parentheses matching to handle nested tuples like (col1, col2).
        """
        match = re.search(r"ENGINE\s*=\s*Replicated\w*MergeTree\s*\(", ddl)
        if not match:
            return ddl

        start = match.end() - 1  # position of '('
        depth = 0
        pos = start
        while pos < len(ddl):
            if ddl[pos] == "(":
                depth += 1
            elif ddl[pos] == ")":
                depth -= 1
                if depth == 0:
                    return ddl[:start] + ddl[pos + 1:]
            pos += 1
        return ddl

    # ── DDL Generation & Execution ───────────────────────────────────

    def _generate_ddl(self):
        if not self.selected_tables:
            self._log("Нет выбранных таблиц", "WARN")
            return
        if not self.source_client:
            self._log("Source не подключён", "ERROR")
            return

        ddl_scripts: list[str] = []
        databases_seen: set[str] = set()

        for db, table in sorted(self.selected_tables):
            if db not in databases_seen:
                ddl_scripts.append(f"CREATE DATABASE IF NOT EXISTS `{db}`;")
                databases_seen.add(db)

            key = (db, table)
            if key not in self.table_ddls:
                self._show_ddl_preview(db, table)

            raw_ddl = self.table_ddls.get(key, "")
            cleaned = self._clean_replicated_engine(raw_ddl)
            cleaned = re.sub(r"^CREATE\s+TABLE", "CREATE OR REPLACE TABLE", cleaned, count=1)
            ddl_scripts.append(cleaned + ";")

        self.ddl_mig_text.delete("1.0", tk.END)
        self.ddl_mig_text.insert("1.0", "\n\n".join(ddl_scripts))
        self._log(f"Сгенерирован DDL для {len(self.selected_tables)} таблиц")

    def _create_ddl_on_destination(self):
        if not self.dest_client:
            self._log("Destination не подключён", "ERROR")
            return

        ddl_text = self.ddl_mig_text.get("1.0", tk.END).strip()
        if not ddl_text:
            self._log("Нет DDL для выполнения", "WARN")
            return

        def _do():
            self._set_buttons_state(False)
            statements = [s.strip() for s in ddl_text.split(";") if s.strip()]

            for i, stmt in enumerate(statements, 1):
                try:
                    self.dest_client.command(stmt)
                    self._log(f"Выполнено ({i}/{len(statements)}): {stmt[:80]}...")
                except Exception as e:
                    self._log(f"ОШИБКА ({i}/{len(statements)}): {e}", "ERROR")

            # Verify
            for db, table in self.selected_tables:
                if self._verify_table_exists(db, table):
                    self._log(f"Проверка OK: `{db}`.`{table}` существует на destination")
                else:
                    self._log(f"Проверка FAIL: `{db}`.`{table}` НЕ найдена на destination", "ERROR")

            self._set_buttons_state(True)

        threading.Thread(target=_do, daemon=True).start()

    def _verify_table_exists(self, database: str, table: str) -> bool:
        try:
            result = self.dest_client.query(
                "SELECT count() FROM system.tables WHERE database = %(db)s AND name = %(tbl)s",
                parameters={"db": database, "tbl": table},
            ).result_rows
            return bool(result and result[0][0] > 0)
        except Exception:
            return False

    # ── Data Migration ───────────────────────────────────────────────

    def _migrate_data(self):
        if not self.source_client or not self.dest_client:
            self._log("Нужны оба подключения", "ERROR")
            return

        sql_text = self.sql_text.get("1.0", tk.END).strip()
        if not sql_text:
            self._log("Нет SELECT SQL. Сгенерируйте запросы.", "WARN")
            return

        statements = [s.strip().rstrip(";") for s in sql_text.split(";") if s.strip()]
        tables_sorted = sorted(self.selected_tables)

        if len(statements) != len(tables_sorted):
            self._log(
                f"Кол-во SQL ({len(statements)}) не совпадает с выбранными таблицами ({len(tables_sorted)})",
                "ERROR",
            )
            return

        def _do():
            self._set_buttons_state(False)
            total = len(statements)

            for i, ((db, table), select_sql) in enumerate(zip(tables_sorted, statements), 1):
                try:
                    self._log(f"Миграция ({i}/{total}): `{db}`.`{table}`...")
                    result = self.source_client.query(select_sql)

                    if not result.result_rows:
                        self._log(f"  Нет данных для `{db}`.`{table}`")
                        continue

                    col_names = result.column_names
                    self.dest_client.insert(
                        table=f"`{db}`.`{table}`",
                        data=result.result_rows,
                        column_names=col_names,
                    )
                    self._log(f"  Мигрировано {len(result.result_rows)} строк в `{db}`.`{table}`")
                except Exception as e:
                    self._log(f"  ОШИБКА миграции `{db}`.`{table}`: {e}", "ERROR")

            self._log("Миграция завершена")
            self._set_buttons_state(True)

        threading.Thread(target=_do, daemon=True).start()

    # ── UI Helpers ───────────────────────────────────────────────────

    def _log(self, message: str, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {level}: {message}\n"

        def _append():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, line)
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)

        self.root.after(0, _append)

    def _copy_to_clipboard(self, text: str):
        text = text.strip()
        if text:
            self.root.clipboard_clear()
            self.root.clipboard_append(text)
            self._log("Скопировано в буфер обмена")

    def _set_buttons_state(self, enabled: bool):
        state = tk.NORMAL if enabled else tk.DISABLED

        def _do():
            self.btn_gen_ddl.config(state=state)
            self.btn_create_ddl.config(state=state)
            self.btn_migrate.config(state=state)

        self.root.after(0, _do)


def main():
    load_dotenv()
    root = tk.Tk()
    CHMigrateApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
