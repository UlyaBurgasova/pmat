import csv
import os
from abc import ABC, abstractmethod


class SingletonMeta(type):
    """Синглтон метакласс для Database."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=SingletonMeta):
    """Класс-синглтон базы данных с таблицами, хранящимися в файлах."""

    def __init__(self):
        self.tables = {}

    def register_table(self, table_name, table):
        self.tables[table_name] = table

    def insert(self, table_name, data):
        table = self.tables.get(table_name)
        if table:
            table.insert(data)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def select(self, table_name, *args):
        table = self.tables.get(table_name)
        return table.select(*args) if table else None

    def join(self, table1_name, table2_name, join_attr1="id", join_attr2="id"):
        """Выполнение JOIN между двумя таблицами по указанным атрибутам."""
        table1 = self.tables.get(table1_name)
        table2 = self.tables.get(table2_name)

        if not table1 or not table2:
            raise ValueError("One or both tables do not exist.")

        joined_data = []
        for row1 in table1.data:
            for row2 in table2.data:
                if row1[join_attr1] == row2[join_attr2]:
                    joined_data.append({**row1, **row2})

        return joined_data

    def multi_join(
        self, table1_name, table2_name, table3_name, join_attrs1, join_attrs2
    ):
        """Выполнение последовательного JOIN между тремя таблицами."""
        if len(join_attrs1) != 2 or len(join_attrs2) != 2:
            raise ValueError(
                "Each join_attrs parameter must contain exactly two attributes."
            )

        first_join_attr1, first_join_attr2 = join_attrs1
        second_join_attr1, second_join_attr2 = join_attrs2

        first_join = self.join(
            table1_name, table2_name, first_join_attr1, first_join_attr2
        )
        first_join_table = TempTable(first_join)
        self.register_table("first_join_table", first_join_table)
        final_join = self.join(
            "first_join_table", table3_name, second_join_attr1, second_join_attr2
        )
        return final_join

    def aggregate(self, table_name_or_data, method, column):
        if isinstance(table_name_or_data, str):
            table = self.tables.get(table_name_or_data)
            values = [float(row[column]) for row in table.data if column in row]
        else:
            values = [float(row[column]) for row in table_name_or_data if column in row]

        if not values:
            raise ValueError(f"Column {column} does not contain valid data.")

        if method == "avg":
            return sum(values) / len(values)
        elif method == "max":
            return max(values)
        elif method == "min":
            return min(values)
        elif method == "count":
            return len(values)
        else:
            raise ValueError(f"Unknown aggregate method: {method}")


class TempTable:
    """Временная таблица для хранения промежуточных данных при JOIN."""

    def __init__(self, data):
        self.data = data


class Table(ABC):  # pragma: no cover
    """Абстрактный базовый класс для таблиц с вводом/выводом файлов CSV."""

    @abstractmethod
    def insert(self, data):
        pass

    @abstractmethod
    def select(self, *args):
        pass


class EmployeeTable(Table):
    ATTRS = ("e_id", "department_id", "name", "age", "salary")
    FILE_PATH = "employee_table.csv"

    def __init__(self):
        self.data = []
        self.load()

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split()))
        for existing_entry in self.data:
            if (
                existing_entry["e_id"] == entry["e_id"]
                and existing_entry["department_id"] == entry["department_id"]
            ):
                raise ValueError(
                    "Duplicate e_id and department_id pair is not allowed."
                )

        self.data.append(entry)
        self.save()

    def select(self, start_id, end_id):
        return [
            entry for entry in self.data if start_id <= int(entry["e_id"]) <= end_id
        ]

    def save(self):
        with open(self.FILE_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
        else:
            self.data = []


class DepartmentTable(Table):
    ATTRS = ("d_id", "department_name")
    FILE_PATH = "department_table.csv"

    def __init__(self):
        self.data = []
        self.load()

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split()))
        for existing_entry in self.data:
            if existing_entry["d_id"] == entry["d_id"]:
                raise ValueError("Duplicate d_id is not allowed.")

        self.data.append(entry)
        self.save()

    def select(self, department_name):
        return [
            entry for entry in self.data if entry["department_name"] == department_name
        ]

    def save(self):
        with open(self.FILE_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
        else:
            self.data = []


class ProjectTable(Table):
    ATTRS = ("p_id", "project_name", "budget", "manager_id")
    FILE_PATH = "project_table.csv"

    def __init__(self):
        self.data = []
        self.load()

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split()))
        for existing_entry in self.data:
            if existing_entry["p_id"] == entry["p_id"]:
                raise ValueError("Duplicate p_id is not allowed.")

        self.data.append(entry)
        self.save()

    def select(self, budget_threshold):
        return [
            entry for entry in self.data if float(entry["budget"]) > budget_threshold
        ]

    def save(self):
        with open(self.FILE_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
        else:
            self.data = []
