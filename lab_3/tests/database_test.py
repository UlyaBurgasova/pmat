import os
import tempfile

import pytest
from database.database import Database, DepartmentTable, EmployeeTable, ProjectTable


@pytest.fixture
def temp_employee_file():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(b"e_id,department_id,name,age,salary\n")
        temp_file.write(b"1,101,John,30,50000\n")
        temp_file.write(b"2,102,Jane,25,60000\n")
        temp_file.close()
        yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def temp_department_file():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(b"d_id,department_name\n")
        temp_file.write(b"101,HR\n")
        temp_file.write(b"102,IT\n")
        temp_file.close()
        yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def temp_project_file():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(b"p_id,project_name,budget,manager_id\n")
        temp_file.write(b"1,Alpha,100000,1\n")
        temp_file.write(b"2,Beta,50000,2\n")
        temp_file.close()
        yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def database(temp_employee_file, temp_department_file, temp_project_file):
    db = Database()
    employee_table = EmployeeTable()
    employee_table.FILE_PATH = temp_employee_file
    employee_table.load()
    department_table = DepartmentTable()
    department_table.FILE_PATH = temp_department_file
    department_table.load()
    project_table = ProjectTable()
    project_table.FILE_PATH = temp_project_file
    project_table.load()
    db.register_table("employees", employee_table)
    db.register_table("departments", department_table)
    db.register_table("projects", project_table)
    return db


def test_employee_table_insert_and_select(database):
    db = database
    db.insert("employees", "3 103 Justin 35 70000")
    selected = db.select("employees", 1, 3)
    assert len(selected) == 3
    assert selected[0]["name"] == "John"
    assert selected[1]["name"] == "Jane"
    assert selected[2]["name"] == "Justin"


def test_department_table_insert_and_select(database):
    db = database
    db.insert("departments", "103 SMM")
    selected = db.select("departments", "HR")
    assert len(selected) == 1
    assert selected[0]["department_name"] == "HR"


def test_project_table_insert_and_select(database):
    db = database
    db.insert("projects", "3 Charlie 20000 1")
    selected = db.select("projects", 60000)
    assert len(selected) == 1
    assert selected[0]["project_name"] == "Alpha"


def test_join_two_tables(database):
    db = database
    joined = db.join("employees", "departments", "department_id", "d_id")
    assert len(joined) == 2
    assert joined[0]["department_name"] == "HR"
    assert joined[1]["department_name"] == "IT"


def test_multi_join(database):
    db = database
    joined = db.multi_join(
        "employees",
        "departments",
        "projects",
        ("department_id", "d_id"),
        ("e_id", "manager_id"),
    )
    assert len(joined) == 2
    assert joined[0]["project_name"] == "Alpha"


def test_aggregate(database):
    db = database
    avg_salary = db.aggregate("employees", "avg", "salary")
    max_salary = db.aggregate("employees", "max", "salary")
    min_salary = db.aggregate("employees", "min", "salary")
    count_salary = db.aggregate("employees", "count", "salary")
    assert avg_salary == 55000.0
    assert max_salary == 60000.0
    assert min_salary == 50000.0
    assert count_salary == 2
    with pytest.raises(ValueError, match="Unknown aggregate method: sum"):
        db.aggregate("employees", "sum", "salary")


def test_two_joins_and_aggregation(database):
    db = database
    joined_data = db.multi_join(
        "employees",
        "departments",
        "projects",
        ("department_id", "d_id"),
        ("e_id", "manager_id"),
    )
    max_budget = db.aggregate(joined_data, "max", "budget")
    count_projects = db.aggregate(joined_data, "count", "p_id")
    assert max_budget == 100000.0
    assert count_projects == 2


def test_duplicate_inserts(database):
    db = database
    with pytest.raises(
        ValueError, match="Duplicate e_id and department_id pair is not allowed."
    ):
        db.insert("employees", "1 101 John 30 50000")
    with pytest.raises(ValueError, match="Duplicate d_id is not allowed."):
        db.insert("departments", "101 HR")
    with pytest.raises(ValueError, match="Duplicate p_id is not allowed."):
        db.insert("projects", "1 Alpha 100000 1")


def test_missing_table(database):
    with pytest.raises(ValueError, match="Table unknown does not exist."):
        database.insert("unknown", "1 101 John 30 50000")


def test_join_missing_table(database):
    with pytest.raises(ValueError, match="One or both tables do not exist."):
        database.join("employees", "unknown", "department_id", "u_id")


def test_missing_attr_in_multi_join(database):
    with pytest.raises(
        ValueError,
        match="Each join_attrs parameter must contain exactly two attributes.",
    ):
        database.multi_join(
            "employees",
            "departments",
            "projects",
            "department_id",
            ("e_id", "manager_id"),
        )


def test_aggregate_missing_column(database):
    with pytest.raises(
        ValueError, match="Column personal_project_count does not contain valid data."
    ):
        database.aggregate("employees", "avg", "personal_project_count")
