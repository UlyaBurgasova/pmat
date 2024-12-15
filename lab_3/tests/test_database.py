import pytest
import os
import tempfile
from database.database import Database, EmployeeTable, DepartmentTable

@pytest.fixture
def temp_employee_file():
    """ Создаем временный файл для таблицы рабочих """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

@pytest.fixture
def temp_department_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

#Пример, как используются фикстуры
@pytest.fixture
def database(temp_employee_file, temp_department_file):
    """ Данная фикстура задает БД и определяет таблицы. """
    db = Database() 

    # Используем временные файлы для тестирования файлового ввода-вывода в EmployeeTable и DepartmentTable
    employee_table = EmployeeTable()
    employee_table.FILE_PATH = temp_employee_file
    department_table = DepartmentTable()
    department_table.FILE_PATH = temp_department_file

    db.register_table("employees", employee_table)
    db.register_table("departments", department_table)

    return db

def test_insert_employee(database):
    database.insert("employees", "1 Alice 30 70000")
    database.insert("employees", "2 Bob 28 60000")

    # Проверяем вставку, подгружая с CSV
    employee_data = database.select("employees", 1, 2)
    print(employee_data)
    assert len(employee_data) == 2
    assert employee_data[0] == {'id': '1', 'name': 'Alice', 'age': '30', 'salary': '70000'}
    assert employee_data[1] == {'id': '2', 'name': 'Bob', 'age': '28', 'salary': '60000'}

def test_unique_indices_in_employee_table(database):
    """ Проверяет уникальность пары id и department_id. """
    database.insert("employees", "1 10 Alice 30 70000")
    with pytest.raises(ValueError, match="Duplicate id and department_id pair is not allowed."):
        database.insert("employees", "1 10 Bob 28 60000")

def test_join_employees_departments(database):
   
    database.insert("departments", "10 Sales")
    database.insert("departments", "20 Marketing")
    database.insert("employees", "1 10 Alice 30 70000")
    database.insert("employees", "2 20 Bob 28 60000")
    database.insert("employees", "3 10 Charlie 35 80000")
   
    join_result = database.join("departments", "employees", join_attr="id")
    
    expected_result = [
        {'department_id': '10', 'department_name': 'Sales', 'id': '1', 'name': 'Alice', 'age': '30', 'salary': '70000'},
        {'department_id': '20', 'department_name': 'Marketing', 'id': '2', 'name': 'Bob', 'age': '28', 'salary': '60000'},
        {'department_id': '10', 'department_name': 'Sales', 'id': '3', 'name': 'Charlie', 'age': '35', 'salary': '80000'}
    ]
    assert join_result == expected_result

def test_select_employees_by_id_range(database):
    database.insert("employees", "1 10 Alice 30 70000")
    database.insert("employees", "2 20 Bob 28 60000")
    database.insert("employees", "3 10 Charlie 35 80000")

    selected = database.select("employees", 2, 3)
    expected_selection = [
        {'id': '2', 'department_id': '20', 'name': 'Bob', 'age': '28', 'salary': '60000'},
        {'id': '3', 'department_id': '10', 'name': 'Charlie', 'age': '35', 'salary': '80000'}
    ]
    assert selected == expected_selection
