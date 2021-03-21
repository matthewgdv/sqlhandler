import pytest
from sqlhandler import Sql


@pytest.fixture
def sql():
    sql = Sql.from_memory()
    decl = sql.Declarative
    Relationship, Col, String = decl.Relationship, decl.Column, decl.String

    class Position(sql.TemplatedModel):
        name = Col(String)

    class Company(sql.TemplatedModel):
        name = Col(String)

    class Employee(sql.TemplatedModel):
        name = Col(String)
        position = Relationship.Many.to_one(Position)
        company = Relationship.Many.to_one(Company)

    Position.create()
    Company.create()
    Employee.create()

    with sql.transaction:
        data_eng = Position(name="Data Engineer").insert()
        outplay = Company(name="Outplay").insert()
        me = Employee(name="Matt", position=data_eng, company=outplay).insert()

    return sql


def test_query(sql):
    Position = sql.tables[None].position()
    Company = sql.tables[None].company()
    Employee = sql.tables[None].employee()

    employee, position, company = sql.query(Employee, Position, Company).from_(Employee).join(Position, Company).one()

    assert employee.name == "Matt" and position.name == "Data Engineer" and company.name == "Outplay"
