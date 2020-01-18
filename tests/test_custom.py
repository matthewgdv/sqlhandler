# import pytest


class TestTable:
    def test___new__():  # synced
        assert True


class TestCreateTableAccessor:
    def test___call__(self):  # synced
        assert True


class TestModelMeta:
    def test___new__():  # synced
        assert True

    def test_query():  # synced
        assert True

    def test_create():  # synced
        assert True

    def test_c():  # synced
        assert True

    def test_alias():  # synced
        assert True

    def test_drop():  # synced
        assert True


class TestModel:
    def test_insert(self):  # synced
        assert True

    def test_update(self):  # synced
        assert True

    def test_delete(self):  # synced
        assert True

    def test_clone(self):  # synced
        assert True


class TestAutoModel:
    def test___tablename__():  # synced
        assert True

    def test_created():  # synced
        assert True

    def test_modified():  # synced
        assert True

    def test_active():  # synced
        assert True


class TestSession:
    def test_query(self):  # synced
        assert True

    def test_execute(self):  # synced
        assert True


class TestQuery:
    def test___str__(self):  # synced
        assert True

    def test_frame(self):  # synced
        assert True

    def test_vector(self):  # synced
        assert True

    def test_literal(self):  # synced
        assert True

    def test_from_(self):  # synced
        assert True

    def test_where(self):  # synced
        assert True

    def test_update(self):  # synced
        assert True

    def test_delete(self):  # synced
        assert True

    def test_subquery(self):  # synced
        assert True


class TestForeignKey:
    pass


class TestRelationship:
    class TestKind:
        pass

    class TestOne:
        def test_to_one():  # synced
            assert True

    class TestMany:
        def test_to_one():  # synced
            assert True

        def test_to_many():  # synced
            assert True

    class Test_TargetEntity:
        pass

    class Test_FutureEntity:
        pass

    def test_build(self):  # synced
        assert True

    def test__build_fk_columns(self):  # synced
        assert True

    def test__build_relationship(self):  # synced
        assert True

    def test__build_association_table(self):  # synced
        assert True

    def test__defer_create_table():  # synced
        assert True

    def test__casing(self):  # synced
        assert True


def test_absolute_namespace():  # synced
    assert True
