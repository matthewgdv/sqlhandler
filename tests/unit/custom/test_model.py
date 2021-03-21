# import pytest


class TestModelMeta:
    def test___new__(self):  # synced
        assert True

    def test___getitem__(self):  # synced
        assert True

    def test_query(self):  # synced
        assert True

    def test_create(self):  # synced
        assert True

    def test_c(self):  # synced
        assert True

    def test_alias(self):  # synced
        assert True

    def test_drop(self):  # synced
        assert True


class TestBaseModel:
    def test_insert(self):  # synced
        assert True

    def test_update(self):  # synced
        assert True

    def test_delete(self):  # synced
        assert True

    def test_clone(self):  # synced
        assert True


class TestModel:
    def test___table_args__(self):  # synced
        assert True


class TestTemplatedModel:
    def test___tablename__(self):  # synced
        assert True

    def test_created(self):  # synced
        assert True

    def test_modified(self):  # synced
        assert True

    def test_active(self):  # synced
        assert True


class TestReflectedModel:
    pass


def test_on_column_reflect():  # synced
    assert True
