# import pytest


class TestSqlBoundMixin:
    def test_from_sql():  # synced
        assert True

    def test_wrapper():  # synced
        assert True


class TestExecutable:
    def test___call__(self):  # synced
        assert True

    def test_execute(self):  # synced
        assert True

    def test__compile_sql(self):  # synced
        assert True

    def test__get_frames_from_cursor():  # synced
        assert True

    def test_get_frame_from_cursor():  # synced
        assert True


class TestStoredProcedure:
    def test__compile_sql(self):  # synced
        assert True


class TestScript:
    def test__compile_sql(self):  # synced
        assert True


class TestTempManager:
    def test___str__(self):  # synced
        assert True

    def test___call__(self):  # synced
        assert True


def test_literalstatement():  # synced
    assert True
