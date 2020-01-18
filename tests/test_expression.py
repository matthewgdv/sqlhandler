# import pytest


class TestExpressionMixin:
    def test_sql(self):  # synced
        assert True

    def test_execute(self):  # synced
        assert True

    def test__prepare_tran(self):  # synced
        assert True

    def test__resolve_tran(self):  # synced
        assert True

    def test__perform_pre_select(self):  # synced
        assert True

    def test__perform_post_select(self):  # synced
        assert True

    def test__perform_pre_select_from_select(self):  # synced
        assert True

    def test__execute_expression_and_determine_rowcount(self):  # synced
        assert True

    def test__perform_post_select_inserts(self):  # synced
        assert True

    def test__perform_post_select_all(self):  # synced
        assert True


class TestSelect:
    def test___str__(self):  # synced
        assert True

    def test_frame(self):  # synced
        assert True

    def test_resolve(self):  # synced
        assert True

    def test_literal(self):  # synced
        assert True

    def test_from_(self):  # synced
        assert True

    def test__select_to_frame(self):  # synced
        assert True


class TestUpdate:
    def test___str__(self):  # synced
        assert True

    def test_resolve(self):  # synced
        assert True

    def test_literal(self):  # synced
        assert True

    def test_set_(self):  # synced
        assert True


class TestInsert:
    def test___str__(self):  # synced
        assert True

    def test_resolve(self):  # synced
        assert True

    def test_literal(self):  # synced
        assert True

    def test_values(self):  # synced
        assert True


class TestDelete:
    def test___str__(self):  # synced
        assert True

    def test_resolve(self):  # synced
        assert True

    def test_literal(self):  # synced
        assert True


class TestSelectInto:
    def test___str__(self):  # synced
        assert True

    def test_resolve(self):  # synced
        assert True

    def test_literal(self):  # synced
        assert True


def test_s_into():  # synced
    assert True
