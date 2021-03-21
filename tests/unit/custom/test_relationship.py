# import pytest


class TestRelationship:
    class TestSettings:
        pass

    class TestKind:
        pass

    class TestOne:
        def test_to_one(self):  # synced
            assert True

    class TestMany:
        def test_to_one(self):  # synced
            assert True

        def test_to_many(self):  # synced
            assert True

        def test_to_self(self):  # synced
            assert True

    class Test_TargetEntity:
        def test_from_model(self):  # synced
            assert True

        def test_from_namespace(self):  # synced
            assert True

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

    def test__defer_create_table(self):  # synced
        assert True

    def test__casing(self):  # synced
        assert True
