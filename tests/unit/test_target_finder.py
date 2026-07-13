from src.orchestra_dbt.target_finder import find_target_in_args


class TestFindTargetInArgs:
    def test_find_target_in_args_success(self):
        args = ["dbt", "source", "freshness", "--target", "test"]
        assert find_target_in_args(args) == "test"

    def test_find_target_in_args_no_target(self):
        args = ["dbt", "source", "freshness"]
        assert find_target_in_args(args) is None
