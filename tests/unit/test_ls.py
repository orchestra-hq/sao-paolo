from src.orchestra_dbt.ls import get_args_for_ls


class TestGetArgsForLs:
    def test_requests_both_path_and_fqn_in_one_invocation(self):
        """Asking twice would re-resolve the same selection."""
        assert get_args_for_ls(())[-6:] == [
            "--output",
            "json",
            "--output-keys",
            "original_file_path",
            "fqn",
            "-q",
        ]

    def test_forwards_user_args_and_drops_unsupported_ones(self):
        args = get_args_for_ls(("--select", "tag:nightly", "--empty"))

        assert "--select" in args
        assert "tag:nightly" in args
        assert "--empty" not in args
