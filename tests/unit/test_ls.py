from src.orchestra_dbt.ls import get_args_for_ls


class TestGetArgsForLs:
    def test_defaults_to_path_output(self):
        assert get_args_for_ls(())[-3:] == ["--output", "path", "-q"]

    def test_can_request_selector_output(self):
        """`--output selector` gives dotted fqns -- what dbt itself emits for feeding
        a selection back into `--select`, and what freshness scoping needs, since
        paths don't resolve for package-owned nodes."""
        assert get_args_for_ls((), "selector")[-3:] == ["--output", "selector", "-q"]

    def test_forwards_user_args_and_drops_unsupported_ones(self):
        args = get_args_for_ls(("--select", "tag:nightly", "--empty"))

        assert "--select" in args
        assert "tag:nightly" in args
        assert "--empty" not in args

    def test_resource_types_and_user_args_are_identical_for_both_outputs(self):
        """The two calls must resolve the *same* selection -- only the output differs."""
        paths = get_args_for_ls(("--select", "tag:nightly"))
        selectors = get_args_for_ls(("--select", "tag:nightly"), "selector")

        assert paths[:-3] == selectors[:-3]
