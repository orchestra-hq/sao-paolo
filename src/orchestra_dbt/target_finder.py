def find_target_in_args(args: list[str]) -> str | None:
    if "--target" not in args:
        return None
    try:
        if args[args.index("--target") + 1]:
            return args[args.index("--target") + 1]
    except IndexError:
        return None
