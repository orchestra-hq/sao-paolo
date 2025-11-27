import yaml
from pathlib import Path


def get_target_inner():
    project = yaml.safe_load(Path("dbt_project.yml").read_text())
    profile_name = project.get("profile", "default")
    print(profile_name)
