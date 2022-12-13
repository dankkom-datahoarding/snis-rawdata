import json
import subprocess
from pathlib import Path


def main():
    with open("metadata.json") as f:
        metadata = json.load(f)
    releasedir = Path("release")
    for dataset_name in metadata["datasets"]:
        for datasubset_name in metadata["datasets"][dataset_name]:
            datasubset_path = releasedir / dataset_name / datasubset_name
            filename = f"{dataset_name}-{datasubset_name}.7z"
            cmd = [
                "7z",
                "a",
                filename,
                f".\\{datasubset_path / '*'}",
            ]
            print(" ".join(cmd))
            subprocess.run(" ".join(cmd))


if __name__ == "__main__":
    main()
