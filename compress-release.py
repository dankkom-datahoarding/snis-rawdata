import json
import subprocess
from pathlib import Path


def main():
    with open("metadata.json") as f:
        metadata = json.load(f)
    releasedir = Path("release")
    for dataset_name in metadata["datasets"]:
        for datasubset_name in metadata["datasets"][dataset_name]:
            datasubset_filepath = (
                releasedir / dataset_name / f"{datasubset_name}.csv"
            )
            filename = f"{dataset_name}-{datasubset_name}.zip"
            cmd = [
                "zip",
                "-9jpr",
                filename,
                str(datasubset_filepath),
            ]
            print(cmd)
            subprocess.run(cmd)


if __name__ == "__main__":
    main()
