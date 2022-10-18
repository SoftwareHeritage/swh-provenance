#!/usr/bin/env python3
# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# type: ignore

import argparse
import logging
from pathlib import Path
import shutil

from swh.dataset.exporters.edges import GraphEdgesExporter
from swh.dataset.exporters.orc import ORCExporter
from swh.graph.webgraph import compress
from swh.provenance.tests.utils import load_repo_data


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Generate a test dataset")
    parser.add_argument(
        "--compress",
        action="store_true",
        default=False,
        help="Also compress the dataset",
    )
    parser.add_argument("--output", help="output directory", default="swhgraph")
    parser.add_argument("dataset", help="dataset name", nargs="+")
    args = parser.parse_args()

    for repo in args.dataset:
        exporters = {"edges": GraphEdgesExporter, "orc": ORCExporter}
        config = {"test_unique_file_id": "all"}
        output_path = Path(args.output) / repo
        data = load_repo_data(repo)
        print(data.keys())

        for name, exporter in exporters.items():
            if (output_path / name).exists():
                shutil.rmtree(output_path / name)
            with exporter(config, output_path / name) as e:
                for object_type, objs in data.items():
                    for obj_dict in objs:
                        e.process_object(object_type, obj_dict)

        if args.compress:
            if (output_path / "compressed").exists():
                shutil.rmtree(output_path / "compressed")
            compress("example", output_path / "orc", output_path / "compressed")


if __name__ == "__main__":
    main()
