import argparse
import pathlib

from shipping_emissions.pipeline.pipeline import run_pipeline

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("dataset_name")
    argparser.add_argument("schema_file", type=pathlib.Path)
    argparser.add_argument("data_files", nargs="+", type=pathlib.Path)
    args = argparser.parse_args()

    run_pipeline(args.dataset_name, args.schema_file, args.data_files)
