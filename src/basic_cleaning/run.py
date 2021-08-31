#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f'Downloading artifact {args.input_artifact}')
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    logger.info("Loading dataset")
    df = pd.read_csv(artifact_local_path)

    logger.info("Basic Cleaning")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f'Save clean csv {args.output_artifact}')
    df.to_csv("clean_sample.csv", index=False)

    logger.info("Save clean csv artifact")
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description
     )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    os.remove(args.output_artifact)
    run.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name for input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type for output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Min price value for outlier replace",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Max price value for outlier replace",
        required=True
    )


    args = parser.parse_args()

    go(args)
