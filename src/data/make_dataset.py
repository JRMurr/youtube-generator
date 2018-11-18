# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from youtube_api_setup import YoutubeWrapper
import os
import yaml

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
# Channels to get info from
SEED_DATA = os.path.abspath(os.path.join(THIS_DIR, 'seed_search.yml'))


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


def getVideoInfo(channels):
    youtube = YoutubeWrapper()
    for channel in channels:
        youtube.getChannelUploads(channel)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    # main()
    with open(SEED_DATA, 'r') as stream:
        seed_youtube = yaml.load(stream)
    getVideoInfo(seed_youtube['channels'])
