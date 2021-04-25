#!/usr/local/bin/python3.7

import os
import sys
import json
import warnings
import argparse

from tramscore import Tramscore
from tramscore.recognize import DirFileRecognizer
from tramscore.recognize import FileRecognizer
from tramscore.recognize import MicrophoneRecognizer
from argparse import RawTextHelpFormatter

warnings.filterwarnings("ignore")

DEFAULT_CONFIG_FILE = "/home/blazoninnovation/testbed/tramstest/tramscore/tramscore.conf"


def init(configpath):
    """ 
    Load config from a JSON file
    """
    try:
        with open(configpath) as f:
            config = json.load(f)
    except IOError as err:
        print(("Cannot open configuration: %s. Exiting" % (str(err))))
        sys.exit(1)

    # create a Tramscore instance
    return Tramscore(config)


def recognizeAllDirectory(path, filename_to_write):
    trams = init(DEFAULT_CONFIG_FILE)
    with open(filename_to_write, 'a') as f:
        for dir_entry in os.listdir(path):
            file_path = os.path.join(path, dir_entry)
            advert = trams.recognize(FileRecognizer, file_path)
            print(advert)
            json.dump(advert, f)
            f.write(os.linesep)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Tramscore: Audio Fingerprinting library",
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('-c', '--config', nargs='?',
                        help='Path to configuration file\n'
                             'Usages: \n'
                             '--config /path/to/config-file\n')
    parser.add_argument('-f', '--fingerprint', nargs='*',
                        help='Fingerprint files in a directory\n'
                             'Usages: \n'
                             '--fingerprint /path/to/directory extension\n'
                             '--fingerprint /path/to/advert\n'
                             '--fingerprint /path/to/advert client_user_id\n'
                             '--fingerprint /path/to/advert client_user_id media_station_id\n')
    parser.add_argument('-r', '--recognize', nargs='*',
                        help='Recognize what is '
                             'playing through the microphone\n'
                             'Usage: \n'
                             '--recognize mic number_of_seconds \n'
                             '--recognize file path/to/file \n' 
                             '--recognize dir /path/to/directory extension\n')
    args = parser.parse_args()

    if not args.fingerprint and not args.recognize:
        parser.print_help()
        sys.exit(0)

    config_file = args.config
    if config_file is None:
        config_file = DEFAULT_CONFIG_FILE
        # print("Using default config file: %s" % (config_file))

    trams = init(config_file)

    if args.fingerprint:
        if len(args.fingerprint) == 1:
            filepath = args.fingerprint[0]
            if os.path.isdir(filepath):
                print("Please specify an extension of adverts!")
                sys.exit(1)
            trams.fingerprint_client_file(filepath)

        elif len(args.fingerprint) == 2:
            filepath = args.fingerprint[0]
            client_user_id = args.fingerprint[1]
            if os.path.isdir(filepath):
                print("Please specify an extension of adverts!")
                sys.exit(1)
            trams.fingerprint_client_file(filepath, int(client_user_id))

        elif len(args.fingerprint) == 3:
            filepath = args.fingerprint[0]
            client_user_id = args.fingerprint[1]
            # media_station_id = args.fingerprint[2]
            media_station_id = [int(id) for id in args.fingerprint[2].split(',')]
            if os.path.isdir(filepath):
                print("Please specify an extension of adverts!")
                sys.exit(1)
            trams.fingerprint_client_file(filepath, client_user_id, media_station_id)

    elif args.recognize:
        # Recognize audio source
        advert = None
        advertlist = []

        if len(args.recognize) == 2:
            source = args.recognize[0]
            opt_arg = args.recognize[1]

            if source in ('mic', 'microphone'):
                advert = trams.recognize(MicrophoneRecognizer, seconds=int(opt_arg))
            elif source == 'file':
                advert = trams.recognize(FileRecognizer, opt_arg)
            if advert is not None:
                advert["advert_name"] = (advert["advert_name"]).decode('utf-8')
                advert["file_sha1"] = advert["file_sha1"]
                # advert["file_sha1"] = (advert["file_sha1"]).decode('utf-8')
                # decoded_advert = repr(advert).decode('string_escape')
                print((json.dumps(advert)))
            else:
                print(advert)

        # Trying Directory recognizing
        if len(args.recognize) == 3:
            source = args.recognize[0]
            directory = args.recognize[1]
            extension = args.recognize[2]

            if source in ('dir', 'directory'):
                print(("Recognizing all .%s files in the %s directory" % (extension, directory)))
                advertlist = trams.recognize_directory(directory, ["." + extension], 4)

            for advert in advertlist:
                advert = trams.recognize(DirFileRecognizer, advert)
                if advert is not None:
                    advert["advert_name"] = (advert["advert_name"]).decode('utf-8')
                    advert["file_sha1"] = advert["file_sha1"]
                    # advert["file_sha1"] = (advert["file_sha1"]).decode('utf-8')
                    print((json.dumps(advert)))
                else:
                    print("\n")
                    print(advert)

    sys.exit(0)
