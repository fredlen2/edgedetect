import os
import fnmatch
from hashlib import sha1

import numpy as np
from pydub import AudioSegment
from pydub.utils import audioop

from . import wavio

# try:
#     range = xrange
# except NameError:
#     pass


def unique_hash(filepath, blocksize=2**20):
    """ Small function to generate a hash to uniquely generate
    a file. Inspired by MD5 version here:
    http://stackoverflow.com/a/1131255/712997

    Works with large files. 
    """
    s = sha1()
    with open(filepath, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            s.update(buf)
    return s.hexdigest().upper()


def find_files(path, extensions):
    # Allow both with ".mp3" and without "mp3" to be used for extensions
    extensions = [e.replace(".", "") for e in extensions]

    for dirpath, dirnames, files in os.walk(path):
        for extension in extensions:
            for f in fnmatch.filter(files, "*.%s" % extension):
                p = os.path.join(dirpath, f)
                yield (p, extension)


def read(filename, limit=None):
    """
    Reads any file supported by pydub (ffmpeg) and returns the data contained
    within. If file reading fails due to input being a 24-bit wav file,
    wavio is used as a backup.

    Can be optionally limited to a certain amount of seconds from the start
    of the file by specifying the `limit` parameter. This is the amount of
    seconds from the start of the file.

    returns: (channels, samplerate)
    """
    # pydub does not support 24-bit wav files, use wavio when this occurs
    try:
        file_name, file_extension = os.path.splitext(filename)
        # This method will play back file types whose extension matches the coded
        # This includes wav and mp3 so we should be good
        audiofile = AudioSegment.from_file(filename, format=file_extension[1:])

        # audiofile = AudioSegment.from_file(filename, format="mp3")

        # audiofile = AudioSegment.from_file(filename)

        if limit:
            audiofile = audiofile[:limit * 1000]

        data = np.fromstring(audiofile._data, np.int16)

        channels = []
        for chn in range(audiofile.channels):
            channels.append(data[chn::audiofile.channels])

        fs = audiofile.frame_rate
    except audioop.error:
        fs, _, audiofile = wavio.readwav(filename)

        if limit:
            audiofile = audiofile[:limit * 1000]

        audiofile = audiofile.T
        audiofile = audiofile.astype(np.int16)

        channels = []
        for chn in audiofile:
            channels.append(chn)

    # return channels, audiofile.frame_rate, unique_hash(filename)
    return channels, audiofile.frame_rate, unique_hash(filename), float(len(audiofile)) / 1000.0


def path_to_advertname(path):
    """
    Extracts song name from a filepath. Used to identify which adverts
    have already been fingerprinted on disk.
    """
    return os.path.splitext(os.path.basename(path))[0]
