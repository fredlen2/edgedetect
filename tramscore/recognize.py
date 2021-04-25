# encoding: utf-8
import time

import numpy as np
import pyaudio

from . import fingerprint
from . import decoder


class BaseRecognizer(object):

    def __init__(self, tramscore):
        self.tramscore = tramscore
        self.Fs = fingerprint.DEFAULT_FS

    def _recognize(self, *data):
        matches = []
        total_hashes = 0
        for d in data:
            extracted_matches = self.tramscore.find_matches(d, Fs=self.Fs)
            total_hashes += extracted_matches[1]
            matches.extend(extracted_matches[0])
        return self.tramscore.align_matches(matches, total_hashes)

    def recognize(self):
        pass  # base class does nothing


class DirFileRecognizer(BaseRecognizer):
    def __init__(self, tramscore):
        super(DirFileRecognizer, self).__init__(tramscore)

    def recognize_file(self, filename):
        frames, self.Fs, file_hash, audio_length = decoder.read(filename, self.tramscore.limit)

        t = time.time()
        match = self._recognize(*frames)
        t = time.time() - t

        if match:
            match['match_time'] = t

        return match

    def recognize(self, filename):
        return self.recognize_file(filename)


class FileRecognizer(BaseRecognizer):
    def __init__(self, tramscore):
        super(FileRecognizer, self).__init__(tramscore)

    def recognize_file(self, filename):
        frames, self.Fs, file_hash, audio_length = decoder.read(filename, self.tramscore.limit)

        t = time.time()
        match = self._recognize(*frames)
        t = time.time() - t

        if match:
            match['match_time'] = t

        return match

    def recognize(self, filename):
        return self.recognize_file(filename)


class MicrophoneRecognizer(BaseRecognizer):
    default_chunksize   = 8192
    default_format      = pyaudio.paInt16
    default_channels    = 2
    default_samplerate  = 44100

    def __init__(self, tramscore):
        super(MicrophoneRecognizer, self).__init__(tramscore)
        self.audio = pyaudio.PyAudio()
        self.stream = None
        self.data = []
        self.channels = MicrophoneRecognizer.default_channels
        self.chunksize = MicrophoneRecognizer.default_chunksize
        self.samplerate = MicrophoneRecognizer.default_samplerate
        self.recorded = False

    def start_recording(self, channels=default_channels,
                        samplerate=default_samplerate,
                        chunksize=default_chunksize):
        print("* start recording")
        self.chunksize = chunksize
        self.channels = channels
        self.recorded = False
        self.samplerate = samplerate

        if self.stream:
            self.stream.stop_stream()
            self.stream.close()

        self.stream = self.audio.open(
            format=self.default_format,
            channels=channels,
            rate=samplerate,
            input=True,
            frames_per_buffer=chunksize,
        )

        self.data = [[] for i in range(channels)]

    def process_recording(self):
        # print("* recording ...")
        data = self.stream.read(self.chunksize)
        nums = np.fromstring(data, np.int16)
        for c in range(self.channels):
            self.data[c].extend(nums[c::self.channels])

    def stop_recording(self):
        print("* done recording!")
        self.stream.stop_stream()
        self.stream.close()
        self.stream = None
        self.recorded = True

    def recognize_recording(self):
        if not self.recorded:
            raise NoRecordingError("Recording was not complete/begun")
        return self._recognize(*self.data)

    def get_recorded_time(self):
        return len(self.data[0]) / self.rate

    def recognize(self, seconds=10):
        self.start_recording()
        for i in range(0, int(self.samplerate / self.chunksize
                              * int(seconds))):
            self.process_recording()
        self.stop_recording()
        return self.recognize_recording()


class NoRecordingError(Exception):
    pass
