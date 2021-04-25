from __future__ import absolute_import, print_function
import multiprocessing
import os
import traceback
import sys

from . import fingerprint
from . import decoder
from . database import get_database, Database


class Tramscore(object):

    ADVERT_ID = "advert_id"
    ADVERT_NAME = 'advert_name'
    CONFIDENCE = 'confidence'
    MATCH_TIME = 'match_time'
    OFFSET = 'offset'
    OFFSET_SECS = 'offset_seconds'
    AUDIO_LENGTH = 'audio_length'
    RELATIVE_CONFIDENCE = 'relative_confidence'
    CLIENT_USER_ID = 'client_user_id'
    MEDIA_STATION_ID = 'mediastation_id'

    def __init__(self, config):
        super(Tramscore, self).__init__()

        self.config = config

        # initialize db
        db_cls = get_database(config.get("database_type", None))

        self.db = db_cls(**config.get("database", {}))
        self.db.setup()

        # if we should limit seconds fingerprinted,
        # None|-1 means use entire track
        self.limit = self.config.get("fingerprint_limit", None)
        if self.limit == -1:  # for JSON compatibility
            self.limit = None
        self.get_fingerprinted_adverts()
        # self.get_client_fingerprinted_adverts(client_user_id)

    def get_client_fingerprinted_adverts(self, client_ID):
        # get adverts previously indexed
        self.adverts = self.db.get_client_adverts(client_ID)
        self.adverthashes_set = set()  # to know which ones we've computed before
        for advert in self.adverts:
            advert_hash = advert[Database.FIELD_FILE_SHA1]
            self.adverthashes_set.add(advert_hash)

    def get_fingerprinted_adverts(self):
        # get adverts previously indexed
        self.adverts = self.db.get_adverts()
        self.adverthashes_set = set()  # to know which ones we've computed before
        for advert in self.adverts:
            advert_hash = advert[Database.FIELD_FILE_SHA1]
            self.adverthashes_set.add(advert_hash)

    def fingerprint_directory(self, path, extensions, nprocesses=None):
        # Try to use the maximum amount of processes if not given.
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if nprocesses <= 0 else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        filenames_to_fingerprint = []
        for filename, _ in decoder.find_files(path, extensions):

            # don't refingerprint already fingerprinted files
            if decoder.unique_hash(filename) in self.adverthashes_set:
                print(("%s already fingerprinted, continuing..." % filename))
                continue

            filenames_to_fingerprint.append(filename)

        # Prepare _fingerprint_worker input
        worker_input = list(zip(filenames_to_fingerprint,
                           [self.limit] * len(filenames_to_fingerprint)))

        # Send off our tasks
        iterator = pool.imap_unordered(_fingerprint_worker,
                                       worker_input)

        # Loop till we have all of them
        while True:
            try:
                advert_name, hashes, file_hash, audio_length = next(iterator)
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed fingerprinting")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                # sid = self.db.insert_advert(advert_name, file_hash)
                sid = self.db.insert_advert(advert_name, file_hash, audio_length)

                self.db.insert_hashes(sid, hashes)
                self.db.set_advert_fingerprinted(sid)
                self.get_fingerprinted_adverts()

        pool.close()
        pool.join()

    def recognize_directory(self, path, extensions, nprocesses=None):
        # Try to use the maximum amount of processes if not given.
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if nprocesses <= 0 else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        filenames_to_recognize = []
        for filename, _ in decoder.find_files(path, extensions):
            """
            # don't refingerprint already fingerprinted files
            if decoder.unique_hash(filename) in self.adverthashes_set:
                print ("%s already recognized, continuing..." % filename)
                continue
            """
            filenames_to_recognize.append(filename)

        return filenames_to_recognize

        # Prepare _recognize_worker input
        worker_input = list(zip(filenames_to_recognize,
                                [self.limit] * len(filenames_to_recognize)))

        # Send off our tasks
        iterator = pool.imap_unordered(_recognize_worker,
                                       worker_input)

        # Loop till we have all of them
        while True:
            try:
                filename = next(iterator)
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed recognition")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                
                return filename
                # """
                # sid = self.db.insert_advert(advert_name, file_hash)
                #
                # self.db.insert_hashes(sid, hashes)
                # self.db.set_advert_fingerprinted(sid)
                # self.get_fingerprinted_adverts()
                # """
        pool.close()
        pool.join()

    def fingerprint_file(self, filepath, advert_name=None):
        advertname = decoder.path_to_advertname(filepath)
        advert_hash = decoder.unique_hash(filepath)
        advert_name = advert_name or advertname
        # don't refingerprint already fingerprinted files
        if advert_hash in self.adverthashes_set:
            print(("%s already fingerprinted, continuing..." % advert_name))
        else:
            # advert_name, hashes, file_hash = _fingerprint_worker(
            advert_name, hashes, file_hash, audio_length = _fingerprint_worker(
                filepath,
                self.limit,
                advert_name=advert_name
            )
            # sid = self.db.insert_advert(advert_name, file_hash)
            sid = self.db.insert_advert(advert_name, file_hash, audio_length)

            self.db.insert_hashes(sid, hashes)
            self.db.set_advert_fingerprinted(sid)
            self.get_fingerprinted_adverts()

    def fingerprint_client_file(self, filepath, client_user_id=None, mediastation_id=None):
        advertname = decoder.path_to_advertname(filepath)
        advert_hash = decoder.unique_hash(filepath)
        # advert_name = advert_name or advertname
        advert_name = advertname
        client_user_id = client_user_id
        mediastation_id = mediastation_id

        print(filepath)
        print("\nAdvert Name: %s" % advertname)
        print("\nClient user ID is: %s and media station ID(s): %s" % (client_user_id, mediastation_id))
        for media_station in mediastation_id:
            print("\n media station: %s " % media_station)

        # don't refingerprint already fingerprinted files
        if advert_hash in self.adverthashes_set:
            print("%s already fingerprinted, continuing..." % advert_name)
        else:
            # advert_name, hashes, file_hash = _fingerprint_worker(
            advert_name, hashes, file_hash, audio_length = _fingerprint_worker(
                filepath,
                self.limit,
                advert_name=advert_name
            )
            sid = self.db.insert_client_advert(advert_name, file_hash, audio_length, client_user_id)

            try:
                print("Trying to insert media stations for advert id: %s" % sid)
                for media_station in mediastation_id:
                    self.db.insert_client_advert_media_stations(sid, media_station)
            except:
                print("Sorry, couldn't insert media stations for Advert: %s" % advert_name)

            # print("\n..... %s \n....." % hashes)
            # hashes = _convert_hashes(hashes)
            self.db.insert_hashes(sid, hashes)

            self.db.set_advert_fingerprinted(sid)
            # self.get_fingerprinted_adverts()
            self.get_client_fingerprinted_adverts(client_user_id)
            # try:
            #     self.db.update_client_campaign(advert_name, client_user_id)
            # except Exception as err:
            #     print("failed to update client campaign status...")
            #     print(err)

    def find_matches(self, samples, Fs=fingerprint.DEFAULT_FS):
        hashes = fingerprint.fingerprint(samples, Fs=Fs)
        # return self.db.return_matches(hashes)
        mapper = {}
        total_hashes = 0
        for hash, offset in hashes:
            mapper[hash.upper()[:fingerprint.FINGERPRINT_REDUCTION]] = offset
            total_hashes += 1
        return self.db.return_matches(mapper), total_hashes

    def align_matches(self, matches, total_hashes):
        # def align_matches(self, matches):
        """
            Finds hash matches that align in time with other matches and finds
            consensus about which hashes are "true" signal from the audio.

            Returns a dictionary with match information.
        """
        # align by diffs
        diff_counter = {}
        largest = 0
        largest_count = 0
        advert_id = -1
        for tup in matches:
            sid, diff = tup
            if diff not in diff_counter:
                diff_counter[diff] = {}
            if sid not in diff_counter[diff]:
                diff_counter[diff][sid] = 0
            diff_counter[diff][sid] += 1

            if diff_counter[diff][sid] > largest_count:
                largest = diff
                largest_count = diff_counter[diff][sid]
                advert_id = sid

        # extract idenfication
        media_stations = self.db.get_client_advert_media_stations_by_id(advert_id)
        # convert query results to a list
        media_stations_list = (','.join([str(media["mediastation_id"]) for media in media_stations])).split(',')
        # print("\nMedia Stations List: %s\n" % media_stations_list)

        advert = self.db.get_client_advert_by_id(advert_id)
        if advert:
            # TODO: Clarify what `get_advert_by_id` should return.
            advertname = advert.get(Tramscore.ADVERT_NAME, None)
        else:
            return None

        # return match info
        nseconds = round(float(largest) / fingerprint.DEFAULT_FS *
                         fingerprint.DEFAULT_WINDOW_SIZE *
                         fingerprint.DEFAULT_OVERLAP_RATIO, 5)
        advert = {
            Tramscore.ADVERT_ID: advert_id,
            Tramscore.ADVERT_NAME: advertname.encode("utf8"),
            Tramscore.CONFIDENCE: largest_count,
            Tramscore.AUDIO_LENGTH: advert.get(Database.AUDIO_LENGTH, None),
            Tramscore.RELATIVE_CONFIDENCE: (largest_count * 100) / float(total_hashes),
            Tramscore.OFFSET: int(largest),
            Tramscore.OFFSET_SECS: nseconds,
            # Database.FIELD_FILE_SHA1: advert.get(Database.FIELD_FILE_SHA1, None).encode("utf8"),
            Database.FIELD_FILE_SHA1: advert.get(Database.FIELD_FILE_SHA1, None),
            Database.CLIENT_USER_ID: advert.get(Database.CLIENT_USER_ID, None),
            Database.MEDIA_STATION_ID: media_stations_list
        }
        return advert

    def recognize(self, recognizer, *options, **kwoptions):
        r = recognizer(self)
        return r.recognize(*options, **kwoptions)


def _convert_hashes(hashes=None):
    new_hashes = set()
    if hashes:
        for i in hashes:
            new_i = (i[0], int(i[1]))
            new_hashes.add(new_i)
    return new_hashes


def _fingerprint_worker(filename, limit=None, advert_name=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass

    advertname, extension = os.path.splitext(os.path.basename(filename))
    advert_name = advert_name or advertname
    # channels, Fs, file_hash = decoder.read(filename, limit)
    channels, Fs, file_hash, audio_length = decoder.read(filename, limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        print(("Fingerprinting channel %d/%d for %s" % (channeln + 1,
                                                       channel_amount,
                                                       filename)))
        hashes = fingerprint.fingerprint(channel, Fs=Fs)
        print(("Finished channel %d/%d for %s" % (channeln + 1, channel_amount,
                                                 filename)))
        result |= set(hashes)

    # return advert_name, result, file_hash
    return advert_name, result, file_hash, audio_length


def _recognize_worker(filename, limit=None, advert_name=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass
    """
    advertname, extension = os.path.splitext(os.path.basename(filename))
    advert_name = advert_name or advertname
    channels, Fs, file_hash = decoder.read(filename, limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        print("Fingerprinting channel %d/%d for %s" % (channeln + 1,
                                                       channel_amount,
                                                       filename))
        hashes = fingerprint.fingerprint(channel, Fs=Fs)
        print("Finished channel %d/%d for %s" % (channeln + 1, channel_amount,
                                                 filename))
        result |= set(hashes)

    return advert_name, result, file_hash
    """
    return filename


def chunkify(lst, n):
    """
    Splits a list into roughly n equal parts.
    http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
    """
    return [lst[i::n] for i in range(n)]
