from __future__ import absolute_import

import sys
import math
import logging

import MySQLdb as mysql
from MySQLdb.cursors import DictCursor

from .database import Database
from .fingerprint import FINGERPRINT_REDUCTION

if sys.version_info[0] != 2:
    import queue
    from itertools import zip_longest, chain
else:
    # 3.x renames
    from itertools import izip_longest as zip_longest, chain
    import Queue as queue

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(process)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class SQLDatabase(Database):
    """
    Queries:

    1) Find duplicates (shouldn't be any, though):

        select `hash`, `advert_id`, `offset`, count(*) cnt
        from fingerprints
        group by `hash`, `advert_id`, `offset`
        having cnt > 1
        order by cnt asc;

    2) Get number of hashes by advert:

        select advert_id, advert_name, count(advert_id) as num
        from fingerprints
        natural join adverts
        group by advert_id
        order by count(advert_id) desc;

    3) get hashes with highest number of collisions

        select
            hash,
            count(distinct advert_id) as n
        from fingerprints
        group by `hash`
        order by n DESC;

    => 26 different adverts with same fingerprint (392 times):

        select adverts.advert_name, fingerprints.offset
        from fingerprints natural join adverts
        where fingerprints.hash = "08d3c833b71c60a7b620322ac0c0aba7bf5a3e73";
    """

    type = "mysql"

    # tables
    FINGERPRINTS_TABLENAME = "trams_fingerprint"
    ADVERTS_TABLENAME = "trams_advert"
    ADVERTS_MEDIA_STATIONS_TABLENAME = "trams_advert_media_stations"
    MEDIA_STATION_TABLENAME = "trams_mediastation"
    CLIENT_USER_TABLENAME = "trams_clientuser"
    CAMPAIGN_TABLENAME = "trams_campaignmanager"

    ADVERTS_CONSTRAINT = "fk1_trams_advert_media_stations"
    MEDIA_STATION_CONSTRAINT = "fk2_trams_advert_media_stations"

    # fields
    FIELD_FINGERPRINTED = "fingerprinted"
    MEDIA_STATION_ID = "id"
    ID = "id"
    FIELD_CAMPAIGN_STATUS = "campaign_status"
    FIELD_CAMPAIGN_FILE_NAME = "advert_file_name"

    # creates
    CREATE_FINGERPRINTS_TABLE = """
        CREATE TABLE IF NOT EXISTS `%s` (
             `%s` binary(%s) not null,
             `%s` mediumint unsigned not null,
             `%s` int unsigned not null,
        INDEX (%s),
        UNIQUE KEY `unique_constraint` (%s, %s, %s),
        FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE
    ) ENGINE=INNODB;""" % (
        FINGERPRINTS_TABLENAME, Database.FIELD_HASH, str(math.ceil(FINGERPRINT_REDUCTION / 2.)),
        Database.FIELD_ADVERT_ID, Database.FIELD_OFFSET, Database.FIELD_HASH,
        Database.FIELD_ADVERT_ID, Database.FIELD_OFFSET, Database.FIELD_HASH,
        Database.FIELD_ADVERT_ID, ADVERTS_TABLENAME, Database.FIELD_ADVERT_ID
    )

    CREATE_ADVERTS_TABLE = """
        CREATE TABLE IF NOT EXISTS `%s` (
            `%s` mediumint unsigned not null auto_increment,
            `%s` varchar(250) not null,
            `%s` tinyint default 0,
            `%s` binary(20) not null,
            `%s` float,
            `%s` int(11) not null,
        PRIMARY KEY (`%s`),
        UNIQUE KEY `%s` (`%s`),
        FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE
    ) ENGINE=INNODB;""" % (
        ADVERTS_TABLENAME, Database.FIELD_ADVERT_ID, Database.FIELD_ADVERTNAME, FIELD_FINGERPRINTED,
        Database.FIELD_FILE_SHA1, Database.AUDIO_LENGTH, Database.CLIENT_USER_ID,
        Database.FIELD_ADVERT_ID, Database.FIELD_ADVERT_ID, Database.FIELD_ADVERT_ID,
        Database.CLIENT_USER_ID, CLIENT_USER_TABLENAME, ID
    )

    # many to many relation table
    CREATE_ADVERTS_MEDIA_STATIONS_TABLE = """
                CREATE TABLE IF NOT EXISTS `%s` (
                     `%s` mediumint unsigned not null auto_increment,
                     `%s` mediumint unsigned not null,
                     `%s` int(11) not null,
                PRIMARY KEY (`%s`),
                CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE,
                CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE
            ) ENGINE=INNODB;""" % (
        ADVERTS_MEDIA_STATIONS_TABLENAME, ID, Database.FIELD_ADVERT_ID, Database.MEDIA_STATION_ID, ID,
        ADVERTS_CONSTRAINT, Database.FIELD_ADVERT_ID, ADVERTS_TABLENAME, Database.FIELD_ADVERT_ID,
        MEDIA_STATION_CONSTRAINT, Database.MEDIA_STATION_ID, MEDIA_STATION_TABLENAME, MEDIA_STATION_ID
    )

    # inserts (ignores duplicates)
    INSERT_FINGERPRINT = """
        INSERT IGNORE INTO %s (%s, %s, %s) 
        values (UNHEX(%%s), %%s, %%s);
    """ % (
        FINGERPRINTS_TABLENAME,
        Database.FIELD_HASH,
        Database.FIELD_ADVERT_ID,
        Database.FIELD_OFFSET
    )

    # inserts advert in database
    INSERT_ADVERT = """
        INSERT INTO %s (%s, %s, %s) 
        values (%%s, UNHEX(%%s), %%s);
    """ % (
        ADVERTS_TABLENAME,
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.AUDIO_LENGTH
    )

    # inserts advert with client ID in database
    INSERT_CLIENT_ADVERT = """
        INSERT INTO %s (%s, %s, %s, %s) 
        values (%%s, UNHEX(%%s), %%s, %%s);
    """ % (
        ADVERTS_TABLENAME,
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.AUDIO_LENGTH,
        Database.CLIENT_USER_ID
    )

    # inserts advert media houses ID in database
    INSERT_CLIENT_ADVERT_MEDIA_STATIONS = """
        INSERT INTO %s (%s, %s) 
        values (%%s, %%s);
    """ % (
        ADVERTS_MEDIA_STATIONS_TABLENAME,
        Database.FIELD_ADVERT_ID,
        Database.MEDIA_STATION_ID
    )

    # selects
    SELECT = """
        SELECT %s, %s 
        FROM %s 
        WHERE %s = UNHEX(%%s);
    """ % (
        Database.FIELD_ADVERT_ID,
        Database.FIELD_OFFSET,
        FINGERPRINTS_TABLENAME,
        Database.FIELD_HASH
    )

    SELECT_MULTIPLE = """
        SELECT HEX(%s), %s, %s 
        FROM %s 
        WHERE %s IN (%%s);
    """ % (
        Database.FIELD_HASH,
        Database.FIELD_ADVERT_ID,
        Database.FIELD_OFFSET,
        FINGERPRINTS_TABLENAME,
        Database.FIELD_HASH
    )

    SELECT_ALL = """
        SELECT %s, %s 
        FROM %s;
    """ % (
        Database.FIELD_ADVERT_ID,
        Database.FIELD_OFFSET,
        FINGERPRINTS_TABLENAME
    )

    SELECT_ADVERT = """
        SELECT %s, HEX(%s) as %s, %s 
        FROM %s 
        WHERE %s = %%s;
    """ % (
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.FIELD_FILE_SHA1,
        Database.AUDIO_LENGTH,
        ADVERTS_TABLENAME,
        Database.FIELD_ADVERT_ID
    )

    SELECT_CLIENT_ADVERT = """
        SELECT %s, HEX(%s) as %s, %s, %s 
        FROM %s 
        WHERE %s = %%s;
        """ % (
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.FIELD_FILE_SHA1,
        Database.CLIENT_USER_ID,
        Database.AUDIO_LENGTH,
        ADVERTS_TABLENAME,
        Database.FIELD_ADVERT_ID
    )

    # SELECT_CLIENT_ADVERT = """
    #         SELECT %s, HEX(%s) as %s, %s, %s, %s.%s FROM %s, %s WHERE %s.%s = %%s;
    #         """ % (Database.FIELD_ADVERTNAME, Database.FIELD_FILE_SHA1, Database.FIELD_FILE_SHA1,
    #                Database.CLIENT_USER_ID, Database.AUDIO_LENGTH, ADVERTS_MEDIA_STATIONS_TABLENAME,
    #                Database.MEDIA_STATION_ID, ADVERTS_TABLENAME, ADVERTS_MEDIA_STATIONS_TABLENAME,
    #                ADVERTS_TABLENAME, Database.FIELD_ADVERT_ID)

    SELECT_CLIENT_ADVERT_MEDIA_STATIONS = """
        SELECT %s 
        FROM %s 
        WHERE %s = %%s;
        """ % (
        Database.MEDIA_STATION_ID,
        ADVERTS_MEDIA_STATIONS_TABLENAME,
        Database.FIELD_ADVERT_ID
    )

    SELECT_ADVERTS = """
        SELECT %s, %s, HEX(%s) as %s 
        FROM %s 
        WHERE %s = 1;
    """ % (
        Database.FIELD_ADVERT_ID,
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.FIELD_FILE_SHA1,
        ADVERTS_TABLENAME,
        FIELD_FINGERPRINTED
    )

    SELECT_CLIENT_ADVERTS = """
        SELECT %s, %s, HEX(%s) as %s, %s 
        FROM %s 
        WHERE %s = 1 AND %s = %%s;
    """ % (
        Database.FIELD_ADVERT_ID,
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.FIELD_FILE_SHA1,
        Database.CLIENT_USER_ID,
        ADVERTS_TABLENAME,
        FIELD_FINGERPRINTED,
        Database.CLIENT_USER_ID
    )

    SELECT_NUM_FINGERPRINTS = """
        SELECT COUNT(*) as n 
        FROM %s;
    """ % (
        FINGERPRINTS_TABLENAME
    )

    SELECT_UNIQUE_ADVERT_IDS = """
        SELECT COUNT(DISTINCT %s) as n 
        FROM %s 
        WHERE %s = 1;
    """ % (
        Database.FIELD_ADVERT_ID,
        ADVERTS_TABLENAME,
        FIELD_FINGERPRINTED
    )

    # Updates a fingerprinted advert
    # WHERE %s LIKE %s AND %s = %%s
    UPDATE_CLIENT_CAMPAIGN = """
        UPDATE %s
        SET %s = 1
        WHERE %s LIKE %s AND %s = %%s;
    """ % (
            CAMPAIGN_TABLENAME,
            FIELD_CAMPAIGN_STATUS,
            FIELD_CAMPAIGN_FILE_NAME,
            Database.FIELD_ADVERTNAME,
            Database.CLIENT_USER_ID
    )

    # update
    UPDATE_ADVERT_FINGERPRINTED = """
        UPDATE %s 
        SET %s = 1 
        WHERE %s = %%s;
    """ % (
        ADVERTS_TABLENAME,
        FIELD_FINGERPRINTED,
        Database.FIELD_ADVERT_ID
    )

    # drops
    DROP_ADVERTS_MEDIA_STATIONS = """DROP TABLE IF EXISTS %s;""" % ADVERTS_MEDIA_STATIONS_TABLENAME
    DROP_FINGERPRINTS = """DROP TABLE IF EXISTS %s;""" % FINGERPRINTS_TABLENAME
    DROP_ADVERTS = """DROP TABLE IF EXISTS %s;""" % ADVERTS_TABLENAME

    # delete
    # DELETE_UNFINGERPRINTED = """
    #     DELETE FROM %s, %s WHERE %s = 0 OR %s = %%s;
    # """ % (FINGERPRINTS_TABLE, ADVERTS_TABLENAME, FIELD_FINGERPRINTED, Database.FIELD_ADVERT_ID)

    # delete
    DELETE_UNFINGERPRINTED = """
        DELETE FROM %s WHERE %s = 0;
    """ % (ADVERTS_TABLENAME, FIELD_FINGERPRINTED)

    def __init__(self, **options):
        super(SQLDatabase, self).__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self):
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

    def setup(self):
        """
        Creates any non-existing tables required for tramscore to function.

        This also removes all adverts that have been added but have no
        fingerprints associated with them.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.CREATE_ADVERTS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            cur.execute(self.CREATE_ADVERTS_MEDIA_STATIONS_TABLE)
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def empty(self):
        """
        Drops tables created by tramscore and then creates them again
        by calling `SQLDatabase.setup`.

        .. warning:
            This will result in a loss of data
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.DROP_ADVERTS_MEDIA_STATIONS_TABLE)
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_ADVERTS)
        self.setup()

    def delete_unfingerprinted_adverts(self):
        """
        Removes all adverts that have no fingerprints associated with them.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_adverts(self):
        """
        Returns number of adverts the database has fingerprinted.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.SELECT_UNIQUE_ADVERT_IDS)

            for count, in cur:
                return count
            return 0

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints the database has fingerprinted.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)

            for count, in cur:
                return count
            return 0

    def set_advert_fingerprinted(self, sid):
        """
        Set the fingerprinted flag to TRUE (1) once a song has been completely
        fingerprinted in the database.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.UPDATE_ADVERT_FINGERPRINTED, (sid,))

    def get_adverts(self):
        """
        Return adverts that have the fingerprinted flag set TRUE (1).
        """
        with self.cursor(cursor_type=DictCursor, charset="utf8") as cur:
            cur.execute(self.SELECT_ADVERTS)
            for row in cur:
                yield row

    def get_advert_by_id(self, sid):
        """
        Returns advert by its ID.
        """
        with self.cursor(cursor_type=DictCursor, charset="utf8") as cur:
            cur.execute(self.SELECT_ADVERT, (sid,))
            return cur.fetchone()

    def insert(self, hash, sid, offset):
        """
        Insert a (sha1, advert_id, offset) row into database.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.INSERT_FINGERPRINT, (hash, sid, offset))

    # def insert_advert(self, advertname, file_hash):
    def insert_advert(self, advertname, file_hash, audio_length):
        """
        Inserts advert in the database and returns the ID of the inserted record.
        """
        with self.cursor(charset="utf8") as cur:
            # cur.execute(self.INSERT_ADVERT, (advertname, file_hash))
            cur.execute(self.INSERT_ADVERT, (advertname, file_hash, audio_length))
            return cur.lastrowid

    def query(self, hash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        # select all if no key
        query = self.SELECT_ALL if hash is None else self.SELECT

        with self.cursor(charset="utf8") as cur:
            cur.execute(query)
            for sid, offset in cur:
                yield (sid, offset)

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => advert_id, offset
        values into the database.
        """
        values = []
        for hash, offset in hashes:
            values.append((hash, sid, offset))

        base_query = "INSERT IGNORE INTO %s (%s, %s, %s) values " % \
                     (self.FINGERPRINTS_TABLENAME, Database.FIELD_HASH, Database.FIELD_ADVERT_ID, Database.FIELD_OFFSET)
        with self.cursor(charset="utf8") as cur:
            values.sort(key=lambda tup: tup[0])
            cur.execute("START TRANSACTION;")
            for split_values in grouper(values, 1000):
                cur.executemany(self.INSERT_FINGERPRINT, split_values)
                values2tuple = tuple(chain.from_iterable(split_values))
                query = base_query + ', '.join(['(UNHEX(%s), %s, %s)'] * len(split_values))
                query += ";"
                cur.execute(query, values2tuple)
            cur.execute("COMMIT;")

    def return_matches(self, mapper):
        """
        Return the (advert_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        # Create a dictionary of hash => offset pairs for later lookups
        # mapper = {}
        # for hash, offset in hashes:
        #     mapper[hash.upper()] = offset

        # Get an iteratable of all the hashes we need
        # values = list(mapper.keys())
        vals = list(mapper.keys())

        with self.cursor(charset="utf8") as cur:
            # for split_values in grouper(values, 1000):
            #     vals = list(split_values)
            # Create our IN part of the query
            query = self.SELECT_MULTIPLE
            query = query % ', '.join(['UNHEX(%s)'] * len(vals))

            cur.execute(query, vals)

            for hash, sid, offset in cur:
                # (sid, db_offset - advert_sampled_offset)
                yield (sid, offset - mapper[hash])

    def insert_client_advert(self, advertname, file_hash, audio_length, client_user_id):
        """
        Inserts advert in the database and returns the ID of the inserted record.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.INSERT_CLIENT_ADVERT, (advertname, file_hash, audio_length, client_user_id))
            return cur.lastrowid

    def insert_client_advert_media_stations(self, advert_id, mediastation_id):
        """
        Inserts media_stations for advert in the database and returns the ID of the inserted record.
        """
        with self.cursor(charset="utf8") as cur:
            cur.execute(self.INSERT_CLIENT_ADVERT_MEDIA_STATIONS,
                        (advert_id, mediastation_id))
            # return cur.lastrowid

    def get_client_adverts(self, client_user_id):
        """
        Return adverts that have the fingerprinted flag set TRUE (1) and client_user_id matching with client id.
        """
        with self.cursor(cursor_type=DictCursor, charset="utf8") as cur:
            cur.execute(self.SELECT_CLIENT_ADVERTS, (client_user_id,))
            for row in cur:
                yield row

    def get_client_advert_by_id(self, sid):
        """
        Returns advert by its ID.
        """
        with self.cursor(cursor_type=DictCursor, charset="utf8") as cur:
            cur.execute(self.SELECT_CLIENT_ADVERT, (sid,))
            return cur.fetchone()

    def get_client_advert_media_stations_by_id(self, sid):
        """
        Returns advert by its ID.
        """
        with self.cursor(cursor_type=DictCursor, charset="utf8") as cur:
            cur.execute(self.SELECT_CLIENT_ADVERT_MEDIA_STATIONS, (sid,))
            return cur.fetchall()
            # return cur.fetchone()

    def update_client_campaign(self, advert_name, client_user_id):
        """
        Returns advert media_stations by their IDs.
        """
        with self.cursor(cursor_type=DictCursor, charset="utf8") as cur:
            cur.execute(self.UPDATE_CLIENT_CAMPAIGN, (advert_name, client_user_id))

    def __getstate__(self):
        return self._options,

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return ([_f for _f in values if _f] for values
            in zip_longest(fillvalue=fillvalue, *args))


def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)
    return cursor


class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.


    ```python
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
    ```
    """
    # _cache = queue.Queue(maxsize=5)

    def __init__(self, cursor_type=mysql.cursors.Cursor, **options):
        super(Cursor, self).__init__()

        self._cache = queue.Queue(maxsize=5)

        try:
            self.temp_conn = self._cache.get_nowait()
            # self._cache.task_done()

            logger.info("MYSQL: setting connection to get items from cache...")

        # except (queue.Empty, mysql.MySQLError) as err:
        except:
            self.temp_conn = mysql.connect(**options)
            logger.error("MYSQL queue empty error: trying to establish new connection...")
            # logger.error("MYSQL queue empty error: trying to establish new connection...CAUSE: %s" % err)
        # else:
        #     # Ping the connection before using it from the cache.
        #     self.temp_conn.ping(True)

        self.conn = self.temp_conn
        self.conn.autocommit(False)
        self.cursor_type = cursor_type

    @classmethod
    def clear_cache(cls):
        cls._cache = queue.Queue(maxsize=5)
        # cls.__class__._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor(self.cursor_type)
        logger.info("MYSQL: running query...")
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a MySQL related error we try to rollback the cursor.
        if extype is mysql.MySQLError:
            self.cursor.rollback()
            logger.error("MYSQL ERROR: trying to rollback...")

        # if exvalue:
        #     logger.info("MYSQL CURSOR EXIT: Exvalue: %s ..." % exvalue)
        # if traceback:
        #     logger.info("MYSQL CURSOR EXIT: Traceback: %s ..." % traceback)

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
            # self.__class__._cache.put_nowait(self.conn)
            self._cache.task_done()
            logger.info("MYSQL Cache: trying to put items back into queue...")
        # except (queue.Full, mysql.MySQLError) as err:
        except:
            self.conn.close()
            logger.error("MYSQL ERROR: trying to close connection...")
            # logger.error("MYSQL ERROR: trying to close connection...CAUSE: %s" % err)

        # added for test
        else:
            if self.conn:
                self.conn.close()
                logger.info("MYSQL Connection: Closing connection...")
