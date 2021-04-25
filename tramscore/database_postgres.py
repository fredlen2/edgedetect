""" Class for interacting with Postgres database.
"""
from __future__ import absolute_import
# from itertools import zip_longest, chain
# import queue
import sys
import math
import binascii

if sys.version_info[0] != 2:
    import queue
    from itertools import zip_longest, chain
else:
    # 3.x renames
    from itertools import izip_longest as zip_longest, chain
    import Queue as queue

try:
    import psycopg2
except ImportError as err:
    print("Module not installed", err)
    sys.exit(1)

from psycopg2.extras import DictCursor, RealDictCursor
from .database import Database
from .fingerprint import FINGERPRINT_REDUCTION


class PostgresDatabase(Database):
    """ Class to interact with Postgres databases.
    The queries should be self evident, but they are documented in the event
    that they aren't :)
    """

    type = "postgresql"

    # The number of hashes to insert at a time
    NUM_HASHES = 10000

    # Tables
    FINGERPRINTS_TABLENAME = "trams_fingerprint"
    ADVERTS_TABLENAME = "trams_advert"

    ADVERTS_MEDIA_STATIONS_TABLENAME = "trams_advert_media_stations"
    MEDIA_STATION_TABLENAME = "trams_mediastation"
    CLIENT_USER_TABLENAME = "trams_clientuser"
    CAMPAIGN_TABLENAME = "trams_campaignmanager"

    ADVERTS_CONSTRAINT = "fk1_trams_advert_media_stations"
    MEDIA_STATION_CONSTRAINT = "fk2_trams_advert_media_stations"

    # fields
    MEDIA_STATION_ID = "id"
    ID = "id"

    # Schema
    DEFAULT_SCHEMA = 'public'

    # Fields
    FIELD_HASH = 'hash'
    FIELD_ADVERT_ID = 'advert_id'
    FIELD_OFFSET = 'time_offset'
    FIELD_ADVERTNAME = 'advert_name'
    FIELD_FINGERPRINTED = "fingerprinted"
    FIELD_CAMPAIGN_STATUS = "campaign_status"
    FIELD_CAMPAIGN_FILE_NAME = "advert_file_name"

    # int NOT NULL REFERENCES %s(%s) ON DELETE CASCADE ON UPDATE CASCADE
    # creates postgres table if it doesn't exist
    CREATE_FINGERPRINTS_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s bytea NOT NULL,
            %s uuid NOT NULL REFERENCES %s(%s) ON DELETE CASCADE ON UPDATE CASCADE,
            %s bigint NOT NULL,
            CONSTRAINT comp_key UNIQUE (%s, %s, %s)
        );""" % (
                FINGERPRINTS_TABLENAME,
                FIELD_HASH,         # actual fingerprint itself
                FIELD_ADVERT_ID,    # advert id (fkey to adverts tables)
                ADVERTS_TABLENAME,  # Adverts table
                FIELD_ADVERT_ID,    # foreign key
                FIELD_OFFSET,       # offset relative to START of advert
                FIELD_HASH,         # unique constraint
                FIELD_ADVERT_ID,    # unique constraint
                FIELD_OFFSET        # unique constraint

            )

    # Creates an index on fingerprint itself for webscale
    CREATE_FINGERPRINT_INDEX = """
        DO $$
            BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM   pg_class c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE  c.relname = 'fingerprint_index'
                AND    n.nspname = '%s'
            ) THEN
            CREATE INDEX fingerprint_index ON %s.%s (%s);
            END IF;
        END$$;
        """ % (
            DEFAULT_SCHEMA,
            DEFAULT_SCHEMA,
            FINGERPRINTS_TABLENAME,
            FIELD_HASH
        )

    # many to many relations table
    # serial
    CREATE_ADVERTS_MEDIA_STATIONS_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s uuid DEFAULT uuid_generate_v4() NOT NULL,
            %s uuid not null REFERENCES %s(%s) ON DELETE CASCADE ON UPDATE CASCADE,
            %s int not null REFERENCES %s(%s) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (%s)
        );""" % (
            ADVERTS_MEDIA_STATIONS_TABLENAME,
            ID,
            Database.FIELD_ADVERT_ID,
            ADVERTS_TABLENAME,
            Database.FIELD_ADVERT_ID,
            Database.MEDIA_STATION_ID,
            MEDIA_STATION_TABLENAME,
            MEDIA_STATION_ID,
            ID
    )

    # CONSTRAINT con_client FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE
    # serial
    # Creates the table that stores advert information.
    CREATE_ADVERTS_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s uuid DEFAULT uuid_generate_v4() NOT NULL,
            %s varchar(250) NOT NULL,
            %s boolean default FALSE,
            %s bytea not null,
            %s real,
            %s int not null REFERENCES %s(%s) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (%s),
            CONSTRAINT uni_que UNIQUE (%s) 
        );""" % (
                ADVERTS_TABLENAME,
                FIELD_ADVERT_ID,  # uuid
                FIELD_ADVERTNAME,  # advertname when we fingerprinted it
                FIELD_FINGERPRINTED,  # whether we processed the advert
                Database.FIELD_FILE_SHA1,   # advert file hash
                Database.AUDIO_LENGTH,     # stores original audio length
                Database.CLIENT_USER_ID,  # clientuser id
                CLIENT_USER_TABLENAME,  # clientuser table
                ID,                 # clientuser id column
                FIELD_ADVERT_ID,  # pkey on advertid
                FIELD_ADVERT_ID  # unique key on advert_id
            )

    # UNHEX(%s)        # decode(%%s, 'hex') convert_from(decode('%s', 'hex'), 'utf8')
    # Inserts (ignores duplicates)
    INSERT_FINGERPRINT = """
        INSERT INTO %s (%s, %s, %s)
        values (fred_unhex_bytea(%%s), %%s, %%s) ON CONFLICT DO NOTHING;
        """ % (
            FINGERPRINTS_TABLENAME,
            FIELD_HASH,
            FIELD_ADVERT_ID,
            FIELD_OFFSET
        )

    # Inserts advert information.
    INSERT_ADVERT = """
        INSERT INTO %s (%s, %s, %s)
        values (%%s, fred_unhex_bytea(%%s), %%s)
        RETURNING %s;
        """ % (
            ADVERTS_TABLENAME,
            FIELD_ADVERTNAME,
            Database.FIELD_FILE_SHA1,
            Database.AUDIO_LENGTH,
            FIELD_ADVERT_ID
        )

    # UNHEX(%s) as %s
    # inserts advert with client ID in database
    INSERT_CLIENT_ADVERT = """
        INSERT INTO %s (%s, %s, %s, %s) 
        values (%%s, fred_unhex_bytea(%%s), %%s, %%s)
        RETURNING %s;
        """ % (
            ADVERTS_TABLENAME,
            Database.FIELD_ADVERTNAME,
            Database.FIELD_FILE_SHA1,
            Database.AUDIO_LENGTH,
            Database.CLIENT_USER_ID,
            FIELD_ADVERT_ID
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

    # Select a single advert given a hex value.
    # # WHERE %s = decode(%%s, 'hex');
    SELECT = """
        SELECT %s, %s
        FROM %s
        WHERE %s = fred_unhex_bytea(%%s);
        """ % (
            FIELD_ADVERT_ID,
            FIELD_OFFSET,
            FINGERPRINTS_TABLENAME,
            FIELD_HASH
        )

    # Selects multiple fingerprints based on hashes
    SELECT_MULTIPLE = """
        SELECT fred_hex(%s) as %s, %s, %s
        FROM %s
        WHERE %s IN %%s;
        """ % (
            FIELD_HASH,
            FIELD_HASH,
            FIELD_ADVERT_ID,
            FIELD_OFFSET,
            FINGERPRINTS_TABLENAME,
            FIELD_HASH
        )

    # Selects all adverts/fingerprints from the fingerprints table.
    SELECT_ALL = """
        SELECT %s, %s
        FROM %s;
        """ % (
            FIELD_ADVERT_ID,
            FIELD_OFFSET,
            FINGERPRINTS_TABLENAME
        )

    # Selects a given advert.
    SELECT_ADVERT = """
        SELECT %s, fred_hex(%s) as %s, %s
        FROM %s
        WHERE %s = %%s
        """ % (
            FIELD_ADVERTNAME,
            Database.FIELD_FILE_SHA1,
            Database.FIELD_FILE_SHA1,
            Database.AUDIO_LENGTH,
            ADVERTS_TABLENAME,
            FIELD_ADVERT_ID
        )

    SELECT_CLIENT_ADVERT = """
        SELECT %s, fred_hex(%s) as %s, %s, %s 
        FROM %s WHERE %s = %%s;
        """ % (
        Database.FIELD_ADVERTNAME,
        Database.FIELD_FILE_SHA1,
        Database.FIELD_FILE_SHA1,
        Database.CLIENT_USER_ID,
        Database.AUDIO_LENGTH,
        ADVERTS_TABLENAME,
        Database.FIELD_ADVERT_ID
    )

    SELECT_CLIENT_ADVERT_MEDIA_STATIONS = """
        SELECT %s 
        FROM %s 
        WHERE %s = %%s;
        """ % (
        Database.MEDIA_STATION_ID,
        ADVERTS_MEDIA_STATIONS_TABLENAME,
        Database.FIELD_ADVERT_ID
    )

    # Selects all FINGERPRINTED adverts.
    SELECT_ADVERTS = """
        SELECT %s, %s, fred_hex(%s) as %s
        FROM %s WHERE %s = True;
        """ % (
            FIELD_ADVERT_ID,
            FIELD_ADVERTNAME,
            Database.FIELD_FILE_SHA1,
            Database.FIELD_FILE_SHA1,
            ADVERTS_TABLENAME,
            FIELD_FINGERPRINTED
    )

    SELECT_CLIENT_ADVERTS = """
        SELECT %s, %s, %s, fred_hex(%s) as %s 
        FROM %s 
        WHERE %s = True AND %s = %%s;
            """ % (
        Database.FIELD_ADVERT_ID,
        Database.FIELD_ADVERTNAME,
        Database.CLIENT_USER_ID,
        Database.FIELD_FILE_SHA1,
        Database.FIELD_FILE_SHA1,
        ADVERTS_TABLENAME,
        FIELD_FINGERPRINTED,
        Database.CLIENT_USER_ID
    )

    # Returns the number of fingerprints
    SELECT_NUM_FINGERPRINTS = """
        SELECT COUNT(*) as n
        FROM %s
        """ % (
            FINGERPRINTS_TABLENAME
        )

    # Selects unique advert ids
    SELECT_UNIQUE_ADVERT_IDS = """
        SELECT COUNT(DISTINCT %s) as n
        FROM %s
        WHERE %s = True;
        """ % (
            FIELD_ADVERT_ID,
            ADVERTS_TABLENAME,
            FIELD_FINGERPRINTED
        )

    # Drops the adverts_media_stations table (removes EVERYTHING!)
    DROP_ADVERTS_MEDIA_STATIONS = """
        DROP TABLE IF EXISTS %s;
        """ % (
        ADVERTS_MEDIA_STATIONS_TABLENAME
    )

    # Drops the fingerprints table (removes EVERYTHING!)
    DROP_FINGERPRINTS = """
        DROP TABLE IF EXISTS %s;""" % (
            FINGERPRINTS_TABLENAME
        )

    # Drops the adverts table (removes EVERYTHING!)
    DROP_ADVERTS = """
        DROP TABLE IF EXISTS %s;
        """ % (
            ADVERTS_TABLENAME
        )

    # Updates a fingerprinted advert
    UPDATE_ADVERT_FINGERPRINTED = """
        UPDATE %s
        SET %s = True
        WHERE %s = %%s
        """ % (
            ADVERTS_TABLENAME,
            FIELD_FINGERPRINTED,
            FIELD_ADVERT_ID
        )

    # Updates a fingerprinted advert
    UPDATE_CLIENT_CAMPAIGN = """
            UPDATE %s
            SET %s = True
            WHERE %s LIKE %s AND %s = %%s
            """ % (
        CAMPAIGN_TABLENAME,
        FIELD_CAMPAIGN_STATUS,
        FIELD_CAMPAIGN_FILE_NAME,
        Database.FIELD_ADVERTNAME,
        Database.CLIENT_USER_ID
    )

    # Deletes all unfingerprinted adverts.
    DELETE_UNFINGERPRINTED = """
        DELETE
        FROM %s
        WHERE %s = False;
        """ % (
            ADVERTS_TABLENAME,
            FIELD_FINGERPRINTED
        )

    def __init__(self, **options):
        """ Creates the DB layout, creates connection, etc.
        """
        super(PostgresDatabase, self).__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self):
        """
        Clear the cursor cache, we don't want any stale connections from
        the previous process.
        """
        Cursor.clear_cache()

    def setup(self):
        """
        Creates any non-existing tables required for tramscore to function.
        This also removes all adverts that have been added but have no
        fingerprints associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.CREATE_ADVERTS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            cur.execute(self.CREATE_FINGERPRINT_INDEX)
            cur.execute(self.CREATE_ADVERTS_MEDIA_STATIONS_TABLE)
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def empty(self):
        """
        Drops tables created by tramscore and then creates them again
        by calling `PostgresDatabase.setup`.
        This will result in a loss of data, so this might not
        be what you want.
        """
        with self.cursor() as cur:
            cur.execute(self.DROP_ADVERTS_MEDIA_STATIONS_TABLE)
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_ADVERTS)
        self.setup()

    def delete_unfingerprinted_adverts(self):
        """
        Removes all adverts that have no fingerprints associated with them.
        This might not be applicable either.
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_adverts(self):
        """
        Returns number of adverts the database has fingerprinted.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_UNIQUE_ADVERT_IDS)
            for count, in cur:
                return count
            return 0

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints present.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)

            for count, in cur:
                return count
            return 0

    def set_advert_fingerprinted(self, sid):
        """
        Toggles fingerprinted flag to TRUE once an advert has been completely
        fingerprinted in the database.
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_ADVERT_FINGERPRINTED, (sid,))

    def get_adverts(self):
        """
        Generator to return adverts that have the fingerprinted
        flag set TRUE, i.e, they are completely processed.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_ADVERTS)
            for row in cur:
                yield row

    def get_advert_by_id(self, sid):
        """
        Returns advert by its ID.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_ADVERT, (sid,))
            return cur.fetchone()

    def insert(self, bhash, sid, offset):
        """
        Insert a (sha1, advert_id, offset) row into database.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, bhash, sid, offset)

    # def insert_advert(self, advertname):
    def insert_advert(self, advertname, file_hash, audio_length):
        """
        Inserts advert in the database and returns the ID of the inserted record.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_ADVERT, (advertname, file_hash, audio_length, ))
            return cur.fetchone()[0]
            # cur.execute(self.INSERT_ADVERT, (advertname, ))
            # return cur.lastrowid

    def query(self, bhash):
        """
        Return all tuples associated with hash.
        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        query = self.SELECT
        if not bhash:
            query = self.SELECT_ALL

        with self.cursor() as cur:
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
        for bhash, offset in hashes:
            values.append((bhash, sid, offset))

        # base_query = "INSERT IGNORE INTO %s (%s, %s, %s) values " % \
        base_query = "INSERT INTO %s (%s, %s, %s) values " % \
                     (self.FINGERPRINTS_TABLENAME, Database.FIELD_HASH, Database.FIELD_ADVERT_ID, Database.FIELD_OFFSET)
        with self.cursor() as cur:
            values.sort(key=lambda tup: tup[0])
            cur.execute("START TRANSACTION;")
            for split_values in grouper(values, 1000):

                # print("\n....***** %s ******...\n\n" % split_values)

                cur.executemany(self.INSERT_FINGERPRINT, split_values)
                values2tuple = tuple(chain.from_iterable(split_values))
                query = base_query + ', '.join(["(fred_unhex_bytea(%s), %s, %s)"] * len(split_values))
                # query = base_query + ', '.join(["UNHEX(%s), %s, %s) ON CONFLICT DO NOTHING"] * len(split_values))
                query += " ON CONFLICT DO NOTHING;"

                # print("\n.... %s ....\n" % query)

                cur.execute(query, values2tuple)
            cur.execute("COMMIT;")

    def return_matches(self, mapper):
        """
        Return the (advert_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values as a generator.
        """
        # Get an iteratable of all the hashes we need
        vals = list(mapper.keys())

        with self.cursor() as cur:
            # Create our IN part of the query
            query = self.SELECT_MULTIPLE
            # query = query % ', '.join(["decode(%s, 'hex')"] * len(vals))
            query = query % ', '.join(['(fred_unhex_bytea(%s)'] * len(vals))

            cur.execute(query, vals)

            for bhash, sid, offset in cur:
                bhash = binascii.hexlify(bhash).upper()
                yield (sid, offset - mapper[bhash])

    def insert_client_advert(self, advertname, file_hash, audio_length, client_user_id):
        """
        Inserts advert in the database and returns the ID of the inserted record.
        """
        print("\n......\n  %s  \n.......\n" % advertname)
        print("\n......\n  %s  \n.......\n" % file_hash)
        print("\n......\n  %s  \n.......\n" % audio_length)
        print("\n......\n  %s  \n.......\n" % client_user_id)
        with self.cursor() as cur:
            cur.execute(self.INSERT_CLIENT_ADVERT, (advertname, file_hash, audio_length, client_user_id))
            return cur.fetchone()[0]
            # return cur.lastrowid

    def insert_client_advert_media_stations(self, advert_id, mediastation_id):
        """
        Inserts media_stations for advert in the database
        returns the ID of the inserted record.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_CLIENT_ADVERT_MEDIA_STATIONS,
                        (advert_id, mediastation_id))
            # return cur.lastrowid

    def get_client_adverts(self, client_user_id):
        """
        Return adverts that have the fingerprinted flag set TRUE (1) and client_user_id matching with client id.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_CLIENT_ADVERTS, (client_user_id,))
            for row in cur:
                yield row

    def get_client_advert_by_id(self, sid):
        """
        Returns advert by its ID.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_CLIENT_ADVERT, (sid,))
            return cur.fetchone()

    def get_client_advert_media_stations_by_id(self, sid):
        """
        Returns advert media_stations by their IDs.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_CLIENT_ADVERT_MEDIA_STATIONS, (sid,))
            return cur.fetchall()
            # return cur.fetchone()

    def update_client_campaign(self, advert_name, client_user_id):
        """
        Returns advert media_stations by their IDs.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.UPDATE_CLIENT_CAMPAIGN, (advert_name, client_user_id,))

    def __getstate__(self):
        return self._options,

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)


def grouper(iterable, num, fillvalue=None):
    """ Groups values.
    """
    args = [iter(iterable)] * num
    return ([_f for _f in values if _f] for values
            in zip_longest(fillvalue=fillvalue, *args))


def cursor_factory(**factory_options):
    """ Initializes the cursor, ex passes hostname, port,
    etc.
    """
    def cursor(**options):
        """ Builds a cursor.
        """
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

    def __init__(self, cursor_type=DictCursor, **options):
        super(Cursor, self).__init__()

        self._cache = queue.Queue(maxsize=5)

        try:
            conn = self._cache.get_nowait()
        except queue.Empty:
            conn = psycopg2.connect(**options)
        else:
            # Ping the connection before using it from the cache.
            conn.cursor().execute('SELECT 1')

        self.conn = conn
        # self.conn.autocommit(False)
        self.cursor_type = cursor_type

    @classmethod
    def clear_cache(cls):
        """ Clears the cache.
        """
        cls._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor(cursor_factory=self.cursor_type)
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a Postgres related error we try to rollback the cursor.
        if extype in [psycopg2.DatabaseError, psycopg2.InternalError]:
            self.conn.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except queue.Full:
            self.conn.close()
