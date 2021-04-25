
import abc


class Database(object, metaclass=abc.ABCMeta):
    FIELD_FILE_SHA1 = 'file_sha1'
    FIELD_ADVERT_ID = 'advert_id'
    FIELD_ADVERTNAME = 'advert_name'
    FIELD_OFFSET = 'time_offset'
    FIELD_HASH = 'hash'
    AUDIO_LENGTH = 'audio_length'
    CLIENT_USER_ID = 'client_user_id'
    # CLIENT_ID = 0
    MEDIA_STATION_ID = 'mediastation_id'

    # Name of your Database subclass, this is used in configuration
    # to refer to your class
    type = None

    def __init__(self):
        super(Database, self).__init__()

    def before_fork(self):
        """
        Called before the database instance is given to the new process
        """
        pass

    def after_fork(self):
        """
        Called after the database instance has been given to the new process

        This will be called in the new process.
        """
        pass

    def setup(self):
        """
        Called on creation or shortly afterwards.
        """
        pass

    @abc.abstractmethod
    def empty(self):
        """
        Called when the database should be cleared of all data.
        """
        pass

    @abc.abstractmethod
    def delete_unfingerprinted_adverts(self):
        """
        Called to remove any advert entries that do not have any fingerprints
        associated with them.
        """
        pass

    @abc.abstractmethod
    def get_num_adverts(self):
        """
        Returns the amount of adverts in the database.
        """
        pass

    @abc.abstractmethod
    def get_num_fingerprints(self):
        """
        Returns the number of fingerprints in the database.
        """
        pass

    @abc.abstractmethod
    def set_advert_fingerprinted(self, sid):
        """
        Sets a specific advert as having all fingerprints in the database.

        sid: Advert identifier
        """
        pass

    @abc.abstractmethod
    def get_adverts(self):
        """
        Returns all fully fingerprinted adverts in the database.
        """
        pass

    @abc.abstractmethod
    def get_advert_by_id(self, sid):
        """
        Return a advert by its identifier

        sid: Advert identifier
        """
        pass

    @abc.abstractmethod
    def insert(self, hash, sid, offset):
        """
        Inserts a single fingerprint into the database.

          hash: Part of a sha1 hash, in hexadecimal format
           sid: Advert identifier this fingerprint is off
        offset: The offset this hash is from
        """
        pass

    @abc.abstractmethod
    def insert_advert(self, advert_name):
        """
        Inserts a advert name into the database, returns the new
        identifier of the advert.

        advert_name: The name of the advert.
        """
        pass

    @abc.abstractmethod
    def query(self, hash):
        """
        Returns all matching fingerprint entries associated with
        the given hash as parameter.

        hash: Part of a sha1 hash, in hexadecimal format
        """
        pass

    @abc.abstractmethod
    def get_iterable_kv_pairs(self):
        """
        Returns all fingerprints in the database.
        """
        pass

    @abc.abstractmethod
    def insert_hashes(self, sid, hashes):
        """
        Insert a multitude of fingerprints.

           sid: Advert identifier the fingerprints belong to
        hashes: A sequence of tuples in the format (hash, offset)
        -   hash: Part of a sha1 hash, in hexadecimal format
        - offset: Offset this hash was created from/at.
        """
        pass

    @abc.abstractmethod
    def return_matches(self, hashes):
        """
        Searches the database for pairs of (hash, offset) values.

        hashes: A sequence of tuples in the format (hash, offset)
        -   hash: Part of a sha1 hash, in hexadecimal format
        - offset: Offset this hash was created from/at.

        Returns a sequence of (sid, offset_difference) tuples.

                      sid: Advert identifier
        offset_difference: (offset - database_offset)
        """
        pass

    @abc.abstractmethod
    def insert_client_advert(self, advert_name, client_user_id):
        """
        Inserts client advert name into the database, returns the new
        identifier of the advert.

        advert_name: The name of the advert.
        client_user_id: The id of the current user using tramsweb.
        """
        pass

    @abc.abstractmethod
    def insert_client_advert_media_stations(self, advert_id, mediastation_id):
        """
        Inserts client selected media stations for advert name into the database,
        returns the new identifier of the advert.

        advert_id: The id of the advert.
        mediastation_id: The id of the media stations selected by tramsweb user.
        """
        pass

    @abc.abstractmethod
    def get_client_adverts(self):
        """
        Returns all fully fingerprinted adverts in the database.
        """
        pass

    @abc.abstractmethod
    def get_client_advert_by_id(self, sid):
        """
        Return client advert by its identifier

        sid: Advert identifier
        """
        pass

    @abc.abstractmethod
    def get_client_advert_media_stations_by_id(self, sid):
        """
        Return client advert media stations by its identifier

        sid: Advert identifier
        """
        pass

    @abc.abstractmethod
    def update_client_campaign(self, advert_name, client_user_id):
        """
        Update client campaign status to True after successfully
        processing advert_file
        """
        pass


def get_database(database_type=None):
    # Default to using the mysql database
    database_type = database_type or "mysql"
    # Lower all the input.
    database_type = database_type.lower()

    if database_type == 'postgresql':
        import tramscore.database_postgres
    elif database_type == 'mysql':
        import tramscore.database_sql

    for db_cls in Database.__subclasses__():
        if db_cls.type == database_type:
            return db_cls

    raise TypeError("Unsupported database type supplied, Fred.")


# Import our default database handler
# import tramscore.database_sql
