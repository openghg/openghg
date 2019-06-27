""" Describes a single instrument at a site
    An instrument contains DataSources

    DataSources could also be called sensors

"""

__all__ = ["Instrument"]


class Instrument:
    """ This class holds information regarding an instrument.
        An instrument may be a set of Datasources or a single
        Datasource at a Site.

        Instrument instances should be created with the
        Instrument.create() function

    """
    _instrument_root = "instruments"

    def __init__(self):
        """ This only creates a null Instrument
            Create should be used to create Instrument objects
        """
        self._uuid = None
        self._name = None
        self._creation_datetime = None
        self._stored = False
        # self._height = None
        # self._site = None
        # self._network = None
        self._labels = {}
        self._datasources = []
        self._species = {}
    
    @staticmethod
    def create(name, **kwargs):
        """ Creates an Instrument object

            Args:
                name (str): Name of instrument
                **kwargs: Keyword arguments to be added to the labels
                dictionary
            Returns:
                Instrument: Instrument object
        """
        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now


        i = Instrument()
        i._uuid = _create_uuid()
        i._creation_datetime = _get_datetime_now()
        # TODO - might not need name
        i._name = name
        # Here labels will be the metadata associated with each Datasource
        # associated with this Instrument
        # Save the passed keywords as 
        i._labels = kwargs

        return i

    def is_null(self):
        """ Check if this is a null Instrument

            Returns:
                bool: True if null
        """
        return self._uuid is None

    def to_data(self):
        """ Creates a JSON serialisable dictionary to store this object
            in the object store

            Returns:
                dict: Dictionary created from this object
        """
        from Acquire.ObjectStore import datetime_to_string as _datetime_to_string

        datasource_uuids = [d._uuid for d in self._datasources]

        d = {}
        d["UUID"] = self._uuid
        d["name"] = self._name
        d["creation_datetime"] = _datetime_to_string(self._creation_datetime)
        d["datasources"] = datasource_uuids
        d["labels"] = self._labels
        d["stored"] = self._stored

        return d

    @staticmethod
    def from_data(data, shallow):
        """ Creates an Instrument object from a JSON file

            Args:
                data (dict): JSON data from which to create object
                shallow (bool): If True don't load related Datasources
            Returns:
                Instrument: Instrument object from data
        """
        if data is None or len(data) == 0:
            return Instrument()

        from Acquire.ObjectStore import string_to_datetime as _string_to_datetime
        from HUGS.Modules import Datasource as _Datasource

        i = Instrument()
        i._uuid = data["UUID"]
        i._name = data["name"]
        stored = data["stored"]

        if stored and not shallow:
            datasource_uuids = data["datasources"]
            for uuid in datasource_uuids:
                i._datasources.append(_Datasource.load(uuid=uuid))
        else:
            i._datasources = data["datasources"]

        i._creation_datetime = _string_to_datetime(data["creation_datetime"])
        
        i._labels = data["labels"]
        i._stored = False

        return i

    def save(self, bucket=None):
        """ Save this Instrument as a JSON object on the object store
    
            This function also saves the name and UID of the Instrument
            to the object store with a key

            {instrument_root}/name/{instrument_name}/{instrument_uid}
            
            Args:
                bucket (dict): Bucket to hold data
            Returns:
                None
        """
        if self.is_null():
            return
        
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        self._stored = True
        instrument_key = "%s/uuid/%s" % (Instrument._instrument_root, self._uuid)
        _ObjectStore.set_object_from_json(bucket=bucket, key=instrument_key, data=self.to_data())

        # Get the datasources to save themselves to the object store
        for d in self._datasources:
            d.save(bucket=bucket)

        encoded_name = _string_to_encoded(self._name)
        string_key = "%s/name/%s/%s" % (Instrument._instrument_root, encoded_name, self._uuid)
        _ObjectStore.set_string_object(bucket=bucket, key=string_key, string_data=self._uuid)

    @staticmethod
    def load(bucket=None, uuid=None, name=None, shallow=False):
        """ Load an Instrument from the object store either by name or UUID

            uuid or name must be passed to the function

            Args:
                bucket (dict, default=None): Bucket to hold data
                uuid (str, default=None): UUID of Instrument
                name (str, default=None): Name of Instrument
                shallow (bool, default=False): If True don't load related Datasources
            Returns:
                Instrument: Instrument object
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if uuid is None and name is None:
            raise ValueError("Both uuid and name cannot be None")

        if bucket is None:
            bucket = _get_bucket()
        if uuid is None:
            uuid = Instrument._get_uid_from_name(bucket=bucket, name=name)

        key = "%s/uuid/%s" % (Instrument._instrument_root, uuid)
        data = _ObjectStore.get_object_from_json(bucket=bucket, key=key)

        return Instrument.from_data(data, shallow)

    # Need the DataSources associated with this Instrument
    # def get_datasources(self):
    #     """ Returns a JSON serialisable dictionary of the DataSources
    #         associated with this Instrument

    #         Returns:
    #             dict: Dictionary of DataSources and related information
    #     """        
    #     return self._datasources
        
    @staticmethod
    def _get_uid_from_name(bucket, name):
        """ Gets the UID of an instrument from its name

            Args:
                bucket (dict): Bucket holding data
                name (str): Name of Instrument
            Returns:
                str: UID of Instrument
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import string_to_encoded as _string_to_encoded

        encoded_name = _string_to_encoded(name)

        prefix = "%s/name/%s" % (Instrument._instrument_root, encoded_name)

        uuid = _ObjectStore.get_all_object_names(bucket=bucket, prefix=prefix)

        if(len(uuid) > 1):
            raise ValueError("There should only be one instrument with this name")

        return uuid[0].split("/")[-1]
    

    @staticmethod
    def exists(uuid, bucket=None):
        """ Uses an ID of some kind to query whether or not this is a new
            Instrument and should be created

            TODO - update this when I have a clearer idea of how to ID Instruments

            Args:
                uuid (str): ID of Instrument
            Returns:
                bool: True if Instrument exists 
        """
        from HUGS.ObjectStore import exists as _exists
        from HUGS.ObjectStore import get_bucket as _get_bucket

        if bucket is None:
            bucket = _get_bucket()

        # Query object store for Instrument
        return _exists(bucket=bucket, uuid=uuid)


    def species_query(self, species):
        """ Check the list of species passed and returns the UUID of each 
            Datasource associated with that species

            Args:
                species (list): List of species
            Returns:
                dict: Dictionary of keys for Datasources for each species keyed as species : value
                Where value is the UUID of the Datasource for that species or False if no Datasource
                is found
        """
        found = {}
        for s in species:
            if s in self._species:
                # Species : datasource_UUID
                found[s] = self._species[s]
            else:
                found[s] = False

        return found


    def get_labels(self):
        """ Returns the labels dictionary

            Returns:
                dict: Labels dictionary for this object
        """
        return self._labels


    def add_datasource(self, datasource):
        """ Add a Datasource to this Instrument along with accompanying metadata
            on the Datasource.

            Args:
                datsource (Datasource): Datasource object to add
            Returns:
                None
        """
        metadata = {}
        
        # TODO - how to properly get the gas name?
        # metadata["gas_type"] = datasource._name
        metadata["date_range"] = datasource.get_daterange()
        metadata["gas"] = datasource._labels["gas"]
        # More datas?

        self.add_label(datasource._uuid, metadata)

        self._datasources.append(datasource)

    def add_data(self, gas_data):
        """ Create or get an exisiting Datasource for each gas in the file

            TODO - currently this function will only take data from a single Datasource
            
            Args:
                gas_data (list): List of tuples of gas name, datasource ID and Pandas.DataFrame 
                to add to the Instrument
            Returns:
                None
        """
        from HUGS.Modules import Datasource as _Datasource
        from HUGS.Processing import parse_gases as _parse_gases

        # Rework this to for the segmentation of data within the Datasource
        for gas_name, inlet, datasource_id, data in gas_data:
            if _Datasource.exists(datasource_id=datasource_id):
                datasource = _Datasource.load(uuid=datasource_id)
                # TODO - add metadata in here - append to existing?
            else:
                datasource = _Datasource.create(name=gas_name)
                # datasource.add_metadata(metadata)

            # Should there be multiple inlets saved in a single Datasource?
            # Or should these be split into separate datasources?

            # Store the name and datasource_id
            self._species[gas_name] = datasource_id
            # Add the dataframe to the datasource
            datasource.add_data(data)
            # Add the Datasource to this Instrument
            self.add_datasource(datasource)


    def search_labels(self, search_term):
            """ Search the labels of this Instrument

                WIP
            """
            return False

    def add_label(self, key, value):
        """ Add a label to the labels dictionary.

            Note: this may overwrite existing data

            Args:
                key (str): Key for label
                value (str): Value for label
            Returns:
                None
        """
        self._labels[key] = value

    def get_uuid(self):
        """ Return the UUID of this Instrument

            Returns:
                str: UUID of Instrument
        """
        return self._uuid

    def get_creation_datetime(self):
        """ Return the creation datetime of this Instrument

            Returns:
                datetime: Creation datetime
        """
        return self._creation_datetime



        






        










        

