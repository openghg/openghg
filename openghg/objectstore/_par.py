import json
from datetime import datetime
from typing import Optional, TypeVar

PAR_TYPE = TypeVar("PAR_TYPE", bound="PAR")


class PAR:
    def __init__(
        self,
        uri: Optional[str] = None,
        par_id: Optional[str] = None,
        par_name: Optional[str] = None,
        time_created: Optional[datetime] = None,
        time_expires: Optional[datetime] = None,
    ):
        self.uri = uri
        self.par_id = par_id
        self.par_name = par_name
        self.time_created = time_created
        self.time_expires = time_expires

    @staticmethod
    def from_json(json_str: str) -> PAR_TYPE:
        """Create a PAR from JSON

        Args:
            json_str: JSON string
        Returns:
            PAR: PAR created some JSON
        """
        data = json.loads(json_str)

        p = PAR()

        p.uri = data["uri"]
        p.par_id = data["par_id"]
        p.par_name = data["par_name"]
        p.time_created = datetime.fromisoformat(data["time_created"])
        p.time_expires = datetime.fromisoformat(data["time_expires"])

        return p

    def to_json(self) -> str:
        """Serialise class to JSON

        Returns:
            str: Class as JSON serialised string
        """
        data = {}

        data["uri"] = self.uri
        data["par_id"] = self.par_id
        data["par_name"] = self.par_name
        data["time_created"] = self.time_created.isoformat()
        data["time_expires"] = self.time_expires.isoformat()

        return json.dumps(data)

    def upload():
        pass

    def has_expired(self) -> bool:
        """Checks if the expiry time of the PAR has passed

        Returns:
            bool: True if expired, else False
        """
        from openghg.objectstore import get_datetime_now

        return self.time_expires < get_datetime_now()
