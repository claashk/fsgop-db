from typing import Optional, Iterable, Union, Iterator
from datetime import datetime, timedelta
from pathlib import Path
import logging

from .sqlite_db import SqliteDatabase
from .person import Person, PersonProperty, NameAdapter
from .vehicle import Vehicle, VehicleProperty
from .vehicle import SINGLE_ENGINE_PISTON, MOTOR_GLIDER, GLIDER, ULTRALIGHT
from .vehicle import WINCH, UNDEFINED, TOURING_MOTOR_GLIDER
from .mission import Mission

from .utils import kwargs_from, get_value, set_value

logger = logging.getLogger(__name__)


LAUNCH_TYPE_WINCH = "winch"
LAUNCH_TYPE_AEROTOW = "airtow"
LAUNCH_TYPE_SELF = "self"
LAUNCH_TYPE_OTHER = "other"

MOTOR_PLANES = {SINGLE_ENGINE_PISTON, TOURING_MOTOR_GLIDER, ULTRALIGHT}

LOG_STRINGS = {
    LAUNCH_TYPE_WINCH: "W",
    LAUNCH_TYPE_AEROTOW: "FS",
    LAUNCH_TYPE_SELF: "ES"
}

DATE_FORMAT = "%Y-%m-%d"


schema_v3 = {
    'flights': {
        'columns': [
            {'name': 'id',
             'dtype': 'int(11)',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'plane_id',
             'dtype': 'int(11)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "planes(id)"},
            {'name': 'pilot_id',
             'dtype': 'int(11)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(id)"},
            {'name': 'copilot_id',
             'dtype': 'int(11)',
             'allows_null': True,
             'force_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(id)"},
            {'name': 'type',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'mode',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'departed',
             'dtype': 'tinyint(1)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'landed',
             'dtype': 'tinyint(1)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towflight_landed',
             'dtype': 'tinyint(1)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'launch_method_id',
             'dtype': 'int(11)',
             'allows_null': True,
             'force_null': True,
             'default_value': None,
             'extra': '',
             'references': "launch_methods(id)"},
            {'name': 'departure_location',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'landing_location',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'num_landings',
             'dtype': 'int(11)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'departure_time',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'landing_time',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towplane_id',
             'dtype': 'int(11)',
             'allows_null': True,
             'force_null': True,
             'default_value': None,
             'extra': '',
             'references': "planes(id)"},
            {'name': 'towflight_mode',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towflight_landing_location',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towflight_landing_time',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towpilot_id',
             'dtype': 'int(11)',
             'allows_null': True,
             'force_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(id)"},
            {'name': 'pilot_last_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'pilot_first_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'copilot_last_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'copilot_first_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towpilot_last_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towpilot_first_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'comments',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'accounting_notes',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None}],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('id', 1)]},
            {'name': 'accounting_notes_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('accounting_notes', 1)]},
            {'name': 'copilot_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('copilot_id', 1)]},
            {'name': 'departed_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('departed', 1)]},
            {'name': 'departure_location_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('departure_location', 1)]},
            {'name': 'departure_time_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('departure_time', 1)]},
            {'name': 'landed_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('landed', 1)]},
            {'name': 'landing_location_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('landing_location', 1)]},
            {'name': 'landing_time_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('landing_time', 1)]},
            {'name': 'launch_method_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('launch_method_id', 1)]},
            {'name': 'mode_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('mode', 1)]},
            {'name': 'pilot_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('pilot_id', 1)]},
            {'name': 'plane_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('plane_id', 1)]},
            {'name': 'status_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('departed', 1), ('landed', 1),
                         ('towflight_landed', 1)]},
            {'name': 'towflight_landed_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('towflight_landed', 1)]},
            {'name': 'towflight_landing_location_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('towflight_landing_location', 1)]},
            {'name': 'towflight_landing_time_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('towflight_landing_time', 1)]},
            {'name': 'towflight_mode_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('towflight_mode', 1)]},
            {'name': 'towpilot_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('towpilot_id', 1)]},
            {'name': 'towplane_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('towplane_id', 1)]},
            {'name': 'type_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('type', 1)]}]},
    'launch_methods': {
        'columns': [
            {'name': 'id',
             'dtype': 'int(11)',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'short_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'log_string',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'keyboard_shortcut',
             'dtype': 'varchar(1)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'type',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'towplane_registration',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'person_required',
             'dtype': 'tinyint(1)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'comments',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None}],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('id', 1)]}]},
    'people': {
        'columns': [
            {'name': 'id',
             'dtype': 'int(11)',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'last_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'first_name',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'club',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'nickname',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'club_id',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'comments',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'medical_validity',
             'dtype': 'date',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'check_medical_validity',
             'dtype': 'tinyint(1)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None}],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('id', 1)]},
            {'name': 'club_id_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('club_id', 1)]},
            {'name': 'club_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('club', 1)]}]},
    'planes': {
        'columns': [
            {'name': 'id',
             'dtype': 'int(11)',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'registration',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'club',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'num_seats',
             'dtype': 'int(11)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'type',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'category',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'callsign',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'comments',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None}],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('id', 1)]},
            {'name': 'club_index_for_planes',
             'is_unique': False,
             'is_primary': False,
             'columns': [('club', 1)]},
            {'name': 'registration_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('registration', 1)]}]},
    'schema_migrations': {
        'columns': [
            {'name': 'version',
             'dtype': 'varchar(255)',
             'allows_null': False,
             'default_value': None,
             'extra': '',
             'references': None}],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('version', 1)]}]}
}


class Repository(object):
    """Repository based on startkladde database

    Args:
        db: Database using startkladde schema. Either a database or a string/path
        **kwargs: Keyword arguments passed verbatim to open
    """
    native_types = {
        "people": Person,
        "launch_methods": Vehicle,
        "planes": Vehicle,
        "flights": Mission
    }

    field_aliases = {
        "people": {"id": "uid"},
        "launch_methods": {"id": "uid",
                           "towplane_registration": "registration",
                           "name": "model"},
        "planes": {"id": "uid", "type": "model"},
        "flights": {"id": "uid",
                    "plane_id": "vehicle_uid",
                    "pilot_id": "pilot_uid",
                    "copilot_id": "copilot_uid",
                    "num_landings": "num_stints",
                    "departure_time": "begin",
                    "landing_time": "end",
                    "launch_method_id": "launch_uid",  # temporary assignment
                    "departure_location": "origin",
                    "landing_location": "destination"}
    }

    def __init__(self,
                 db: Union[str, Path, SqliteDatabase] = "",
                 **kwargs) -> None:
        self._db = None
        self._winch_missions = None  # cache for auto-generated winch missions
        self._launch_methods = None  # maps launch methods to vehicles
        if db:
            self.open(db=db, **kwargs)

    def __enter__(self) -> "Repository":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self, db: Union[str, Path, SqliteDatabase], **kwargs) -> None:
        """Open repository on a new database

        Args:
            db: Database or path to database file
            **kwargs: Keyword arguments passed verbatim to
                :class:`~fsgop.db.SqliteDatabase`

        """
        self.close()
        if isinstance(db, SqliteDatabase):
            self._db = db
        else:
            self._db = SqliteDatabase(db=db, schema=schema_v3, **kwargs)

    def close(self) -> None:
        """Close this repository"""
        self._launch_methods = None
        self._winch_missions = None
        if self._db is not None:
            self._db.disconnect()
            self._db = None

    def vehicle_for_launch_method(self, uid: int) -> Vehicle:
        """Get vehicle uid for a launch method

        Args:
            uid: Launch method uid in native database

        Return:
            Vehicle representing the desired launch method
        """
        if self._launch_methods is None:
            for _v in self.launch_methods(): pass
        return self._launch_methods[uid]

    def winch_launch_for(self, mission: Mission, vehicle: Vehicle) -> Mission:
        """Get winch launch for a given mission

        Use one mission per launch method, date and location.

        Uid and vehicle of the new winch mission are not set.

        Args:
            mission: Mission for which to create a winch launch
            vehicle: Vehicle (winch) for this mission

        Return:
            New mission representing a winch session used to launch this mission
        """
        lm_uid = int(mission.launch)  # initial assignment
        begin = mission.begin
        try:
            launch = self._winch_missions[(lm_uid, mission.origin)]

            if begin is not None and launch.begin <= begin < launch.end:
                return launch

            if begin is None and mission.end is not None:
                if mission.end.date() == launch.begin.date():
                    return launch
        except KeyError:
            pass
        # create a new mission
        launch = Mission.winch_launch_for(mission=mission, vehicle=vehicle)
        launch.comments = str("Auto-generated winch mission for launch method "
                              f"{lm_uid}")
        self._winch_missions[(lm_uid, mission.origin)] = launch
        return launch

    def get(self,
            table: str,
            adapt_names: bool = False,
            ignore: Optional[Iterable] = None,
            **kwargs) -> Iterator[tuple]:
        """Read data from table

        Args:
            table: Table name from which to extract data
            adapt_names: If True, names will be converted using NameAdapter
            ignore: Iterable of fields to ignore. Defaults to ``None``.
            **kwargs: Keyword arguments passed verbatim to Database.select

        Yields:
            tuple containing the original record and the imported record in
            native data format
        """
        aliases = self.field_aliases[table]
        rectype = self._db.schema[table].create_record_type(aliases=aliases)
        generator = self._db.select(table, rectype=rectype, **kwargs)
        if adapt_names:
            generator, rectype = NameAdapter.apply(generator)
        _type = self.native_types[table]
        if ignore is None:
            fields = rectype._fields
        else:
            fields = set(rectype._fields) - set(ignore)
        layout = _type.layout(allow=fields)
        for rec in generator:
            yield rec, _type(**kwargs_from(rec, layout=layout))

    def persons(self) -> Iterator[Person]:
        """Get persons from database

        Yields:
            One :class:Person instance per person found in db
        """
        for rec, person in self.get("people", adapt_names=True):
            email = get_value(rec.comments, "email")
            if email is not None:
                PersonProperty(kind="email", value=email).add_to(person)
                person.comments = set_value("email", None, person.comments)
            if rec.medical_validity is not None:
                t = datetime.strptime(rec.medical_validity, DATE_FORMAT)
                t += timedelta(hours=24)  # valid on expiration date
                PersonProperty(kind="medical",
                               value="class-2",
                               valid_until=t).add_to(person)
            #TODO add property for club membership
            yield person

    def launch_methods(self) -> Iterator[Vehicle]:
        """Get launch methods from startkladde database

        Since there is no launch method concept in the native database, each
        launch method will be converted to a vehicle. To avoid conflicts with
        planes, which are also modelled as vehicles, launch method IDs are
        re-assigned and the internal _launch_methods dictionary is filled with
        the original uid as key and the new uid as value.

        Launch methods using type "other" are mapped to the native UNDEFINED
        value, since it is considered an error, if the launch method type is not
        specified. Self launch will be mapped to category ``None``, since no
        vehicle is associated with self launches in the native data model.

        Yields:
            One :class:`~fsgop.db.Vehicle` instance per launch method
        """
        categories = {LAUNCH_TYPE_WINCH: WINCH,
                      LAUNCH_TYPE_AEROTOW: SINGLE_ENGINE_PISTON,
                      LAUNCH_TYPE_SELF: None,
                      LAUNCH_TYPE_OTHER: UNDEFINED}
        i0 = self._db.max_id("planes") + 1
        self._launch_methods = dict()
        for uid, (rec, vehicle) in enumerate(self.get("launch_methods"), i0):
            vehicle.uid = uid
            vehicle.manufacturer = "UNDEFINED"
            vehicle.serial_number = f"sk-launch-method-{rec.uid}"
            vehicle.category = categories[rec.type]
            self._launch_methods[rec.uid] = vehicle
            yield vehicle

    def planes(self) -> Iterator[Vehicle]:
        """Get planes from startkladde database

        Yields:
            One :class:`~fsgop.db.Vehicle` instance per plane in db
        """
        categories = {"glider": GLIDER,
                      "airplane": SINGLE_ENGINE_PISTON,
                      "ultralight": ULTRALIGHT,
                      "motorglider": MOTOR_GLIDER}
        for rec, vehicle in self.get("planes", ignore=["category"]):
            vehicle.manufacturer = "UNDEFINED"
            vehicle.serial_number = f"sk-plane-{rec.uid}"
            vehicle.category = categories[rec.category]
            if rec.club is not None:
                VehicleProperty(kind="operator", value=rec.club).add_to(vehicle)
            yield vehicle

    def vehicles(self) -> Iterator[Vehicle]:
        """Get all vehicles from startkladde database

        Iterates over all planes and launch methods.

        Yields:
            One :class:`~fsgop.db.Vehicle` instance per plane and launch method
        """
        yield from self.planes()
        yield from self.launch_methods()

    def missions(self) -> Iterator[Mission]:
        if self._winch_missions is not None:
            raise RuntimeError("Cannot invoke missions() concurrently")
        self._winch_missions = dict()
        categories = {
            "normal": Mission.categories["normal flight"],
            "training_1": Mission.categories["supervised solo"],
            "training_2": Mission.categories["dual flight instruction"],
            "guest_external": Mission.categories["passenger flight"],
            "guest_private": Mission.categories["passenger flight"]
        }

        uid = self._db.max_id("flights") + 1
        for rec, mission in self.get("flights",
                                     adapt_names=True,
                                     order="departure_time"):
            mission.kind = categories[rec.type]
            launch_vehicle = self.vehicle_for_launch_method(rec.launch_uid)
            if launch_vehicle.category == WINCH:
                launch = self.winch_launch_for(mission, vehicle=launch_vehicle)
                if launch.uid is None:
                    launch.uid = uid
                    uid += 1
                yield launch
            elif launch_vehicle.category in MOTOR_PLANES:
                if rec.towplane_id is not None:
                    launch_vehicle = Vehicle(uid=int(rec.towplane_id))
                launch = Mission.aerotow_for(
                             mission,
                             vehicle=launch_vehicle,
                             pilot=rec.towpilot_id,
                             destination=rec.towflight_landing_location,
                             end=rec.towflight_landing_time
                         )
                launch.uid = uid
                uid += 1
                yield launch
            elif launch_vehicle.category is None:
                launch = mission
            else:
                raise ValueError("Invalid launch vehicle category: "
                                 f"'{launch_vehicle.category}'")
            mission.launch = launch
            if mission.pilot and mission.pilot.uid is None:
                logger.warning(f"Mission {mission} uses unknown Pilot "
                               f"{mission.pilot}. Set pilot to <None>")
                mission.pilot = None
            if mission.copilot and mission.copilot.uid is None:
                logger.warning(f"Mission {mission} uses unknown Copilot "
                               f"{mission.copilot}. Set copilot to <None>")
                mission.copilot = None
            yield mission
        self._winch_missions = None  # indicate, that method can be invoked
        return
