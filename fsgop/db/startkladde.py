#from __future__ import annotations
#from collections.abc import Iterator
from typing import Iterator
from datetime import datetime, timedelta

from .sqlite_db import SqliteDatabase
from .person import Person, PersonProperty, NameAdapter
from .vehicle import Vehicle, VehicleProperty
from .vehicle import SINGLE_ENGINE_PISTON, MOTOR_GLIDER, GLIDER, ULTRALIGHT
from .vehicle import WINCH, UNDEFINED
from .mission import Mission
from .mission import NORMAL_FLIGHT, GUEST_FLIGHT, ONE_SEATED_TRAINING
from .mission import TWO_SEATED_TRAINING, WINCH_OPERATION, AEROTOW

from .utils import kwargs_from, get_value, set_value

LAUNCH_TYPE_WINCH = "winch"
LAUNCH_TYPE_AEROTOW = "airtow"
LAUNCH_TYPE_SELF = "self"
LAUNCH_TYPE_OTHER = "other"

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


def iter_persons(db: SqliteDatabase) -> Iterator[Person]:
    """Get persons from a startkladde database

    Args:
        db: Database us startkladde schema

    Yields:
        One :class:Person instance per person found in db
    """
    rectype = db.schema["people"].create_record_type(aliases={"id": "uid"})
    people, _type = NameAdapter.apply(db.select("people", rectype=rectype))
    layout = Person.layout(allow=_type._fields)
    for rec in people:
        person = Person(**kwargs_from(rec, layout=layout))
        email = get_value(rec.comments, "email")
        if email is not None:
            PersonProperty(name="email", value=email).add_to(person)
            person.comments = set_value("email", None, person.comments)
        if rec.medical_validity is not None:
            t = datetime.strptime(rec.medical_validity, DATE_FORMAT)
            t += timedelta(hours=24)  # valid on expiration date
            PersonProperty(name="medical",
                           value="class 2",
                           valid_until=t).add_to(person)
        yield person


def iter_launch_methods(db: SqliteDatabase) -> Iterator[Vehicle]:
    """Get launch methods from startkladde database

    This will return each launch method as is, i.e. without modifying its
    uid, despite the fact, that uid clashes will occur, if vehicles from launch
    methods are mixed with those from planes.

    Launch methods using type "other" are mapped to the native UNDEFINED value,
    since it should generally be considered an error, if the launch method type
    cannot be specified. Self launch will be mapped to category None, since
    these have no real value in the native data model.

    Args:
        db: Database using the startkladde schema

    Yields:
        One :class:`~fsgop.db.Vehicle` instance per launch method
    """
    aliases = {
        "id": "uid",
        "towplane_registration": "registration",
        "name": "model"
    }
    categories = {
        LAUNCH_TYPE_WINCH: WINCH,
        LAUNCH_TYPE_AEROTOW: SINGLE_ENGINE_PISTON,
        LAUNCH_TYPE_SELF: None,
        LAUNCH_TYPE_OTHER: UNDEFINED
    }
    method = db.schema["launch_methods"].create_record_type(aliases=aliases)
    layout = Vehicle.layout(allow=method._fields)
    for rec in db.select("launch_methods", rectype=method):
        vehicle = Vehicle(**kwargs_from(rec, layout=layout))
        vehicle.manufacturer = "UNDEFINED"
        vehicle.serial_number = f"sk-launch-method-{rec.uid}"
        vehicle.category = categories[rec.type]
        yield vehicle


def iter_planes(db: SqliteDatabase) -> Iterator[Vehicle]:
    """Get planes from startkladde database

    Args:
        db: Database using the startkladde schema

    Yields:
        One :class:`~fsgop.db.Vehicle` instance per plane in db
    """
    categories = {
        "glider": GLIDER,
        "airplane": SINGLE_ENGINE_PISTON,
        "ultralight": ULTRALIGHT,
        "motorglider": MOTOR_GLIDER
    }
    aliases = {"id": "uid", "type": "model"}
    plane = db.schema["planes"].create_record_type(aliases=aliases)
    layout = Vehicle.layout(allow=set(plane._fields) - {"category"})
    for rec in db.select("planes", rectype=plane):
        vehicle = Vehicle(**kwargs_from(rec, layout=layout))
        vehicle.manufacturer = "UNDEFINED"
        vehicle.serial_number = f"sk-plane-{rec.uid}"
        vehicle.category = categories[rec.category]
        if rec.club is not None:
            VehicleProperty(name="operator", value=rec.club).add_to(vehicle)
        yield vehicle


def iter_vehicles(db: SqliteDatabase) -> Iterator[Vehicle]:
    """Get all vehicles from startkladde database

    This will reset the uid of each launch method to None to avoid conflicts with
    plane uids. The original uid can be retrieved from the serial number.

    Args:
        db: Database using the startkladde schema

    Yields:
        One :class:`~fsgop.db.Vehicle` instance per plane and launch method
    """
    yield from iter_planes(db)
    for vehicle in iter_launch_methods(db):
        # launch method uids cannot be kept, because they are not unique
        vehicle.uid = None
        yield vehicle


def iter_missions(db: SqliteDatabase) -> Iterator[Mission]:
    aliases = {
        "id": "uid",
        "plane_id": "vehicle_uid",
        "pilot_id": "pilot_uid",
        "copilot_id": "copilot_uid",
        "num_landings": "num_stints",
        "departure_time": "begin",
        "landing_time": "end",
        "departure_location": "origin",
        "landing_location": "destination"
    }
    categories = {
        "normal": NORMAL_FLIGHT,
        "training_1": ONE_SEATED_TRAINING,
        "training_2": TWO_SEATED_TRAINING,
        "guest_external": GUEST_FLIGHT,
        "guest_private": GUEST_FLIGHT
    }
    rectype = db.schema["flights"].create_record_type(aliases=aliases)
    lm_type = db.schema["launch_methods"].create_record_type(aliases={"id": "uid"})
    flights, _type = NameAdapter.apply(db.select("flights",
                                                 order="departure_time",
                                                 rectype=rectype))
    layout = Mission.layout(allow=_type._fields)

    next_uid = db.max_id("flights") + 1
    winches = dict()  # winch cache
    for rec in flights:
        mission = Mission(**kwargs_from(rec, layout=layout))
        mission.category = categories[rec.type]
        lm = db.unique("launch_methods",
                       where="id=:uid",
                       rectype=lm_type,
                       uid=rec.launch_method_id)
        if lm.type == LAUNCH_TYPE_WINCH:
            # We use a single winch mission per launch method here -> simple
            try:
               w = winches[lm.uid]
            except KeyError:
               winch = Vehicle(manufacturer="UNDEFINED",
                               serial_number=f"sk-launch-method-{lm.uid}",
                               category=WINCH)
               w = Mission(uid=next_uid,
                           vehicle=winch,
                           pilot=None,
                           category=WINCH_OPERATION,
                           num_stints=None,
                           launch=next_uid,
                           origin=rec.origin,
                           begin=None,
                           destination=rec.origin,  # winch does not move
                           end=None,
                           charge_person=rec.pilot_uid,
                           comments="Auto-generated winch mission for "
                                    f"launch method {lm.uid}")
               winches[lm.uid] = w
               next_uid += 1
               yield w

            mission.launch = w
        elif lm.type == LAUNCH_TYPE_AEROTOW:
            # create aerotow for this flight
            if rec.towplane_id is not None:
                towplane = int(rec.towplane_id)
            else:
                towplane = Vehicle(manufacturer="UNDEFINED",
                                   serial_number=f"sk-launch-method-{lm.uid}")
            launch = Mission(
                uid=next_uid,
                vehicle=towplane,
                pilot=rec.towpilot_id,
                category=AEROTOW,
                num_stints=rec.num_stints,
                launch=next_uid,
                origin=rec.origin,
                begin=rec.begin,
                destination=rec.towflight_landing_location,
                end=rec.towflight_landing_time,
                charge_person=rec.pilot_uid,
                comments=f"Auto-generated aerotow for flight {mission.uid}")
            mission.launch = launch
            next_uid += 1
            yield launch
        elif lm.type == LAUNCH_TYPE_SELF:
            mission.launch = mission.uid
        else:
            raise ValueError(f"Invalid Launch Type: {lm.type}")
        yield mission

