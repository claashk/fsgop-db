from .table_info import TableInfo

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


def get_schema() -> dict:
    """Get schema for startkladde database

    Returns:
        Dictionary containing table name as key and TableInfo as value for
        each table of the Startkladde database
    """
    return {k: TableInfo.from_list(k, **v) for k, v in schema_v3.items()}
