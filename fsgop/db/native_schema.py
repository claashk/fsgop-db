tables = {
    'people': {
        'columns': [
            {'name': 'uid',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'last_name',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'first_name',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'title',
             'dtype': 'varchar(32)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'birthday',
             'dtype': 'date',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'birthplace',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'count',
             'dtype': 'int',
             'allows_null': False,
             'default_value': 1,
             'extra': '',
             'references': None},
            {'name': 'kind',
             'dtype': 'int',
             'allows_null': False,
             'default_value': 1,
             'extra': '',
             'references': None},
            {'name': 'comments',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None}
        ],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('uid', 1)]},
            {'name': 'people_by_name_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('last_name', 1), ('first_name', 1)]}
        ]
    },
    'person_properties': {
        'columns': [
            {'name': 'uid',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'person',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'valid_from',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'valid_until',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'kind',
             'dtype': 'varchar(64)',
             'allows_null': False,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'value',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
        ],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('uid', 1)]},
            {'name': 'person_properties_by_person_name_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('person', 1), ('kind', 1), ('valid_until', 1)]}
        ]
    },
    'vehicles': {
        'columns': [
            {'name': 'uid',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'manufacturer',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'model',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'serial_number',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'num_seats',
             'dtype': 'smallint',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'category',
             'dtype': 'int',
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
             'columns': [('uid', 1)]},
         ]
    },
    'vehicle_properties': {
        'columns': [
            {'name': 'uid',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'vehicle',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': '',
             'references': "vehicles(uid)"},
            {'name': 'valid_from',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'valid_until',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'kind',
             'dtype': 'varchar(64)',
             'allows_null': False,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'value',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
        ],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('uid', 1)]},
            {'name': 'vehicle_properties_by_name_value',
             'is_unique': True,
             'is_primary': False,
             'columns': [('kind', 1), ('value', 1), ('valid_until', -1)]}
        ]
    },
    'missions': {
        'columns': [
            {'name': 'uid',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
            {'name': 'vehicle',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': '',
             'references': "vehicles(uid)"},
            {'name': 'pilot',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'copilot',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'passenger1',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'passenger2',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'passenger3',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'passenger4',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'category',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'num_stints',
             'dtype': 'int',
             'allows_null': True,
             'default_value': 1,
             'extra': '',
             'references': None},
            {'name': 'launch',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "missions(uid)"},
            {'name': 'origin',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'begin',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'destination',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'end',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'off_block_utc',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'on_block_utc',
             'dtype': 'datetime',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'engine_hours_begin',
             'dtype': 'float',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'engine_hours_end',
             'dtype': 'varchar(64)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
            {'name': 'charge_person',
             'dtype': 'int',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': "people(uid)"},
            {'name': 'comments',
             'dtype': 'varchar(255)',
             'allows_null': True,
             'default_value': None,
             'extra': '',
             'references': None},
        ],
        'indices': [
            {'name': 'PRIMARY',
             'is_unique': True,
             'is_primary': True,
             'columns': [('uid', 1)]},
            {'name': 'missions_by_date_value',
             'is_unique': True,
             'is_primary': False,
             'columns': [('begin', 1), ('vehicle', 1)]}
        ]
    },
    'schema_migrations': {
        'columns': [
            {'name': 'uid',
             'dtype': 'int',
             'allows_null': False,
             'default_value': None,
             'extra': 'auto_increment',
             'references': None},
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
             'columns': [('uid', 1)]}]}
}

vehicle_keys_v1 = {
    1: "single engine piston",
    2: "ultralight",
    3: "touring motor glider",
    4: "motor glider",
    5: "glider",
    11: "winch",
    12: "car",
    9999: "undefined"  # use this value only to indicate errors/warnings
}

vehicle_keys_v2 = {
    0: "undefined",
    10: "aircraft",
    11: "single engine piston",
    12: "multi engine piston",
    13: "touring motor glider",
    14: "glider",
    30: "ultralight",
    40: "helicopter",
    50: "balloon",
    60: "car",
    65: "winch"
}

person_property_types_v1 = {
    10: "licence",
    11: "certificate",  # medical, radio
    40: "accounting information",
    50: "contact information"
}



