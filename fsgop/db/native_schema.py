schema_v1 = {
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
             'columns': [('last_name', 1), ('first_name', 2)]}
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
            {'name': 'key',
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
            {'name': 'person_properties_by_person_key_index',
             'is_unique': False,
             'is_primary': False,
             'columns': [('person', 1), ('key', 2), ('valid_until', 3)]}
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
            {'name': 'key',
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
            {'name': 'vehicle_properties_by_key_value',
             'is_unique': True,
             'is_primary': False,
             'columns': [('key', 1), ('value', 2), ('valid_until', 3)]}
        ]
    },
    'mission': {
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
             'allows_null': False,
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
             'allows_null': False,
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
             'allows_null': False,
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
             'columns': [('begin', 1), ('vehicle', 2)]}
        ]
    },
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
