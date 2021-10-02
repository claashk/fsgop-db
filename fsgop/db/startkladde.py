schema_v3 = {'flights': [{'name': 'id',
                          'dtype': 'int(11)',
                          'allows_null': False,
                          'index': 'PRI',
                          'default_value': None,
                          'extra': 'auto_increment',
                          'references': None},
                         {'name': 'plane_id',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'pilot_id',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'copilot_id',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'type',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'mode',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'departed',
                          'dtype': 'tinyint(1)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'landed',
                          'dtype': 'tinyint(1)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towflight_landed',
                          'dtype': 'tinyint(1)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'launch_method_id',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'departure_location',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'landing_location',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'num_landings',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'departure_time',
                          'dtype': 'datetime',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'landing_time',
                          'dtype': 'datetime',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towplane_id',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towflight_mode',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towflight_landing_location',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towflight_landing_time',
                          'dtype': 'datetime',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towpilot_id',
                          'dtype': 'int(11)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'pilot_last_name',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'pilot_first_name',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'copilot_last_name',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'copilot_first_name',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towpilot_last_name',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'towpilot_first_name',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'comments',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': '',
                          'default_value': None,
                          'extra': '',
                          'references': None},
                         {'name': 'accounting_notes',
                          'dtype': 'varchar(255)',
                          'allows_null': True,
                          'index': 'MUL',
                          'default_value': None,
                          'extra': '',
                          'references': None}],
             'launch_methods': [{'name': 'id',
                                 'dtype': 'int(11)',
                                 'allows_null': False,
                                 'index': 'PRI',
                                 'default_value': None,
                                 'extra': 'auto_increment',
                                 'references': None},
                                {'name': 'name',
                                 'dtype': 'varchar(255)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'short_name',
                                 'dtype': 'varchar(255)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'log_string',
                                 'dtype': 'varchar(255)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'keyboard_shortcut',
                                 'dtype': 'varchar(1)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'type',
                                 'dtype': 'varchar(255)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'towplane_registration',
                                 'dtype': 'varchar(255)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'person_required',
                                 'dtype': 'tinyint(1)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None},
                                {'name': 'comments',
                                 'dtype': 'varchar(255)',
                                 'allows_null': True,
                                 'index': '',
                                 'default_value': None,
                                 'extra': '',
                                 'references': None}],
             'people': [{'name': 'id',
                         'dtype': 'int(11)',
                         'allows_null': False,
                         'index': 'PRI',
                         'default_value': None,
                         'extra': 'auto_increment',
                         'references': None},
                        {'name': 'last_name',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'first_name',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'club',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': 'MUL',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'nickname',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'club_id',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': 'MUL',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'comments',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'medical_validity',
                         'dtype': 'date',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'check_medical_validity',
                         'dtype': 'tinyint(1)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None}],
             'planes': [{'name': 'id',
                         'dtype': 'int(11)',
                         'allows_null': False,
                         'index': 'PRI',
                         'default_value': None,
                         'extra': 'auto_increment',
                         'references': None},
                        {'name': 'registration',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': 'MUL',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'club',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': 'MUL',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'num_seats',
                         'dtype': 'int(11)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'type',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'category',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'callsign',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None},
                        {'name': 'comments',
                         'dtype': 'varchar(255)',
                         'allows_null': True,
                         'index': '',
                         'default_value': None,
                         'extra': '',
                         'references': None}],
             'schema_migrations': [{'name': 'version',
                                    'dtype': 'varchar(255)',
                                    'allows_null': False,
                                    'index': 'PRI',
                                    'default_value': None,
                                    'extra': '',
                                    'references': None}]}
