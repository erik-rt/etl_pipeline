class sql():
    drop_query = 'DROP TABLE IF EXISTS'
    create_query = 'CREATE TABLE IF NOT EXISTS'
    table_attributes = {
        'table1': '''(
            attribute1 varchar(256) NOT NULL,
            attribute2 numeric(18,0),
            attribute3 int4,
            attribute4 timestamp
            CONSTRAINT attribute1_pkey PRIMARY KEY (attribute1)
        )''',
        'table2': '''(
            attribute5 bigint
        )'''
    }
