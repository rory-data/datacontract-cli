-- https://docs.teradata.com/r/Lake-Working-with-SQL/SQL-Data-Types

CREATE TABLE field_showcase
(
  field_primary_key           INTEGER PRIMARY KEY,             -- Primary key
  field_not_null              INTEGER NOT NULL,                -- Not null
  field_varchar               VARCHAR,                         -- Variable-length string
  field_nvarchar              VARCHAR,                         -- Variable-length Unicode string (Teradata uses VARCHAR)
  field_number                DECIMAL,                         -- Number (Teradata uses DECIMAL)
  field_float                 FLOAT,                           -- Float
  field_date                  DATE,                            -- Date and Time down to day precision
  field_binary_float          REAL,                            -- 32-bit floating point number (Teradata equivalent)
  field_binary_double         DOUBLE PRECISION,                -- 64-bit floating point number
  field_timestamp             TIMESTAMP(6),                    -- Timestamp with fractional second precision of 6, no timezones
  field_timestamp_tz          TIMESTAMP(6) WITH TIME ZONE,     -- Timestamp with fractional second precision of 6, with timezones (TZ)
  field_timestamp_ltz         TIMESTAMP(6) WITH TIME ZONE,     -- Timestamp with fractional second precision of 6, with timezone support
  field_interval_year         INTERVAL YEAR(2) TO MONTH,       -- Interval of time in years and months with (2) precision
  field_interval_day          INTERVAL DAY(2) TO SECOND(6),    -- Interval of time in days, hours, minutes and seconds with (2 / 6) precision
  field_raw                   BYTE,                            -- Large raw binary data (Teradata equivalent)
  field_rowid                 VARCHAR(18),                     -- Base 64 string representing a unique row address
  field_urowid                VARCHAR(18),                     -- Base 64 string representing the logical address
  field_char                  CHAR(10),                        -- Fixed-length string
  field_nchar                 CHAR(10),                        -- Fixed-length Unicode string (Teradata uses CHAR)
  field_clob                  CLOB,                            -- Character large object
  field_nclob                 CLOB,                            -- National character large object (Teradata uses CLOB)
  field_blob                  BLOB,                            -- Binary large object
  field_bfile                 BLOB                             -- Binary file (Teradata uses BLOB)
)