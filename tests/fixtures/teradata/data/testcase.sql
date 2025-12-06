CREATE TABLE CHECKS_TESTCASE (
  CTC_ID                            DECIMAL PRIMARY KEY,              -- Primary key
  DESCRIPTION                       VARCHAR(30) NOT NULL,             -- Description
  AMOUNT                            DECIMAL(10),                      -- Amount purchased
  QUALITY                           DECIMAL,                          -- Percentage of checks passed
  CUSTOM_ATTRIBUTES                 CLOB,                             -- Custom attributes
  FIELD_VARCHAR                     VARCHAR(100),                     -- Variable-length string
  FIELD_NVARCHAR                    VARCHAR(100),                     -- Variable-length Unicode string (Teradata uses VARCHAR)
  FIELD_NUMBER                      DECIMAL,                          -- Number
  FIELD_FLOAT                       FLOAT,                            -- Float
  FIELD_DATE                        DATE,                             -- Date and Time down to day precision
  FIELD_BINARY_FLOAT                REAL,                             -- 32-bit floating point number
  FIELD_BINARY_DOUBLE               DOUBLE PRECISION,                 -- 64-bit floating point number
  FIELD_TIMESTAMP                   TIMESTAMP(6),                     -- Timestamp with fractional second precision of 6, no timezones
  FIELD_TIMESTAMP_TZ                TIMESTAMP(6) WITH TIME ZONE,      -- Timestamp with fractional second precision of 6, with timezones (TZ)
  FIELD_TIMESTAMP_LTZ               TIMESTAMP(6) WITH TIME ZONE,      -- Timestamp with fractional second precision of 6, with timezone support
  FIELD_INTERVAL_YEAR               INTERVAL YEAR(2) TO MONTH,        -- Interval of time in years and months with (2) precision
  FIELD_INTERVAL_DAY                INTERVAL DAY(2) TO SECOND(6),     -- Interval of time in days, hours, minutes and seconds with (2 / 6) precision
  FIELD_RAW                         BYTE,                             -- Large raw binary data
  FIELD_ROWID                       VARCHAR(18),                      -- Base 64 string representing a unique row address
  FIELD_UROWID                      VARCHAR(18),                      -- Base 64 string representing the logical address
  FIELD_CHAR                        CHAR(10),                         -- Fixed-length string
  FIELD_NCHAR                       CHAR(10),                         -- Fixed-length Unicode string (Teradata uses CHAR)
  FIELD_CLOB                        CLOB,                             -- Character large object
  FIELD_NCLOB                       CLOB,                             -- National character large object (Teradata uses CLOB)
  FIELD_BLOB                        BLOB,                             -- Binary large object
  FIELD_BFILE                       BLOB,                             -- Binary file (Teradata uses BLOB)
  CONSTRAINT check_quality_is_percentage CHECK (QUALITY BETWEEN 0 AND 100)
);

INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (1,   'One',   1, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (2,   'Two',   2, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (3, 'Three',   3, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (4,  'Four',   4, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (5,  'Five',   5, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (6,   'Six',   6, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (7, 'Seven', 100,  95);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY) VALUES (8, 'Eight',   8, 100);
INSERT INTO CHECKS_TESTCASE (CTC_ID, DESCRIPTION, AMOUNT, QUALITY, CUSTOM_ATTRIBUTES)
  VALUES (9, 'Nine',  50,  10, '{ "quality": "junk", "description": "Spare parts" }');
