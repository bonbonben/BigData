--
-- PARKING VIOLATIONS
--

DROP TABLE IF EXISTS parking_violations;

CREATE TABLE parking_violations(
  summons_number BIGINT NOT NULL,
  issue_date VARCHAR(10),
  violation_code INTEGER,
  violation_county VARCHAR(5),
  violation_description VARCHAR(30),
  violation_location INTEGER,
  violation_precinct INTEGER,
  violation_time VARCHAR(5),
  time_first_observed VARCHAR(5),
  meter_number VARCHAR(8),
  issuer_code INTEGER,
  issuer_command VARCHAR(9),
  issuer_precinct INTEGER,
  issuing_agency CHAR(1),
  plate_id VARCHAR(13),
  plate_type VARCHAR(3),
  registration_state VARCHAR(2),
  street_name VARCHAR(30),
  vehicle_body_type VARCHAR(4),
  vehicle_color VARCHAR(5),
  vehicle_make VARCHAR(5),
  vehicle_year INTEGER
);

LOAD DATA LOCAL INFILE 'parking-violations.csv'
INTO TABLE parking_violations
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n';

--
-- OPEN VIOLAITONS
--

DROP TABLE IF EXISTS open_violations;

CREATE TABLE open_violations(
  summons_number BIGINT NOT NULL,
  plate VARCHAR(12),
  license_type VARCHAR(3),
  county VARCHAR(5),
  state VARCHAR(2),
  precinct INTEGER,
  issuing_agency VARCHAR(35),
  violation VARCHAR(30),
  violation_status VARCHAR(29),
  issue_date VARCHAR(10),
  violation_time VARCHAR(6),
  judgment_entry_date VARCHAR(10),
  amount_due DECIMAL(6,2),
  payment_amount DECIMAL(6,2),
  penalty_amount INTEGER,
  fine_amount INTEGER,
  interest_amount DECIMAL(6,2),
  reduction_amount DECIMAL(6,2)
);


LOAD DATA LOCAL INFILE 'open-violations.csv'
INTO TABLE open_violations
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n';

--
-- VIOLATION CODES
--

DROP TABLE IF EXISTS violation_codes;

CREATE TABLE violation_codes(
  code INTEGER,
  definition VARCHAR(778),
  manhattan_96th_st_below VARCHAR(39),
  all_other_areas VARCHAR(38),
  PRIMARY KEY(code)
);

LOAD DATA LOCAL INFILE 'violation-codes.csv'
INTO TABLE violation_codes
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n';


--
-- PLATE TYPES
--

DROP TABLE IF EXISTS plate_types;

CREATE TABLE plate_types(
  code CHAR(3) NOT NULL,
  description VARCHAR(27),
  PRIMARY KEY(code)
);

LOAD DATA LOCAL INFILE 'plate-types.csv'
INTO TABLE plate_types
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n';


--
-- POLICE PRECINCTS
--

DROP TABLE IF EXISTS police_precincts;

CREATE TABLE police_precincts(
  number INTEGER NOT NULL,
  name VARCHAR(32) NOT NULL,
  street_name VARCHAR(30) NOT NULL,
  borough VARCHAR(13) NOT NULL,
  PRIMARY KEY (number)
);

LOAD DATA LOCAL INFILE 'police-precincts.csv'
INTO TABLE police_precincts
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n';


