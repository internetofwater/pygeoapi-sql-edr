-- Copyright 2025 Lincoln Institute of Land Policy
-- SPDX-License-Identifier: MIT

-- Clean slate
DROP TABLE IF EXISTS landing_observations;
DROP TABLE IF EXISTS edr_parameters;
DROP TABLE IF EXISTS edr_locations;
DROP TABLE IF EXISTS airports;

-- Airports (define WKT geometry)
CREATE TABLE airports (
    `code` VARCHAR(10) PRIMARY KEY,
    `name` VARCHAR(100),
    `city` VARCHAR(100),
    `state` VARCHAR(2)
);

-- EDR locations use airport code directly
CREATE TABLE airport_locations (
    `id` VARCHAR(10) PRIMARY KEY,  -- same as airport code
    `label` VARCHAR(100),
    `geometry_wkt` POINT NOT NULL,
    FOREIGN KEY (id) REFERENCES airports(code)
);

-- Parameter definitions
CREATE TABLE airport_parameters (
    `id` VARCHAR(50) PRIMARY KEY,
    `name` VARCHAR(50),
    `units` VARCHAR(50),
    `description` TEXT
);

-- Observations for EDR
CREATE TABLE landing_observations (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `location_id` VARCHAR(10) NOT NULL,  -- airport code
    `time` datetime NOT NULL,
    `parameter_id` VARCHAR(50) NOT NULL,
    `value` DOUBLE NOT NULL,
    `airline` VARCHAR(100),
    FOREIGN KEY (location_id) REFERENCES airports(code),
    FOREIGN KEY (parameter_id) REFERENCES airport_parameters(id)
);

-- Airports (used as EDR locations)
INSERT INTO airports (code, name, city, state) VALUES
('DCA', 'Ronald Reagan Washington National Airport', 'Washington', 'DC'),
('IAD', 'Washington Dulles International Airport', 'Dulles', 'VA'),
('BWI', 'Baltimore/Washington International Airport', 'Baltimore', 'MD');

-- EDR locations (same IDs as airport codes)
INSERT INTO airport_locations (id, label, geometry_wkt) VALUES
('DCA', 'DCA Airport', ST_GeomFromText('POINT(-77.0377 38.8512)')),
('IAD', 'IAD Airport', ST_GeomFromText('POINT(-77.4558 38.9531)')),
('BWI', 'BWI Airport', ST_GeomFromText('POINT(-76.6684 39.1754)'));

-- Define a parameter: number of landings
INSERT INTO airport_parameters (id, name, units, description) VALUES
('landings', 'Daily plane landings', 'count', 'Number of planes landed');

-- Landing observations grouped by airport and airline
INSERT INTO landing_observations (location_id, time, parameter_id, value, airline) VALUES
('DCA', '2025-04-30', 'landings', 89, 'American Airlines'),
('DCA', '2025-05-01', 'landings', 90, 'American Airlines'),
('DCA', '2025-05-02', 'landings', 85, 'American AirLines'),
('DCA', '2025-05-03', 'landings', 87, 'American AirLines'),
('DCA', '2025-05-04', 'landings', 88, 'American AirLines'),
('IAD', '2025-05-01', 'landings', 200, 'United Airlines'),
('IAD', '2025-05-02', 'landings', 50, 'United Airlines'),
('IAD', '2025-05-03', 'landings', 303, 'United Airlines'),
('BWI', '2025-05-01', 'landings', 41, 'Southwest Airlines'),
('BWI', '2025-05-02', 'landings', 40, 'Southwest Airlines'),
('BWI', '2025-05-03', 'landings', 40, 'Southwest Airlines');
