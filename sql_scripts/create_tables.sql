-- sql_scripts/create_tables.sql (CORRIGIDO com aspas duplas)

-- Apagar tabelas se existirem, para garantir a recriação limpa
DROP TABLE IF EXISTS results CASCADE;
DROP TABLE IF EXISTS races CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS constructors CASCADE;

CREATE TABLE constructors (
    "constructorId" INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE drivers (
    "driverId" INT PRIMARY KEY,
    fullname TEXT
);

CREATE TABLE races (
    "raceId" INT PRIMARY KEY,
    year INT,
    name TEXT,
    date DATE
);

CREATE TABLE results (
    "resultId" INT PRIMARY KEY,
    "raceId" INT,
    "driverId" INT,
    "constructorId" INT,
    "positionOrder" INT,
    points INT,
    "fastestLapTime" TIME,
    FOREIGN KEY ("raceId") REFERENCES races("raceId"),
    FOREIGN KEY ("driverId") REFERENCES drivers("driverId"),
    FOREIGN KEY ("constructorId") REFERENCES constructors("constructorId")
);