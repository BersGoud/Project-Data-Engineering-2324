/*
CREATE SCHEMA IF NOT EXISTS cleansed;

DROP TABLE IF EXISTS cleansed.aankomst, cleansed.banen, cleansed.klant, cleansed.luchthavens, cleansed.maatschappijen, cleansed.planning, cleansed.vertrek, cleansed.vliegtuig, cleansed.vliegtuigtype, cleansed.vlucht, cleansed.weer CASCADE;


CREATE TABLE cleansed.aankomst (
    Vluchtid VARCHAR(6),
    Vliegtuigcode VARCHAR(6),
    Terminal CHAR(1),
    Gate VARCHAR(2),
    Baan CHAR(1),
    Bezetting SMALLINT,
    Vracht CHAR(1),
    Aankomsttijd TIMESTAMP
);

CREATE TABLE cleansed.banen (
    Baannummer CHAR(1),
    Code VARCHAR(7),
    Naam VARCHAR(30),
    Lengte SMALLINT
);

CREATE TABLE cleansed.luchthavens (
    Airport VARCHAR(30),
    City VARCHAR(20),
    Country VARCHAR(20),
    IATA CHAR(3),
    ICAO CHAR(4),
    Lat FLOAT,
    Lon FLOAT,
    Alt SMALLINT,
    TZ VARCHAR(4),
    DST CHAR(1),
    TzName VARCHAR(20)
);


CREATE TABLE cleansed.klant (
    Vluchtid VARCHAR(7),        -- Behoud van de lengte maar als VARCHAR voor efficiëntie
    Operatie DECIMAL(2,1),      -- Numeriek datatype voor waarden zoals '7.2' die een decimaal kunnen bevatten
    Faciliteiten DECIMAL(2,1),  -- Eveneens numeriek datatype voor scores/waarden zoals '8.5'
    Shops DECIMAL(2,1)          -- Numeriek datatype voor waarden zoals '6.9', mogelijk beoordelingen of scores
);


CREATE TABLE cleansed.maatschappijen (
    Name VARCHAR(50),     -- Gebruik VARCHAR om ruimte te besparen wanneer namen korter zijn dan 50 karakters
    IATA VARCHAR(3),      -- VARCHAR is geschikt omdat sommige IATA-codes speciale tekens kunnen bevatten
    ICAO VARCHAR(3)       -- VARCHAR wordt gebruikt omdat er inconsistente waarden zoals 'N/A' of '\N' kunnen voorkomen
);

CREATE TABLE cleansed.planning (
    Vluchtnr VARCHAR(6),      -- Gebruik VARCHAR voor mogelijke variabiliteit in lengte
    Airlinecode CHAR(2),      -- Behoud CHAR voor consistente, korte codes zoals '9W'
    Destcode CHAR(3),         -- Behoud CHAR voor standaard IATA luchthaven codes
    Planterminal CHAR(1),     -- Behoud CHAR voor enkel karakter waarden
    Plangate VARCHAR(2),      -- Gebruik VARCHAR, kan efficiënter zijn afhankelijk van variatie
    Plantijd TIME             -- Gebruik TIME datatype voor tijd, indien de tijd in 24-uurs notatie is
);

CREATE TABLE cleansed.vertrek (
    Vluchtid VARCHAR(6),       -- VARCHAR wordt gebruikt voor flexibele opslag
    Vliegtuigcode VARCHAR(7),  -- VARCHAR om ruimte te besparen, gezien lengtes kunnen variëren
    Terminal CHAR(1),          -- Behoud van CHAR voor een enkel karakter
    Gate VARCHAR(2),           -- VARCHAR voor mogelijke variatie in lengte
    Baan CHAR(1),              -- Behoud van CHAR voor consistente, enkele karakteropslag
    Bezetting SMALLINT,        -- SMALLINT voor numerieke gegevens die niet veel ruimte nodig hebben
    Vracht CHAR(1),            -- Behoud van CHAR, hoewel lijkt dat er vaak geen data is
    Vertrektijd TIMESTAMP      -- TIMESTAMP voor exacte datum en tijd opslag
);*/
-- Schema aanmaken indien het nog niet bestaat
CREATE SCHEMA IF NOT EXISTS cleansed;

-- Tabellen verwijderen indien ze bestaan
DROP TABLE IF EXISTS cleansed.aankomst, cleansed.banen, cleansed.klant, cleansed.luchthavens, cleansed.maatschappijen, cleansed.planning, cleansed.vertrek, cleansed.vliegtuig, cleansed.vliegtuigtype, cleansed.vlucht, cleansed.weer CASCADE;

CREATE TABLE cleansed.aankomst (
    "vluchtid" VARCHAR(6),
    "vliegtuigcode" VARCHAR(6),
    "terminal" CHAR(1),
    "gate" VARCHAR(2),
    "baan" CHAR(1),
    "bezetting" SMALLINT,
    "vracht" CHAR(1),
    "aankomsttijd" TIMESTAMP
);

CREATE TABLE cleansed.banen (
    "baannummer" CHAR(1),
    "code" VARCHAR(7),
    "naam" VARCHAR(30),
    "lengte" SMALLINT
);

CREATE TABLE cleansed.luchthavens (
    "airport" VARCHAR(30),
    "city" VARCHAR(20),
    "country" VARCHAR(20),
    "iata" CHAR(3),
    "icao" CHAR(4),
    "lat" FLOAT,
    "lon" FLOAT,
    "alt" SMALLINT,
    "tz" VARCHAR(4),
    "dst" CHAR(1),
    "tzname" VARCHAR(20)
);

CREATE TABLE cleansed.klant (
    "vluchtid" VARCHAR(7),
    "operatie" DECIMAL(2,1),
    "faciliteiten" DECIMAL(2,1),
    "shops" DECIMAL(2,1)
);

CREATE TABLE cleansed.maatschappijen (
    "name" VARCHAR(50),
    "iata" VARCHAR(3),
    "icao" VARCHAR(3)
);

CREATE TABLE cleansed.planning (
    "vluchtnr" VARCHAR(6),
    "airlinecode" CHAR(2),
    "destcode" CHAR(3),
    "planterminal" CHAR(1),
    "plangate" VARCHAR(2),
    "plantijd" TIME
);

CREATE TABLE cleansed.vertrek (
    "vluchtid" VARCHAR(6),
    "vliegtuigcode" VARCHAR(7),
    "terminal" CHAR(1),
    "gate" VARCHAR(2),
    "baan" CHAR(1),
    "bezetting" SMALLINT,
    "vracht" CHAR(1),
    "vertrektijd" TIMESTAMP
);

CREATE TABLE cleansed.vliegtuig (
    "airlinecode" CHAR(2),
    "vliegtuigcode" CHAR(7),
    "vliegtuigtype" CHAR(3),
    "bouwjaar" CHAR(4)
);

CREATE TABLE cleansed.vliegtuigtype (
    "iata" CHAR(3),
    "icao" CHAR(4),
    "merk" CHAR(20),
    "type" CHAR(30),
    "wake" CHAR(1),
    "cat" CHAR(3),
    "capaciteit" CHAR(3),
    "vracht" CHAR(1)
);

CREATE TABLE cleansed.vlucht (
    "vluchtid" CHAR(6),
    "vluchtnr" CHAR(6),
    "airlinecode" CHAR(3),
    "destcode" CHAR(3),
    "vliegtuigcode" CHAR(7),
    "datum" CHAR(10)
);

CREATE TABLE cleansed.weer (
    "datum" CHAR(10),
    "ddvec" CHAR(3),
    "fhvec" CHAR(3),
    "fg" CHAR(3),
    "fhx" CHAR(3),
    "fhxh" CHAR(2),
    "fhn" CHAR(3),
    "fjnh" CHAR(2),
    "fxx" CHAR(3),
    "fxxh" CHAR(2),
    "tg" CHAR(4),
    "tn" CHAR(4),
    "tnh" CHAR(2),
    "tx" CHAR(4),
    "txh" CHAR(2),
    "t10n" CHAR(4),
    "t10nh" CHAR(2),
    "sq" CHAR(3),
    "sp" CHAR(3),
    "q" CHAR(4),
    "dr" CHAR(3),
    "rh" CHAR(4),
    "rhx" CHAR(4),
    "rhxh" CHAR(2),
    "pg" CHAR(5),
    "px" CHAR(5),
    "pxh" CHAR(2),
    "pn" CHAR(5),
    "pnh" CHAR(2),
    "vvn" CHAR(3),
    "vvnf" CHAR(2),
    "vvx" CHAR(3),
    "vvxh" CHAR(2),
    "ng" CHAR(2),
    "ug" CHAR(3),
    "ux" CHAR(3),
    "uxh" CHAR(2),
    "un" CHAR(3),
    "unh" CHAR(2),
    "ev2" CHAR(4)
);