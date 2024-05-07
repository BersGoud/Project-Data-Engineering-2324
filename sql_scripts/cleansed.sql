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
);
