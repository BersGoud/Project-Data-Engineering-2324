-- Creëer schema indien het nog niet bestaat
CREATE SCHEMA IF NOT EXISTS dw;

-- Verwijder de tabellen indien ze bestaan
DROP TABLE IF EXISTS dw.vlucht_fct, dw.luchthaven_dim, dw.vliegtuig_dim, dw.weer_dim, dw.klant_dim, dw.maatschappij_dim CASCADE;

-- Dimensie tabel voor luchthavens
CREATE TABLE dw.luchthaven_dim (
    "airport_id" SERIAL PRIMARY KEY,
    "airport" VARCHAR(150),  -- airport
    "city" VARCHAR(150),     -- city
    "country" VARCHAR(150),  -- country
    "iata" CHAR(10),         -- iata
    "icao" CHAR(10),         -- icao
    "lat" FLOAT,             -- lat
    "lon" FLOAT,             -- lon
    "alt" SMALLINT,          -- alt
    "tz" VARCHAR(15),        -- tz
    "dst" CHAR(15),          -- dst
    "tzname" VARCHAR(50)     -- tzname
);

-- Dimensie tabel voor vliegtuigen
CREATE TABLE dw.vliegtuig_dim (
    "aircraft_id" SERIAL PRIMARY KEY,
    "airlinecode" CHAR(10),      -- airlinecode
    "vliegtuigcode" CHAR(10) UNIQUE,    -- vliegtuigcode
    "vliegtuigtype" CHAR(10),    -- vliegtuigtype
    "bouwjaar" CHAR(10),         -- bouwjaar
    "merk" CHAR(100),            -- merk
    "type" CHAR(150),            -- type
    "wake" CHAR(20),             -- wake
    "cat" CHAR(20),              -- cat
    "capaciteit" CHAR(5),        -- capaciteit
    "vracht" CHAR(5)             -- vracht
);

-- Dimensie tabel voor weerdata
CREATE TABLE dw.weer_dim (
    "weer_id" SERIAL PRIMARY KEY,
    "datum" CHAR(10) UNIQUE,
    "ddvec" CHAR(10),
    "fhvec" CHAR(10),
    "fg" CHAR(10),
    "fhx" CHAR(10),
    "fhxh" CHAR(10),
    "fhn" CHAR(10),
    "fhnh" CHAR(10),
    "fxx" CHAR(10),
    "fxxh" CHAR(10),
    "tg" CHAR(10),
    "tn" CHAR(10),
    "tnh" CHAR(10),
    "tx" CHAR(10),
    "txh" CHAR(10),
    "t10n" CHAR(10),
    "t10nh" CHAR(10),
    "sq" CHAR(10),
    "sp" CHAR(10),
    "q" CHAR(10),
    "dr" CHAR(10),
    "rh" CHAR(10),
    "rhx" CHAR(10),
    "rhxh" CHAR(10),
    "pg" CHAR(10),
    "px" CHAR(10),
    "pxh" CHAR(10),
    "pn" CHAR(10),
    "pnh" CHAR(10),
    "vvn" CHAR(10),
    "vvnh" CHAR(10),
    "vvx" CHAR(10),
    "vvxh" CHAR(10),
    "ng" CHAR(10),
    "ug" CHAR(10),
    "ux" CHAR(10),
    "uxh" CHAR(10),
    "un" CHAR(10),
    "unh" CHAR(10),
    "ev2" CHAR(10)
);

-- Dimensie tabel voor luchtvaartmaatschappijen
CREATE TABLE dw.maatschappij_dim (
    "name" VARCHAR(50),                -- name
    "iata" VARCHAR(3) UNIQUE,                 -- iata
    "icao" VARCHAR(3)                  -- icao
);

-- Feiten tabel
CREATE TABLE dw.vlucht_fct (
    "vluchtid" SERIAL PRIMARY KEY,
    "vluchtnr" VARCHAR(20),  -- vluchtnr
    "maatschappij_id" VARCHAR(3),     -- airlinecode
    "destcode" VARCHAR(3),    -- destcode
    "vliegtuigcode" VARCHAR(15),  -- vliegtuigcode
    "bezetting" SMALLINT,     -- bezetting
    "vracht" CHAR(5),         -- vracht
    "aankomsttijd" TIMESTAMP, -- aankomsttijd
    "vertrektijd" TIMESTAMP,  -- vertrektijd
    "weer_id" INTEGER,
    "dest_luchthaven_id" INTEGER,
    FOREIGN KEY ("maatschappij_id") REFERENCES dw.maatschappij_dim("iata"),
    FOREIGN KEY ("vliegtuigcode") REFERENCES dw.vliegtuig_dim("vliegtuigcode"),
    FOREIGN KEY ("weer_id") REFERENCES dw.weer_dim("weer_id"),
    FOREIGN KEY ("dest_luchthaven_id") REFERENCES dw.luchthaven_dim("airport_id")
);

-- Dimensie tabel voor klanttevredenheid
CREATE TABLE dw.klant_dim (
    "customer_id" SERIAL PRIMARY KEY,
    "vluchtid" INTEGER,            -- vluchtid
    "operatie" DECIMAL(2,1),           -- operatie
    "faciliteiten" DECIMAL(2,1),       -- faciliteiten
    "shops" DECIMAL(2,1),              -- shops
    FOREIGN KEY ("vluchtid") REFERENCES dw.vlucht_fct("vluchtid")
);