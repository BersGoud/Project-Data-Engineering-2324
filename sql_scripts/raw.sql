-- Schema aanmaken indien het nog niet bestaat
CREATE SCHEMA IF NOT EXISTS raw;

-- Tabellen verwijderen indien ze bestaan
DROP TABLE IF EXISTS raw.aankomst, raw.banen, raw.klant, raw.luchthavens, raw.maatschappijen, raw.planning, raw.vertrek, raw.vliegtuig, raw.vliegtuigtype, raw.vlucht, raw.weer CASCADE;


CREATE TABLE raw.aankomst (
    "Vluchtid" VARCHAR(10),         -- 6 karakters lang, gebaseerd op de langste waarde 936013
    "Vliegtuigcode" VARCHAR(8),    -- 6 karakters lang, zoals VKL859
    "Terminal" VARCHAR(1),         -- 1 karakter lang, zoals A, B, C
    "Gate" VARCHAR(2),             -- 2 karakters lang, zoals C1, A2
    "Baan" VARCHAR(1),             -- 1 karakter lang, voor de baan nummers
    "Bezetting" VARCHAR(3),        -- Maximaal 3 karakters, voor getallen zoals 71
    "Vracht" VARCHAR(3),           -- Lijkt leeg te zijn in de dataset, maar reserveer 1 karakter voor consistentie
    "Aankomsttijd" VARCHAR(19)     -- Datum en tijd in het formaat "YYYY-MM-DD HH:MM:SS"
);


CREATE TABLE raw.banen (
    Baannummer VARCHAR(1),      -- 1 karakter lang, voor baannummers 1-6
    Code VARCHAR(7),            -- 7 karakters lang, zoals '18R-36L'
    Naam VARCHAR(30),           -- Geschatte lengte voor namen zoals 'Stépan Breedveldbaan'
    Lengte VARCHAR(4)           -- 4 karakters lang, voldoende voor getallen zoals 3600
);

CREATE TABLE raw.klant (
    Vluchtid VARCHAR(7),      -- 7 karakters lang, gebaseerd op de langste waarde 1317406
    Operatie VARCHAR(3),      -- 3 karakters lang, bijv. '7.2'
    Faciliteiten VARCHAR(3),  -- 3 karakters lang, bijv. '8.5'
    Shops VARCHAR(3)          -- 3 karakters lang, maximaal nodig voor getallen zoals '6.9'
);


CREATE TABLE raw.luchthavens (
    Airport VARCHAR(30),     -- Geschatte lengte voor namen zoals 'Bamyan Airport'
    City VARCHAR(20),        -- Geschatte lengte voor stadsnamen zoals 'Bamyan'
    Country VARCHAR(20),     -- Geschatte lengte voor landnamen zoals 'Afghanistan'
    IATA VARCHAR(3),         -- 3 karakters, bijv. 'BIN'
    ICAO VARCHAR(4),         -- 4 karakters, bijv. 'OABN'
    Lat VARCHAR(10),         -- Lengte voor breedtegraden zoals '34.816667'
    Lon VARCHAR(10),         -- Lengte voor lengtegraden zoals '67.816667'
    Alt VARCHAR(4),          -- 4 karakters voor hoogte zoals '2550'
    TZ VARCHAR(4),           -- 4 karakters voor tijdzone offset zoals '4.5'
    DST VARCHAR(1),          -- 1 karakter voor DST, bijv. 'N'
    TzName VARCHAR(20)           -- Geschatte lengte voor tijdzone namen zoals 'Asia/Kabul'
);


CREATE TABLE raw.maatschappijen (
    Name VARCHAR(50),     -- Voldoende lengte om namen zoals "Tom's & co airliners" te bevatten
    IATA VARCHAR(3),      -- 3 karakters, rekening houdend met bijzondere invoeren zoals '&T'
    ICAO VARCHAR(3)       -- 3 karakters, voor standaard ICAO codes, zelfs met invoeren zoals 'N/A' of '\N'
);

CREATE TABLE raw.planning (
    Vluchtnr VARCHAR(6),      -- 6 karakters lang, genoeg voor waarden zoals '9W2888'
    Airlinecode VARCHAR(2),   -- 2 karakters lang, zoals '9W'
    Destcode VARCHAR(3),      -- 3 karakters, standaard IATA luchthaven codes zoals 'DEL'
    Planterminal VARCHAR(1),  -- 1 karakter, zoals 'D'
    Plangate VARCHAR(2),      -- 2 karakters, zoals 'D2'
    Plantijd VARCHAR(8)       -- 8 karakters, ruimte voor tijden zoals '2:10 PM'
);

CREATE TABLE raw.vertrek (
    Vluchtid VARCHAR(6),       -- 6 karakters lang, gebaseerd op de langste waarde zoals '935995'
    Vliegtuigcode VARCHAR(7),  -- 7 karakters lang, zoals 'VEZY741'
    Terminal VARCHAR(1),       -- 1 karakter lang, zoals 'B'
    Gate VARCHAR(2),           -- 2 karakters lang, zoals 'B5'
    Baan VARCHAR(1),           -- 1 karakter lang, voor baan nummers zoals '2'
    Bezetting VARCHAR(3),      -- Maximaal 3 karakters, voor getallen zoals '85'
    Vracht VARCHAR(1),         -- 1 karakter lang, lijkt leeg te zijn in de dataset maar voor consistentie
    Vertrektijd VARCHAR(19)    -- Datum en tijd in het formaat "YYYY-MM-DD HH:MM:SS", zoals '2014-01-01 03:33:00'
);


CREATE TABLE raw.vliegtuig (
    Airlinecode VARCHAR(2),     -- 2 karakters lang, zoals 'TK'
    Vliegtuigcode VARCHAR(7),   -- 7 karakters lang, zoals 'VDL5829'
    Vliegtuigtype VARCHAR(3),   -- 3 karakters lang, voor typeaanduidingen zoals '321'
    Bouwjaar VARCHAR(4)         -- 4 karakters lang, voor het bouwjaar zoals '1970'
);

CREATE TABLE raw.vliegtuigtype (
    IATA VARCHAR(3),        -- 3 karakters lang, zoals '100'
    ICAO VARCHAR(4),        -- 4 karakters lang, zoals 'F100'
    Merk VARCHAR(20),       -- Geschatte lengte voor merknamen zoals 'British Aerospace'
    Type VARCHAR(30),       -- Geschatte lengte voor types zoals 'BAe 146-200 Pax'
    Wake VARCHAR(1),        -- 1 karakter, categorie van de wake turbulence, zoals 'M'
    Cat VARCHAR(3),         -- 3 karakters, categorie van het vliegtuiggebruik, zoals 'Pax'
    Capaciteit VARCHAR(3),  -- 3 karakters, voor capaciteit zoals '115'
    Vracht VARCHAR(1)       -- Voldoende voor kleine getallen zoals '5'
);

CREATE TABLE raw.vlucht (
    Vluchtid VARCHAR(6),        -- 6 karakters lang, zoals '935995'
    Vluchtnr VARCHAR(6),        -- 6 karakters lang, zoals 'EZY741'
    Airlinecode VARCHAR(3),     -- 3 karakters lang, zoals 'EZY'
    Destcode VARCHAR(3),        -- 3 karakters, voor luchthavencodes zoals 'ACE'
    Vliegtuigcode VARCHAR(7),   -- 7 karakters lang, zoals 'VEZY741'
    Datum VARCHAR(10)           -- Datum in het formaat "YYYY-MM-DD", zoals '2014-01-01'
);

CREATE TABLE raw.weer (
    Datum VARCHAR(10),     -- Datum in het formaat "YYYY-MM-DD"
    DDVEC VARCHAR(3),      -- Windrichting in graden
    FHVEC VARCHAR(3),      -- Uurgemiddelde windsnelheid (in 0.1 m/s)
    FG VARCHAR(3),         -- Daggemiddelde windsnelheid (in 0.1 m/s)
    FHX VARCHAR(3),        -- Maximale uurgemiddelde windsnelheid (in 0.1 m/s)
    FHXH VARCHAR(2),       -- Uurvak van maximale uurgemiddelde windsnelheid
    FHN VARCHAR(3),        -- Minimale uurgemiddelde windsnelheid (in 0.1 m/s)
    FHNH VARCHAR(2),       -- Uurvak van minimale uurgemiddelde windsnelheid
    FXX VARCHAR(3),        -- Maximale windstoot (in 0.1 m/s)
    FXXH VARCHAR(2),       -- Uurvak van maximale windstoot
    TG VARCHAR(4),         -- Daggemiddelde temperatuur (in 0.1 graden Celsius)
    TN VARCHAR(4),         -- Minimum temperatuur (in 0.1 graden Celsius)
    TNH VARCHAR(2),        -- Tijdvak van minimum temperatuur
    TX VARCHAR(4),         -- Maximum temperatuur (in 0.1 graden Celsius)
    TXH VARCHAR(2),        -- Tijdvak van maximum temperatuur
    T10N VARCHAR(4),       -- Minimum temperatuur op 10 cm hoogte (in 0.1 graden Celsius)
    T10NH VARCHAR(2),      -- Tijdvak van minimum temperatuur op 10 cm hoogte
    SQ VARCHAR(3),         -- Zonneschijnduur (in 0.1 uur) berekend uit globale straling
    SP VARCHAR(3),         -- Percentage van de langst mogelijke zonneschijnduur
    Q VARCHAR(4),          -- Globale straling (in J/cm2)
    DR VARCHAR(3),         -- Duur van de neerslag (in 0.1 uur)
    RH VARCHAR(4),         -- Neerslaghoeveelheid (in 0.1 mm)
    RHX VARCHAR(4),        -- Maximum uur-neerslaghoeveelheid (in 0.1 mm)
    RHXH VARCHAR(2),       -- Uurvak van maximum uur-neerslaghoeveelheid
    PG VARCHAR(5),         -- Daggemiddelde luchtdruk (in 0.1 hPa)
    PX VARCHAR(5),         -- Maximum luchtdruk (in 0.1 hPa)
    PXH VARCHAR(2),        -- Uurvak van maximum luchtdruk
    PN VARCHAR(5),         -- Minimum luchtdruk (in 0.1 hPa)
    PNH VARCHAR(2),        -- Uurvak van minimum luchtdruk
    VVN VARCHAR(3),        -- Minimum zicht
    VVNH VARCHAR(2),       -- Uurvak van minimum zicht
    VVX VARCHAR(3),        -- Maximum zicht
    VVXH VARCHAR(2),       -- Uurvak van maximum zicht
    NG VARCHAR(2),         -- Gemiddelde bewolking (in achtsten, van 0 tot 9)
    UG VARCHAR(3),         -- Gemiddelde relatieve vochtigheid (in procenten)
    UX VARCHAR(3),         -- Maximum relatieve vochtigheid (in procenten)
    UXH VARCHAR(2),        -- Uurvak van maximum relatieve vochtigheid
    UN VARCHAR(3),         -- Minimum relatieve vochtigheid (in procenten)
    UNH VARCHAR(2),        -- Uurvak van minimum relatieve vochtigheid
    EV2 VARCHAR(4)         -- Potentiële verdamping (in 0.1 mm)
);
