-- Schema aanmaken indien het nog niet bestaat
CREATE SCHEMA IF NOT EXISTS archived;

-- Tabellen verwijderen indien ze bestaan
DROP TABLE IF EXISTS archived.aankomst, archived.banen, archived.klant, archived.luchthavens, archived.maatschappijen, archived.planning, archived.vertrek, archived.vliegtuig, archived.vliegtuigtype, archived.vlucht, archived.weer CASCADE;


CREATE TABLE archived.aankomst (
    Vluchtid CHAR(6),         -- 6 karakters lang, gebaseerd op de langste waarde 936013
    Vliegtuigcode CHAR(6),    -- 6 karakters lang, zoals VKL859
    Terminal CHAR(1),         -- 1 karakter lang, zoals A, B, C
    Gate CHAR(2),             -- 2 karakters lang, zoals C1, A2
    Baan CHAR(1),             -- 1 karakter lang, voor de baan nummers
    Bezetting CHAR(3),        -- Maximaal 3 karakters, voor getallen zoals 71
    Vracht CHAR(1),           -- Lijkt leeg te zijn in de dataset, maar reserveer 1 karakter voor consistentie
    Aankomsttijd CHAR(19)     -- Datum en tijd in het formaat "YYYY-MM-DD HH:MM:SS"
);


CREATE TABLE archived.banen (
    Baannummer CHAR(1),      -- 1 karakter lang, voor baannummers 1-6
    Code CHAR(7),            -- 7 karakters lang, zoals '18R-36L'
    Naam CHAR(30),           -- Geschatte lengte voor namen zoals 'Stépan Breedveldbaan'
    Lengte CHAR(4)           -- 4 karakters lang, voldoende voor getallen zoals 3600
);

CREATE TABLE archived.klant (
    Vluchtid CHAR(7),      -- 7 karakters lang, gebaseerd op de langste waarde 1317406
    Operatie CHAR(3),      -- 3 karakters lang, bijv. '7.2'
    Faciliteiten CHAR(3),  -- 3 karakters lang, bijv. '8.5'
    Shops CHAR(3)          -- 3 karakters lang, maximaal nodig voor getallen zoals '6.9'
);


CREATE TABLE archived.luchthavens (
    Airport CHAR(30),     -- Geschatte lengte voor namen zoals 'Bamyan Airport'
    City CHAR(20),        -- Geschatte lengte voor stadsnamen zoals 'Bamyan'
    Country CHAR(20),     -- Geschatte lengte voor landnamen zoals 'Afghanistan'
    IATA CHAR(3),         -- 3 karakters, bijv. 'BIN'
    ICAO CHAR(4),         -- 4 karakters, bijv. 'OABN'
    Lat CHAR(10),         -- Lengte voor breedtegraden zoals '34.816667'
    Lon CHAR(10),         -- Lengte voor lengtegraden zoals '67.816667'
    Alt CHAR(4),          -- 4 karakters voor hoogte zoals '2550'
    TZ CHAR(4),           -- 4 karakters voor tijdzone offset zoals '4.5'
    DST CHAR(1),          -- 1 karakter voor DST, bijv. 'N'
    TzName CHAR(20)           -- Geschatte lengte voor tijdzone namen zoals 'Asia/Kabul'
);


CREATE TABLE archived.maatschappijen (
    Name CHAR(50),     -- Voldoende lengte om namen zoals "Tom's & co airliners" te bevatten
    IATA CHAR(3),      -- 3 karakters, rekening houdend met bijzondere invoeren zoals '&T'
    ICAO CHAR(3)       -- 3 karakters, voor standaard ICAO codes, zelfs met invoeren zoals 'N/A' of '\N'
);

CREATE TABLE archived.planning (
    Vluchtnr CHAR(6),      -- 6 karakters lang, genoeg voor waarden zoals '9W2888'
    Airlinecode CHAR(2),   -- 2 karakters lang, zoals '9W'
    Destcode CHAR(3),      -- 3 karakters, standaard IATA luchthaven codes zoals 'DEL'
    Planterminal CHAR(1),  -- 1 karakter, zoals 'D'
    Plangate CHAR(2),      -- 2 karakters, zoals 'D2'
    Plantijd CHAR(8)       -- 8 karakters, ruimte voor tijden zoals '2:10 PM'
);

CREATE TABLE archived.vertrek (
    Vluchtid CHAR(6),       -- 6 karakters lang, gebaseerd op de langste waarde zoals '935995'
    Vliegtuigcode CHAR(7),  -- 7 karakters lang, zoals 'VEZY741'
    Terminal CHAR(1),       -- 1 karakter lang, zoals 'B'
    Gate CHAR(2),           -- 2 karakters lang, zoals 'B5'
    Baan CHAR(1),           -- 1 karakter lang, voor baan nummers zoals '2'
    Bezetting CHAR(3),      -- Maximaal 3 karakters, voor getallen zoals '85'
    Vracht CHAR(1),         -- 1 karakter lang, lijkt leeg te zijn in de dataset maar voor consistentie
    Vertrektijd CHAR(19)    -- Datum en tijd in het formaat "YYYY-MM-DD HH:MM:SS", zoals '2014-01-01 03:33:00'
);


CREATE TABLE archived.vliegtuig (
    Airlinecode CHAR(2),     -- 2 karakters lang, zoals 'TK'
    Vliegtuigcode CHAR(7),   -- 7 karakters lang, zoals 'VDL5829'
    Vliegtuigtype CHAR(3),   -- 3 karakters lang, voor typeaanduidingen zoals '321'
    Bouwjaar CHAR(4)         -- 4 karakters lang, voor het bouwjaar zoals '1970'
);

CREATE TABLE archived.vliegtuigtype (
    IATA CHAR(3),        -- 3 karakters lang, zoals '100'
    ICAO CHAR(4),        -- 4 karakters lang, zoals 'F100'
    Merk CHAR(20),       -- Geschatte lengte voor merknamen zoals 'British Aerospace'
    Type CHAR(30),       -- Geschatte lengte voor types zoals 'BAe 146-200 Pax'
    Wake CHAR(1),        -- 1 karakter, categorie van de wake turbulence, zoals 'M'
    Cat CHAR(3),         -- 3 karakters, categorie van het vliegtuiggebruik, zoals 'Pax'
    Capaciteit CHAR(3),  -- 3 karakters, voor capaciteit zoals '115'
    Vracht CHAR(1)       -- Voldoende voor kleine getallen zoals '5'
);

CREATE TABLE archived.vlucht (
    Vluchtid CHAR(6),        -- 6 karakters lang, zoals '935995'
    Vluchtnr CHAR(6),        -- 6 karakters lang, zoals 'EZY741'
    Airlinecode CHAR(3),     -- 3 karakters lang, zoals 'EZY'
    Destcode CHAR(3),        -- 3 karakters, voor luchthavencodes zoals 'ACE'
    Vliegtuigcode CHAR(7),   -- 7 karakters lang, zoals 'VEZY741'
    Datum CHAR(10)           -- Datum in het formaat "YYYY-MM-DD", zoals '2014-01-01'
);

CREATE TABLE archived.weer (
    Datum CHAR(10),     -- Datum in het formaat "YYYY-MM-DD"
    DDVEC CHAR(3),      -- Windrichting in graden
    FHVEC CHAR(3),      -- Uurgemiddelde windsnelheid (in 0.1 m/s)
    FG CHAR(3),         -- Daggemiddelde windsnelheid (in 0.1 m/s)
    FHX CHAR(3),        -- Maximale uurgemiddelde windsnelheid (in 0.1 m/s)
    FHXH CHAR(2),       -- Uurvak van maximale uurgemiddelde windsnelheid
    FHN CHAR(3),        -- Minimale uurgemiddelde windsnelheid (in 0.1 m/s)
    FHNH CHAR(2),       -- Uurvak van minimale uurgemiddelde windsnelheid
    FXX CHAR(3),        -- Maximale windstoot (in 0.1 m/s)
    FXXH CHAR(2),       -- Uurvak van maximale windstoot
    TG CHAR(4),         -- Daggemiddelde temperatuur (in 0.1 graden Celsius)
    TN CHAR(4),         -- Minimum temperatuur (in 0.1 graden Celsius)
    TNH CHAR(2),        -- Tijdvak van minimum temperatuur
    TX CHAR(4),         -- Maximum temperatuur (in 0.1 graden Celsius)
    TXH CHAR(2),        -- Tijdvak van maximum temperatuur
    T10N CHAR(4),       -- Minimum temperatuur op 10 cm hoogte (in 0.1 graden Celsius)
    T10NH CHAR(2),      -- Tijdvak van minimum temperatuur op 10 cm hoogte
    SQ CHAR(3),         -- Zonneschijnduur (in 0.1 uur) berekend uit globale straling
    SP CHAR(3),         -- Percentage van de langst mogelijke zonneschijnduur
    Q CHAR(4),          -- Globale straling (in J/cm2)
    DR CHAR(3),         -- Duur van de neerslag (in 0.1 uur)
    RH CHAR(4),         -- Neerslaghoeveelheid (in 0.1 mm)
    RHX CHAR(4),        -- Maximum uur-neerslaghoeveelheid (in 0.1 mm)
    RHXH CHAR(2),       -- Uurvak van maximum uur-neerslaghoeveelheid
    PG CHAR(5),         -- Daggemiddelde luchtdruk (in 0.1 hPa)
    PX CHAR(5),         -- Maximum luchtdruk (in 0.1 hPa)
    PXH CHAR(2),        -- Uurvak van maximum luchtdruk
    PN CHAR(5),         -- Minimum luchtdruk (in 0.1 hPa)
    PNH CHAR(2),        -- Uurvak van minimum luchtdruk
    VVN CHAR(3),        -- Minimum zicht
    VVNH CHAR(2),       -- Uurvak van minimum zicht
    VVX CHAR(3),        -- Maximum zicht
    VVXH CHAR(2),       -- Uurvak van maximum zicht
    NG CHAR(2),         -- Gemiddelde bewolking (in achtsten, van 0 tot 9)
    UG CHAR(3),         -- Gemiddelde relatieve vochtigheid (in procenten)
    UX CHAR(3),         -- Maximum relatieve vochtigheid (in procenten)
    UXH CHAR(2),        -- Uurvak van maximum relatieve vochtigheid
    UN CHAR(3),         -- Minimum relatieve vochtigheid (in procenten)
    UNH CHAR(2),        -- Uurvak van minimum relatieve vochtigheid
    EV2 CHAR(4)         -- Potentiële verdamping (in 0.1 mm)
);
