-- Schema aanmaken indien het nog niet bestaat
CREATE SCHEMA IF NOT EXISTS raw;

-- Tabellen verwijderen indien ze bestaan
DROP TABLE IF EXISTS raw.aankomst, raw.banen, raw.klant, raw.luchthavens, raw.maatschappijen, raw.planning, raw.vertrek, raw.vliegtuig, raw.vliegtuigtype, raw.vlucht, raw.weer CASCADE;

CREATE TABLE raw.aankomst (
    "vluchtid" VARCHAR(10),        
    "vliegtuigcode" VARCHAR(8),    
    "terminal" VARCHAR(1),         
    "gate" VARCHAR(2),             
    "baan" VARCHAR(1),             
    "bezetting" VARCHAR(3),        
    "vracht" VARCHAR(3),           
    "aankomsttijd" VARCHAR(19)     
);

CREATE TABLE raw.banen (
    "baannummer" VARCHAR(1),      
    "code" VARCHAR(7),            
    "naam" VARCHAR(30),           
    "lengte" VARCHAR(4)           
);

CREATE TABLE raw.klant (
    "vluchtid" VARCHAR(7),      
    "operatie" VARCHAR(3),      
    "faciliteiten" VARCHAR(3),  
    "shops" VARCHAR(3)          
);

CREATE TABLE raw.luchthavens (
    "airport" VARCHAR(150),     
    "city" VARCHAR(150),        
    "country" VARCHAR(150),     
    "iata" VARCHAR(10),         
    "icao" VARCHAR(10),         
    "lat" VARCHAR(15),         
    "lon" VARCHAR(15),         
    "alt" VARCHAR(15),          
    "tz" VARCHAR(15),           
    "dst" VARCHAR(15),          
    "tzname" VARCHAR(50)       
);

CREATE TABLE raw.maatschappijen (
    "name" VARCHAR(50),     
    "iata" VARCHAR(3),      
    "icao" VARCHAR(3)       
);

CREATE TABLE raw.planning (
    "vluchtnr" VARCHAR(15),      
    "airlinecode" VARCHAR(5),   
    "destcode" VARCHAR(3),      
    "planterminal" VARCHAR(1),  
    "plangate" VARCHAR(5),      
    "plantijd" VARCHAR(8)       
);

CREATE TABLE raw.vertrek (
    "vluchtid" VARCHAR(15),       
    "vliegtuigcode" VARCHAR(15),  
    "terminal" VARCHAR(5),       
    "gate" VARCHAR(5),           
    "baan" VARCHAR(5),           
    "bezetting" VARCHAR(55),      
    "vracht" VARCHAR(5),         
    "vertrektijd" VARCHAR(20)    
);

CREATE TABLE raw.vliegtuig (
    "airlinecode" VARCHAR(10),     
    "vliegtuigcode" VARCHAR(10),   
    "vliegtuigtype" VARCHAR(10),   
    "bouwjaar" VARCHAR(10)         
);

CREATE TABLE raw.vliegtuigtype (
    "iata" VARCHAR(15),        
    "icao" VARCHAR(15),        
    "merk" VARCHAR(100),       
    "type" VARCHAR(150),       
    "wake" VARCHAR(20),        
    "cat" VARCHAR(20),         
    "capaciteit" VARCHAR(5),  
    "vracht" VARCHAR(5)       
);

CREATE TABLE raw.vlucht (
    "vluchtid" VARCHAR(10),       
    "vluchtnr" VARCHAR(10),       
    "airlinecode" VARCHAR(5),    
    "destcode" VARCHAR(5),       
    "vliegtuigcode" VARCHAR(15),  
    "datum" VARCHAR(10)          
);

CREATE TABLE raw.weer (
    "datum" VARCHAR(10),     
    "ddvec" VARCHAR(10),      
    "fhvec" VARCHAR(10),      
    "fg" VARCHAR(10),         
    "fhx" VARCHAR(10),        
    "fhxh" VARCHAR(10),       
    "fhn" VARCHAR(10),        
    "fhnh" VARCHAR(10),       
    "fxx" VARCHAR(10),        
    "fxxh" VARCHAR(10),       
    "tg" VARCHAR(10),         
    "tn" VARCHAR(10),         
    "tnh" VARCHAR(10),        
    "tx" VARCHAR(10),         
    "txh" VARCHAR(10),        
    "t10n" VARCHAR(10),       
    "t10nh" VARCHAR(10),      
    "sq" VARCHAR(10),         
    "sp" VARCHAR(10),         
    "q" VARCHAR(10),          
    "dr" VARCHAR(10),         
    "rh" VARCHAR(10),         
    "rhx" VARCHAR(10),        
    "rhxh" VARCHAR(10),       
    "pg" VARCHAR(10),         
    "px" VARCHAR(10),         
    "pxh" VARCHAR(10),        
    "pn" VARCHAR(10),         
    "pnh" VARCHAR(10),        
    "vvn" VARCHAR(10),        
    "vvnh" VARCHAR(10),       
    "vvx" VARCHAR(10),        
    "vvxh" VARCHAR(10),       
    "ng" VARCHAR(10),         
    "ug" VARCHAR(10),         
    "ux" VARCHAR(10),         
    "uxh" VARCHAR(10),        
    "un" VARCHAR(10),         
    "unh" VARCHAR(10),        
    "ev2" VARCHAR(10)         
);
