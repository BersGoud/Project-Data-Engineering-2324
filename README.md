# Project-Data-Engineering-2324

## Gemaakt door Bers Goudantov, 2ITAI

### Overzicht

Dit project is een data-engineering toepassing die gebruik maakt van AWS, PostgreSQL, en diverse data analysetools om een complete end-to-end data pipeline te bieden via prefect. Hieronder volgen de instructies voor het opzetten en uitvoeren van het project.

### Installatie en Configuratie

1. **Container Starten**

   - Start de container via de `.devcontainer` map, **niet** via `docker-compose.yml` in de root.

2. **AWS Credentials**

   - Update je `~/.aws/credentials` bestand om de juiste AWS-toegangsgegevens te bevatten.

3. **Project Uitvoeren**

   - Voer het project uit via `main.py`.

4. **AWS Crawler**
   - Voer de bestaande AWS crawler uit om Parquet-bestanden te importeren naar Athena.
   - Verwijder de `unsaved` folder indien deze aanwezig is.

### Toegang en Beheer

1. **PGAdmin**

   - Toegang tot PGAdmin via: [http://localhost:8888](http://localhost:8888)
   - Standaard inloggegevens:
     - E-mail: jouw e-mailadres
     - Wachtwoord: `Newpassword`
   - Deze gegevens kunnen worden aangepast in `docker-compose.yml` in de `.devcontainer` map.

2. **Server Toevoegen in PGAdmin**
   - Ga naar `Page general` en stel `Name` in op `postgres`.
   - Ga naar `Page connection` en stel de volgende gegevens in:
     - `Hostname/Address`: `db`
     - `Username`: `postgres`
     - `Password`: `Newpassword`

### Dashboard en Data-analyse

- De map `Dashboard BI` bevat het dashboard notebook voor Business Intelligence (BI).
- Zorg ervoor dat de AWS crawler is uitgevoerd om de Parquet-bestanden te importeren naar Athena voordat je de BI-dashboard start.

### Notes

- Zorg ervoor dat je de container start vanuit de `.devcontainer` map en niet de `docker-compose.yml` in de root.
- Update je `~/.aws/credentials` met de juiste AWS-toegangsgegevens.
- Voer het project uit via `main.py` en zorg ervoor dat je de AWS crawler hebt uitgevoerd om Parquet-bestanden te importeren naar Athena.
- Verwijder de `unsaved` folder in de s3 bucket indien deze aanwezig is om conflicten te vermijden met AWS Glue crawler.
- Voer ook requirements.txt uit.

---
