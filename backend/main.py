import psycopg2
#import model
import json
import datetime
import dotenv
import os
from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

dotenv.load_dotenv(dotenv.find_dotenv())

# Connect to your postgres DB
conn = psycopg2.connect(
    host=os.getenv("DBHOST","localhost"),
    port=os.getenv("DBPORT"," 5432"),
    database=os.getenv("DATABASE","postgres"),
    user=os.getenv("DBUSER","postgres"),
    password=os.getenv("PASSWORD","postgres"),
)

app = FastAPI()


@app.middleware("http")
async def rollback_failed_transactions(request, call_next):
    try:
        return await call_next(request)
    except Exception:
        # A failed SQL statement poisons the current transaction in psycopg2.
        # Roll it back so following requests can run normally.
        try:
            conn.rollback()
        except Exception:
            pass
        raise

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"]
)

## Models

class Kunde(BaseModel):
    kunde_id: int
    name: str
    telefonnummer: str
    mailadresse: str
    adresse_rechung: str
    adresse_liefer: str

class Sendung(BaseModel):
    sendung_id: int
    groesse: float
    gewicht: float
    anmerkung: str
    adresse_liefer: str
    tour_id: int
    kunde_id: int

class Fahrer(BaseModel):
    fahrer_id: int
    fuehrerschein: str
    name: str

class Fahrzeug(BaseModel):
    fahrzeug_id: int
    defekt: bool
    kennzeichen: str
    verteilungszentrum_id: int

class Verteilungszentrum(BaseModel):
    verteilungszentrum_id: int
    adresse: str
    telefonnummer: str

class Tour(BaseModel):
    tour_id: int
    tour_standart: str
    tour_zeit: str

class Sendungsverfolgung(BaseModel):
    versendet: bool
    datum: datetime.date
    sendung_id: int
    verteilungszentrum_id: int

class FahrerFaehrtTour(BaseModel):
    datum: datetime.date
    fahrer_id: int
    tour_id: int

class FahrerFaehrtFahrzeug(BaseModel):
    datum: datetime.date
    fahrer_id: int
    fahrzeug_id: int

## -------------------------------------

@app.get("/")
def read_root():
    return {"Hello": "World"}

# Open a cursor to perform database operations
cur = conn.cursor()

def ensure_frontend_views():
    """Create or refresh DB views used by the main frontend screens."""
    # Keep legacy and current tour column names compatible.
    cur.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'versand_dienstleister'
                  AND table_name = 'tour'
            ) THEN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'versand_dienstleister'
                      AND table_name = 'tour'
                      AND column_name = 'tour_standart'
                ) THEN
                    ALTER TABLE versand_dienstleister.tour
                    ADD COLUMN tour_standart varchar(1000);
                END IF;

                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'versand_dienstleister'
                      AND table_name = 'tour'
                      AND column_name = 'tour_route'
                ) THEN
                    UPDATE versand_dienstleister.tour
                    SET tour_standart = COALESCE(tour_standart, tour_route);
                END IF;
            END IF;
        END
        $$;
        """
    )

    cur.execute(
        """
        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_kunden AS
        SELECT k.kunde_id,
               k.name,
               k.telefonnummer,
               k.mailadresse,
               k.adresse_rechung,
               k.adresse_liefer,
               COUNT(s.sendung_id) AS paket_anzahl
        FROM versand_dienstleister.kunde k
        LEFT JOIN versand_dienstleister.sendung s
            ON s.kunde_id = k.kunde_id
        GROUP BY k.kunde_id, k.name, k.telefonnummer, k.mailadresse, k.adresse_rechung, k.adresse_liefer;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_fahrzeuge AS
        SELECT f.fahrzeug_id, f.kennzeichen, v.adresse, f.defekt
        FROM versand_dienstleister.fahrzeug f
        INNER JOIN versand_dienstleister.verteilungszentrum v
            ON f.verteilungszentrum_id = v.verteilungszentrum_id;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_defekte_fahrzeuge AS
        SELECT fahrzeug_id, kennzeichen, adresse
        FROM versand_dienstleister.v_frontend_fahrzeuge
        WHERE defekt = TRUE;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_fahrer_faehrt_tour AS
        SELECT ft.datum, ft.fahrer_id, f.name AS fahrer_name,
               ft.tour_id, t.tour_standart, t.tour_zeit
        FROM versand_dienstleister.fahrer_faehrt_tour ft
        INNER JOIN versand_dienstleister.fahrer f
            ON f.fahrer_id = ft.fahrer_id
        INNER JOIN versand_dienstleister.tour t
            ON t.tour_id = ft.tour_id;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_fahrer_faehrt_fahrzeug AS
        SELECT ff.datum, ff.fahrer_id, f.name AS fahrer_name,
               ff.fahrzeug_id, v.kennzeichen, v.defekt
        FROM versand_dienstleister.fahrer_faehrt_fahrzeug ff
        INNER JOIN versand_dienstleister.fahrer f
            ON f.fahrer_id = ff.fahrer_id
        INNER JOIN versand_dienstleister.fahrzeug v
            ON v.fahrzeug_id = ff.fahrzeug_id;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_touren_mit_paketen AS
        SELECT t.tour_id,
               t.tour_standart,
               t.tour_zeit,
               COUNT(s.sendung_id) AS paket_anzahl,
               COALESCE(STRING_AGG(s.sendung_id::text, ', ' ORDER BY s.sendung_id), 'Keine Pakete') AS paket_ids
        FROM versand_dienstleister.tour t
        LEFT JOIN versand_dienstleister.sendung s
            ON s.tour_id = t.tour_id
        GROUP BY t.tour_id, t.tour_standart, t.tour_zeit;

        DROP VIEW IF EXISTS versand_dienstleister.v_frontend_touren_mit_fahrzeug_und_paketen;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_touren_mit_fahrzeug_und_paketen AS
        WITH paket_agg AS (
            SELECT s.tour_id,
                   COUNT(s.sendung_id) AS paket_anzahl,
                   COALESCE(SUM(s.gewicht), 0) AS gesamtgewicht,
                   COALESCE(STRING_AGG(s.sendung_id::text, ', ' ORDER BY s.sendung_id), 'Keine Pakete') AS paket_ids
            FROM versand_dienstleister.sendung s
            GROUP BY s.tour_id
        ),
        fahrzeug_agg AS (
            SELECT ft.tour_id,
                   COALESCE(STRING_AGG(DISTINCT f.fahrzeug_id::text, ', ' ORDER BY f.fahrzeug_id::text), 'Kein Fahrzeug') AS fahrzeug_ids,
                   COALESCE(STRING_AGG(DISTINCT f.kennzeichen, ', ' ORDER BY f.kennzeichen), 'Kein Fahrzeug') AS kennzeichen
            FROM versand_dienstleister.fahrer_faehrt_tour ft
            INNER JOIN versand_dienstleister.fahrer_faehrt_fahrzeug ff
                ON ff.fahrer_id = ft.fahrer_id
               AND ff.datum = ft.datum
            INNER JOIN versand_dienstleister.fahrzeug f
                ON f.fahrzeug_id = ff.fahrzeug_id
            GROUP BY ft.tour_id
        )
        SELECT t.tour_id,
               t.tour_standart,
               t.tour_zeit,
               COALESCE(fahrzeug_agg.fahrzeug_ids, 'Kein Fahrzeug') AS fahrzeug_ids,
               COALESCE(fahrzeug_agg.kennzeichen, 'Kein Fahrzeug') AS kennzeichen,
               COALESCE(paket_agg.paket_anzahl, 0) AS paket_anzahl,
               COALESCE(paket_agg.paket_ids, 'Keine Pakete') AS paket_ids,
               COALESCE(paket_agg.gesamtgewicht, 0) AS gesamtgewicht
        FROM versand_dienstleister.tour t
        LEFT JOIN paket_agg
            ON paket_agg.tour_id = t.tour_id
        LEFT JOIN fahrzeug_agg
            ON fahrzeug_agg.tour_id = t.tour_id;

        CREATE OR REPLACE VIEW versand_dienstleister.v_frontend_sendung_count AS
        SELECT COUNT(*) AS anzahl_sendungen
        FROM versand_dienstleister.sendung;
        """
    )
    conn.commit()

try:
    ensure_frontend_views()
except Exception as e:
    conn.rollback()
    print(f"Views konnten nicht erstellt werden: {e}")

# Utils
def print_json(text):
    print(json.dumps(text, indent=2))

def to_json_liste(rows, description):
    records = []
    for row in rows:
        record = {}
        for i, column in enumerate(description):
            if type(row[i]) == datetime.date:
                record[column.name] = row[i].strftime("%Y-%m-%d")
            else:
                record[column.name] = row[i]
        records.append(record)
    return records

#APIs +++++++++++++++++++++++++++++++++++++++++++

@app.put("/admin/hard_reset")
def hard_reset():
    sql = """--sql
    DROP schema IF exists versand_dienstleister CASCADE;
    CREAT schema versand_dienstleister;"""
    cur.execute(sql)

    sql = """--sql
        CREAT table versand_dienstleister.kunde (
            kunde_id int primary key,
            name varchar(1000),
            telefonnummer varchar(20),
            mailadresse varchar(1000),
            adresse_rechung varchar(1000),
            adresse_liefer varchar(1000)
        );

        CREAT table versand_dienstleister.verteilungszentrum (
            verteilungszentrum_id int primary key,
            adresse varchar(1000),
            telefonnummer varchar(20)
        );

        CREAT table versand_dienstleister.tour (
            tour_id int primary key,
            tour_route varchar(1000),
            tour_zeit time
        );

        CREAT table versand_dienstleister.sendung (
            sendung_id int primary key,
            groesse float,
            gewicht float,
            anmerkung varchar(1000),
            adresse_liefer varchar(1000),
            tour_id int references versand_dienstleister.tour(tour_id),
            kunde_id int references versand_dienstleister.kunde(kunde_id)
        );

        CREAT table versand_dienstleister.fahrzeug (
            fahrzeug_id int primary key,
            defekt boolean,
            kennzeichen varchar(30),
            verteilungszentrum_id int references versand_dienstleister.verteilungszentrum(verteilungszentrum_id)
        );

        CREAT table versand_dienstleister.fahrer (
            fahrer_id int primary key,
            fuehrerschein varchar(1000),
            name varchar(100)
        );

        CREAT table versand_dienstleister.fahrer_faehrt_tour (
            datum date,
            fahrer_id int references versand_dienstleister.fahrer(fahrer_id),
            tour_id int references versand_dienstleister.tour(tour_id)
        );

        CREAT table versand_dienstleister.fahrer_faehrt_fahrzeug (
            datum date,
            fahrer_id int references versand_dienstleister.fahrer(fahrer_id),
            fahrzeug_id int references versand_dienstleister.fahrzeug(fahrzeug_id)
        );

        CREAT table versand_dienstleister.sendungsverfolgung (
            versendet boolean, 
            datum date,
            sendung_id int references versand_dienstleister.sendung(sendung_id),
            verteilungszentrum_id int references versand_dienstleister.verteilungszentrum(verteilungszentrum_id)
        );
    """

    cur.execute(sql)

    sql = """--sql
        INSERT INTO versand_dienstleister.verteilungszentrum (verteilungszentrum_id, adresse, telefonnummer) VALUES
        (1,'Logistikstraße 10, Berlin','030123456'),
        (2,'Industriestraße 5, Hamburg','040987654'),
        (3,'Transportweg 8, München','089555444'),
        (4,'Frachtallee 22, Köln','022112233'),
        (5,'Versandring 9, Frankfurt','069998877');

        -- Fahrer
        INSERT INTO versand_dienstleister.fahrer (fahrer_id, fuehrerschein, name) VALUES
        (1,'B1234567','Max Müller'),
        (2,'B7654321','Anna Schmidt'),
        (3,'C9876543','Lukas Weber'),
        (4,'B5554443','Sophie Fischer'),
        (5,'C1112223','Daniel Wagner'),
        (6,'B3334445','Laura Becker'),
        (7,'C6667778','Tim Hoffmann'),
        (8,'B8889990','Julia Koch'),
        (9,'C1113335','Felix Bauer'),
        (10,'B2224446','Nina Richter');

        INSERT INTO versand_dienstleister.fahrzeug (fahrzeug_id, defekt, kennzeichen, verteilungszentrum_id) VALUES
        (1,false,'B-LG-1001',1),
        (2,false,'HH-LG-2002',2),
        (3,true,'M-LG-3003',3),
        (4,false,'B-LG-1004',1),
        (5,false,'K-LG-5001',4),
        (6,false,'F-LG-6001',5),
        (7,true,'HH-LG-2005',2),
        (8,false,'M-LG-3007',3),
        (9,false,'K-LG-5010',4),
        (10,false,'F-LG-6011',5),
        (11,false,'B-LG-1012',1),
        (12,false,'HH-LG-2013',2);

        -- Kunden
        INSERT INTO versand_dienstleister.kunde (kunde_id, name, telefonnummer, mailadresse, adresse_rechung, adresse_liefer) VALUES
        (1,'TechStore GmbH','030555111','info@techstore.de','Alexanderplatz 1, Berlin','Alexanderplatz 1, Berlin'),
        (2,'Bürobedarf AG','040222333','kontakt@bueroag.de','Hafenstraße 12, Hamburg','Hafenstraße 12, Hamburg'),
        (3,'MediPharm GmbH','089777888','service@medipharm.de','Marienplatz 7, München','Marienplatz 7, München'),
        (4,'AutoParts KG','022144455','info@autoparts.de','Domstraße 3, Köln','Domstraße 3, Köln'),
        (5,'FreshFood GmbH','069666555','kontakt@freshfood.de','Zeil 20, Frankfurt','Zeil 20, Frankfurt'),
        (6,'OfficePlus','030111999','mail@officeplus.de','Potsdamer Platz 4, Berlin','Potsdamer Platz 4, Berlin'),
        (7,'IT Solutions','040555222','support@itsol.de','Speicherstadt 8, Hamburg','Speicherstadt 8, Hamburg'),
        (8,'HealthLog','089333111','contact@healthlog.de','Sendlinger Tor 5, München','Sendlinger Tor 5, München'),
        (9,'BauProfi','022188877','mail@bauprofi.de','Rheinweg 9, Köln','Rheinweg 9, Köln'),
        (10,'CityMarket','069333222','info@citymarket.de','Hauptwache 1, Frankfurt','Hauptwache 1, Frankfurt');

        -- Touren
        INSERT INTO versand_dienstleister.tour (tour_id, tour_route, tour_zeit) VALUES
        (1,'Berlin -> Potsdam -> Brandenburg','08:00:00'),
        (2,'Hamburg -> Lübeck -> Kiel','09:30:00'),
        (3,'München -> Augsburg -> Ingolstadt -> Regensburg','10:45:00'),
        (4,'Köln -> Bonn -> Düsseldorf','12:00:00'),
        (5,'Frankfurt -> Mainz -> Wiesbaden -> Darmstadt','13:30:00'),
        (6,'Stuttgart -> Ulm -> Friedrichshafen -> Konstanz','15:00:00'),
        (7,'Leipzig -> Dresden','16:30:00'),
        (8,'Nürnberg -> Bayreuth -> Bamberg -> Erlangen','18:00:00');

        -- Sendungen (viele Testdaten)
        INSERT INTO versand_dienstleister.sendung (sendung_id, groesse, gewicht, anmerkung, adresse_liefer, tour_id, kunde_id) VALUES
        (1,2,5.5,'Vorsichtig behandeln','Alexanderplatz 1, Berlin',1,1),
        (2,1,1.2,'Dokumente','Hafenstraße 12, Hamburg',2,2),
        (3,3,10.0,'Medizinische Geräte','Marienplatz 7, München',3,3),
        (4,2,4.7,'Computerteile','Alexanderplatz 1, Berlin',1,1),
        (5,1,0.5,'Briefsendung','Domstraße 3, Köln',4,4),
        (6,4,20.0,'Große Lieferung','Zeil 20, Frankfurt',5,5),
        (7,2,6.2,'Elektronik','Potsdamer Platz 4, Berlin',1,6),
        (8,3,9.5,'Serverteile','Speicherstadt 8, Hamburg',2,7),
        (9,2,7.3,'Medikamente','Sendlinger Tor 5, München',3,8),
        (10,5,25.0,'Bauteile','Rheinweg 9, Köln',4,9),
        (11,1,0.8,'Kleines Paket','Hauptwache 1, Frankfurt',5,10),
        (12,2,3.2,'Zubehör','Alexanderplatz 1, Berlin',1,1),
        (13,3,8.1,'Hardware','Speicherstadt 8, Hamburg',2,7),
        (14,2,6.4,'Laborbedarf','Sendlinger Tor 5, München',3,8),
        (15,4,15.0,'Werkzeug','Rheinweg 9, Köln',4,9),
        (16,1,0.9,'Dokumente','Zeil 20, Frankfurt',5,5),
        (17,2,5.1,'Elektronik','Alexanderplatz 1, Berlin',1,6),
        (18,3,11.2,'Computer','Hafenstraße 12, Hamburg',2,2),
        (19,2,4.9,'Pharma','Marienplatz 7, München',3,3),
        (20,4,17.5,'Baumaterial','Domstraße 3, Köln',4,4),
        (21,1,1.0,'Brief','Zeil 20, Frankfurt',5,5),
        (22,2,3.5,'Tablet','Alexanderplatz 1, Berlin',6,1),
        (23,3,7.6,'IT Zubehör','Speicherstadt 8, Hamburg',6,7),
        (24,2,6.1,'Medizin','Sendlinger Tor 5, München',6,8),
        (25,4,19.0,'Maschinenteil','Rheinweg 9, Köln',7,9),
        (26,1,1.3,'Kleinteile','Hauptwache 1, Frankfurt',7,10),
        (27,3,8.8,'Computerteile','Alexanderplatz 1, Berlin',8,6),
        (28,2,4.3,'Büromaterial','Hafenstraße 12, Hamburg',8,2),
        (29,4,14.0,'Werkzeuge','Domstraße 3, Köln',8,4),
        (30,2,6.0,'Lebensmittelprobe','Zeil 20, Frankfurt',8,5);

        INSERT INTO versand_dienstleister.fahrer_faehrt_fahrzeug (datum, fahrer_id, fahrzeug_id) VALUES
        ('2025-03-01',1,1),
        ('2025-03-01',2,2),
        ('2025-03-01',3,3),
        ('2025-03-01',4,4),
        ('2025-03-02',5,5),
        ('2025-03-02',6,6),
        ('2025-03-02',7,7),
        ('2025-03-02',8,8),
        ('2025-03-03',9,9),
        ('2025-03-03',10,10);

        -- Fahrer fährt Tour
        INSERT INTO versand_dienstleister.fahrer_faehrt_tour (datum, fahrer_id, tour_id) VALUES
        ('2025-03-01',1,1),
        ('2025-03-01',2,2),
        ('2025-03-01',3,3),
        ('2025-03-01',4,4),
        ('2025-03-02',5,5),
        ('2025-03-02',6,6),
        ('2025-03-02',7,7),
        ('2025-03-03',8,8),
        ('2025-03-03',9,1),
        ('2025-03-03',10,2);

        INSERT INTO versand_dienstleister.sendungsverfolgung (versendet, datum, sendung_id, verteilungszentrum_id) VALUES
        (true,'2025-03-01',1,1),
        (true,'2025-03-01',2,2),
        (true,'2025-03-01',3,3),
        (true,'2025-03-01',4,1),
        (true,'2025-03-02',5,4),
        (true,'2025-03-02',6,5),
        (true,'2025-03-02',7,1),
        (true,'2025-03-02',8,2),
        (true,'2025-03-02',9,3),
        (true,'2025-03-02',10,4),
        (false,'2025-03-03',11,5),
        (false,'2025-03-03',12,1),
        (false,'2025-03-03',13,2),
        (false,'2025-03-03',14,3),
        (false,'2025-03-03',15,4);
    """
    cur.execute(sql)
    ensure_frontend_views()
    print("Hardresetet")

# Kunde ------------------------------------------
@app.get("/kunde/")
def get_alle_Kunden():
    cur.execute("SELECT * FROM versand_dienstleister.v_frontend_kunden;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/kunde/{id}")
def get_Kunde_id(id: int):
    cur.execute(
        "SELECT * FROM versand_dienstleister.kunde WHERE kunde_id = %s;", (id,)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/kunde/")
def create_kunde(kunde: Kunde):
    cur.execute(
        """INSERT INTO versand_dienstleister.kunde 
            (kunde_id, name, telefonnummer, mailadresse, adresse_rechung, adresse_liefer) 
        VALUES (%s,%s,%s,%s,%s,%s);
        """, (kunde.kunde_id, kunde.name, kunde.telefonnummer, kunde.mailadresse, kunde.adresse_rechung, kunde.adresse_liefer)
    )
    conn.commit()
    return {"status": "success", "message": "Kunde created"}

@app.get("/kunde/{id}/sendungen/")
def get_Sendungen_von_Kunde(id: int):
    cur.execute(
        """SELECT s.sendung_id, s.groesse, s.gewicht, s.anmerkung
        FROM versand_dienstleister.sendung s
        WHERE s.kunde_id = %s;
    """, (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/kunde/{id}")
def update_kunde(id: int, kunde: Kunde):
    cur.execute(
        """UPDATE versand_dienstleister.kunde 
            SET name = %s, telefonnummer = %s, mailadresse = %s, adresse_rechung = %s, adresse_liefer = %s 
        WHERE kunde_id = %s;
        """, (kunde.name, kunde.telefonnummer, kunde.mailadresse, kunde.adresse_rechung, kunde.adresse_liefer, id)
    )
    conn.commit()
    return {"status": "success", "message": "Kunde updated"}

@app.delete("/kunde/{id}")
def delete_kunde(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.kunde WHERE kunde_id = %s;
        """, (id,)
    )
    conn.commit()
    return {"status": "success", "message": "Kunde deleted"}

# Sendung ------------------------------------------
@app.get("/sendung/")
def get_alle_Sendungen():
    cur.execute(
        "SELECT * FROM versand_dienstleister.sendung;"
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/{id}")
def get_Sendung_id(id: int):
    cur.execute(
        "SELECT * FROM versand_dienstleister.sendung WHERE sendung_id = %s;", (id,)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/{id}/verlauf/")
def get_sendungsverlauf(id: int):
    cur.execute(
        """SELECT sv.sendung_id,
                  sv.datum,
                  sv.versendet,
                  sv.verteilungszentrum_id,
                  v.adresse AS verteilungszentrum_adresse
           FROM versand_dienstleister.sendungsverfolgung sv
           LEFT JOIN versand_dienstleister.verteilungszentrum v
               ON v.verteilungszentrum_id = sv.verteilungszentrum_id
           WHERE sv.sendung_id = %s
           ORDER BY sv.datum DESC;""",
        (id,)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/heavy/")
def get_schwere_Sendungen():
    cur.execute(
        """SELECT *
           FROM versand_dienstleister.sendung
           WHERE gewicht > 10
           ORDER BY sendung_id;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/unverplant/")
def get_unverplante_Sendungen():
    cur.execute(
        """SELECT s.*
           FROM versand_dienstleister.sendung s
           LEFT OUTER JOIN versand_dienstleister.tour t
               ON s.tour_id = t.tour_id
           WHERE t.tour_id IS NULL
           ORDER BY s.sendung_id;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/{id}/verteilungszentrum/")
def get_Sendung_Verteilungszenter(id: int):
    cur.execute(
        """SELECT v.*
        FROM versand_dienstleister.sendung s
        INNER JOIN versand_dienstleister.verteilungszentrum v
        ON sendung.sendung_id = v.sendung_id
        WHERE sendung_id = %s;
        """, (id,)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/sendung/")
def create_sendung(sendung: Sendung):
    # tour_id 0 bedeutet keine Tour -> NULL
    tour_id = sendung.tour_id if sendung.tour_id != 0 else None
    
    cur.execute(
        """--sql
        START TRANSACTION;
        INSERT INTO versand_dienstleister.sendung
        (sendung_id, groesse, gewicht, anmerkung, adresse_liefer, tour_id, kunde_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s);
        
        INSERT INTO versand_dienstleister.sendungsverfolgung
        (sendung_id, datum, versendet)
        VALUES (%s, CURRENT_DATE, False);
        COMMIT;""", (sendung.sendung_id, sendung.groesse, sendung.gewicht, sendung.anmerkung, sendung.adresse_liefer, tour_id, sendung.kunde_id, sendung.sendung_id)
    )
    conn.commit()
    return {"status": "success", "message": "Sendung created"}

@app.put("/sendung/{id}")
def update_sendung(id: int, sendung: Sendung):
    # tour_id 0 bedeutet keine Tour -> NULL
    tour_id = sendung.tour_id if sendung.tour_id != 0 else None
    
    cur.execute(
        """UPDATE versand_dienstleister.sendung 
            SET groesse = %s, gewicht = %s, anmerkung = %s, adresse_liefer = %s, tour_id = %s, kunde_id = %s 
        WHERE sendung_id = %s;
        """, (sendung.groesse, sendung.gewicht, sendung.anmerkung, sendung.adresse_liefer, tour_id, sendung.kunde_id, id)
    )
    conn.commit()
    return {"status": "success", "message": "Sendung updated"}

@app.delete("/sendung/{id}")
def delete_sendung(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.sendung WHERE sendung_id = %s;
        """, (id,)
    )
    conn.commit()
    return {"status": "success", "message": "Sendung deleted"}

# Fahrer ------------------------------------------
@app.get("/fahrer/")
def get_alle_Fahrer():
    cur.execute("SELECT * FROM versand_dienstleister.fahrer;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrer/{id}")
def get_Fahrer_id(id: int):
    cur.execute("SELECT * FROM versand_dienstleister.fahrer WHERE fahrer_id = %s;", (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrer/{id}/tour/") # TODO: hier noch die Fahrzeug Kenzeichen
def get_Fahrer_Tour(id: int):
    cur.execute(
        """SELECT f.*, t.*
            FROM versand_dienstleister.fahrer f
            INNER JOIN versand_dienstleister.fahrer_faehrt_tour ft
            ON ft.fahrer_id = f.fahrer_id
            INNER JOIN versand_dienstleister.tour t
            ON t.tour_id = ft.tour_id
            WHERE ft.fahrer_id = %s;""", (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrer-faehrt-tour/")
def get_fahrer_faehrt_tour_liste():
    cur.execute(
        """SELECT *
             FROM versand_dienstleister.v_frontend_fahrer_faehrt_tour
             ORDER BY datum DESC, fahrer_id ASC;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrer-faehrt-fahrzeug/")
def get_fahrer_faehrt_fahrzeug_liste():
    cur.execute(
        """SELECT *
             FROM versand_dienstleister.v_frontend_fahrer_faehrt_fahrzeug
             ORDER BY datum DESC, fahrer_id ASC;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/fahrer-faehrt-fahrzeug/{datum}/{fahrer_id}/{fahrzeug_id}")
def update_fahrer_faehrt_fahrzeug(datum: str, fahrer_id: int, fahrzeug_id: int, zuordnung: FahrerFaehrtFahrzeug):
    cur.execute(
        """UPDATE versand_dienstleister.fahrer_faehrt_fahrzeug
           SET datum = %s, fahrer_id = %s, fahrzeug_id = %s
           WHERE datum = %s AND fahrer_id = %s AND fahrzeug_id = %s;""",
        (zuordnung.datum, zuordnung.fahrer_id, zuordnung.fahrzeug_id, datum, fahrer_id, fahrzeug_id)
    )
    conn.commit()
    return {"status": "success", "message": "Zuordnung aktualisiert"}

@app.delete("/fahrer-faehrt-fahrzeug/{datum}/{fahrer_id}/{fahrzeug_id}")
def delete_fahrer_faehrt_fahrzeug(datum: str, fahrer_id: int, fahrzeug_id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.fahrer_faehrt_fahrzeug
           WHERE datum = %s AND fahrer_id = %s AND fahrzeug_id = %s;""",
        (datum, fahrer_id, fahrzeug_id)
    )
    conn.commit()
    return {"status": "success", "message": "Zuordnung gelöscht"}

@app.get("/fahrer-faehrt-tour/{datum}/{fahrer_id}/{tour_id}")
def get_fahrer_faehrt_tour(datum: str, fahrer_id: int, tour_id: int):
    cur.execute(
        """SELECT *
           FROM versand_dienstleister.v_frontend_fahrer_faehrt_tour
           WHERE datum = %s AND fahrer_id = %s AND tour_id = %s;""",
        (datum, fahrer_id, tour_id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/fahrer-faehrt-tour/")
def create_fahrer_faehrt_tour(zuordnung: FahrerFaehrtTour):
        cur.execute(
                """INSERT INTO versand_dienstleister.fahrer_faehrt_tour
                     (datum, fahrer_id, tour_id)
                     VALUES (%s, %s, %s);""",
                (zuordnung.datum, zuordnung.fahrer_id, zuordnung.tour_id)
        )
        conn.commit()
        return {"status": "success", "message": "Zuordnung erstellt"}

@app.put("/fahrer-faehrt-tour/{datum}/{fahrer_id}/{tour_id}")
def update_fahrer_faehrt_tour(datum: str, fahrer_id: int, tour_id: int, zuordnung: FahrerFaehrtTour):
    cur.execute(
        """UPDATE versand_dienstleister.fahrer_faehrt_tour
           SET datum = %s, fahrer_id = %s, tour_id = %s
           WHERE datum = %s AND fahrer_id = %s AND tour_id = %s;""",
        (zuordnung.datum, zuordnung.fahrer_id, zuordnung.tour_id, datum, fahrer_id, tour_id)
    )
    conn.commit()
    return {"status": "success", "message": "Zuordnung aktualisiert"}

@app.delete("/fahrer-faehrt-tour/{datum}/{fahrer_id}/{tour_id}")
def delete_fahrer_faehrt_tour(datum: str, fahrer_id: int, tour_id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.fahrer_faehrt_tour
           WHERE datum = %s AND fahrer_id = %s AND tour_id = %s;""",
        (datum, fahrer_id, tour_id)
    )
    conn.commit()
    return {"status": "success", "message": "Zuordnung gelöscht"}

@app.post("/fahrer/")
def create_fahrer(fahrer: Fahrer):
    cur.execute(
        """INSERT INTO versand_dienstleister.fahrer
        (fahrer_id, fuehrerschein, name)
        VALUES (%s, %s, %s);
        """, (fahrer.fahrer_id, fahrer.fuehrerschein, fahrer.name)
    )
    conn.commit()
    return {"status": "success", "message": "Fahrer created"}

@app.put("/fahrer/{id}")
def update_fahrer(id: int, fahrer: Fahrer):
    cur.execute(
        """UPDATE versand_dienstleister.fahrer
        SET fuehrerschein = %s, name = %s
        WHERE fahrer_id = %s;
        """, (fahrer.fuehrerschein, fahrer.name, id)
        )
    conn.commit()
    return {"status": "success", "message": "Fahrer updated"}

@app.delete("/fahrer/{id}")
def delete_fahrer(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.fahrer WHERE fahrer_id = %s;
        """, (id,)
    )
    conn.commit()
    return {"status": "success", "message": "Fahrer deleted"}


# Fahrzeuge ------------------------------------------
@app.get("/fahrzeug/")
def get_alle_Fahrzeuge():
    cur.execute(
        """SELECT *
           FROM versand_dienstleister.v_frontend_fahrzeuge;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrzeug/{id}")
def get_Fahrzeug_id(id: int):
    cur.execute("SELECT * FROM versand_dienstleister.fahrzeug WHERE fahrzeug_id = %s;", (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrzeug/defekt/")
def get_defekte_Fahrzeuge():
    cur.execute(
        """SELECT *
           FROM versand_dienstleister.v_frontend_defekte_fahrzeuge;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/fahrzeug/")
def create_fahrzeug(fahrzeug: Fahrzeug):
    cur.execute(
        """INSERT INTO versand_dienstleister.fahrzeug 
            (fahrzeug_id, defekt, kennzeichen, verteilungszentrum_id) 
        VALUES (%s,%s,%s,%s);
        """, (fahrzeug.fahrzeug_id, fahrzeug.defekt, fahrzeug.kennzeichen, fahrzeug.verteilungszentrum_id)
    )
    conn.commit()
    return {"status": "success", "message": "Fahrzeug created"}

@app.put("/fahrzeug/{id}")
def update_fahrzeug(id: int, fahrzeug: Fahrzeug):
    cur.execute(
        """UPDATE versand_dienstleister.fahrzeug 
            SET defekt = %s, kennzeichen = %s, verteilungszentrum_id = %s 
        WHERE fahrzeug_id = %s;
        """, (fahrzeug.defekt, fahrzeug.kennzeichen, fahrzeug.verteilungszentrum_id, id)
    )
    conn.commit()
    return {"status": "success", "message": "Fahrzeug updated"}

@app.delete("/fahrzeug/{id}")
def delete_fahrzeug(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.fahrzeug WHERE fahrzeug_id = %s;
        """, (id,)
    )
    conn.commit()
    return {"status": "success", "message": "Fahrzeug deleted"}

# Touren ------------------------------------------
@app.get("/tour/")
def get_alle_Touren():
    cur.execute(
        """SELECT *
        FROM versand_dienstleister.tour t;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/tour/{id}")
def get_Tour_id(id: int):
    cur.execute("SELECT * FROM versand_dienstleister.tour WHERE tour_id = %s;", (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/tour/{id}/fahrer") # TODO: Fahrzeug Kenzeichen
def get_Tour_Fahrer(id: int):
    cur.execute(
        """SELECT t.*, f.*
            FROM versand_dienstleister.tour t
            INNER JOIN versand_dienstleister.fahrer_faehrt_tour ft
            ON ft.tour_id = t.tour_id
            INNER JOIN versand_dienstleister.fahrer f
            ON f.fahrer_id = ft.fahrer_id
            WHERE ft.tour_id = %s;""", (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/tour/mit-paketen/")
def get_touren_mit_paketen():
    cur.execute(
                """SELECT *
                     FROM versand_dienstleister.v_frontend_touren_mit_paketen
                     ORDER BY tour_id;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/tour/")
def create_tour(tour: Tour):
    cur.execute(
        """INSERT INTO versand_dienstleister.tour 
            (tour_id, tour_standart, tour_zeit) 
        VALUES (%s,%s,%s);
        """, (tour.tour_id, tour.tour_standart, tour.tour_zeit)
    )
    conn.commit()
    return {"status": "success", "message": "Tour created"}

@app.put("/tour/{id}")
def update_tour(id: int, tour: Tour):
    cur.execute(
        """UPDATE versand_dienstleister.tour 
            SET tour_standart = %s, tour_zeit = %s 
        WHERE tour_id = %s;
        """, (tour.tour_standart, tour.tour_zeit, id)
    )
    conn.commit()
    return {"status": "success", "message": "Tour updated"}

@app.delete("/tour/{id}")
def delete_tour(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.tour WHERE tour_id = %s;
        """, (id,)
    )
    conn.commit()
    return {"status": "success", "message": "Tour deleted"}

# Verteilungszentrum ---------------------
@app.get("/verteilungszentrum/")
def get_alle_Verteilungszetrum():
    cur.execute("SELECT * FROM versand_dienstleister.verteilungszentrum;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/verteilungszentrum/{id}")
def get_alle_Verteilungszetrum(id: int):
    cur.execute(
        """SELECT * 
        FROM versand_dienstleister.verteilungszentrum 
        WHERE verteilungszentrum_id = %s;""", (id,))
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/verteilungszentrum/")
def create_verteilungszentrum(verteilungszentrum: Verteilungszentrum):
    cur.execute(
        """INSERT INTO versand_dienstleister.verteilungszentrum
            (verteilungszentrum_id, adresse, telefonnummer)
        VALUES (%s,%s,%s);
        """, (verteilungszentrum.verteilungszentrum_id, verteilungszentrum.adresse, verteilungszentrum.telefonnummer)
    )
    conn.commit()
    return {"status": "success", "message": "Verteilungszentrum created"}

@app.put("/verteilungszentrum/{id}")
def update_verteilungszentrum(id: int, vzeitelungszentrum: Verteilungszentrum):
    cur.execute(
        """UPDATE versand_dienstleister.verteilungszentrum 
        SET adresse = %s, telefonnummer = %s 
        WHERE verteilungszentrum_id = %s;
        """, (vzeitelungszentrum.adresse, vzeitelungszentrum.telefonnummer, id)
    )
    conn.commit()
    return {"status": "success", "message": "Verteilungszentrum updated"}

@app.delete("/verteilungszentrum/{id}")
def delete_verteilungszentrum(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.verteilungszentrum WHERE verteilungszentrum_id = %s;
        """, (id,)
    )
    conn.commit()
    return {"status": "success", "message": "Verteilungszentrum deleted"}
# Addons -------

# Aggregation Queries
@app.get("/sendung/count/")
def get_sendung_count():
    cur.execute("SELECT anzahl_sendungen FROM versand_dienstleister.v_frontend_sendung_count;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/average-weight/")
def get_average_weight():
    cur.execute("SELECT AVG(gewicht) AS durchschnittsgewicht FROM versand_dienstleister.sendung;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/total-weight/")
def get_total_weight():
    cur.execute("SELECT SUM(gewicht) AS gesamtgewicht FROM versand_dienstleister.sendung;")
    return to_json_liste(cur.fetchall(), cur.description)

# GROUP BY Queries
@app.get("/sendung/group-by-kunde/")
def get_sendungen_per_kunde():
    cur.execute("""
        SELECT kunde_id, COUNT(*) AS anzahl_sendungen
        FROM versand_dienstleister.sendung
        GROUP BY kunde_id;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrzeug/group-by-verteilungszentrum/")
def get_fahrzeuge_per_verteilungszentrum():
    cur.execute("""
        SELECT verteilungszentrum_id, COUNT(*) AS anzahl_fahrzeuge
        FROM versand_dienstleister.fahrzeug
        GROUP BY verteilungszentrum_id;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrer/group-by-tour/")
def get_touren_per_fahrer():
    cur.execute("""
        SELECT fahrer_id, COUNT(tour_id) AS anzahl_touren
        FROM versand_dienstleister.fahrer_faehrt_tour
        GROUP BY fahrer_id;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

# JOIN + GROUP BY Queries
@app.get("/kunde/group-by-sendung/")
def get_sendungen_per_kunde_joined():
    cur.execute("""
        SELECT k.name, COUNT(s.sendung_id) AS anzahl_sendungen
        FROM versand_dienstleister.kunde k
        INNER JOIN versand_dienstleister.sendung s
        ON k.kunde_id = s.kunde_id
        GROUP BY k.name;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

# Additional useful queries
@app.get("/sendung/with-kunde/")
def get_sendungen_with_kunde():
    cur.execute("""
        SELECT s.sendung_id, s.groesse, s.gewicht, s.anmerkung, k.name as kunde_name
        FROM versand_dienstleister.sendung s
        INNER JOIN versand_dienstleister.kunde k
        ON s.kunde_id = k.kunde_id;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/with-verteilungszentrum/")
def get_sendungen_with_verteilungszentrum():
    cur.execute("""
        SELECT sv.sendung_id, v.adresse
        FROM versand_dienstleister.sendungsverfolgung sv
        INNER JOIN versand_dienstleister.verteilungszentrum v
        ON sv.verteilungszentrum_id = v.verteilungszentrum_id;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/mit-fahrzeug/")
def get_sendungen_mit_fahrzeug():
    cur.execute(
        """SELECT *
           FROM versand_dienstleister.v_frontend_touren_mit_fahrzeug_und_paketen
           ORDER BY tour_zeit, tour_id;"""
    )
    return to_json_liste(cur.fetchall(), cur.description)

# Extras ------------

# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "healthy"}

# Database connection status
@app.get("/db-status")
def db_status():
    try:
        cur.execute("SELECT 1")
        return {"database": "connected"}
    except Exception as e:
        return {"database": "disconnected", "error": str(e)}

# API Ende ++++++++++++++++++++++++++++

def get_alle_Fahrer_fahren_Tour():
    cur.execute(
        """SELECT f.name, t.tour_id
        FROM versand_dienstleister.fahrer f
        INNER JOIN versand_dienstleister.fahrer_faehrt_tour ft
        ON f.fahrer_id = ft.fahrer_id
        INNER JOIN versand_dienstleister.tour t
        ON ft.tour_id = t.tour_id;
    """
    )
    return to_json_liste(cur.fetchall(), cur.description)



def get_all_data():
    cur.execute(
        """SELECT * FROM versand_dienstleister.kunde;
        SELECT * FROM versand_dienstleister.sendung;
        SELECT * FROM versand_dienstleister.fahrer;
        SELECT * FROM versand_dienstleister.fahrzeug;
        SELECT * FROM versand_dienstleister.tour;
        SELECT * FROM versand_dienstleister.fahrer_faehrt_tour;
        SELECT * FROM versand_dienstleister.sendungsverfolgung;
    """
    )
    return to_json_liste(cur.fetchall(), cur.description)

def tests():
    # Überprüfe alle Fahrer und ihre Touren
    print("Alle Fahrer und ihre Touren:")
    print(get_alle_Fahrer_fahren_Tour())

    # Überprüfe alle Fahrzeuge
    print("\nAlle Fahrzeuge:")
    print_json(get_alle_Fahrzeuge())

    # Überprüfe alle schweren Sendungen
    print("\nSchwere Sendungen:")
    print_json(get_schwere_Sendungen())

    # Überprüfe alle Daten
    print("\nAlle Daten:")
    print_json(get_all_data())
    print_json(get_Kunde_id(1))

#if __name__ == "__main__":
#    tests()

if __name__ == "__main__":
    ensure_frontend_views()
    uvicorn.run("app:app", port=7000, reload=True)