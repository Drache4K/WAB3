import psycopg2
#import model
import json
import datetime
import dotenv
import os
from fastapi import FastAPI
import uvicorn

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
    tour_stopps: int
    tour_zeit: str

class Sendungsverfolgung(BaseModel):
    versendet: bool
    datum: datetime.date
    sendung_id: int
    verteilungszentrum_id: int

## -------------------------------------

@app.get("/")
def read_root():
    return {"Hello": "World"}

# Open a cursor to perform database operations
cur = conn.cursor()
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
            tour_stopps int,
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
        INSERT INTO versand_dienstleister.tour (tour_id, tour_stopps, tour_zeit) VALUES
        (1,5,'08:00:00'),
        (2,3,'09:30:00'),
        (3,7,'10:45:00'),
        (4,4,'12:00:00'),
        (5,6,'13:30:00'),
        (6,8,'15:00:00'),
        (7,2,'16:30:00'),
        (8,5,'18:00:00');

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
    print("Hardresetet")

# Kunde ------------------------------------------
@app.get("/kunde/")
def get_alle_Kunden():
    cur.execute("SELECT * FROM versand_dienstleister.kunde;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/kunde/{id}")
def get_Kunde_id(id: int):
    cur.execute(
        "SELECT * FROM versand_dienstleister.kunde WHERE kunde_id = %s;", str(id)
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
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/kunde/{id}")
def update_kunde(id: int, kunde: Kunde):
    cur.execute(
        """UPDATE versand_dienstleister.kunde 
            SET name = %s, telefonnummer = %s, mailadresse = %s, adresse_rechung = %s, adresse_liefer = %s 
        WHERE kunde_id = %s;
        """, (kunde.name, kunde.telefonnummer, kunde.mailadresse, kunde.adresse_rechung, kunde.adresse_liefer, id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.delete("/kunde/{id}")
def delete_kunde(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.kunde WHERE kunde_id = %s;
        """, (id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

# Sendung ------------------------------------------
@app.get("/sendung/")
def get_alle_Sendungen():
    cur.execute(
        "SELECT * FROM versand_dienstleister.sendung;"
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/{id}")
def get_Sendung_id(id):
    cur.execute(
        "SELECT * FROM versand_dienstleister.sendung WHERE sendung_id = %s;", str(id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/heavy/")
def get_schwere_Sendungen():
    cur.execute(
        """SELECT sendung_id, gewicht
        FROM versand_dienstleister.sendung
        WHERE gewicht > 10;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/sendung/{id}/verteilungszentrum/")
def get_Sendung_Verteilungszenter(id):
    cur.execute(
        """SELECT v.*
        FROM versand_dienstleister.sendung s
        INNER JOIN versand_dienstleister.verteilungszentrum v
        ON sendung.sendung_id = v.sendung_id
        WHERE sendung_id = %s;
        """, str(id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/sendung/")
def create_sendung(sendung: Sendung):
    cur.execute(
        """INSERT INTO versand_dienstleister.sendung 
            (sendung_id, groesse, gewicht, anmerkung, adresse_liefer, tour_id, kunde_id) 
        VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (sendung.sendung_id, sendung.groesse, sendung.gewicht, sendung.anmerkung, sendung.adresse_liefer, sendung.tour_id, sendung.kunde_id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/sendung/{id}")
def update_sendung(id: int, sendung: Sendung):
    cur.execute(
        """UPDATE versand_dienstleister.sendung 
            SET groesse = %s, gewicht = %s, anmerkung = %s, adresse_liefer = %s, tour_id = %s, kunde_id = %s 
        WHERE sendung_id = %s;
        """, (sendung.groesse, sendung.gewicht, sendung.anmerkung, sendung.adresse_liefer, sendung.tour_id, sendung.kunde_id, id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.delete("/sendung/{id}")
def delete_sendung(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.sendung WHERE sendung_id = %s;
        """, (id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

# Fahrer ------------------------------------------
@app.get("/fahrer/")
def get_alle_Fahrer():
    cur.execute("SELECT * FROM versand_dienstleister.fahrer;")
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrer/{id}")
def get_Fahrer_id(id):
    cur.execute("SELECT * FROM versand_dienstleister.fahrer WHERE fahrer_id = %s;", str(id))
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
            WHERE ft.fahrer_id = %s;""", str(id))
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/fahrer/")
def create_fahrer(fahrer: Fahrer):
    cur.execute(
        """INSERT INTO versand_dienstleister.fahrer
        (fahrer_id, fuehrerschein, name)
        VALUES (%s,%s,%s);
        """, (fahrer.fahrer_id, fahrer.fuehrerschein, fahrer.name)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/fahrer/{id}")
def update_fahrer(id: int, fahrer: Fahrer):
    cur.execute(
        """UPDATE versand_dienstleister.fahrer
        SET fuehrerschein = %s, name = %s
        WHERE fahrer_id = %s;
        """, (fahrer.fuehrerschein, fahrer.name, id)
        )
    return to_json_liste(cur.fetchall(), cur.description)

@app.delete("/fahrer/{id}")
def delete_fahrer(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.fahrer WHERE fahrer_id = %s;
        """, (id)
    )
    return to_json_liste(cur.fetchall(), cur.description)


# Fahrzeuge ------------------------------------------
@app.get("/fahrzeug/")
def get_alle_Fahrzeuge():
    cur.execute(
        """SELECT f.fahrzeug_id, f.kennzeichen, v.adresse, f.defekt
        FROM versand_dienstleister.fahrzeug f
        INNER JOIN versand_dienstleister.verteilungszentrum v
        ON f.verteilungszentrum_id = v.verteilungszentrum_id;
    """
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/fahrzeug/{id}")
def get_Fahrzeug_id(id):
    cur.execute("SELECT * FROM versand_dienstleister.fahrzeug WHERE fahrzeug_id = %s;", str(id))
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/fahrzeug/")
def create_fahrzeug(fahrzeug: Fahrzeug):
    cur.execute(
        """INSERT INTO versand_dienstleister.fahrzeug 
            (fahrzeug_id, defekt, kennzeichen, verteilungszentrum_id) 
        VALUES (%s,%s,%s,%s);
        """, (fahrzeug.fahrzeug_id, fahrzeug.defekt, fahrzeug.kennzeichen, fahrzeug.verteilungszentrum_id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/fahrzeug/{id}")
def update_fahrzeug(id: int, fahrzeug: Fahrzeug):
    cur.execute(
        """UPDATE versand_dienstleister.fahrzeug 
            SET defekt = %s, kennzeichen = %s, verteilungszentrum_id = %s 
        WHERE fahrzeug_id = %s;
        """, (fahrzeug.defekt, fahrzeug.kennzeichen, fahrzeug.verteilungszentrum_id, id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.delete("/fahrzeug/{id}")
def delete_fahrzeug(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.fahrzeug WHERE fahrzeug_id = %s;
        """, (id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

# Touren ------------------------------------------
@app.get("/tour/")
def get_alle_Touren():
    cur.execute(
        """SELECT *
        FROM versand_dienstleister.tour t;
    """)
    return to_json_liste(cur.fetchall(), cur.description)

@app.get("/tour/{id}")
def get_Tour_id(id):
    cur.execute("SELECT * FROM versand_dienstleister.tour WHERE tour_id = %s;", str(id))
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
            WHERE ft.tour_id = %s;""", str(id))
    return to_json_liste(cur.fetchall(), cur.description)

@app.post("/tour/")
def create_tour(tour: Tour):
    cur.execute(
        """INSERT INTO versand_dienstleister.tour 
            (tour_id, tour_stopps, tour_zeit) 
        VALUES (%s,%s,%s);
        """, (tour.tour_id, tour.tour_stopps, tour.tour_zeit)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/tour/{id}")
def update_tour(id: int, tour: Tour):
    cur.execute(
        """UPDATE versand_dienstleister.tour 
            SET tour_stopps = %s, tour_zeit = %s 
        WHERE tour_id = %s;
        """, (tour.tour_stopps, tour.tour_zeit, id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.delete("/tour/{id}")
def delete_tour(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.tour WHERE tour_id = %s;
        """, (id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

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
        WHERE verteilungszentrum_id = %s;""", str(id))
    return to_json_liste(cur.fetchall(), cur.description)

@app.put("/verteilungszentrum/{id}")
def update_verteilungszentrum(id: int, vzeitelungszentrum: Verteilungszentrum):
    cur.execute(
        """UPDATE versand_dienstleister.verteilungszentrum 
        SET adresse = %s, telefonnummer = %s 
        WHERE verteilungszentrum_id = %s;
        """, (vzeitelungszentrum.adresse, vzeitelungszentrum.telefonnummer, id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

@app.delete("/verteilungszentrum/{id}")
def delete_verteilungszentrum(id: int):
    cur.execute(
        """DELETE FROM versand_dienstleister.verteilungszentrum WHERE verteilungszentrum_id = %s;
        """, (id)
    )
    return to_json_liste(cur.fetchall(), cur.description)

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
    uvicorn.run("app:app", port=7000, reload=True)