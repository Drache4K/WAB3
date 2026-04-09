import json

class Kunde:
    def __init__(self, kunde_id, name, telefonnummer, mailadresse, adresse_rechung, adresse_liefer):
        self.kunde_id = kunde_id
        self.name = name
        self.telefonnummer = telefonnummer
        self.mailadresse = mailadresse
        self.adresse_rechung = adresse_rechung
        self.adresse_liefer = adresse_liefer
    def to_json(self):
        return json.dumps(__dict__)

class Verteilungszentrum:
    def __init__(self, verteilungszentrum_id, adresse, telefonnummer):
        self.verteilungszentrum_id = verteilungszentrum_id
        self.adresse = adresse
        self.telefonnummer = telefonnummer
    def to_json(self):
        return json.dumps(__dict__)

class Tour:
    def __init__(self, tour_id, tour_stopps, tour_zeit):
        self.tour_id = tour_id
        self.tour_stopps = tour_stopps
        self.tour_zeit = tour_zeit
    def to_json(self):
        return json.dumps(__dict__)

class Sendung:
    def __init__(self, sendung_id, groesse, gewicht, anmerkung, adresse_liefer, tour_id=None, kunde_id=None):
        self.sendung_id = sendung_id
        self.groesse = groesse
        self.gewicht = gewicht
        self.anmerkung = anmerkung
        self.adresse_liefer = adresse_liefer
        self.tour_id = tour_id
        self.kunde_id = kunde_id
    def to_json(self):
        return json.dumps(__dict__)

class Fahrzeug:
    def __init__(self, fahrzeug_id, defekt, kennzeichen, verteilungszentrum_id):
        self.fahrzeug_id = fahrzeug_id
        self.defekt = defekt
        self.kennzeichen = kennzeichen
        self.verteilungszentrum_id = verteilungszentrum_id
    def to_json(self):
        return json.dumps(__dict__)
        

class Fahrer:
    def __init__(self, fahrer_id, fuehrerschein, name):
        self.fahrer_id = fahrer_id
        self.fuehrerschein = fuehrerschein
        self.name = name
    def to_json(self):
        return json.dumps(__dict__)

class FahrerFaehrtTour:
    def __init__(self, datum, fahrer_id, tour_id):
        self.datum = datum
        self.fahrer_id = fahrer_id
        self.tour_id = tour_id
    def to_json(self):
        return json.dumps(__dict__)

class FahrerFaehrtFahrzeug:
    def __init__(self, datum, fahrer_id, fahrzeug_id):
        self.datum = datum
        self.fahrer_id = fahrer_id
        self.fahrzeug_id = fahrzeug_id
    def to_json(self):
        return json.dumps(__dict__)

class Sendungsverfolgung:
    def __init__(self, versendet, datum, sendung_id=None, verteilungszentrum_id=None):
        self.versendet = versendet
        self.datum = datum
        self.sendung_id = sendung_id
        self.verteilungszentrum_id = verteilungszentrum_id
    def to_json(self):
        return json.dumps(__dict__)
