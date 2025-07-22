-- Database schema for the sampling tool with production-like structure
-- Three main tables matching the production database schema

-- Table 1: Kundenstamm (Customer Master Data)
CREATE TABLE IF NOT EXISTS kundenstamm (
    pk NVARCHAR(152) PRIMARY KEY,
    banknummer VARCHAR(20),
    kundennummer VARCHAR(20),
    stichtag DATE,
    personennummer_pseudonym BIGINT,
    kundennummer_fusionierter_kunde VARCHAR(20),
    banknummer_fusionierter_kunde VARCHAR(20),
    art_kundenstammvertrag INT,
    geburtsdatum_gruendungsdatum_pseudonym BIGINT,
    geburtsort_pseudonym BIGINT,
    person_angelegt_am DATE,
    rechtsform DECIMAL(29),
    rechtsformauspraegung DECIMAL(3),
    rechtsform_binaer VARCHAR(25),
    rechtsformauspraegung_beschreibung_1 VARCHAR(200),
    rechtsformauspraegung_beschreibung_2 VARCHAR(400),
    grundform VARCHAR(25),
    staatsangehoerigkeit_nationalitaet_bezeichnung_pseudonym BIGINT,
    ausstellende_behoerde_ausweis VARCHAR(50),
    ausstellungsdatum_ausweis DATE,
    ausweisart VARCHAR(40),
    ausweiskopie_vorhanden CHAR(1),
    ausweisnummer_pseudonym BIGINT,
    eingetragen_am DATE,
    gueltig_bis_ausweis DATE,
    legitimation_geprueft_am DATE,
    legitimationsverfahren VARCHAR(25),
    ort_registergericht VARCHAR(32),
    registerart DECIMAL(3),
    registernummer_pseudonym BIGINT,
    vorname_fuer_par_24c_kwg_pseudonym BIGINT,
    nachname_fuer_par_24c_kwg_pseudonym BIGINT,
    firmenname_fuer_par_24c_kwg_pseudonym BIGINT,
    nachname_pseudonym BIGINT,
    vorname_pseudonym BIGINT,
    vollstaendiger_name_pseudonym BIGINT,
    risikoklasse_nach_gwg INT,
    person_ist_pep CHAR(1),
    letzte_bearbeitung_wirtschaftlich_berechtigte DATE,
    aktualitaet_der_kundendaten_wurde_ueberprueft DATE,
    strasse_pseudonym BIGINT,
    postleitzahl_pseudonym BIGINT,
    ort_pseudonym BIGINT,
    land_bezeichnung_pseudonym BIGINT
);

-- Table 2: Softfact View
CREATE TABLE IF NOT EXISTS softfact_vw (
    pk NVARCHAR(203) PRIMARY KEY,
    banknummer VARCHAR(20),
    banknummer_fusionierter_kunde VARCHAR(20),
    stichtag DATE,
    feststellung_wirtschaftlich_berechtigter INT,
    guid UNIQUEIDENTIFIER,
    kundennummer VARCHAR(20),
    kundennummer_fusionierter_kunde VARCHAR(20),
    personennummer_pseudonym BIGINT,
    personennummer_2_kunde_pseudonym BIGINT,
    rollentyp INT,
    softfact_laufende_nummer INT,
    softfactartschluessel VARCHAR(11),
    softfacttyp DECIMAL(2),
    schluesselart VARCHAR(250),
    softfactartbezeichnung VARCHAR(250),
    statistikschluessel VARCHAR(250)
);

-- Table 3: Kontodaten View (Account Data View)
CREATE TABLE IF NOT EXISTS kontodaten_vw (
    pk NVARCHAR(152) PRIMARY KEY,
    guid UNIQUEIDENTIFIER,
    banknummer VARCHAR(20),
    banknummer_fusionierter_kunde VARCHAR(20),
    stichtag DATE,
    personennummer_pseudonym BIGINT,
    kontonummer_pseudonym BIGNT,
    kundennummer_fusionierter_kunde BIGINT,
    kontoeroeffnung DATE,
    konto_fuer_fremde_rechnung CHAR(1),
    anderkonto CHAR(1),
    treuhandkonto CHAR(1),
    aufloesungskennzeichen CHAR(1),
    kontoaenderungsdatum DATE,
    geschaeftsart DECIMAL(3),
    spartenschluessel VARCHAR(2)
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_kundenstamm_stichtag ON kundenstamm(stichtag);
CREATE INDEX IF NOT EXISTS idx_kundenstamm_banknummer ON kundenstamm(banknummer);
CREATE INDEX IF NOT EXISTS idx_kundenstamm_kundennummer ON kundenstamm(kundennummer);

CREATE INDEX IF NOT EXISTS idx_softfact_stichtag ON softfact_vw(stichtag);
CREATE INDEX IF NOT EXISTS idx_softfact_banknummer ON softfact_vw(banknummer);
CREATE INDEX IF NOT EXISTS idx_softfact_kundennummer ON softfact_vw(kundennummer);

CREATE INDEX IF NOT EXISTS idx_kontodaten_stichtag ON kontodaten_vw(stichtag);
CREATE INDEX IF NOT EXISTS idx_kontodaten_banknummer ON kontodaten_vw(banknummer);
CREATE INDEX IF NOT EXISTS idx_kontodaten_personennummer ON kontodaten_vw(personennummer_pseudonym);
