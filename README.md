Créer la data base chu 
Create database ChuData ; 
Use ChuData
Lister les permissions actuelle dans CLOUDERA 
hdfs dfs -ls -R /user/cloudera/ChuData
activer les permissions partt : 
hdfs dfs -chmod -R 777 /user/cloudera/ChuData

DIMENSIONS 
PATIENT 
CREATE TABLE IF NOT EXISTS Dimension_Patient (
  IdPatient INT,
  Age INT,
  Sexe STRING,
  Departement STRING, 
  EstDeces BOOLEAN,
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ChuData/Patient';
DATE 
CREATE TABLE IF NOT EXISTS Dimension_Date (
  IdDate INT,
  Jour INT,
  Mois INT,
  Annee INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ChuData/Date';

PROFESSIONNEL : 
CREATE TABLE IF NOT EXISTS Dimension_Professionnel (
  Nom STRING,
  Prenom STRING,
  Specialite STRING, 
  IdProfessionnel INT,
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ChuData/Professionnel';
Diagnostic : 
CREATE TABLE IF NOT EXISTS Dimension_Diagnostic (
  Description_Diagnostic STRING,
  Code_Diagnostic STRING,
  Jours_Hospitalisation INT, 
 Identifiant_patient INT,
  IdConsultation INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ChuData/Diagnostic';
Localisation : 
CREATE TABLE IF NOT EXISTS Dimension_Localisation (
  IdLocalisation INT,
  Departement STRING,
  Ville STRING,
  Pays STRING,
  Etablissement STRING,
  Satisfaction_Etablissement DECIMAL
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ChuData/Localisation';
Table des faits : 
CREATE TABLE IF NOT EXISTS Faits_Consultation (
  FK_IdPatient INT,
  FK_IdDate INT,
  FK_IdProfessionnel INT,
  FK_IdConsultation INT,
  Nombre_Consultation INT,
  Nombre_Hospitalisation INT,
  Nombre_Deces INT,
  Taux_Satisfaction DECIMAL
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '';
STORED AS TEXTFILE
LOCATION '/user/cloudera/ChuData/Faits_consultation';
Peuplement des dimensions ( vérifier si les fichiers sont stockés dans user/cloudera ou user/hive)
LOAD DATA INPATH '/user/cloudera/ChuData/Patient/Patient.txt' INTO TABLE Dimension_Patient;
LOAD DATA INPATH '/user/cloudera/ChuData/Date/Date.txt' INTO TABLE Dimension_Date;
LOAD DATA INPATH '/user/cloudera/ChuData/Professionnel/Professionnel.txt' INTO TABLE Dimension_Professionnel;
LOAD DATA INPATH '/user/cloudera/ChuData/Diagnostic/Diagnostic.txt' INTO TABLE Dimension_Diagnostic;
LOAD DATA INPATH '/user/cloudera/ChuData/Localisation/Localisation.txt' INTO TABLE Dimension_Localisation;
Créer la table des faits 
CREATE TABLE Faits_Consultation (
  IdPatient INT,
  IdDate INT,
  IdProfessionnel INT,
  IdConsultation INT,
  IdLocalisation INT,
  Nombre_Consultation INT,
  Nombre_Hospitalisation INT,
  Nombre_Deces INT,
  Taux_Satisfaction DECIMAL(10, 2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
Loader la table des faits 
INSERT INTO TABLE chudata.faits_consultation
SELECT
  d.id_patient AS IdPatient,
  da.id_date AS IdDate,
  p.idprofessionnel AS IdProfessionnel,
  d.id_consultation AS IdConsultation,
  l.idlocalisation AS IdLocalisation,
  COUNT(d.id_consultation) AS Nombre_Consultation,
  d.jour_hospitalisation AS Nombre_Hospitalisation,
  SUM(CASE WHEN pa.est_deces = TRUE THEN 1 ELSE 0 END) AS Nombre_Deces,
  AVG(1.taux_reco_brut) AS Taux_Satisfaction
FROM
  diagnostic d
JOIN
  date da ON d.id_consultation = da.id_date
JOIN
  patient pa ON d.id_consultation = pa.idpatient
JOIN
  professionnel p ON d.id_consultation = p.idprofessionnel
JOIN
  localisation l ON p.idprofessionnel = l.idlocalisation
GROUP BY
  d.id_patient, da.id_date, p.idprofessionnel, d.id_consultation, l.idlocalisation, d.jour_hospitalisation;

Partitionnement 
Localisation
CREATE EXTERNAL TABLE IF NOT EXISTS Localisation_partition (
    Idlocalisation INT,
    commune STRING,
    pays STRING,
    Etablissement STRING,
    Satisfaction_Etablissement FLOAT
)
COMMENT 'table Localisation_partition'
PARTITIONED BY (departement STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/user/hive/ChuData/Localisation';
Insert localisation 
INSERT OVERWRITE TABLE Localisation_partition PARTITION (departement)
SELECT
    Idlocalisation,
    commune,
    pays,
    raison_sociale_site,
    taux_reco_brut,
    departement
FROM
    localisation;

