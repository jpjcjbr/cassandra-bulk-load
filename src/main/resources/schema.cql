CREATE KEYSPACE IF NOT EXISTS dados WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE dados;
CREATE TABLE IF NOT EXISTS bolsa_familia (UF text, CITY text, RECEIVER text, VALUE decimal, MONTH text, PRIMARY KEY ((MONTH, UF, CITY), RECEIVER));
