-- SCRIPT CASSANDRA
CREATE KEYSPACE IF NOT EXISTS oldtech 
    WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

USE oldtech;

CREATE TABLE IF NOT EXISTS venda (
id uuid Primary Key,
nota_fiscal int,
vendedor text,
total text
);

-- SCRIPT MySQL Workbench
CREATE DATABASE oldtech;

USE oldtech;

CREATE TABLE IF NOT EXISTS venda (
id int auto_increment Primary Key,
nota_fiscal int,
vendedor varchar(100),
total varchar(100)
);