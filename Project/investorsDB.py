import mysql.connector

conn = mysql.connector.connect(user='itc6107', password='itc6107', host='127.0.0.1')

cursor = conn.cursor()

# create DB
cursor.execute(
    """
DROP DATABASE IF EXISTS InvestorsDB;

CREATE DATABASE InvestorsDB;
USE InvestorsDB;

CREATE TABLE Investors (
    id      CHAR(20) NOT NULL primary key,
    name    CHAR(255) NOT NULL, 
    city CHAR(255) NOT NULL
);

CREATE TABLE Portfolios (
    id      CHAR(20) NOT NULL primary key,
    name    CHAR(255) NOT NULL,  
    cumulative BOOLEAN NOT NULL
);

CREATE TABLE Investors_Portfolios (
    iid      CHAR(20),
    FOREIGN KEY (iid) REFERENCES Investors(id),
    pid      CHAR(20),
    FOREIGN KEY (pid) REFERENCES Portfolios(id),
    PRIMARY KEY (iid,pid)
);
CREATE TABLE Inv1_P11(
    Date DATE NOT NULL, 
    total_value DECIMAL(20,1) NOT NULL, 
    daily_change DECIMAL(20,1) NOT NULL,
    percentage_change DECIMAL(20,5)NOT NULL
);
CREATE TABLE Inv1_P12(
    Date DATE NOT NULL , 
    total_value DECIMAL(20,1) NOT NULL, 
    daily_change DECIMAL(20,1) NOT NULL,
    percentage_change DECIMAL(20,5)NOT NULL
);
CREATE TABLE Inv2_P21(
    Date DATE NOT NULL, 
    total_value DECIMAL(20,1) NOT NULL, 
    daily_change DECIMAL(20,1) NOT NULL,
    percentage_change DECIMAL(20,5)NOT NULL
);
CREATE TABLE Inv2_P22(
    Date DATE NOT NULL, 
    total_value DECIMAL(20,1) NOT NULL, 
    daily_change DECIMAL(20,1) NOT NULL,
    percentage_change DECIMAL(20,5)NOT NULL
);
CREATE TABLE Inv3_P31(
    Date DATE NOT NULL, 
    total_value DECIMAL(20,1) NOT NULL, 
    daily_change DECIMAL(20,1) NOT NULL,
    percentage_change DECIMAL(20,5)NOT NULL
);
CREATE TABLE Inv3_P32(
    Date DATE NOT NULL, 
    total_value DECIMAL(20,1) NOT NULL, 
    daily_change DECIMAL(20,1) NOT NULL,
    percentage_change DECIMAL(20,5)NOT NULL
)
"""
)
conn.commit
cursor.close()
conn.close()

conn = mysql.connector.connect(user='itc6107', password='itc6107', host='127.0.0.1',
                               database='InvestorsDB')
cursor = conn.cursor()

# populate DB
cursor.execute("INSERT INTO Investors(ID, NAME,CITY) VALUES ('Inv1', 'Smith', 'New York');")
cursor.execute("INSERT INTO Investors(ID, NAME,CITY) VALUES ('Inv2', 'Jones','San Fransisco');")
cursor.execute("INSERT INTO Investors(ID, NAME,CITY) VALUES ('Inv3', 'Clark', 'Chicago');")

cursor.execute("INSERT INTO Portfolios(ID, NAME,CUMULATIVE) VALUES ('P11', 'Portfolio11', '1');")
cursor.execute("INSERT INTO Portfolios(ID, NAME,CUMULATIVE) VALUES ('P12', 'Portfolio12', '0');")
cursor.execute("INSERT INTO Portfolios(ID, NAME,CUMULATIVE) VALUES ('P21', 'Portfolio21', '1');")
cursor.execute("INSERT INTO Portfolios(ID, NAME,CUMULATIVE) VALUES ('P22', 'Portfolio22', '1');")
cursor.execute("INSERT INTO Portfolios(ID, NAME,CUMULATIVE) VALUES ('P31', 'Portfolio31', '0');")
cursor.execute("INSERT INTO Portfolios(ID, NAME,CUMULATIVE) VALUES ('P32', 'Portfolio32', '1');")
conn.commit()
conn.close()


conn = mysql.connector.connect(user='itc6107', password='itc6107', host='127.0.0.1',
                               database='InvestorsDB')
cursor = conn.cursor()
cursor.execute("INSERT INTO Investors_Portfolios (IID,PID) VALUES ('Inv1', 'P11');")
cursor.execute("INSERT INTO Investors_Portfolios (IID,PID) VALUES ('Inv1', 'P12');")
cursor.execute("INSERT INTO Investors_Portfolios (IID,PID) VALUES ('Inv2', 'P21');")
cursor.execute("INSERT INTO Investors_Portfolios (IID,PID) VALUES ('Inv2', 'P22');")
cursor.execute("INSERT INTO Investors_Portfolios (IID,PID) VALUES ('Inv3', 'P31');")
cursor.execute("INSERT INTO Investors_Portfolios (IID,PID) VALUES ('Inv3', 'P32');")

conn.commit()
conn.close()
