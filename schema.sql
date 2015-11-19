CREATE TABLE category
(
cID int,
cName varchar(255),
PRIMARY KEY (cID)
);

CREATE TABLE manufacturer
(
mID int,
mName varchar(255),
mAddress varchar(255),
mPhoneNumber int,
mWarrantlyPeriod int,
PRIMARY KEY (mID)
);

CREATE TABLE part
(
pID int,
pName varchar(255),
pPrice int,
mID int,
cID int,
pAvailableQuantity int,
PRIMARY KEY (pID),
CONSTRAINT p_mID FOREIGN KEY (mID) 
REFERENCES manufacturer(mID) ON DELETE CASCADE,
CONSTRAINT p_cID FOREIGN KEY (cID) 
REFERENCES category(cID) ON DELETE CASCADE
);

CREATE TABLE salesperson
(
sID int,
sName varchar(255),
sAddress varchar(255),
sPhoneNumber int,
PRIMARY KEY (sID)
);

CREATE TABLE transaction
(
tID int,
pID int,
sID int,
tDate DATE,
PRIMARY KEY (tID),
CONSTRAINT t_pID FOREIGN KEY (pID) 
REFERENCES part(pID) ON DELETE CASCADE,
CONSTRAINT t_sID FOREIGN KEY (sID) 
REFERENCES salesperson(sID) ON DELETE CASCADE
);