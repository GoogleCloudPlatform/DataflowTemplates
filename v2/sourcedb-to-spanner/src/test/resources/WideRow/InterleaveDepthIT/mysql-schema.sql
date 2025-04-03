CREATE TABLE Level1 (
  Id BIGINT NOT NULL PRIMARY KEY,
  Name VARCHAR(100)
);

CREATE TABLE Level2 (
  Level1Id BIGINT NOT NULL,
  Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Id),
  FOREIGN KEY (Level1Id) REFERENCES Level1(Id) ON DELETE CASCADE
);

CREATE TABLE Level3 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Id),
  FOREIGN KEY (Level1Id, Level2Id) REFERENCES Level2(Level1Id, Id) ON DELETE CASCADE
);

CREATE TABLE Level4 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id) REFERENCES Level3(Level1Id, Level2Id, Id) ON DELETE CASCADE
);

CREATE TABLE Level5 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id, Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id, Level4Id) REFERENCES Level4(Level1Id, Level2Id, Level3Id, Id) ON DELETE CASCADE
);

CREATE TABLE Level6 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Level5Id BIGINT NOT NULL,
  Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id) REFERENCES Level5(Level1Id, Level2Id, Level3Id, Level4Id, Id) ON DELETE CASCADE
);

CREATE TABLE Level7 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Level5Id BIGINT NOT NULL,
  Level6Id BIGINT NOT NULL,
  Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id, Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id)
    REFERENCES Level6(Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Id)
    ON DELETE CASCADE
);

INSERT INTO Level1 (Id, Name) VALUES (1, 'Level1-Row');

INSERT INTO Level2 (Level1Id, Id, Name) VALUES (1, 1, 'Level2-Row');

INSERT INTO Level3 (Level1Id, Level2Id, Id, Name) VALUES (1, 1, 1, 'Level3-Row');

INSERT INTO Level4 (Level1Id, Level2Id, Level3Id, Id, Name) VALUES (1, 1, 1, 1, 'Level4-Row');

INSERT INTO Level5 (Level1Id, Level2Id, Level3Id, Level4Id, Id, Name) VALUES (1, 1, 1, 1, 1, 'Level5-Row');

INSERT INTO Level6 (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Id, Name) VALUES (1, 1, 1, 1, 1, 1, 'Level6-Row');

INSERT INTO Level7 (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id, Id, Name) VALUES (1, 1, 1, 1, 1, 1, 1, 'Level7-Row');
