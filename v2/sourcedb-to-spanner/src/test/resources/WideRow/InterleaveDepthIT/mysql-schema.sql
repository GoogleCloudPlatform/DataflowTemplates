CREATE TABLE Level1 (
  Level1Id BIGINT NOT NULL PRIMARY KEY,
  Name VARCHAR(100)
);

CREATE TABLE Level2 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id),
  FOREIGN KEY (Level1Id) REFERENCES Level1(Level1Id) ON DELETE CASCADE
);

CREATE TABLE Level3 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id),
  FOREIGN KEY (Level1Id, Level2Id) REFERENCES Level2(Level1Id, Level2Id) ON DELETE CASCADE
);

CREATE TABLE Level4 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id) REFERENCES Level3(Level1Id, Level2Id, Level3Id) ON DELETE CASCADE
);

CREATE TABLE Level5 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Level5Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id, Level4Id) REFERENCES Level4(Level1Id, Level2Id, Level3Id, Level4Id) ON DELETE CASCADE
);

CREATE TABLE Level6 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Level5Id BIGINT NOT NULL,
  Level6Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id) REFERENCES Level5(Level1Id, Level2Id, Level3Id, Level4Id, Level5Id) ON DELETE CASCADE
);

CREATE TABLE Level7 (
  Level1Id BIGINT NOT NULL,
  Level2Id BIGINT NOT NULL,
  Level3Id BIGINT NOT NULL,
  Level4Id BIGINT NOT NULL,
  Level5Id BIGINT NOT NULL,
  Level6Id BIGINT NOT NULL,
  Level7Id BIGINT NOT NULL,
  Name VARCHAR(100),
  PRIMARY KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id, Level7Id),
  FOREIGN KEY (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id) REFERENCES Level6(Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id) ON DELETE CASCADE
);


INSERT INTO Level1 (Level1Id, Name) VALUES (1, 'Level 1 - A');

INSERT INTO Level2 (Level1Id, Level2Id, Name) VALUES (1, 10, 'Level 2 - A1');

INSERT INTO Level3 (Level1Id, Level2Id, Level3Id, Name) VALUES (1, 10, 100, 'Level 3 - A1a');

INSERT INTO Level4 (Level1Id, Level2Id, Level3Id, Level4Id, Name) VALUES (1, 10, 100, 1000, 'Level 4 - A1a1');

INSERT INTO Level5 (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Name) VALUES (1, 10, 100, 1000, 10000, 'Level 5 - A1a1a');

INSERT INTO Level6 (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id, Name) VALUES (1, 10, 100, 1000, 10000, 100000, 'Level 6 - A1a1a1');

INSERT INTO Level7 (Level1Id, Level2Id, Level3Id, Level4Id, Level5Id, Level6Id, Level7Id, Name) VALUES (1, 10, 100, 1000, 10000, 100000, 1000000, 'Level 7 - A1a1a1a');
