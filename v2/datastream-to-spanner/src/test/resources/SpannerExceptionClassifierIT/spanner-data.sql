
INSERT INTO Authors (author_id, name) values (1, 'J.R.R. Tolkien');
INSERT INTO Authors (author_id, name) values (2, 'Jane Austen');
INSERT INTO Authors (author_id, name) values (3, 'Douglas Adams');

INSERT INTO Books (id, author_id, title) values (1, 1, 'The Lord of the Rings');
INSERT INTO Books (id, author_id, title) values (2, 3, 'The Hitchhikers Guide to the Galaxy');

INSERT INTO ForeignKeyParent(id, name) values (1, 'parent1');
INSERT INTO ForeignKeyParent(id, name) values (2, 'parent2');

INSERT INTO ForeignKeyChild(id, parent_id, parent_name) values (1, 1, 'parent1');
INSERT INTO ForeignKeyChild(id, parent_id, parent_name) values (2, 2, 'parent2');

INSERT INTO MultiKeyTable(id1, id2, id3, name) values (1, 1, 1, 'name1');
INSERT INTO MultiKeyTable(id1, id2, id3, name) values (2, 2, 2, 'name2');

INSERT INTO GenPK(id1, part1, name) values (1, 1, 'name1');
INSERT INTO GenPK(id1, part1, name) values (2, 10, 'name2');