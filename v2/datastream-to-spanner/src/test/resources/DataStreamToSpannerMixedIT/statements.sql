INSERT INTO Authors (author_id, name, last_update) VALUES
    (1, 'Jane Austen', NOW()),
    (2, 'Charles Dickens', NOW()),
    (3, 'Leo Tolstoy', NOW()),
    (4, 'Stephen King', NOW());

INSERT INTO Books (id, title, author_id) VALUES
     (1, 'Pride and Prejudice', 1),
     (2, 'Oliver Twist', 2),
     (3, 'War and Peace', 3);

INSERT INTO Genre (genre_id, name) VALUES
    (1, 'Romance')
    (2, 'Historical Fiction'),
    (3, 'Horror'),;