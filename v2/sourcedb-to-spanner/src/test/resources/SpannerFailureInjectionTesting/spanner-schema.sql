DROP TABLE IF EXISTS `Authors`;
CREATE TABLE `Authors` (
                         `author_id`    INT64 NOT NULL,
                         `name`  STRING(200)
) PRIMARY KEY (`author_id`);

DROP TABLE IF EXISTS `Books`;
CREATE TABLE `Books` (
                       `author_id` INT64 NOT NULL,
                       `book_id`   INT64 NOT NULL,
                       `name`      STRING(200)
) PRIMARY KEY (`author_id`, `book_id`),
  INTERLEAVE IN PARENT `Authors` ON DELETE CASCADE;