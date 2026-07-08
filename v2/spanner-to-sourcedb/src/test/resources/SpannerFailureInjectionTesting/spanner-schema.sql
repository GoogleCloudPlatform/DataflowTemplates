DROP TABLE IF EXISTS `Authors`;
CREATE TABLE `Authors` (
                         `author_id`    INT64 NOT NULL,
                         `name`  STRING(MAX)
) PRIMARY KEY (`author_id`);

DROP TABLE IF EXISTS `Books`;
CREATE TABLE `Books` (
                       `author_id` INT64 NOT NULL,
                       `book_id`   INT64 NOT NULL,
                       `name`      STRING(MAX)
) PRIMARY KEY (`author_id`, `book_id`),
  INTERLEAVE IN PARENT `Authors` ON DELETE CASCADE;

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);