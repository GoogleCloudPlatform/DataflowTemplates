CREATE TABLE `Category` (
  `category_id` tinyint NOT NULL, -- To: category_id INT64
  `name` varchar(25),             -- To: full_name STRING(25) Column name renamed
  `last_update` timestamp,        -- To: Column dropped in spanner
  PRIMARY KEY (`category_id`)
);