CREATE TABLE `alltypes` (
  `bool_field` tinyint(1) NOT NULL,
  `int64_field` bigint NOT NULL,
  `float64_field` double NOT NULL,
  `string_field` text NOT NULL,
  `bytes_field` longblob NOT NULL,
  `timestamp_field` datetime NOT NULL,
  `date_field` date NOT NULL,
  `numeric_field` decimal(38,9) NOT NULL,
  `val` int DEFAULT NULL,
  PRIMARY KEY (`bool_field`,`int64_field`,`float64_field`,`string_field`(255),`bytes_field`(255),`timestamp_field`,`date_field`,`numeric_field`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `my_table` (
  `id` int NOT NULL,
  `val` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO `alltypes` VALUES (1,123456789012345,3.14159265359,'This is a test string for MySQL.',0x5468697320697320736F6D652062696E6172792064617461,'2024-12-20 10:30:00','2024-12-20',12345.123400000,5);
INSERT INTO `my_table` VALUES (2,100),(3,100),(100,5);

