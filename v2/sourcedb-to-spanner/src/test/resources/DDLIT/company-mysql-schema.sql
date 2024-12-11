CREATE TABLE `company` (
                           `company_id` int(11) PRIMARY KEY NOT NULL,
                           `company_name` varchar(100) DEFAULT NULL,
                           `created_on` date
);

INSERT INTO `company` VALUES
                          (1,'gog','1998-09-04'),
                          (2,'app','1976-04-01'),
                          (3,'ama','1994-07-05');

CREATE TABLE `employee` (
                            `employee_id` int(11) PRIMARY KEY NOT NULL,
                            `company_id` int(11) DEFAULT NULL,
                            `employee_name` varchar(100) DEFAULT NULL,
                            `employee_address` varchar(100) DEFAULT NULL,
                            `created_on` date
);

INSERT INTO `employee` VALUES
                           (100,1,'emp1','add1','1996-01-01'),
                           (101,1,'emp2','add2','1999-01-01'),
                           (102,1,'emp3','add3','2012-01-01'),
                           (300,3,'emp300','add300','1996-01-01');

CREATE TABLE `employee_attribute` (
                                      `employee_id` int(11) NOT NULL,
                                      `attribute_name` varchar(100) NOT NULL,
                                      `value` varchar(100) DEFAULT NULL,
                                      `updated_on` date,
                                      PRIMARY KEY (`employee_id`,`attribute_name`)
);

INSERT INTO `employee_attribute` VALUES
                                     (100,'iq','150','2024-06-10'),
                                     (101,'iq','120','2024-06-10'),
                                     (102,'iq','20','2024-06-10'),
                                     (300,'endurance','20','2024-06-10');

CREATE TABLE `vendor` (
                      `vendor_id` INT AUTO_INCREMENT PRIMARY KEY,
                      `first_name` VARCHAR(255) NOT NULL,
                      `last_name` VARCHAR(255) NOT NULL,
                      `email` VARCHAR(255) UNIQUE NOT NULL,
                      `full_name` VARCHAR(512) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)),
                      INDEX full_name_idx (full_name)
);

INSERT INTO vendor (vendor_id, first_name, last_name, email) VALUES
                                     (1, 'David', 'Lee', 'david.lee@example.com'),
                                     (2, 'Sarah', 'Jones', 'sarah.jones@example.com'),
                                     (3, 'Michael', 'Brown', 'michael.brown@example.com');

CREATE TABLE `mysql_extra` (
                           `test_id` int(11) PRIMARY KEY NOT NULL,
                           `test_name` varchar(100) DEFAULT NULL
);

CREATE VIEW company_view AS  SELECT company_id FROM company;