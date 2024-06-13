CREATE TABLE `company` (
    `company_id` INT PRIMARY KEY,
    `company_name` VARCHAR(100),
    `created_on` DATE DEFAULT NOW()
);

CREATE TABLE `employee` (
    `employee_id` INT PRIMARY KEY,
    `company_id` INT,
    `employee_name` VARCHAR(100),
    `employee_address` VARCHAR(100),
    `created_on` DATE DEFAULT NOW()
);

CREATE TABLE `employee_attribute` (
    `employee_id` INT,
    `attribute_name` VARCHAR(100),
    `value` VARCHAR(100),
    `updated_on` DATE DEFAULT NOW(),
    PRIMARY KEY (`employee_id`, `attribute_name`)
);

INSERT INTO `company` (`company_id`,`company_name`,`created_on`) VALUES (1,'gog','1998-09-04');
INSERT INTO `company` (`company_id`,`company_name`,`created_on`) VALUES (2,'app','1976-04-01');
INSERT INTO `company` (`company_id`,`company_name`,`created_on`) VALUES (3,'ama','1994-07-05');

INSERT INTO `employee` (`employee_id`, `company_id`, `employee_name`, `employee_address`, `created_on`) VALUES (100, 1, 'emp1', 'add1', '1996-01-01');
INSERT INTO `employee` (`employee_id`, `company_id`, `employee_name`, `employee_address`, `created_on`) VALUES (101, 1, 'emp2', 'add2', '1999-01-01');
INSERT INTO `employee` (`employee_id`, `company_id`, `employee_name`, `employee_address`, `created_on`) VALUES (102, 1, 'emp3', 'add3', '2012-01-01');

INSERT INTO `employee` (`employee_id`, `company_id`, `employee_name`, `employee_address`, `created_on`) VALUES (300, 3, 'emp300', 'add300', '1996-01-01');

INSERT INTO employee_attribute (`employee_id`, `attribute_name`, `value`) VALUES (100, 'iq', '150');
INSERT INTO employee_attribute (`employee_id`, `attribute_name`, `value`) VALUES (101, 'iq', '120');
INSERT INTO employee_attribute (`employee_id`, `attribute_name`, `value`) VALUES (102, 'iq', '20');
INSERT INTO employee_attribute (`employee_id`, `attribute_name`, `value`) VALUES (300, 'endurance', '20');
