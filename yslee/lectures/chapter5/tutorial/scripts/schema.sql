USE dev;

CREATE TABLE `test`(
  `id` int(10) AUTO_INCREMENT NOT NULL COMMENT 'sequence id',
  `name` VARCHAR(10) NOT NULL COMMENT 'name',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='test table for spark jdbc test';

SHOW TABLES FROM dev;

