USE records;

CREATE TABLE `customer_load_transactions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `loadDate` datetime DEFAULT NULL,
  `fileName` varchar(255) DEFAULT NULL,
  `stageTable` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE stage;

CREATE TABLE `customer_orders` (
  `customerId` int DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `contactName` varchar(255) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `accountCreated` date DEFAULT NULL,
  `orderId` int DEFAULT NULL,
  `orderDate` date DEFAULT NULL,
  `processingDate` date DEFAULT NULL,
  `fileName` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE dimension;

CREATE TABLE `dim_countries` (
  `id` int NOT NULL AUTO_INCREMENT,
  `country_name` varchar(255) DEFAULT NULL,
  `inserted_by_name` varchar(255) DEFAULT NULL,
  `inserted_date` datetime DEFAULT NULL,
  `last_update_by_name` varchar(255) DEFAULT NULL,
  `last_update_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=251 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `dim_customer` (
  `id` int NOT NULL AUTO_INCREMENT,
  `customer_id` int DEFAULT NULL,
  `customer_name` varchar(255) DEFAULT NULL,
  `customer_contact_name` varchar(255) DEFAULT NULL,
  `customer_account_creation` date DEFAULT NULL,
  `inserted_by_name` varchar(255) DEFAULT NULL,
  `inserted_date` datetime DEFAULT NULL,
  `last_update_by_name` varchar(255) DEFAULT NULL,
  `last_update_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1005 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

USE fact;

CREATE TABLE `fact_orders` (
  `factOrdersKey` int NOT NULL AUTO_INCREMENT,
  `dateKey` int DEFAULT NULL,
  `customerKey` int DEFAULT NULL,
  `countryKey` int DEFAULT NULL,
  `OrderId` int DEFAULT NULL,
  `OrderDate` date DEFAULT NULL,
  PRIMARY KEY (`factOrdersKey`)
) ENGINE=InnoDB AUTO_INCREMENT=2414 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


