CREATE TABLE `StockInfo` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `name` varchar(255) DEFAULT NULL,
 `isin` varchar(128) DEFAULT NULL,
 `symbol` varchar(128) DEFAULT NULL,
 `wkn` varchar(128) DEFAULT NULL,
 `description` text,
 `StockMarket` varchar(255) DEFAULT NULL,
 `last_update_time` timestamp NULL DEFAULT NULL,
 PRIMARY KEY (`id`),
 UNIQUE KEY `symbol` (`symbol`),
 UNIQUE KEY `isin` (`isin`),
 UNIQUE KEY `wkn` (`wkn`)
) ENGINE=InnoDB AUTO_INCREMENT=12290 DEFAULT CHARSET=utf8mb4

	
CREATE TABLE `StockValueChange` (
 `id` int(11) NOT NULL,
 `timestamp` datetime NOT NULL,
 `open` float NOT NULL,
 `high` float NOT NULL,
 `low` float NOT NULL,
 `close` float NOT NULL,
 `adjusted_close` float NOT NULL,
 `volume` float NOT NULL,
 `dividend_amount` float NOT NULL,
 `split_coefficient` float NOT NULL,
 PRIMARY KEY (`id`,`timestamp`),
 CONSTRAINT `id_fk` FOREIGN KEY (`id`) REFERENCES `StockInfo` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
