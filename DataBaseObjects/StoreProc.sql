USE dimension;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `load_dim_countries`()
BEGIN
    DECLARE cur_timestamp DATETIME;
    DECLARE cur_user VARCHAR(255);

    -- Get current user and timestamp
    SET cur_timestamp = NOW();
    SET cur_user = USER();

    -- Merge stage data into dim_countries table
    INSERT INTO dimension.dim_countries (
        country_name,
        inserted_by_name,
        inserted_date,
        last_update_by_name,
        last_update_date
    )
    SELECT DISTINCT
        source.country,
        cur_user,
        cur_timestamp,
        cur_user,
        cur_timestamp
    FROM stage.customer_orders AS source
    WHERE NOT EXISTS (
        SELECT 1
        FROM dimension.dim_countries AS dc
        WHERE dc.country_name = source.country
    );

    UPDATE dimension.dim_countries AS dc
    INNER JOIN stage.customer_orders AS so ON dc.country_name = so.country
    SET
        dc.last_update_by_name = cur_user,
        dc.last_update_date = cur_timestamp
    WHERE dc.country_name = so.country;

    -- End of the procedure
END//
DELIMITER ;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `load_dim_customer`()
BEGIN
  DECLARE cur_timestamp DATETIME;
  DECLARE cur_user VARCHAR(255);

  -- Get current user and timestamp
  SET cur_timestamp = NOW();
  SET cur_user = USER();

  -- Merge stage data into dim_customer table
  INSERT INTO dimension.dim_customer (
  customer_id,
  customer_name,
  customer_contact_name,
  customer_account_creation,
  inserted_by_name,
  inserted_date,
  last_update_by_name,
  last_update_date
  )
  SELECT DISTINCT
    source.customerId,
    source.name,
    source.contactName,
    source.accountCreated,
    cur_user,
    cur_timestamp,
    cur_user,
    cur_timestamp
  FROM stage.customer_orders AS source
	 WHERE NOT EXISTS (
			SELECT 1
			FROM dimension.dim_customer AS dc
			WHERE dc.customer_id = source.customerId
		);

    UPDATE dimension.dim_customer AS dc
    INNER JOIN stage.customer_orders AS so ON dc.customer_id = so.customerId
    SET
        dc.last_update_by_name = cur_user,
        dc.last_update_date = cur_timestamp
    WHERE dc.customer_id = so.customerId;

  -- End of the procedure
END//
DELIMITER ;


USE fact;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `load_fact_orders`()
BEGIN
    DECLARE cur_timestamp DATETIME;
    DECLARE cur_user VARCHAR(255);

    -- Get current user and timestamp
    SET cur_timestamp = NOW();
    SET cur_user = USER();

    -- Merge stage data into fact_orders table
    INSERT INTO fact.fact_orders (
		dateKey,
        customerKey,
        countryKey,
        OrderId,
        OrderDate
    )
    SELECT
		CAST(DATE_FORMAT(STR_TO_DATE(source.processingDate, '%Y-%m-%d'), '%Y%m%d') AS UNSIGNED) AS dateKey,
        dc.id AS customerKey,
        dco.id AS countryKey,
        source.OrderID,
        source.OrderDate
    FROM stage.customer_orders AS source
    INNER JOIN dimension.dim_customer AS dc ON source.customerId = dc.customer_id
    INNER JOIN dimension.dim_countries AS dco ON source.country = dco.country_name
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact.fact_orders AS fo
        WHERE fo.customerKey = dc.id
        AND fo.countryKey = dco.id
        AND fo.dateKey = CAST(DATE_FORMAT(STR_TO_DATE(source.processingDate, '%Y-%m-%d'), '%Y%m%d') AS UNSIGNED)
    );

    -- End of the procedure
END//
DELIMITER ;


USE widget;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `create_customer_5_ordersummary_view`()
BEGIN
    -- Create the SQL statement for the view
    SET @sql = CONCAT(
		'CREATE OR REPLACE VIEW widget.customer_5_orders AS ',
		'SELECT customer_id, customer_name, country_name, order_quantity ',
		'FROM widget.customer_order_summary ',
		'WHERE order_quantity >= 5 '
    );

    -- Execute the SQL statement
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Display a success message
    SELECT 'The customer 5 order summary view has been created.' AS Message;
END//
DELIMITER ;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `create_customer_ordersummary_view`()
BEGIN
    -- Create the SQL statement for the view
    SET @sql = CONCAT(
        'CREATE OR REPLACE VIEW widget.customer_order_summary AS ',
        'SELECT dc.customer_id, dc.customer_name, dco.country_name, COUNT(fo.factOrdersKey) AS order_quantity ',
        'FROM fact.fact_orders AS fo ',
        'JOIN dimension.dim_customer AS dc ON fo.customerKey = dc.id ',
        'JOIN dimension.dim_countries AS dco ON fo.countryKey = dco.id ',
        'GROUP BY dc.customer_id, dc.customer_name, dco.country_name'
    );

    -- Execute the SQL statement
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Display a success message
    SELECT 'The customer order summary view has been created.' AS Message;
END//
DELIMITER ;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `create_monthly_order_views`()
BEGIN
    DECLARE i INT;
    DECLARE current_month INT;
    DECLARE current_year INT;
    DECLARE view_name VARCHAR(50);

    -- Get the current month and year
    SET current_month = MONTH(CURDATE());
    SET current_year = YEAR(CURDATE());

    -- Loop through 12 months to create views
    SET i = 1;
    WHILE i <= 12 DO
        SET view_name = CONCAT('widget_monthly_', i);

        -- Create the SQL statement for the view
        SET @sql = CONCAT(
            'CREATE OR REPLACE VIEW widget.', view_name, ' AS ',
            'SELECT * FROM widget.widget_orders ',
            'WHERE MONTH(OrderDate) = ', i, ' AND YEAR(OrderDate) = ', current_year, ';'
        );

        -- Execute the SQL statement
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
        SET current_month = current_month - 1;
        IF current_month = 0 THEN
            SET current_month = 12;
            SET current_year = current_year - 1;
        END IF;
    END WHILE;

    -- Display a success message
    SELECT 'The monthly order views have been created.' AS Message;
END//
DELIMITER ;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `create_orders_principal_view`()
BEGIN
    -- Create the SQL statement for the view
    SET @sql = CONCAT(
        'CREATE OR REPLACE VIEW widget.widget_orders AS ',
        'SELECT dc.customer_id, dc.customer_name, dc.customer_contact_name, ',
        'dco.country_name, fo.OrderId, fo.OrderDate ',
        'FROM fact.fact_orders AS fo ',
        'JOIN dimension.dim_customer AS dc ON fo.customerKey = dc.id ',
        'JOIN dimension.dim_countries AS dco ON fo.countryKey = dco.id'
    );

    -- Execute the SQL statement
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Display a success message
    SELECT 'The fact view has been created.' AS Message;
END//
DELIMITER ;

DELIMITER //
CREATE DEFINER=`Master`@`localhost` PROCEDURE `create_recent_60_customer_orders_view`()
BEGIN
    -- Create the SQL statement for the view
    SET @sql = CONCAT(
		'CREATE OR REPLACE VIEW widget.recent_60_customer_orders AS ',
        'SELECT c.customer_id, c.customer_name, COUNT(c.OrderId) AS total_orders ',
        'FROM widget_orders c ',
        'WHERE c.OrderDate >= CURDATE() - INTERVAL 60 DAY ',
        'GROUP BY c.customer_id, c.customer_name;'
    );

    -- Execute the SQL statement
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Display a success message
     SELECT 'The recent 60 customer orders view has been created.' AS Message;
END//
DELIMITER ;


