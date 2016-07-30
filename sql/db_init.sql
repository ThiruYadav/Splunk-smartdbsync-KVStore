CREATE TABLE IF NOT EXISTS PRODUCTS
(
	productID 		SERIAL PRIMARY KEY,
	description 		VARCHAR(200),
	manufacturer 		VARCHAR(50),
	unitprice 		DECIMAL(12,2),
	country_of_origin 	VARCHAR(30),
	weight_kg 		DECIMAL(5,4)
);


CREATE TABLE IF NOT EXISTS CHANGETRACK_PRODUCTS(
	changeID		BIGSERIAL PRIMARY KEY,
	operation		char(1)   NOT NULL,
	stamp			timestamp NOT NULL,
	productID		INTEGER
);

CREATE OR REPLACE FUNCTION process_change_products() RETURNS TRIGGER AS $process_change_products$
    BEGIN
        --
        -- Create a row in emp_audit to reflect the operation performed on emp,
        -- make use of the special variable TG_OP to work out the operation.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO CHANGETRACK_PRODUCTS (operation,stamp,productID) SELECT 'D', now(), OLD.productID;
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO CHANGETRACK_PRODUCTS (operation,stamp,productID) SELECT 'U', now(), OLD.productID;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO CHANGETRACK_PRODUCTS (operation,stamp,productID) SELECT 'I', now(), NEW.productID;
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$process_change_products$ LANGUAGE plpgsql;

CREATE TRIGGER changetrack_products
AFTER INSERT OR UPDATE OR DELETE ON PRODUCTS
    FOR EACH ROW EXECUTE PROCEDURE process_change_products();

-- Make up some products as test data
INSERT INTO PRODUCTS
(description,manufacturer,unitprice,country_of_origin,weight_kg) VALUES
('Jar of Dirt','Capt. Jack Sparrow',82.85,'UK',1.0),
('Sack of Potatoes','Pete''s Potatoes',3.25,'USA',2.5),
('Macbook Pro','Apple Computer',1999.95,'China',1.1),
('iPad Pro','Apple Computer',999.95,'China',0.8),
('100 Ducks .999 Silver','Yeager Poured Silver',0.65,'USA',0.1),
('Fancy Fez','Splunk',10000,'USA','0.05')
;


SELECT * from PRODUCTS;
SELECT * FROM CHANGETRACK_PRODUCTS;


-- Now do some updates

--UPDATE PRODUCTS SET manufacturer='Davy Jones' where manufacturer='Capt. Jack Sparrow';
--DELETE FROM PRODUCTS where manufacturer='Apple Computer';

-- Inflation!
--UPDATE PRODUCTS SET unitprice = unitprice * 1.2; 

SELECT * FROM CHANGETRACK_PRODUCTS;
