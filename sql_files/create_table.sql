DROP TABLE IF EXISTS clean_store_transactions;
CREATE TABLE clean_store_transactions(STORE_ID varchar(50), STORE_COUNTRY varchar(3), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, TRANSACTION_DATE date);
