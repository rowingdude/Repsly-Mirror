DELIMITER //

-- Clients upsert procedure
CREATE PROCEDURE upsert_client(
    IN p_clientid INT,
    IN p_timestamp BIGINT,
    IN p_code VARCHAR(50),
    IN p_name VARCHAR(255),
    IN p_active BOOLEAN,
    IN p_tag TEXT,
    IN p_territory TEXT,
    IN p_representativecode VARCHAR(20),
    IN p_representativename VARCHAR(80),
    IN p_streetaddress VARCHAR(255),
    IN p_zip VARCHAR(20),
    IN p_zipext VARCHAR(20),
    IN p_city VARCHAR(255),
    IN p_state VARCHAR(255),
    IN p_country VARCHAR(255),
    IN p_email VARCHAR(255),
    IN p_phone VARCHAR(128),
    IN p_mobile VARCHAR(128),
    IN p_website VARCHAR(255),
    IN p_contactname VARCHAR(255),
    IN p_contacttitle VARCHAR(50),
    IN p_note VARCHAR(255),
    IN p_status TEXT,
    IN p_accountcode TEXT,
    IN p_lasttimestamp BIGINT,
    IN p_metacollectiontotalcount INT,
    IN p_metacollectionlasttimestamp BIGINT
)
BEGIN
    INSERT INTO clients (
        clientid, timestamp, code, name, active, tag, territory, 
        representativecode, representativename, streetaddress, zip, 
        zipext, city, state, country, email, phone, mobile, website,
        contactname, contacttitle, note, status, accountcode, 
        lasttimestamp, metacollectiontotalcount, metacollectionlasttimestamp
    ) 
    VALUES (
        p_clientid, p_timestamp, p_code, p_name, p_active, p_tag, 
        p_territory, p_representativecode, p_representativename, 
        p_streetaddress, p_zip, p_zipext, p_city, p_state, p_country,
        p_email, p_phone, p_mobile, p_website, p_contactname, 
        p_contacttitle, p_note, p_status, p_accountcode, p_lasttimestamp,
        p_metacollectiontotalcount, p_metacollectionlasttimestamp
    )
    ON DUPLICATE KEY UPDATE
        timestamp = p_timestamp,
        code = p_code,
        name = p_name,
        active = p_active,
        tag = p_tag,
        territory = p_territory,
        representativecode = p_representativecode,
        representativename = p_representativename,
        streetaddress = p_streetaddress,
        zip = p_zip,
        zipext = p_zipext,
        city = p_city,
        state = p_state,
        country = p_country,
        email = p_email,
        phone = p_phone,
        mobile = p_mobile,
        website = p_website,
        contactname = p_contactname,
        contacttitle = p_contacttitle,
        note = p_note,
        status = p_status,
        accountcode = p_accountcode,
        lasttimestamp = p_lasttimestamp,
        metacollectiontotalcount = p_metacollectiontotalcount,
        metacollectionlasttimestamp = p_metacollectionlasttimestamp;
END //

-- Client Custom Fields upsert procedure
CREATE PROCEDURE upsert_client_customfield(
    IN p_clientid INT,
    IN p_field VARCHAR(255),
    IN p_value TEXT
)
BEGIN
    INSERT INTO clientcustomfields (clientid, field, value)
    VALUES (p_clientid, p_field, p_value)
    ON DUPLICATE KEY UPDATE
        value = p_value;
END //

-- Client Price Lists upsert procedure
CREATE PROCEDURE upsert_client_pricelist(
    IN p_clientid INT,
    IN p_name TEXT
)
BEGIN
    INSERT INTO clientpricelists (clientid, name)
    VALUES (p_clientid, p_name)
    ON DUPLICATE KEY UPDATE
        name = p_name;
END //

-- Client Notes upsert procedure
CREATE PROCEDURE upsert_client_note(
    IN p_clientnoteid INT,
    IN p_timestamp BIGINT,
    IN p_dateandtime DATETIME,
    IN p_representativecode VARCHAR(20),
    IN p_representativename VARCHAR(80),
    IN p_clientcode VARCHAR(50),
    IN p_clientname VARCHAR(255),
    IN p_streetaddress VARCHAR(255),
    IN p_zip VARCHAR(20),
    IN p_zipext VARCHAR(20),
    IN p_city VARCHAR(255),
    IN p_state VARCHAR(255),
    IN p_country VARCHAR(255),
    IN p_email VARCHAR(255),
    IN p_phone VARCHAR(128),
    IN p_mobile VARCHAR(128),
    IN p_territory VARCHAR(80),
    IN p_longitude BIGINT,
    IN p_latitude BIGINT,
    IN p_note TEXT,
    IN p_visitid INT,
    IN p_metacollectiontotalcount INT,
    IN p_metacollectionfirstid INT,
    IN p_metacollectionlastid INT
)
BEGIN
    INSERT INTO clientnotes (
        clientnoteid, timestamp, dateandtime, representativecode,
        representativename, clientcode, clientname, streetaddress,
        zip, zipext, city, state, country, email, phone, mobile,
        territory, longitude, latitude, note, visitid,
        metacollectiontotalcount, metacollectionfirstid, metacollectionlastid
    )
    VALUES (
        p_clientnoteid, p_timestamp, p_dateandtime, p_representativecode,
        p_representativename, p_clientcode, p_clientname, p_streetaddress,
        p_zip, p_zipext, p_city, p_state, p_country, p_email, p_phone,
        p_mobile, p_territory, p_longitude, p_latitude, p_note, p_visitid,
        p_metacollectiontotalcount, p_metacollectionfirstid, p_metacollectionlastid
    )
    ON DUPLICATE KEY UPDATE
        timestamp = p_timestamp,
        dateandtime = p_dateandtime,
        representativecode = p_representativecode,
        representativename = p_representativename,
        clientcode = p_clientcode,
        clientname = p_clientname,
        streetaddress = p_streetaddress,
        zip = p_zip,
        zipext = p_zipext,
        city = p_city,
        state = p_state,
        country = p_country,
        email = p_email,
        phone = p_phone,
        mobile = p_mobile,
        territory = p_territory,
        longitude = p_longitude,
        latitude = p_latitude,
        note = p_note,
        visitid = p_visitid,
        metacollectiontotalcount = p_metacollectiontotalcount,
        metacollectionfirstid = p_metacollectionfirstid,
        metacollectionlastid = p_metacollectionlastid;
END //

DELIMITER ;



DELIMITER //

-- Visits upsert procedure
CREATE PROCEDURE upsert_visit(
   IN p_visitid INT,
   IN p_timestamp BIGINT,
   IN p_date DATETIME,
   IN p_representativecode VARCHAR(20),
   IN p_representativename VARCHAR(80),
   IN p_explicitcheckin BOOLEAN,
   IN p_dateandtimestart DATETIME,
   IN p_dateandtimeend DATETIME,
   IN p_clientcode VARCHAR(50),
   IN p_clientname VARCHAR(255),
   IN p_streetaddress VARCHAR(255),
   IN p_zip VARCHAR(20),
   IN p_zipext VARCHAR(20),
   IN p_city VARCHAR(255),
   IN p_state VARCHAR(255),
   IN p_country VARCHAR(255),
   IN p_territory VARCHAR(80),
   IN p_latitudestart BIGINT,
   IN p_longitudestart BIGINT,
   IN p_latitudeend BIGINT,
   IN p_longitudeend BIGINT,
   IN p_precisionstart INT,
   IN p_precisionend INT,
   IN p_visitstatusbyschedule INT,
   IN p_visitended BOOLEAN,
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionlasttimestamp BIGINT
)
BEGIN
   INSERT INTO visits (
       visitid, timestamp, date, representativecode, representativename,
       explicitcheckin, dateandtimestart, dateandtimeend, clientcode,
       clientname, streetaddress, zip, zipext, city, state, country,
       territory, latitudestart, longitudestart, latitudeend, longitudeend,
       precisionstart, precisionend, visitstatusbyschedule, visitended,
       metacollectiontotalcount, metacollectionlasttimestamp
   )
   VALUES (
       p_visitid, p_timestamp, p_date, p_representativecode, p_representativename,
       p_explicitcheckin, p_dateandtimestart, p_dateandtimeend, p_clientcode,
       p_clientname, p_streetaddress, p_zip, p_zipext, p_city, p_state, p_country,
       p_territory, p_latitudestart, p_longitudestart, p_latitudeend, p_longitudeend,
       p_precisionstart, p_precisionend, p_visitstatusbyschedule, p_visitended,
       p_metacollectiontotalcount, p_metacollectionlasttimestamp
   )
   ON DUPLICATE KEY UPDATE
       timestamp = p_timestamp,
       date = p_date,
       representativecode = p_representativecode,
       representativename = p_representativename,
       explicitcheckin = p_explicitcheckin,
       dateandtimestart = p_dateandtimestart,
       dateandtimeend = p_dateandtimeend,
       clientcode = p_clientcode,
       clientname = p_clientname,
       streetaddress = p_streetaddress,
       zip = p_zip,
       zipext = p_zipext,
       city = p_city,
       state = p_state,
       country = p_country,
       territory = p_territory,
       latitudestart = p_latitudestart,
       longitudestart = p_longitudestart,
       latitudeend = p_latitudeend,
       longitudeend = p_longitudeend,
       precisionstart = p_precisionstart,
       precisionend = p_precisionend,
       visitstatusbyschedule = p_visitstatusbyschedule,
       visitended = p_visitended,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionlasttimestamp = p_metacollectionlasttimestamp;
END //

-- Retail Audits upsert procedure
CREATE PROCEDURE upsert_retail_audit(
   IN p_retailauditid INT,
   IN p_retailauditname VARCHAR(50),
   IN p_cancelled BOOLEAN,
   IN p_clientcode VARCHAR(50),
   IN p_clientname VARCHAR(255),
   IN p_dateandtime DATETIME,
   IN p_representativecode VARCHAR(20),
   IN p_representativename VARCHAR(80),
   IN p_note VARCHAR(255),
   IN p_visitid INT,
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionfirstid INT,
   IN p_metacollectionlastid INT
)
BEGIN
   INSERT INTO retailaudits (
       retailauditid, retailauditname, cancelled, clientcode, clientname,
       dateandtime, representativecode, representativename, note, visitid,
       metacollectiontotalcount, metacollectionfirstid, metacollectionlastid
   )
   VALUES (
       p_retailauditid, p_retailauditname, p_cancelled, p_clientcode, p_clientname,
       p_dateandtime, p_representativecode, p_representativename, p_note, p_visitid,
       p_metacollectiontotalcount, p_metacollectionfirstid, p_metacollectionlastid
   )
   ON DUPLICATE KEY UPDATE
       retailauditname = p_retailauditname,
       cancelled = p_cancelled,
       clientcode = p_clientcode,
       clientname = p_clientname,
       dateandtime = p_dateandtime,
       representativecode = p_representativecode,
       representativename = p_representativename,
       note = p_note,
       visitid = p_visitid,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionfirstid = p_metacollectionfirstid,
       metacollectionlastid = p_metacollectionlastid;
END //

-- Retail Audit Items upsert procedure
CREATE PROCEDURE upsert_retail_audit_item(
   IN p_retailauditid INT,
   IN p_productgroupcode VARCHAR(20),
   IN p_productgroupname VARCHAR(80),
   IN p_productcode VARCHAR(20),
   IN p_productname VARCHAR(80),
   IN p_present BOOLEAN,
   IN p_price DECIMAL(18,4),
   IN p_promotion BOOLEAN,
   IN p_shelfshare DECIMAL(18,4),
   IN p_shelfsharepercent DECIMAL(18,4),
   IN p_soldout BOOLEAN,
   IN p_stock INT
)
BEGIN
   INSERT INTO retailaudititems (
       retailauditid, productgroupcode, productgroupname, productcode,
       productname, present, price, promotion, shelfshare,
       shelfsharepercent, soldout, stock
   )
   VALUES (
       p_retailauditid, p_productgroupcode, p_productgroupname, p_productcode,
       p_productname, p_present, p_price, p_promotion, p_shelfshare,
       p_shelfsharepercent, p_soldout, p_stock
   )
   ON DUPLICATE KEY UPDATE
       productgroupname = p_productgroupname,
       productname = p_productname,
       present = p_present,
       price = p_price,
       promotion = p_promotion,
       shelfshare = p_shelfshare,
       shelfsharepercent = p_shelfsharepercent,
       soldout = p_soldout,
       stock = p_stock;
END //

-- Retail Audit Custom Fields upsert procedure
CREATE PROCEDURE upsert_retail_audit_customfield(
   IN p_retailauditid INT,
   IN p_field VARCHAR(255),
   IN p_value VARCHAR(255)
)
BEGIN
   INSERT INTO retailauditcustomfields (retailauditid, field, value)
   VALUES (p_retailauditid, p_field, p_value)
   ON DUPLICATE KEY UPDATE
       value = p_value;
END //

DELIMITER ;


DELIMITER //

-- Purchase Orders upsert procedure
CREATE PROCEDURE upsert_purchase_order(
   IN p_purchaseorderid INT,
   IN p_transactiontype VARCHAR(50),
   IN p_documenttypeid INT,
   IN p_documenttypename VARCHAR(100),
   IN p_documentstatus VARCHAR(100),
   IN p_documentstatusid INT,
   IN p_documentitemattributecaption VARCHAR(255),
   IN p_dateandtime DATETIME,
   IN p_documentno VARCHAR(50),
   IN p_clientcode VARCHAR(50),
   IN p_clientname VARCHAR(255),
   IN p_documentdate DATETIME,
   IN p_duedate DATETIME,
   IN p_representativecode VARCHAR(20),
   IN p_representativename VARCHAR(80),
   IN p_signatureurl VARCHAR(512),
   IN p_note VARCHAR(255),
   IN p_taxable BOOLEAN,
   IN p_visitid INT,
   IN p_streetaddress VARCHAR(255),
   IN p_zip VARCHAR(20),
   IN p_zipext VARCHAR(20),
   IN p_city VARCHAR(255),
   IN p_state VARCHAR(255),
   IN p_country VARCHAR(255),
   IN p_countrycode VARCHAR(20),
   IN p_originaldocumentnumber TEXT,
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionfirstid INT,
   IN p_metacollectionlastid INT
)
BEGIN
   INSERT INTO purchaseorders (
       purchaseorderid, transactiontype, documenttypeid, documenttypename,
       documentstatus, documentstatusid, documentitemattributecaption,
       dateandtime, documentno, clientcode, clientname, documentdate,
       duedate, representativecode, representativename, signatureurl,
       note, taxable, visitid, streetaddress, zip, zipext, city,
       state, country, countrycode, originaldocumentnumber,
       metacollectiontotalcount, metacollectionfirstid, metacollectionlastid
   )
   VALUES (
       p_purchaseorderid, p_transactiontype, p_documenttypeid, p_documenttypename,
       p_documentstatus, p_documentstatusid, p_documentitemattributecaption,
       p_dateandtime, p_documentno, p_clientcode, p_clientname, p_documentdate,
       p_duedate, p_representativecode, p_representativename, p_signatureurl,
       p_note, p_taxable, p_visitid, p_streetaddress, p_zip, p_zipext, p_city,
       p_state, p_country, p_countrycode, p_originaldocumentnumber,
       p_metacollectiontotalcount, p_metacollectionfirstid, p_metacollectionlastid
   )
   ON DUPLICATE KEY UPDATE
       transactiontype = p_transactiontype,
       documenttypeid = p_documenttypeid,
       documenttypename = p_documenttypename,
       documentstatus = p_documentstatus,
       documentstatusid = p_documentstatusid,
       documentitemattributecaption = p_documentitemattributecaption,
       dateandtime = p_dateandtime,
       documentno = p_documentno,
       clientcode = p_clientcode,
       clientname = p_clientname,
       documentdate = p_documentdate,
       duedate = p_duedate,
       representativecode = p_representativecode,
       representativename = p_representativename,
       signatureurl = p_signatureurl,
       note = p_note,
       taxable = p_taxable,
       visitid = p_visitid,
       streetaddress = p_streetaddress,
       zip = p_zip,
       zipext = p_zipext,
       city = p_city,
       state = p_state,
       country = p_country,
       countrycode = p_countrycode,
       originaldocumentnumber = p_originaldocumentnumber,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionfirstid = p_metacollectionfirstid,
       metacollectionlastid = p_metacollectionlastid;
END //

-- Purchase Order Items upsert procedure
CREATE PROCEDURE upsert_purchase_order_item(
   IN p_purchaseorderid INT,
   IN p_lineno INT,
   IN p_productcode VARCHAR(20),
   IN p_productname VARCHAR(80),
   IN p_unitamount DECIMAL(18,4),
   IN p_unitprice DECIMAL(18,4),
   IN p_packagetypecode VARCHAR(45),
   IN p_packagetypename VARCHAR(40),
   IN p_packagetypeconversion INT,
   IN p_quantity INT,
   IN p_amount DECIMAL(18,4),
   IN p_discountamount DECIMAL(18,4),
   IN p_discountpercent DECIMAL(18,4),
   IN p_taxamount DECIMAL(18,4),
   IN p_taxpercent DECIMAL(18,4),
   IN p_totalamount DECIMAL(18,4),
   IN p_note VARCHAR(255),
   IN p_documentitemattributename VARCHAR(100),
   IN p_documentitemattributeid INT
)
BEGIN
   INSERT INTO purchaseorderitems (
       purchaseorderid, lineno, productcode, productname, unitamount,
       unitprice, packagetypecode, packagetypename, packagetypeconversion,
       quantity, amount, discountamount, discountpercent, taxamount,
       taxpercent, totalamount, note, documentitemattributename,
       documentitemattributeid
   )
   VALUES (
       p_purchaseorderid, p_lineno, p_productcode, p_productname, p_unitamount,
       p_unitprice, p_packagetypecode, p_packagetypename, p_packagetypeconversion,
       p_quantity, p_amount, p_discountamount, p_discountpercent, p_taxamount,
       p_taxpercent, p_totalamount, p_note, p_documentitemattributename,
       p_documentitemattributeid
   )
   ON DUPLICATE KEY UPDATE
       productcode = p_productcode,
       productname = p_productname,
       unitamount = p_unitamount,
       unitprice = p_unitprice,
       packagetypecode = p_packagetypecode,
       packagetypename = p_packagetypename,
       packagetypeconversion = p_packagetypeconversion,
       quantity = p_quantity,
       amount = p_amount,
       discountamount = p_discountamount,
       discountpercent = p_discountpercent,
       taxamount = p_taxamount,
       taxpercent = p_taxpercent,
       totalamount = p_totalamount,
       note = p_note,
       documentitemattributename = p_documentitemattributename,
       documentitemattributeid = p_documentitemattributeid;
END //

-- Purchase Order Custom Attributes upsert procedure
CREATE PROCEDURE upsert_purchase_order_customattribute(
   IN p_purchaseorderid INT,
   IN p_customattributeinfoid VARCHAR(36),
   IN p_title TEXT,
   IN p_type VARCHAR(50),
   IN p_value TEXT
)
BEGIN
   INSERT INTO purchaseordercustomattributes (
       purchaseorderid, customattributeinfoid, title, type, value
   )
   VALUES (
       p_purchaseorderid, p_customattributeinfoid, p_title, p_type, p_value
   )
   ON DUPLICATE KEY UPDATE
       title = p_title,
       type = p_type,
       value = p_value;
END //

-- Document Types upsert procedure
CREATE PROCEDURE upsert_document_type(
   IN p_documenttypeid INT,
   IN p_documenttypename VARCHAR(255)
)
BEGIN
   INSERT INTO documenttypes (documenttypeid, documenttypename)
   VALUES (p_documenttypeid, p_documenttypename)
   ON DUPLICATE KEY UPDATE
       documenttypename = p_documenttypename;
END //

-- Document Statuses upsert procedure
CREATE PROCEDURE upsert_document_status(
   IN p_documentstatusid INT,
   IN p_documenttypeid INT,
   IN p_documentstatusname VARCHAR(255)
)
BEGIN
   INSERT INTO documentstatuses (documentstatusid, documenttypeid, documentstatusname)
   VALUES (p_documentstatusid, p_documenttypeid, p_documentstatusname)
   ON DUPLICATE KEY UPDATE
       documentstatusname = p_documentstatusname;
END //

-- Document Type Pricelists upsert procedure
CREATE PROCEDURE upsert_document_type_pricelist(
   IN p_pricelistid INT,
   IN p_documenttypeid INT,
   IN p_pricelistname VARCHAR(255)
)
BEGIN
   INSERT INTO documenttypepricelists (pricelistid, documenttypeid, pricelistname)
   VALUES (p_pricelistid, p_documenttypeid, p_pricelistname)
   ON DUPLICATE KEY UPDATE
       pricelistname = p_pricelistname;
END //

DELIMITER ;


DELIMITER //

-- Products upsert procedure
CREATE PROCEDURE upsert_product(
   IN p_code VARCHAR(255),
   IN p_name TEXT,
   IN p_productgroupcode VARCHAR(20),
   IN p_productgroupname VARCHAR(80),
   IN p_active BOOLEAN,
   IN p_tag TEXT,
   IN p_unitprice DECIMAL(18,4),
   IN p_ean VARCHAR(20),
   IN p_note VARCHAR(1000),
   IN p_imageurl TEXT,
   IN p_masterproduct VARCHAR(255),
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionfirstid INT,
   IN p_metacollectionlastid INT
)
BEGIN
   INSERT INTO products (
       code, name, productgroupcode, productgroupname, active,
       tag, unitprice, ean, note, imageurl, masterproduct,
       metacollectiontotalcount, metacollectionfirstid, metacollectionlastid
   )
   VALUES (
       p_code, p_name, p_productgroupcode, p_productgroupname, p_active,
       p_tag, p_unitprice, p_ean, p_note, p_imageurl, p_masterproduct,
       p_metacollectiontotalcount, p_metacollectionfirstid, p_metacollectionlastid
   )
   ON DUPLICATE KEY UPDATE
       name = p_name,
       productgroupcode = p_productgroupcode,
       productgroupname = p_productgroupname,
       active = p_active,
       tag = p_tag,
       unitprice = p_unitprice,
       ean = p_ean,
       note = p_note,
       imageurl = p_imageurl,
       masterproduct = p_masterproduct,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionfirstid = p_metacollectionfirstid,
       metacollectionlastid = p_metacollectionlastid;
END //

-- Product Packaging Codes upsert procedure
CREATE PROCEDURE upsert_product_packagingcode(
   IN p_productcode VARCHAR(255),
   IN p_packagingcode VARCHAR(255),
   IN p_isset BOOLEAN
)
BEGIN
   INSERT INTO productpackagingcodes (productcode, packagingcode, isset)
   VALUES (p_productcode, p_packagingcode, p_isset)
   ON DUPLICATE KEY UPDATE
       isset = p_isset;
END //

-- Pricelists upsert procedure
CREATE PROCEDURE upsert_pricelist(
   IN p_id INT,
   IN p_name VARCHAR(255),
   IN p_isdefault BOOLEAN,
   IN p_active BOOLEAN,
   IN p_useprices BOOLEAN
)
BEGIN
   INSERT INTO pricelists (id, name, isdefault, active, useprices)
   VALUES (p_id, p_name, p_isdefault, p_active, p_useprices)
   ON DUPLICATE KEY UPDATE
       name = p_name,
       isdefault = p_isdefault,
       active = p_active,
       useprices = p_useprices;
END //

-- Pricelist Items upsert procedure
CREATE PROCEDURE upsert_pricelist_item(
   IN p_id INT,
   IN p_productid INT,
   IN p_productcode VARCHAR(255),
   IN p_price DOUBLE,
   IN p_active BOOLEAN,
   IN p_clientid VARCHAR(255),
   IN p_manufactureid VARCHAR(255),
   IN p_dateavailablefrom DATETIME,
   IN p_dateavailableto DATETIME,
   IN p_minquantity INT,
   IN p_maxquantity INT,
   IN p_pricelistid INT
)
BEGIN
   INSERT INTO pricelistitems (
       id, productid, productcode, price, active, clientid,
       manufactureid, dateavailablefrom, dateavailableto,
       minquantity, maxquantity, pricelistid
   )
   VALUES (
       p_id, p_productid, p_productcode, p_price, p_active, p_clientid,
       p_manufactureid, p_dateavailablefrom, p_dateavailableto,
       p_minquantity, p_maxquantity, p_pricelistid
   )
   ON DUPLICATE KEY UPDATE
       productid = p_productid,
       productcode = p_productcode,
       price = p_price,
       active = p_active,
       clientid = p_clientid,
       manufactureid = p_manufactureid,
       dateavailablefrom = p_dateavailablefrom,
       dateavailableto = p_dateavailableto,
       minquantity = p_minquantity,
       maxquantity = p_maxquantity,
       pricelistid = p_pricelistid;
END //

-- Forms upsert procedure
CREATE PROCEDURE upsert_form(
   IN p_formid INT,
   IN p_formname VARCHAR(255),
   IN p_clientcode VARCHAR(50),
   IN p_clientname VARCHAR(255),
   IN p_dateandtime DATETIME,
   IN p_representativecode VARCHAR(20),
   IN p_representativename VARCHAR(80),
   IN p_streetaddress VARCHAR(255),
   IN p_zip VARCHAR(20),
   IN p_zipext VARCHAR(20),
   IN p_city VARCHAR(255),
   IN p_state VARCHAR(255),
   IN p_country VARCHAR(255),
   IN p_email VARCHAR(255),
   IN p_phone VARCHAR(128),
   IN p_mobile VARCHAR(128),
   IN p_territory VARCHAR(80),
   IN p_longitude BIGINT,
   IN p_latitude BIGINT,
   IN p_signatureurl VARCHAR(512),
   IN p_visitstart DATETIME,
   IN p_visitend DATETIME,
   IN p_visitid INT,
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionfirstid INT,
   IN p_metacollectionlastid INT
)
BEGIN
   INSERT INTO forms (
       formid, formname, clientcode, clientname, dateandtime,
       representativecode, representativename, streetaddress, zip,
       zipext, city, state, country, email, phone, mobile,
       territory, longitude, latitude, signatureurl, visitstart,
       visitend, visitid, metacollectiontotalcount,
       metacollectionfirstid, metacollectionlastid
   )
   VALUES (
       p_formid, p_formname, p_clientcode, p_clientname, p_dateandtime,
       p_representativecode, p_representativename, p_streetaddress, p_zip,
       p_zipext, p_city, p_state, p_country, p_email, p_phone, p_mobile,
       p_territory, p_longitude, p_latitude, p_signatureurl, p_visitstart,
       p_visitend, p_visitid, p_metacollectiontotalcount,
       p_metacollectionfirstid, p_metacollectionlastid
   )
   ON DUPLICATE KEY UPDATE
       formname = p_formname,
       clientcode = p_clientcode,
       clientname = p_clientname,
       dateandtime = p_dateandtime,
       representativecode = p_representativecode,
       representativename = p_representativename,
       streetaddress = p_streetaddress,
       zip = p_zip,
       zipext = p_zipext,
       city = p_city,
       state = p_state,
       country = p_country,
       email = p_email,
       phone = p_phone,
       mobile = p_mobile,
       territory = p_territory,
       longitude = p_longitude,
       latitude = p_latitude,
       signatureurl = p_signatureurl,
       visitstart = p_visitstart,
       visitend = p_visitend,
       visitid = p_visitid,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionfirstid = p_metacollectionfirstid,
       metacollectionlastid = p_metacollectionlastid;
END //

-- Form Items upsert procedure
CREATE PROCEDURE upsert_form_item(
   IN p_formid INT,
   IN p_field VARCHAR(255),
   IN p_value TEXT,
   IN p_itemorder INT
)
BEGIN
   INSERT INTO formitems (formid, field, value, itemorder)
   VALUES (p_formid, p_field, p_value, p_itemorder)
   ON DUPLICATE KEY UPDATE
       field = p_field,
       value = p_value;
END //

-- Photos upsert procedure
CREATE PROCEDURE upsert_photo(
   IN p_photoid INT,
   IN p_clientcode VARCHAR(50),
   IN p_clientname VARCHAR(255),
   IN p_note VARCHAR(1000),
   IN p_dateandtime DATETIME,
   IN p_photourl VARCHAR(512),
   IN p_representativecode VARCHAR(20),
   IN p_representativename VARCHAR(80),
   IN p_visitid INT,
   IN p_tag TEXT,
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionfirstid INT,
   IN p_metacollectionlastid INT
)
BEGIN
   INSERT INTO photos (
       photoid, clientcode, clientname, note, dateandtime,
       photourl, representativecode, representativename, visitid,
       tag, metacollectiontotalcount, metacollectionfirstid,
       metacollectionlastid
   )
   VALUES (
       p_photoid, p_clientcode, p_clientname, p_note, p_dateandtime,
       p_photourl, p_representativecode, p_representativename, p_visitid,
       p_tag, p_metacollectiontotalcount, p_metacollectionfirstid,
       p_metacollectionlastid
   )
   ON DUPLICATE KEY UPDATE
       clientcode = p_clientcode,
       clientname = p_clientname,
       note = p_note,
       dateandtime = p_dateandtime,
       photourl = p_photourl,
       representativecode = p_representativecode,
       representativename = p_representativename,
       visitid = p_visitid,
       tag = p_tag,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionfirstid = p_metacollectionfirstid,
       metacollectionlastid = p_metacollectionlastid;
END //

DELIMITER ;
DELIMITER //

-- Photos upsert procedure
CREATE PROCEDURE upsert_photo(
    IN p_photoid INT,
    IN p_clientcode VARCHAR(50),
    IN p_clientname VARCHAR(255),
    IN p_note VARCHAR(1000),
    IN p_dateandtime DATETIME,
    IN p_photourl VARCHAR(512),
    IN p_representativecode VARCHAR(20),
    IN p_representativename VARCHAR(80),
    IN p_visitid INT,
    IN p_tag TEXT,
    IN p_metacollectiontotalcount INT,
    IN p_metacollectionfirstid INT,
    IN p_metacollectionlastid INT
)
BEGIN
    INSERT INTO photos (
        photoid, clientcode, clientname, note, dateandtime, photourl,
        representativecode, representativename, visitid, tag,
        metacollectiontotalcount, metacollectionfirstid, metacollectionlastid
    )
    VALUES (
        p_photoid, p_clientcode, p_clientname, p_note, p_dateandtime, p_photourl,
        p_representativecode, p_representativename, p_visitid, p_tag,
        p_metacollectiontotalcount, p_metacollectionfirstid, p_metacollectionlastid
    )
    ON DUPLICATE KEY UPDATE
        clientcode = p_clientcode,
        clientname = p_clientname,
        note = p_note,
        dateandtime = p_dateandtime,
        photourl = p_photourl,
        representativecode = p_representativecode,
        representativename = p_representativename,
        visitid = p_visitid,
        tag = p_tag,
        metacollectiontotalcount = p_metacollectiontotalcount,
        metacollectionfirstid = p_metacollectionfirstid,
        metacollectionlastid = p_metacollectionlastid;
END //

-- Daily Working Time upsert procedure
CREATE PROCEDURE upsert_daily_working_time(
    IN p_dailyworkingtimeid INT,
    IN p_date DATETIME,
    IN p_dateandtimestart DATETIME,
    IN p_dateandtimeend DATETIME,
    IN p_length INT,
    IN p_mileagestart INT,
    IN p_mileageend INT,
    IN p_mileagetotal INT,
    IN p_latitudestart BIGINT,
    IN p_longitudestart BIGINT,
    IN p_latitudeend BIGINT,
    IN p_longitudeend BIGINT,
    IN p_representativecode VARCHAR(20),
    IN p_representativename VARCHAR(80),
    IN p_note VARCHAR(255),
    IN p_tag TEXT,
    IN p_noofvisits INT,
    IN p_minofvisits DATETIME,
    IN p_maxofvisits DATETIME,
    IN p_minmaxvisitstime INT,
    IN p_timeatclient INT,
    IN p_timeattravel INT,
    IN p_metacollectiontotalcount INT,
    IN p_metacollectionfirstid INT,
    IN p_metacollectionlastid INT
)
BEGIN
    INSERT INTO dailyworkingtime (
        dailyworkingtimeid, date, dateandtimestart, dateandtimeend, length,
        mileagestart, mileageend, mileagetotal, latitudestart, longitudestart,
        latitudeend, longitudeend, representativecode, representativename,
        note, tag, noofvisits, minofvisits, maxofvisits, minmaxvisitstime,
        timeatclient, timeattravel, metacollectiontotalcount,
        metacollectionfirstid, metacollectionlastid
    )
    VALUES (
        p_dailyworkingtimeid, p_date, p_dateandtimestart, p_dateandtimeend, p_length,
        p_mileagestart, p_mileageend, p_mileagetotal, p_latitudestart, p_longitudestart,
        p_latitudeend, p_longitudeend, p_representativecode, p_representativename,
        p_note, p_tag, p_noofvisits, p_minofvisits, p_maxofvisits, p_minmaxvisitstime,
        p_timeatclient, p_timeattravel, p_metacollectiontotalcount,
        p_metacollectionfirstid, p_metacollectionlastid
    )
    ON DUPLICATE KEY UPDATE
        date = p_date,
        dateandtimestart = p_dateandtimestart,
        dateandtimeend = p_dateandtimeend,
        length = p_length,
        mileagestart = p_mileagestart,
        mileageend = p_mileageend,
        mileagetotal = p_mileagetotal,
        latitudestart = p_latitudestart,
        longitudestart = p_longitudestart,
        latitudeend = p_latitudeend,
        longitudeend = p_longitudeend,
        representativecode = p_representativecode,
        representativename = p_representativename,
        note = p_note,
        tag = p_tag,
        noofvisits = p_noofvisits,
        minofvisits = p_minofvisits,
        maxofvisits = p_maxofvisits,
        minmaxvisitstime = p_minmaxvisitstime,
        timeatclient = p_timeatclient,
        timeattravel = p_timeattravel,
        metacollectiontotalcount = p_metacollectiontotalcount,
        metacollectionfirstid = p_metacollectionfirstid,
        metacollectionlastid = p_metacollectionlastid;
END //

-- Visit Schedules upsert procedure
CREATE PROCEDURE upsert_visit_schedule(
    IN p_scheduledateandtime DATETIME,
    IN p_clientcode VARCHAR(50),
    IN p_representativecode VARCHAR(20),
    IN p_representativename VARCHAR(80),
    IN p_clientname VARCHAR(255),
    IN p_streetaddress VARCHAR(255),
    IN p_zip VARCHAR(20),
    IN p_zipext VARCHAR(20),
    IN p_city VARCHAR(255),
    IN p_state VARCHAR(255),
    IN p_country VARCHAR(255),
    IN p_territory VARCHAR(80),
    IN p_visitnote TEXT,
    IN p_duedate DATETIME,
    IN p_metacollectiontotalcount INT
)
BEGIN
    INSERT INTO visitschedules (
        scheduledateandtime, clientcode, representativecode, representativename,
        clientname, streetaddress, zip, zipext, city, state, country,
        territory, visitnote, duedate, metacollectiontotalcount
    )
    VALUES (
        p_scheduledateandtime, p_clientcode, p_representativecode, p_representativename,
        p_clientname, p_streetaddress, p_zip, p_zipext, p_city, p_state, p_country,
        p_territory, p_visitnote, p_duedate, p_metacollectiontotalcount
    )
    ON DUPLICATE KEY UPDATE
        representativename = p_representativename,
        clientname = p_clientname,
        streetaddress = p_streetaddress,
        zip = p_zip,
        zipext = p_zipext,
        city = p_city,
        state = p_state,
        country = p_country,
        territory = p_territory,
        visitnote = p_visitnote,
        duedate = p_duedate,
        metacollectiontotalcount = p_metacollectiontotalcount;
END //

-- Visit Realizations upsert procedure
CREATE PROCEDURE upsert_visit_realization(
    IN p_scheduleid VARCHAR(36),
    IN p_projectid VARCHAR(36),
    IN p_employeeid VARCHAR(36),
    IN p_employeecode VARCHAR(20),
    IN p_placeid VARCHAR(36),
    IN p_placecode VARCHAR(20),
    IN p_modifiedutc DATETIME,
    IN p_timezone VARCHAR(255),
    IN p_schedulenote TEXT,
    IN p_status VARCHAR(20),
    IN p_datetimestart DATETIME,
    IN p_datetimestartutc DATETIME,
    IN p_datetimeend DATETIME,
    IN p_datetimeendutc DATETIME,
    IN p_plandatetimestart DATETIME,
    IN p_plandatetimestartutc DATETIME,
    IN p_plandatetimeend DATETIME,
    IN p_plandatetimeendutc DATETIME,
    IN p_metacollectiontotalcount INT
)
BEGIN
    INSERT INTO visitrealizations (
        scheduleid, projectid, employeeid, employeecode, placeid, placecode,
        modifiedutc, timezone, schedulenote, status, datetimestart,
        datetimestartutc, datetimeend, datetimeendutc, plandatetimestart,
        plandatetimestartutc, plandatetimeend, plandatetimeendutc,
        metacollectiontotalcount
    )
    VALUES (
        p_scheduleid, p_projectid, p_employeeid, p_employeecode, p_placeid, p_placecode,
        p_modifiedutc, p_timezone, p_schedulenote, p_status, p_datetimestart,
        p_datetimestartutc, p_datetimeend, p_datetimeendutc, p_plandatetimestart,
        p_plandatetimestartutc, p_plandatetimeend, p_plandatetimeendutc,
        p_metacollectiontotalcount
    )
    ON DUPLICATE KEY UPDATE
        projectid = p_projectid,
        employeeid = p_employeeid,
        employeecode = p_employeecode,
        placeid = p_placeid,
        placecode = p_placecode,
        modifiedutc = p_modifiedutc,
        timezone = p_timezone,
        schedulenote = p_schedulenote,
        status = p_status,
        datetimestart = p_datetimestart,
        datetimestartutc = p_datetimestartutc,
        datetimeend = p_datetimeend,
        datetimeendutc = p_datetimeendutc,
        plandatetimestart = p_plandatetimestart,
        plandatetimestartutc = p_plandatetimestartutc,
        plandatetimeend = p_plandatetimeend,
        plandatetimeendutc = p_plandatetimeendutc,
        metacollectiontotalcount = p_metacollectiontotalcount;
END //

-- Visit Realization Tasks upsert procedure
CREATE PROCEDURE upsert_visit_realization_task(
    IN p_scheduleid VARCHAR(36),
    IN p_entityid VARCHAR(36),
    IN p_tasktype VARCHAR(20),
    IN p_tasknote TEXT,
    IN p_completed BOOLEAN
)
BEGIN
    INSERT INTO visitrealizationtasks (
        scheduleid, entityid, tasktype, tasknote, completed
    )
    VALUES (
        p_scheduleid, p_entityid, p_tasktype, p_tasknote, p_completed
    )
    ON DUPLICATE KEY UPDATE
        tasktype = p_tasktype,
        tasknote = p_tasknote,
        completed = p_completed;
END //

DELIMITER ;

DELIMITER //

-- Representatives upsert procedure
CREATE PROCEDURE upsert_representative(
   IN p_code VARCHAR(20),
   IN p_name VARCHAR(80),
   IN p_note VARCHAR(255),
   IN p_email VARCHAR(256),
   IN p_phone VARCHAR(128),
   IN p_active BOOLEAN,
   IN p_address1 VARCHAR(256),
   IN p_address2 VARCHAR(256),
   IN p_city VARCHAR(256),
   IN p_state VARCHAR(256),
   IN p_zipcode VARCHAR(20),
   IN p_zipcodeext VARCHAR(20),
   IN p_country VARCHAR(256),
   IN p_countrycode VARCHAR(20)
)
BEGIN
   INSERT INTO representatives (
       code, name, note, email, phone, active, address1, address2,
       city, state, zipcode, zipcodeext, country, countrycode
   )
   VALUES (
       p_code, p_name, p_note, p_email, p_phone, p_active, p_address1,
       p_address2, p_city, p_state, p_zipcode, p_zipcodeext,
       p_country, p_countrycode
   )
   ON DUPLICATE KEY UPDATE
       name = p_name,
       note = p_note,
       email = p_email,
       phone = p_phone,
       active = p_active,
       address1 = p_address1,
       address2 = p_address2,
       city = p_city,
       state = p_state,
       zipcode = p_zipcode,
       zipcodeext = p_zipcodeext,
       country = p_country,
       countrycode = p_countrycode;
END //

-- Representative Territories upsert procedure
CREATE PROCEDURE upsert_representative_territory(
   IN p_representative_code VARCHAR(20),
   IN p_territory_path TEXT
)
BEGIN
   INSERT INTO representative_territories (representative_code, territory_path)
   VALUES (p_representative_code, p_territory_path)
   ON DUPLICATE KEY UPDATE
       territory_path = p_territory_path;
END //

-- Representative Attributes upsert procedure
CREATE PROCEDURE upsert_representative_attribute(
   IN p_representative_code VARCHAR(20),
   IN p_title VARCHAR(255),
   IN p_type VARCHAR(20),
   IN p_value TEXT
)
BEGIN
   INSERT INTO representative_attributes (representative_code, title, type, value)
   VALUES (p_representative_code, p_title, p_type, p_value)
   ON DUPLICATE KEY UPDATE
       type = p_type,
       value = p_value;
END //

-- Users upsert procedure
CREATE PROCEDURE upsert_user(
   IN p_id VARCHAR(36),
   IN p_code VARCHAR(20),
   IN p_name VARCHAR(80),
   IN p_email VARCHAR(100),
   IN p_active BOOLEAN,
   IN p_role VARCHAR(80),
   IN p_note VARCHAR(255),
   IN p_phone VARCHAR(128),
   IN p_sendemailenabled BOOLEAN,
   IN p_address1 VARCHAR(256),
   IN p_address2 VARCHAR(256),
   IN p_city VARCHAR(256),
   IN p_state VARCHAR(256),
   IN p_zipcode VARCHAR(20),
   IN p_zipcodeext VARCHAR(20),
   IN p_country VARCHAR(256),
   IN p_countrycode VARCHAR(20),
   IN p_metacollectiontotalcount INT,
   IN p_metacollectionlasttimestamp BIGINT
)
BEGIN
   INSERT INTO users (
       id, code, name, email, active, role, note, phone,
       sendemailenabled, address1, address2, city, state,
       zipcode, zipcodeext, country, countrycode,
       metacollectiontotalcount, metacollectionlasttimestamp
   )
   VALUES (
       p_id, p_code, p_name, p_email, p_active, p_role, p_note,
       p_phone, p_sendemailenabled, p_address1, p_address2,
       p_city, p_state, p_zipcode, p_zipcodeext, p_country,
       p_countrycode, p_metacollectiontotalcount,
       p_metacollectionlasttimestamp
   )
   ON DUPLICATE KEY UPDATE
       code = p_code,
       name = p_name,
       email = p_email,
       active = p_active,
       role = p_role,
       note = p_note,
       phone = p_phone,
       sendemailenabled = p_sendemailenabled,
       address1 = p_address1,
       address2 = p_address2,
       city = p_city,
       state = p_state,
       zipcode = p_zipcode,
       zipcodeext = p_zipcodeext,
       country = p_country,
       countrycode = p_countrycode,
       metacollectiontotalcount = p_metacollectiontotalcount,
       metacollectionlasttimestamp = p_metacollectionlasttimestamp;
END //

-- User Territories upsert procedure
CREATE PROCEDURE upsert_user_territory(
   IN p_user_id VARCHAR(36),
   IN p_territory_path TEXT
)
BEGIN
   INSERT INTO user_territories (user_id, territory_path)
   VALUES (p_user_id, p_territory_path)
   ON DUPLICATE KEY UPDATE
       territory_path = p_territory_path;
END //

-- User Attributes upsert procedure
CREATE PROCEDURE upsert_user_attribute(
   IN p_user_id VARCHAR(36),
   IN p_title VARCHAR(255),
   IN p_value TEXT
)
BEGIN
   INSERT INTO user_attributes (user_id, title, value)
   VALUES (p_user_id, p_title, p_value)
   ON DUPLICATE KEY UPDATE
       value = p_value;
END //

-- User Permissions upsert procedure
CREATE PROCEDURE upsert_user_permission(
   IN p_user_id VARCHAR(36),
   IN p_permission VARCHAR(255)
)
BEGIN
   INSERT INTO user_permissions (user_id, permission)
   VALUES (p_user_id, p_permission)
   ON DUPLICATE KEY UPDATE
       permission = p_permission;
END //

-- Import Status upsert procedure
CREATE PROCEDURE upsert_import_status(
   IN p_importjobid BIGINT,
   IN p_importstatus VARCHAR(20),
   IN p_rowsinserted INT,
   IN p_rowsupdated INT,
   IN p_rowsinvalid INT,
   IN p_rowstotal INT
)
BEGIN
   INSERT INTO import_status (
       importjobid, importstatus, rowsinserted, rowsupdated,
       rowsinvalid, rowstotal
   )
   VALUES (
       p_importjobid, p_importstatus, p_rowsinserted, p_rowsupdated,
       p_rowsinvalid, p_rowstotal
   )
   ON DUPLICATE KEY UPDATE
       importstatus = p_importstatus,
       rowsinserted = p_rowsinserted,
       rowsupdated = p_rowsupdated,
       rowsinvalid = p_rowsinvalid,
       rowstotal = p_rowstotal;
END //

-- Import Warnings upsert procedure
CREATE PROCEDURE upsert_import_warning(
   IN p_importjobid BIGINT,
   IN p_itemid VARCHAR(255),
   IN p_itemname VARCHAR(255),
   IN p_itemstatus TEXT
)
BEGIN
   INSERT INTO import_warnings (importjobid, itemid, itemname, itemstatus)
   VALUES (p_importjobid, p_itemid, p_itemname, p_itemstatus)
   ON DUPLICATE KEY UPDATE
       itemname = p_itemname,
       itemstatus = p_itemstatus;
END //

-- Import Errors upsert procedure
CREATE PROCEDURE upsert_import_error(
   IN p_importjobid BIGINT,
   IN p_itemid VARCHAR(255),
   IN p_itemname VARCHAR(255),
   IN p_itemstatus TEXT
)
BEGIN
   INSERT INTO import_errors (importjobid, itemid, itemname, itemstatus)
   VALUES (p_importjobid, p_itemid, p_itemname, p_itemstatus)
   ON DUPLICATE KEY UPDATE
       itemname = p_itemname,
       itemstatus = p_itemstatus;
END //

DELIMITER ;

