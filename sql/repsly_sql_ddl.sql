create table clients (
   clientid int primary key,
   timestamp bigint,
   code varchar(50),
   name varchar(255),
   active boolean,
   tag text,
   territory text,
   representativecode varchar(20),
   representativename varchar(80),
   streetaddress varchar(255),
   zip varchar(20),
   zipext varchar(20),
   city varchar(255),
   state varchar(255),
   country varchar(255),
   email varchar(255),
   phone varchar(128),
   mobile varchar(128),
   website varchar(255),
   contactname varchar(255),
   contacttitle varchar(50),
   note varchar(255),
   status text,
   accountcode text,
   lasttimestamp bigint,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionlasttimestamp bigint,
   
   index idx_timestamp (timestamp),
   index idx_code (code),
   index idx_name (name),
   index idx_rep_code (representativecode)
);

create table clientcustomfields (
   clientid int,
   field varchar(255),
   value text,
   foreign key (clientid) references clients(clientid),
   index idx_client_field (clientid, field)
);

create table clientpricelists (
   clientid int,
   name text,
   foreign key (clientid) references clients(clientid),
   index idx_client (clientid)
);

create table clientnotes (
   clientnoteid int primary key,
   timestamp bigint,
   dateandtime datetime,
   representativecode varchar(20),
   representativename varchar(80),
   clientcode varchar(50),
   clientname varchar(255),
   streetaddress varchar(255),
   zip varchar(20),
   zipext varchar(20),
   city varchar(255),
   state varchar(255),
   country varchar(255),
   email varchar(255),
   phone varchar(128),
   mobile varchar(128),
   territory varchar(80),
   longitude bigint,
   latitude bigint,
   note text,
   visitid int,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes for common lookups
   index idx_timestamp (timestamp),
   index idx_clientcode (clientcode),
   index idx_representative (representativecode),
   index idx_visitid (visitid),
   foreign key (visitid) references visits(visitid)
);

create table visits (
   visitid int primary key,
   timestamp bigint,
   date datetime,
   representativecode varchar(20),
   representativename varchar(80),
   explicitcheckin boolean,
   dateandtimestart datetime,
   dateandtimeend datetime,
   clientcode varchar(50),
   clientname varchar(255),
   streetaddress varchar(255),
   zip varchar(20),
   zipext varchar(20),
   city varchar(255),
   state varchar(255),
   country varchar(255),
   territory varchar(80),
   latitudestart bigint,
   longitudestart bigint,
   latitudeend bigint,
   longitudeend bigint,
   precisionstart int,
   precisionend int,
   visitstatusbyschedule int,
   visitended boolean,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionlasttimestamp bigint,
   
   -- indexes
   index idx_timestamp (timestamp),
   index idx_clientcode (clientcode),
   index idx_representative (representativecode),
   index idx_dates (dateandtimestart, dateandtimeend),
   index idx_status (visitstatusbyschedule)
);

create table retailaudits (
   retailauditid int primary key,
   retailauditname varchar(50),
   cancelled boolean,
   clientcode varchar(50),
   clientname varchar(255),
   dateandtime datetime,
   representativecode varchar(20),
   representativename varchar(80),
   note varchar(255),
   visitid int,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes
   index idx_clientcode (clientcode),
   index idx_representative (representativecode),
   index idx_visitid (visitid),
   foreign key (visitid) references visits(visitid)
);

create table retailaudititems (
   retailauditid int,
   productgroupcode varchar(20),
   productgroupname varchar(80),
   productcode varchar(20),
   productname varchar(80),
   present boolean,
   price decimal(18,4),
   promotion boolean,
   shelfshare decimal(18,4),
   shelfsharepercent decimal(18,4),
   soldout boolean,
   stock int,
   
   foreign key (retailauditid) references retailaudits(retailauditid),
   index idx_products (productcode, productgroupcode)
);

create table retailauditcustomfields (
   retailauditid int,
   field varchar(255),
   value varchar(255),
   
   foreign key (retailauditid) references retailaudits(retailauditid),
   index idx_audit_field (retailauditid, field)
);

create table purchaseorders (
   purchaseorderid int primary key,
   transactiontype varchar(50),
   documenttypeid int,
   documenttypename varchar(100),
   documentstatus varchar(100),
   documentstatusid int,
   documentitemattributecaption varchar(255),
   dateandtime datetime,
   documentno varchar(50),
   clientcode varchar(50),
   clientname varchar(255),
   documentdate datetime,
   duedate datetime,
   representativecode varchar(20),
   representativename varchar(80),
   signatureurl varchar(512),
   note varchar(255),
   taxable boolean,
   visitid int,
   streetaddress varchar(255),
   zip varchar(20),
   zipext varchar(20),
   city varchar(255),
   state varchar(255),
   country varchar(255),
   countrycode varchar(20),
   originaldocumentnumber text,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes
   index idx_documentno (documentno),
   index idx_clientcode (clientcode),
   index idx_representative (representativecode),
   index idx_dates (documentdate, duedate),
   foreign key (visitid) references visits(visitid)
);

create table purchaseorderitems (
   purchaseorderid int,
   lineno int,
   productcode varchar(20),
   productname varchar(80),
   unitamount decimal(18,4),
   unitprice decimal(18,4),
   packagetypecode varchar(45),
   packagetypename varchar(40),
   packagetypeconversion int,
   quantity int,
   amount decimal(18,4),
   discountamount decimal(18,4),
   discountpercent decimal(18,4),
   taxamount decimal(18,4),
   taxpercent decimal(18,4),
   totalamount decimal(18,4),
   note varchar(255),
   documentitemattributename varchar(100),
   documentitemattributeid int,
   
   primary key (purchaseorderid, lineno),
   foreign key (purchaseorderid) references purchaseorders(purchaseorderid),
   index idx_product (productcode)
);

create table purchaseordercustomattributes (
   purchaseorderid int,
   customattributeinfoid varchar(36),  -- for GUID
   title text,
   type varchar(50),
   value text,
   
   foreign key (purchaseorderid) references purchaseorders(purchaseorderid),
   index idx_attribute (customattributeinfoid)
);


create table documenttypes (
   documenttypeid int primary key,
   documenttypename varchar(255)
);

create table documentstatuses (
   documentstatusid int,
   documenttypeid int,
   documentstatusname varchar(255),
   
   primary key (documentstatusid, documenttypeid),
   foreign key (documenttypeid) references documenttypes(documenttypeid),
   index idx_status (documentstatusid)
);

create table documenttypepricelists (
   pricelistid int,
   documenttypeid int,
   pricelistname varchar(255),
   
   primary key (pricelistid, documenttypeid),
   foreign key (documenttypeid) references documenttypes(documenttypeid),
   index idx_pricelist (pricelistid)
);

create table products (
   code varchar(255) primary key,
   name text,
   productgroupcode varchar(20),
   productgroupname varchar(80),
   active boolean,
   tag text,
   unitprice decimal(18,4),
   ean varchar(20),
   note varchar(1000),
   imageurl text,
   masterproduct varchar(255),
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes
   index idx_productgroup (productgroupcode),
   index idx_ean (ean),
   index idx_masterproduct (masterproduct)
);

create table productpackagingcodes (
   productcode varchar(255),
   packagingcode varchar(255),
   isset boolean,
   
   foreign key (productcode) references products(code),
   index idx_product (productcode)
);


create table pricelists (
   id int primary key,
   name varchar(255),
   isdefault boolean,
   active boolean,
   useprices boolean
);

create table pricelistitems (
   id int primary key,
   productid int,
   productcode varchar(255),
   price double,
   active boolean,
   clientid varchar(255),
   manufactureid varchar(255),
   dateavailablefrom datetime,
   dateavailableto datetime,
   minquantity int,
   maxquantity int,
   pricelistid int,
   
   -- indexes
   index idx_product (productid, productcode),
   index idx_client (clientid),
   index idx_dates (dateavailablefrom, dateavailableto),
   foreign key (pricelistid) references pricelists(id)
);

create table forms (
   formid int primary key,
   formname varchar(255),
   clientcode varchar(50),
   clientname varchar(255),
   dateandtime datetime,
   representativecode varchar(20),
   representativename varchar(80),
   streetaddress varchar(255),
   zip varchar(20),
   zipext varchar(20),
   city varchar(255),
   state varchar(255),
   country varchar(255),
   email varchar(255),
   phone varchar(128),
   mobile varchar(128),
   territory varchar(80),
   longitude bigint,
   latitude bigint,
   signatureurl varchar(512),
   visitstart datetime,
   visitend datetime,
   visitid int,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes
   index idx_client (clientcode),
   index idx_representative (representativecode),
   index idx_dates (visitstart, visitend),
   foreign key (visitid) references visits(visitid)
);

create table formitems (
   formid int,
   field varchar(255),
   value text,
   itemorder int,  -- to maintain question order
   
   primary key (formid, itemorder),
   foreign key (formid) references forms(formid),
   index idx_field (field)
);

create table photos (
   photoid int primary key,
   clientcode varchar(50),
   clientname varchar(255),
   note varchar(1000),
   dateandtime datetime,
   photourl varchar(512),
   representativecode varchar(20),
   representativename varchar(80),
   visitid int,
   tag text,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes
   index idx_client (clientcode),
   index idx_representative (representativecode),
   index idx_visit (visitid),
   index idx_date (dateandtime),
   foreign key (visitid) references visits(visitid)
);

create table dailyworkingtime (
   dailyworkingtimeid int primary key,
   date datetime,
   dateandtimestart datetime,
   dateandtimeend datetime,
   length int,
   mileagestart int,
   mileageend int,
   mileagetotal int,
   latitudestart bigint,
   longitudestart bigint,
   latitudeend bigint,
   longitudeend bigint,
   representativecode varchar(20),
   representativename varchar(80),
   note varchar(255),
   tag text,
   noofvisits int,
   minofvisits datetime,
   maxofvisits datetime,
   minmaxvisitstime int,
   timeatclient int,
   timeattravel int,
   
   -- metadata fields
   metacollectiontotalcount int,
   metacollectionfirstid int,
   metacollectionlastid int,
   
   -- indexes
   index idx_representative (representativecode),
   index idx_dates (date, dateandtimestart, dateandtimeend),
   index idx_visit_times (minofvisits, maxofvisits)
);

create table visitschedules (
   -- composite primary key since there's no single unique id
   scheduledateandtime datetime,
   clientcode varchar(50),
   representativecode varchar(20),
   primary key (scheduledateandtime, clientcode, representativecode),
   
   representativename varchar(80),
   clientname varchar(255),
   streetaddress varchar(255),
   zip varchar(20),
   zipext varchar(20),
   city varchar(255),
   state varchar(255),
   country varchar(255),
   territory varchar(80),
   visitnote text,
   duedate datetime,
   
   -- metadata field
   metacollectiontotalcount int,
   
   -- indexes
   index idx_dates (scheduledateandtime, duedate),
   index idx_representative (representativecode),
   index idx_client (clientcode),
   index idx_territory (territory)
);

create table visitrealizations (
   scheduleid varchar(36) primary key,  -- for GUID
   projectid varchar(36),              -- for GUID
   employeeid varchar(36),             -- for GUID
   employeecode varchar(20),
   placeid varchar(36),                -- for GUID
   placecode varchar(20),
   modifiedutc datetime,
   timezone varchar(255),
   schedulenote text,
   status varchar(20),
   datetimestart datetime,
   datetimestartutc datetime,
   datetimeend datetime,
   datetimeendutc datetime,
   plandatetimestart datetime,
   plandatetimestartutc datetime,
   plandatetimeend datetime,
   plandatetimeendutc datetime,
   
   -- metadata field
   metacollectiontotalcount int,
   
   -- indexes
   index idx_employee (employeeid, employeecode),
   index idx_place (placeid, placecode),
   index idx_modified (modifiedutc),
   index idx_dates (datetimestart, datetimeend),
   index idx_plandates (plandatetimestart, plandatetimeend),
   index idx_status (status)
);

create table visitrealizationtasks (
   scheduleid varchar(36),
   entityid varchar(36),    -- for GUID
   tasktype varchar(20),
   tasknote text,
   completed boolean,
   
   primary key (scheduleid, entityid),
   foreign key (scheduleid) references visitrealizations(scheduleid),
   index idx_tasktype (tasktype),
   index idx_completed (completed)
);

create table representatives (
    code varchar(20) primary key,
    name varchar(80),
    note varchar(255),
    email varchar(256),
    phone varchar(128),
    active boolean,
    address1 varchar(256),
    address2 varchar(256),
    city varchar(256),
    state varchar(256),
    zipcode varchar(20),
    zipcodeext varchar(20),
    country varchar(256),
    countrycode varchar(20),
    
    -- indexes
    index idx_email (email),
    index idx_active (active)
);

create table representative_territories (
    representative_code varchar(20),
    territory_path text,
    
    foreign key (representative_code) references representatives(code),
    index idx_representative (representative_code)
);

create table representative_attributes (
    representative_code varchar(20),
    title varchar(255),
    type varchar(20),
    value text,
    
    foreign key (representative_code) references representatives(code),
    index idx_representative (representative_code)
);

create table users (
    id varchar(36) primary key,  -- for GUID
    code varchar(20),
    name varchar(80),
    email varchar(100),
    active boolean,
    role varchar(80),
    note varchar(255),
    phone varchar(128),
    sendemailenabled boolean,
    address1 varchar(256),
    address2 varchar(256),
    city varchar(256),
    state varchar(256),
    zipcode varchar(20),
    zipcodeext varchar(20),
    country varchar(256),
    countrycode varchar(20),
    
    -- metadata fields
    metacollectiontotalcount int,
    metacollectionlasttimestamp bigint,
    
    -- indexes
    index idx_code (code),
    index idx_email (email),
    index idx_active (active)
);

create table user_territories (
    user_id varchar(36),
    territory_path text,
    
    foreign key (user_id) references users(id),
    index idx_user (user_id)
);

create table user_attributes (
    user_id varchar(36),
    title varchar(255),
    value text,
    
    foreign key (user_id) references users(id),
    index idx_user (user_id)
);

create table user_permissions (
    user_id varchar(36),
    permission varchar(255),
    
    foreign key (user_id) references users(id),
    index idx_user (user_id)
);

create table import_status (
    importjobid bigint primary key,
    importstatus varchar(20),
    rowsinserted int,
    rowsupdated int,
    rowsinvalid int,
    rowstotal int
);

create table import_warnings (
    importjobid bigint,
    itemid varchar(255),
    itemname varchar(255),
    itemstatus text,
    
    foreign key (importjobid) references import_status(importjobid),
    index idx_import (importjobid)
);

create table import_errors (
    importjobid bigint,
    itemid varchar(255),
    itemname varchar(255),
    itemstatus text,
    
    foreign key (importjobid) references import_status(importjobid),
    index idx_import (importjobid)
);

ALTER TABLE clientcustomfields ADD UNIQUE KEY unique_client_field (clientid, field);
ALTER TABLE clientpricelists ADD UNIQUE KEY unique_client_name (clientid, name(255));

ALTER TABLE retailaudititems 
ADD UNIQUE KEY unique_retail_audit_product (retailauditid, productgroupcode, productcode);

ALTER TABLE retailauditcustomfields 
ADD UNIQUE KEY unique_retail_audit_field (retailauditid, field);

ALTER TABLE purchaseordercustomattributes 
ADD UNIQUE KEY unique_po_attribute (purchaseorderid, customattributeinfoid);

ALTER TABLE productpackagingcodes 
ADD UNIQUE KEY unique_product_packaging (productcode, packagingcode);

ALTER TABLE representative_territories 
ADD UNIQUE KEY unique_rep_territory (representative_code, territory_path(255));

ALTER TABLE representative_attributes 
ADD UNIQUE KEY unique_rep_attribute (representative_code, title);

ALTER TABLE user_territories 
ADD UNIQUE KEY unique_user_territory (user_id, territory_path(255));

ALTER TABLE user_attributes 
ADD UNIQUE KEY unique_user_attribute (user_id, title);

ALTER TABLE user_permissions 
ADD UNIQUE KEY unique_user_permission (user_id, permission);

ALTER TABLE import_warnings 
ADD UNIQUE KEY unique_import_warning (importjobid, itemid);

ALTER TABLE import_errors 
ADD UNIQUE KEY unique_import_error (importjobid, itemid);
