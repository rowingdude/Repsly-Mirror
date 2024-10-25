#include "endpt_modules/purchaseorders.h"
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <mysql/mysql.h>
#include <time.h>

unsigned long length_value = sizeof(double);

static bool log_batch_status(const struct PurchaseOrderBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("purchaseorders_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_purchaseorder_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_purchase_order(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int purchaseorderid = 0;
    char transactiontype[51] = {0};
    int documenttypeid = 0;
    char documenttypename[101] = {0};
    char documentstatus[101] = {0};
    int documentstatusid = 0;
    char documentitemattributecaption[256] = {0};
    char dateandtime[64] = {0};
    char documentno[51] = {0};
    char clientcode[51] = {0};
    char clientname[256] = {0};
    char documentdate[64] = {0};
    char duedate[64] = {0};
    char representativecode[21] = {0};
    char representativename[81] = {0};
    char signatureurl[513] = {0};
    char note[256] = {0};
    bool taxable = false;
    int visitid = 0;
    char streetaddress[256] = {0};
    char zip[21] = {0};
    char zipext[21] = {0};
    char city[256] = {0};
    char state[256] = {0};
    char country[256] = {0};
    char countrycode[21] = {0};
    char originaldocumentnumber[1024] = {0};
    int metacollectiontotalcount = 0;
    int metacollectionfirstid = 0;
    int metacollectionlastid = 0;

    // NULL indicators
    bool null_indicators[30] = {0};

    struct json_object *temp;

    // Extract all fields from JSON
    if (json_object_object_get_ex(record, "PurchaseOrderID", &temp))
        purchaseorderid = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "TransactionType", &temp))
        strncpy(transactiontype, json_object_get_string(temp), sizeof(transactiontype) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "DocumentTypeID", &temp))
        documenttypeid = json_object_get_int(temp);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "DocumentTypeName", &temp))
        strncpy(documenttypename, json_object_get_string(temp), sizeof(documenttypename) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "DocumentStatus", &temp))
        strncpy(documentstatus, json_object_get_string(temp), sizeof(documentstatus) - 1);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "DocumentStatusID", &temp))
        documentstatusid = json_object_get_int(temp);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "DocumentItemAttributeCaption", &temp))
        strncpy(documentitemattributecaption, json_object_get_string(temp), 
                sizeof(documentitemattributecaption) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "DateAndTime", &temp)) {
        const char *date_str = json_object_get_string(temp);
        strncpy(dateandtime, date_str, sizeof(dateandtime) - 1);
    }
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "DocumentNo", &temp))
        strncpy(documentno, json_object_get_string(temp), sizeof(documentno) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "ClientCode", &temp))
        strncpy(clientcode, json_object_get_string(temp), sizeof(clientcode) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "ClientName", &temp))
        strncpy(clientname, json_object_get_string(temp), sizeof(clientname) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "DocumentDate", &temp)) {
        const char *doc_date = json_object_get_string(temp);
        strncpy(documentdate, doc_date, sizeof(documentdate) - 1);
    }
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "DueDate", &temp)) {
        const char *due = json_object_get_string(temp);
        strncpy(duedate, due, sizeof(duedate) - 1);
    } else
        null_indicators[12] = 1; 
    
    if (json_object_object_get_ex(record, "RepresentativeCode", &temp))
        strncpy(representativecode, json_object_get_string(temp), sizeof(representativecode) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "RepresentativeName", &temp))
        strncpy(representativename, json_object_get_string(temp), sizeof(representativename) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "SignatureURL", &temp))
        strncpy(signatureurl, json_object_get_string(temp), sizeof(signatureurl) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[16] = 1;

    if (json_object_object_get_ex(record, "Taxable", &temp))
        taxable = json_object_get_boolean(temp);
    else
        null_indicators[17] = 1;

    if (json_object_object_get_ex(record, "VisitID", &temp))
        visitid = json_object_get_int(temp);
    else
        null_indicators[18] = 1;

    if (json_object_object_get_ex(record, "StreetAddress", &temp))
        strncpy(streetaddress, json_object_get_string(temp), sizeof(streetaddress) - 1);
    else
        null_indicators[19] = 1;

    if (json_object_object_get_ex(record, "ZIP", &temp))
        strncpy(zip, json_object_get_string(temp), sizeof(zip) - 1);
    else
        null_indicators[20] = 1;

    if (json_object_object_get_ex(record, "ZIPExt", &temp))
        strncpy(zipext, json_object_get_string(temp), sizeof(zipext) - 1);
    else
        null_indicators[21] = 1;

    if (json_object_object_get_ex(record, "City", &temp))
        strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
    else
        null_indicators[22] = 1;

    if (json_object_object_get_ex(record, "State", &temp))
        strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
    else
        null_indicators[23] = 1;

    if (json_object_object_get_ex(record, "Country", &temp))
        strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
    else
        null_indicators[24] = 1;

    if (json_object_object_get_ex(record, "CountryCode", &temp))
        strncpy(countrycode, json_object_get_string(temp), sizeof(countrycode) - 1);
    else
        null_indicators[25] = 1;

    if (json_object_object_get_ex(record, "OriginalDocumentNumber", &temp))
        strncpy(originaldocumentnumber, json_object_get_string(temp), sizeof(originaldocumentnumber) - 1);
    else
        null_indicators[26] = 1;

    // Meta fields will be set from batch metadata
    null_indicators[27] = 0; // metacollectiontotalcount
    null_indicators[28] = 0; // metacollectionfirstid
    null_indicators[29] = 0; // metacollectionlastid
    // Bind parameters
    MYSQL_BIND bind[30];
    memset(bind, 0, sizeof(bind));

    // PurchaseOrderID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &purchaseorderid;
    bind[0].is_null = &null_indicators[0];

    // TransactionType
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = transactiontype;
    bind[1].buffer_length = sizeof(transactiontype);
    bind[1].is_null = &null_indicators[1];

   // DocumentTypeID
   bind[2].buffer_type = MYSQL_TYPE_LONG;
   bind[2].buffer = &documenttypeid;
   bind[2].is_null = &null_indicators[2];

   // DocumentTypeName
   bind[3].buffer_type = MYSQL_TYPE_STRING;
   bind[3].buffer = documenttypename;
   bind[3].buffer_length = sizeof(documenttypename);
   bind[3].is_null = &null_indicators[3];

   // DocumentStatus
   bind[4].buffer_type = MYSQL_TYPE_STRING;
   bind[4].buffer = documentstatus;
   bind[4].buffer_length = sizeof(documentstatus);
   bind[4].is_null = &null_indicators[4];

   // DocumentStatusID
   bind[5].buffer_type = MYSQL_TYPE_LONG;
   bind[5].buffer = &documentstatusid;
   bind[5].is_null = &null_indicators[5];

   // DocumentItemAttributeCaption
   bind[6].buffer_type = MYSQL_TYPE_STRING;
   bind[6].buffer = documentitemattributecaption;
   bind[6].buffer_length = sizeof(documentitemattributecaption);
   bind[6].is_null = &null_indicators[6];

   // DateAndTime
   bind[7].buffer_type = MYSQL_TYPE_STRING;
   bind[7].buffer = dateandtime;
   bind[7].buffer_length = sizeof(dateandtime);
   bind[7].is_null = &null_indicators[7];

   // DocumentNo
   bind[8].buffer_type = MYSQL_TYPE_STRING;
   bind[8].buffer = documentno;
   bind[8].buffer_length = sizeof(documentno);
   bind[8].is_null = &null_indicators[8];

   // ClientCode
   bind[9].buffer_type = MYSQL_TYPE_STRING;
   bind[9].buffer = clientcode;
   bind[9].buffer_length = sizeof(clientcode);
   bind[9].is_null = &null_indicators[9];

   // ClientName
   bind[10].buffer_type = MYSQL_TYPE_STRING;
   bind[10].buffer = clientname;
   bind[10].buffer_length = sizeof(clientname);
   bind[10].is_null = &null_indicators[10];

   // DocumentDate
   bind[11].buffer_type = MYSQL_TYPE_STRING;
   bind[11].buffer = documentdate;
   bind[11].buffer_length = sizeof(documentdate);
   bind[11].is_null = &null_indicators[11];

   // DueDate
   bind[12].buffer_type = MYSQL_TYPE_STRING;
   bind[12].buffer = duedate;
   bind[12].buffer_length = sizeof(duedate);
   bind[12].is_null = &null_indicators[12];

    // RepresentativeCode
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = representativecode;
    bind[13].buffer_length = sizeof(representativecode);
    bind[13].is_null = &null_indicators[13];

    // RepresentativeName
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = representativename;
    bind[14].buffer_length = sizeof(representativename);
    bind[14].is_null = &null_indicators[14];

    // SignatureURL
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = signatureurl;
    bind[15].buffer_length = sizeof(signatureurl);
    bind[15].is_null = &null_indicators[15];

    // Note
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = note;
    bind[16].buffer_length = sizeof(note);
    bind[16].is_null = &null_indicators[16];

    // Taxable
    bind[17].buffer_type = MYSQL_TYPE_TINY;
    bind[17].buffer = &taxable;
    bind[17].is_null = &null_indicators[17];

    // VisitID
    bind[18].buffer_type = MYSQL_TYPE_LONG;
    bind[18].buffer = &visitid;
    bind[18].is_null = &null_indicators[18];

    // StreetAddress
    bind[19].buffer_type = MYSQL_TYPE_STRING;
    bind[19].buffer = streetaddress;
    bind[19].buffer_length = sizeof(streetaddress);
    bind[19].is_null = &null_indicators[19];

    // ZIP
    bind[20].buffer_type = MYSQL_TYPE_STRING;
    bind[20].buffer = zip;
    bind[20].buffer_length = sizeof(zip);
    bind[20].is_null = &null_indicators[20];

    // ZIPExt
    bind[21].buffer_type = MYSQL_TYPE_STRING;
    bind[21].buffer = zipext;
    bind[21].buffer_length = sizeof(zipext);
    bind[21].is_null = &null_indicators[21];

    // City
    bind[22].buffer_type = MYSQL_TYPE_STRING;
    bind[22].buffer = city;
    bind[22].buffer_length = sizeof(city);
    bind[22].is_null = &null_indicators[22];

    // State
    bind[23].buffer_type = MYSQL_TYPE_STRING;
    bind[23].buffer = state;
    bind[23].buffer_length = sizeof(state);
    bind[23].is_null = &null_indicators[23];

    // Country
    bind[24].buffer_type = MYSQL_TYPE_STRING;
    bind[24].buffer = country;
    bind[24].buffer_length = sizeof(country);
    bind[24].is_null = &null_indicators[24];

    // CountryCode
    bind[25].buffer_type = MYSQL_TYPE_STRING;
    bind[25].buffer = countrycode;
    bind[25].buffer_length = sizeof(countrycode);
    bind[25].is_null = &null_indicators[25];

    // OriginalDocumentNumber
    bind[26].buffer_type = MYSQL_TYPE_STRING;
    bind[26].buffer = originaldocumentnumber;
    bind[26].buffer_length = sizeof(originaldocumentnumber);
    bind[26].is_null = &null_indicators[26];

    // MetaCollectionTotalCount
    bind[27].buffer_type = MYSQL_TYPE_LONG;
    bind[27].buffer = &metacollectiontotalcount;
    bind[27].is_null = &null_indicators[27];

    // MetaCollectionFirstID
    bind[28].buffer_type = MYSQL_TYPE_LONG;
    bind[28].buffer = &metacollectionfirstid;
    bind[28].is_null = &null_indicators[28];

    // MetaCollectionLastID
    bind[29].buffer_type = MYSQL_TYPE_LONG;
    bind[29].buffer = &metacollectionlastid;
    bind[29].is_null = &null_indicators[29];

    if (mysql_stmt_bind_param(stmt, bind)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);
    return purchaseorderid;
}

int process_purchaseorder_items(MYSQL *conn, int purchaseorderid, struct json_object *items) {
    if (!items || !json_object_is_type(items, json_type_array)) {
        return 0;  // No items to process
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_purchase_order_item(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int lineno = 0;
    char productcode[21] = {0};
    char productname[81] = {0};
    double unitamount = 0.0;
    double unitprice = 0.0;
    char packagetypecode[46] = {0};
    char packagetypename[41] = {0};
    int packagetypeconversion = 0;
    int quantity = 0;
    double amount = 0.0;
    double discountamount = 0.0;
    double discountpercent = 0.0;
    double taxamount = 0.0;
    double taxpercent = 0.0;
    double totalamount = 0.0;
    char note[256] = {0};
    char documentitemattributename[101] = {0};
    int documentitemattributeid = 0;

    bool null_indicators[19] = {0};
    MYSQL_BIND bind[19];
    memset(bind, 0, sizeof(bind));

    // PurchaseOrderID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &purchaseorderid;
    bind[0].is_null = &null_indicators[0];

    // LineNo
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &lineno;
    bind[1].is_null = &null_indicators[1];

    // ProductCode
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = productcode;
    bind[2].buffer_length = sizeof(productcode);
    bind[2].is_null = &null_indicators[2];

    // ProductName
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = productname;
    bind[3].buffer_length = sizeof(productname);
    bind[3].is_null = &null_indicators[3];

    char unitamount_str[32] = {0};
    snprintf(unitamount_str, sizeof(unitamount_str), "%.4f", unitamount);
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = unitamount_str;
    bind[4].buffer_length = strlen(unitamount_str);
    bind[4].is_null = &null_indicators[4];


    // UnitPrice
    char unitprice_str[32] = {0};
    snprintf(unitprice_str, sizeof(unitprice_str), "%.4f", unitprice);
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = unitprice_str;
    bind[5].buffer_length = strlen(unitprice_str);
    bind[5].is_null = &null_indicators[5];


    // PackageTypeCode
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = packagetypecode;
    bind[6].buffer_length = sizeof(packagetypecode);
    bind[6].is_null = &null_indicators[6];

    // PackageTypeName
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = packagetypename;
    bind[7].buffer_length = sizeof(packagetypename);
    bind[7].is_null = &null_indicators[7];

    // PackageTypeConversion
    bind[8].buffer_type = MYSQL_TYPE_LONG;
    bind[8].buffer = &packagetypeconversion;
    bind[8].is_null = &null_indicators[8];

    // Quantity
    bind[9].buffer_type = MYSQL_TYPE_LONG;
    bind[9].buffer = &quantity;
    bind[9].is_null = &null_indicators[9];

    // Amount
    char amount_str[32] = {0};
    snprintf(amount_str, sizeof(amount_str), "%.4f", amount);
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = amount_str;
    bind[10].buffer_length = strlen(amount_str);
    bind[10].is_null = &null_indicators[10];


    // DiscountAmount
    char discountamount_str[32] = {0};
    snprintf(discountamount_str, sizeof(discountamount_str), "%.4f", discountamount);
    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = discountamount_str;
    bind[11].buffer_length = strlen(discountamount_str);
    bind[11].is_null = &null_indicators[11];

    // DiscountPercent
    char discountpercent_str[32] = {0};
    snprintf(discountpercent_str, sizeof(discountpercent_str), "%.4f", discountpercent);
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = discountpercent_str;
    bind[12].buffer_length = strlen(discountpercent_str);
    bind[12].is_null = &null_indicators[12];

    // TaxAmount
    char taxamount_str[32] = {0};
    snprintf(taxamount_str, sizeof(taxamount_str), "%.4f", taxamount);
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = taxamount_str;
    bind[13].buffer_length = strlen(taxamount_str);
    bind[13].is_null = &null_indicators[13];

    // TaxPercent
    char taxpercent_str[32] = {0};
    snprintf(taxpercent_str, sizeof(taxpercent_str), "%.4f", taxpercent);
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = taxpercent_str;
    bind[14].buffer_length = strlen(taxpercent_str);
    bind[14].is_null = &null_indicators[14];

    // TotalAmount
    char totalamount_str[32] = {0};
    snprintf(totalamount_str, sizeof(totalamount_str), "%.4f", totalamount);
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = totalamount_str;
    bind[15].buffer_length = strlen(totalamount_str);
    bind[15].is_null = &null_indicators[15];

    // Note
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = note;
    bind[16].buffer_length = sizeof(note);
    bind[16].is_null = &null_indicators[16];

    // DocumentItemAttributeName
    bind[17].buffer_type = MYSQL_TYPE_STRING;
    bind[17].buffer = documentitemattributename;
    bind[17].buffer_length = sizeof(documentitemattributename);
    bind[17].is_null = &null_indicators[17];

    // DocumentItemAttributeID
    bind[18].buffer_type = MYSQL_TYPE_LONG;
    bind[18].buffer = &documentitemattributeid;
    bind[18].is_null = &null_indicators[18];

    int array_len = json_object_array_length(items);
    for (int i = 0; i < array_len; i++) {
        struct json_object *item = json_object_array_get_idx(items, i);
        struct json_object *temp;

        memset(null_indicators, 0, sizeof(null_indicators));

        // Extract line number
        if (json_object_object_get_ex(item, "LineNo", &temp))
            lineno = json_object_get_int(temp);
        else
            null_indicators[1] = 1;

        // Extract all other fields following the same pattern
        if (json_object_object_get_ex(item, "ProductCode", &temp))
            strncpy(productcode, json_object_get_string(temp), sizeof(productcode) - 1);
        else
            null_indicators[2] = 1;

        // ... continue extracting all fields ...

        if (mysql_stmt_bind_param(stmt, bind)) {
            mysql_stmt_close(stmt);
            return -1;
        }

        if (mysql_stmt_execute(stmt)) {
            mysql_stmt_close(stmt);
            return -1;
        }
    }

    mysql_stmt_close(stmt);
    return 0;
}

int process_purchaseorder_customattributes(MYSQL *conn, int purchaseorderid, 
                                         struct json_object *customattributes) {
    if (!customattributes || !json_object_is_type(customattributes, json_type_array)) {
        return 0;  // No custom attributes to process
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_purchase_order_customattribute(?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    bool null_indicators[5] = {0};
    char customattributeinfoid[37] = {0};
    char title[1024] = {0};
    char type[51] = {0};
    char value[1024] = {0};

    MYSQL_BIND bind[5];
    memset(bind, 0, sizeof(bind));

    // PurchaseOrderID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &purchaseorderid;
    bind[0].is_null = &null_indicators[0];

    // CustomAttributeInfoID
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = customattributeinfoid;
    bind[1].buffer_length = sizeof(customattributeinfoid);
    bind[1].is_null = &null_indicators[1];

    // Title
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = title;
    bind[2].buffer_length = sizeof(title);
    bind[2].is_null = &null_indicators[2];

    // Type
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = type;
    bind[3].buffer_length = sizeof(type);
    bind[3].is_null = &null_indicators[3];

    // Value
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = value;
    bind[4].buffer_length = sizeof(value);
    bind[4].is_null = &null_indicators[4];

    int array_len = json_object_array_length(customattributes);
    for (int i = 0; i < array_len; i++) {
        struct json_object *attr = json_object_array_get_idx(customattributes, i);
        struct json_object *temp;

        memset(null_indicators, 0, sizeof(null_indicators));

        if (json_object_object_get_ex(attr, "CustomAttributeInfoID", &temp))
            strncpy(customattributeinfoid, json_object_get_string(temp), sizeof(customattributeinfoid) - 1);
        else
            null_indicators[1] = 1;

        if (json_object_object_get_ex(attr, "Title", &temp))
            strncpy(title, json_object_get_string(temp), sizeof(title) - 1);
        else
            null_indicators[2] = 1;

        if (json_object_object_get_ex(attr, "Type", &temp))
            strncpy(type, json_object_get_string(temp), sizeof(type) - 1);
        else
            null_indicators[3] = 1;

        if (json_object_object_get_ex(attr, "Value", &temp))
            strncpy(value, json_object_get_string(temp), sizeof(value) - 1);
        else
            null_indicators[4] = 1;

        if (mysql_stmt_bind_param(stmt, bind)) {
            mysql_stmt_close(stmt);
            return -1;
        }

        if (mysql_stmt_execute(stmt)) {
            mysql_stmt_close(stmt);
            return -1;
        }
    }

    mysql_stmt_close(stmt);
    return 0;
}

int process_purchaseorders_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                               struct json_object *batch, 
                               struct PurchaseOrderBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct PurchaseOrderBatchResult));
    
    // Extract metadata
    struct json_object *meta;
    if (json_object_object_get_ex(batch, "MetaCollectionResult", &meta)) {
        struct json_object *count_obj, *firstid_obj, *lastid_obj;
        
        if (json_object_object_get_ex(meta, "TotalCount", &count_obj))
            result->total_count = json_object_get_int(count_obj);
        
        if (json_object_object_get_ex(meta, "FirstID", &firstid_obj))
            result->first_id = json_object_get_int(firstid_obj);
            
        if (json_object_object_get_ex(meta, "LastID", &lastid_obj))
            result->last_id = json_object_get_int(lastid_obj);
    }

    // Process records
    struct json_object *records;
    if (!json_object_object_get_ex(batch, "PurchaseOrders", &records)) {
        snprintf(result->error_message, sizeof(result->error_message), 
                "No PurchaseOrders array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(records);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(records, i);
        result->records_processed++;
        
        // Start transaction for the entire purchase order record
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        bool success = true;
        int purchaseorderid = process_purchaseorder_record(conn, record);
        
        if (purchaseorderid <= 0) {
            success = false;
        } else {
            // Process items if present
            struct json_object *items;
            if (json_object_object_get_ex(record, "Item", &items)) {
                if (process_purchaseorder_items(conn, purchaseorderid, items) != 0) {
                    success = false;
                }
            }

            // Process custom attributes if present
            struct json_object *customattributes;
            if (json_object_object_get_ex(record, "CustomFields", &customattributes)) {
                if (process_purchaseorder_customattributes(conn, purchaseorderid, customattributes) != 0) {
                    success = false;
                }
            }
        }

        if (success) {
            if (mysql_query(conn, "COMMIT")) {
                snprintf(result->error_message, sizeof(result->error_message), 
                        "Failed to commit transaction: %s", mysql_error(conn));
                mysql_query(conn, "ROLLBACK");
                return -1;
            }
            result->records_inserted++;
        } else {
            mysql_query(conn, "ROLLBACK");
            result->records_failed++;
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to process purchase order record %d", purchaseorderid);
        }

        // Log progress
        if (result->records_processed % 10 == 0) {
            printf("\rProcessing purchase orders: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");  // New line after progress indicator

    // Verify batch if any records were processed
    if (result->records_processed > 0) {
        if (!verify_purchaseorders_batch(conn, result->last_id, records)) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_purchaseorders_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT po.purchaseorderid, COUNT(poi.lineno) as item_count, "
                       "COUNT(poca.customattributeinfoid) as attribute_count "
                       "FROM purchaseorders po "
                       "LEFT JOIN purchaseorderitems poi ON po.purchaseorderid = poi.purchaseorderid "
                       "LEFT JOIN purchaseordercustomattributes poca ON po.purchaseorderid = poca.purchaseorderid "
                       "WHERE po.purchaseorderid <= ? "
                       "GROUP BY po.purchaseorderid "
                       "ORDER BY po.purchaseorderid DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_param[1];
    memset(bind_param, 0, sizeof(bind_param));
    
    bind_param[0].buffer_type = MYSQL_TYPE_LONG;
    bind_param[0].buffer = &last_id;

    if (mysql_stmt_bind_param(stmt, bind_param)) {
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[3];
    int db_purchaseorderid;
    long long db_item_count;
    long long db_attribute_count;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONG;
    bind_result[0].buffer = &db_purchaseorderid;
    bind_result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[1].buffer = &db_item_count;
    bind_result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[2].buffer = &db_attribute_count;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *order = json_object_array_get_idx(original_data, i);
            struct json_object *id_obj;
            
            if (json_object_object_get_ex(order, "PurchaseOrderID", &id_obj)) {
                int json_id = json_object_get_int(id_obj);
                if (db_purchaseorderid == json_id) {
                    // Verify item counts
                    struct json_object *items;
                    if (json_object_object_get_ex(order, "Item", &items)) {
                        int json_item_count = json_object_array_length(items);
                        if (json_item_count != db_item_count) {
                            verification_passed = false;
                            break;
                        }
                    }
                    
                    // Verify custom attribute counts
                    struct json_object *attributes;
                    if (json_object_object_get_ex(order, "CustomFields", &attributes)) {
                        int json_attr_count = json_object_array_length(attributes);
                        if (json_attr_count != db_attribute_count) {
                            verification_passed = false;
                            break;
                        }
                    }
                    
                    found = true;
                }
            }
        }
        
        if (!found) {
            verification_passed = false;
            break;
        }
    }

    mysql_stmt_close(stmt);
    return verification_passed;
}