#include "endpoints.h"

struct Endpoint endpoints[] = {

    {
        .name = "pricelists",
        .key = "PriceLists",
        .url_format = "https://api.repsly.com/v3/export/pricelists",
        .pagination_type = NONE,
        .tracking_query = "SELECT last_sync FROM endpoint_tracking WHERE endpoint_name = 'pricelists'",
        .update_query = "UPDATE endpoint_tracking SET last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'pricelists'"
    },

    {
        .name = "representatives",
        .key = "Representatives",
        .url_format = "https://api.repsly.com/v3/export/representatives",
        .pagination_type = NONE,
        .tracking_query = "SELECT last_sync FROM endpoint_tracking WHERE endpoint_name = 'representatives'",
        .update_query = "UPDATE endpoint_tracking SET last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'representatives'"
    },
    
    {
        .name = "documenttypes",
        .key = "DocumentTypes",
        .url_format = "https://api.repsly.com/v3/export/documentTypes",
        .pagination_type = NONE,
        .tracking_query = "SELECT last_sync FROM endpoint_tracking WHERE endpoint_name = 'documenttypes'",
        .update_query = "UPDATE endpoint_tracking SET last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'documenttypes'"
    },

    {
        .name = "clients",
        .key = "Clients",
        .url_format = "https://api.repsly.com/v3/export/clients/%s",
        .pagination_type = TIMESTAMP,
        .tracking_query = "SELECT last_timestamp FROM endpoint_tracking WHERE endpoint_name = 'clients'",
        .update_query = "UPDATE endpoint_tracking SET last_timestamp = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'clients'"
    },
    
    {
        .name = "visits",
        .key = "Visits",
        .url_format = "https://api.repsly.com/v3/export/visits/%s",
        .pagination_type = TIMESTAMP,
        .tracking_query = "SELECT last_timestamp FROM endpoint_tracking WHERE endpoint_name = 'visits'",
        .update_query = "UPDATE endpoint_tracking SET last_timestamp = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'visits'"
    },
    
    {
        .name = "users",
        .key = "Users",
        .url_format = "https://api.repsly.com/v3/export/users/%s",
        .pagination_type = TIMESTAMP,
        .tracking_query = "SELECT last_timestamp FROM endpoint_tracking WHERE endpoint_name = 'users'",
        .update_query = "UPDATE endpoint_tracking SET last_timestamp = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'users'"
    },

   {
       .name = "clientnotes",
       .key = "ClientNotes",
       .url_format = "https://api.repsly.com/v3/export/clientnotes/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'clientnotes'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'clientnotes'"
   },

   {
       .name = "forms",
       .key = "Forms",
       .url_format = "https://api.repsly.com/v3/export/forms/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'forms'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'forms'"
   },

   {
       .name = "dailyworkingtime",
       .key = "DailyWorkingTime",
       .url_format = "https://api.repsly.com/v3/export/dailyworkingtime/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'dailyworkingtime'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'dailyworkingtime'"
   },

   {
       .name = "products",
       .key = "Products",
       .url_format = "https://api.repsly.com/v3/export/products/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'products'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'products'"
   },

   {
       .name = "photos",
       .key = "Photos",
       .url_format = "https://api.repsly.com/v3/export/photos/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'photos'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'photos'"
   },

   {
       .name = "purchaseorders",
       .key = "PurchaseOrders",
       .url_format = "https://api.repsly.com/v3/export/purchaseorders/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'purchaseorders'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'purchaseorders'"
   },

   {
       .name = "retailaudits",
       .key = "RetailAudits",
       .url_format = "https://api.repsly.com/v3/export/retailaudits/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'retailaudits'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'retailaudits'"
   },

   {
       .name = "pricelistitems",
       .key = "PricelistItems",
       .url_format = "https://api.repsly.com/v3/export/pricelistsItems/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'pricelistitems'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'pricelistitems'"
   },

   {
       .name = "importstatus",
       .key = "ImportStatus",
       .url_format = "https://api.repsly.com/v3/export/importStatus/%s",
       .pagination_type = ID,
       .tracking_query = "SELECT last_id FROM endpoint_tracking WHERE endpoint_name = 'importstatus'",
       .update_query = "UPDATE endpoint_tracking SET last_id = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'importstatus'"
   },

   {
       .name = "visitrealizations",
       .key = "VisitRealizations",
       .url_format = "https://api.repsly.com/v3/export/visitrealizations?modified=%s&skip=%d",
       .pagination_type = SKIP,
       .tracking_query = "SELECT last_timestamp, skip_count FROM endpoint_tracking WHERE endpoint_name = 'visitrealizations'",
       .update_query = "UPDATE endpoint_tracking SET last_timestamp = ?, skip_count = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'visitrealizations'"
   },

   {
       .name = "visitschedules",
       .key = "VisitSchedules",
       .url_format = "https://api.repsly.com/v3/export/visitschedules/%s/%s",
       .pagination_type = DATERANGE,
       .tracking_query = "SELECT date_start, date_end FROM endpoint_tracking WHERE endpoint_name = 'visitschedules'",
       .update_query = "UPDATE endpoint_tracking SET date_start = ?, date_end = ?, last_sync = NOW(), total_count = total_count + ? WHERE endpoint_name = 'visitschedules'"
   }
};

const int NUM_ENDPOINTS = sizeof(endpoints) / sizeof(endpoints[0]);
