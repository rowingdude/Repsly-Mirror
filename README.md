## Overview

The **Repsly MySQL Mirror Program** is a C-based ETL (Extract, Transform, Load) application designed to clone all data from a Repsly account into a MySQL database. This program automates the extraction of data from the Repsly API, processes it for compatibility, and loads it into a local MySQL database for efficient access and analysis.

## Features

- **Data Extraction**: Automatically fetches data from the Repsly API using HTTP requests.
- **Data Transformation**: Processes and formats the data to align with the MySQL schema.
- **Data Loading**: Loads the transformed data into a MySQL database using the MySQL C API.
- **Incremental Updates**: Supports periodic updates to keep the local database synchronized with Repsly.
- **Error Handling**: Implements logging and error handling to ensure robust operation.

## Requirements

- C Compiler (e.g., GCC)
- MySQL Server
- MySQL C API library
- cURL library for handling HTTP requests
- Repsly API credentials 
- MySQL credentials

## Development is active

I'm using this as a test ETL for a larger project we have coming up. Please feel free to drop a PR. I know there's a lot to improve.

## TO DO LISt

Check out the "Issues" for all of the necessary things to update.