# PGE Potential PSPS Scraper
Made at the request of a professional colleague in the affected area, this script is designed to scrape the PG&amp;E data feed for outage information and store it within a database.  This script is intended to be used within the Esri ArcGIS environment as all the SQL calls are pulling from their library.

What is required for this data set to work out of the box, is source data formatted similiarily to https://gis.cccounty.us/Downloads/General%20County%20Data/CCC_Adddress_Points.zip

If you need to get up and running fast, I would suggest truncating the data once it is in your enterprise database and and writing your own in.

It should be noted that PG&E is probably not going to like 300K calls coming in so be aware, they could flip the script on us pretty quickly.  There may be some ways around it, but I have not explored those yet and/or any alternative data sources as they will need to be checked on a case by case basis.

To configure, adjust the settings within the python script here:

### Defined variables
PGE_premise_lookup:  This is where PG&E allows you to lookup the premise id.

PGE_status_lookup:  This is where you get the status for the premise id.

db_connection:  Location to your SDE connection.  This can be a file path too.

msag_source:  The location of the table that has all your addresses.

data_destination:  Where you want to store the status.  This table will auto create and truncate.  Do not modify the schema.

city_focus:  Allows you to run only a city through.
