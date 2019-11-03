# PGE Potential PSPS Scraper

## HOT NOTE 2019 - Nov - 03
Adjustments to the rebuild process have been made.  It is advisable that you run the script the first time with 1 against your master address list.  This will build out your cache.  When PG&E begins their updates or publishes their updates, you will now be able to set 0 and only gather the updates from their service.

350K seed addresses with 1 set equates out to about 12 hours.  This is due to the fact that it will take the see and use it to capture any and all similar addresses that are in the same city and their account status.


## General Info

Made at the request of a professional colleague in the affected area, this script is designed to scrape the PG&amp;E data feed for outage information and store it within a database.  This script is designed to work in python 2.7+ environment.  It has been tested up to 3.6.

The basic process of this script, from a scraping point of view, is that it hits the backend of this site https://psps.ss.pge.com/.  As this site provides multiple addresses for any query, it does the same thing with any address the script throws at it.  That said, as some address tables do not have Apt#'s or other finer grain details, if the City matches the search city, then it takes it all and drops it into the database along with the current status.

Keep an eye on your rigs resources if you are not targeting a specific city.  Multi-threading, while it works pretty good can have some hiccups.  I ran out of memory during an earlier version, but that was mainly due to a memory leak due to the 1/2 thoughtout method I was using.  It is all fixed now, but it is something to be congnizant of.

If you need to get up and running fast, you will need to format your address table to match the format within the script.  If you can not, you can adjust the SQL queries to match your needs.  Be aware, you need to keep the order of the fields too unless you plan to hunt down each variable and adjust those.  This script is heavily reliant upon the address field which needs to be the everything minus the city and zip code.  City is used to make a match for database writing purposes.

It should be noted that PG&E is probably not going to like 300K calls coming in so be aware, they could flip the script on us pretty quickly.  There may be some ways around it, but I have not explored those yet and/or any alternative data sources as they will need to be checked on a case by case basis.

To configure, adjust the settings within the python script.

Multi-threading is active on this script.  It is current set to 15 workers but that may be changed as I get the number dialed in better.

!!!!  HUGE NOTE:  Data quality is important in your address list.  Do us all a favor and spot check. ;)

### Defined variables
PGE_premise_lookup:  This is where PG&E allows you to lookup the premise id.

PGE_status_lookup:  This is where you get the status for the premise id.

conn_params:  This is required for the pyODBC library to work.

    Driver:  Change this to match your current database implementation.  See https://github.com/mkleehammer/pyodbc/wiki for details.
    Server:  Your server name.  No ''.
    Database:  Your database name.
    UID:  Your user name.  No ''.
    PWD:  Your password. No ''.  

msag_source:  The location of the table that has all your addresses.

data_destination:  This is where the script pulls in your data from the MSAG_Source and updates it for the scripts needs.

account_destination:  This is where the found addresses during the search are stored, account numbers, and status at those addresses.

city_focus:  Allows you to run only a city through.

workers:  Allows you to set how many workers you are using per python run.

rebuild:  This is the important one!  Setting to 0 will use your fully run dataset in account_account_destination to be updated with the latest status.  Cache your accounts before the incident and then run this once the updates start coming out.  This will ensure a faster update and will focus entirely on the accounts you have in your system already.
