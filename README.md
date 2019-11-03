# PGE Potential PSPS Scraper

## General Info

Made at the request of a professional colleague in the affected area, these scripts are designed to scrape the PG&amp;E data feed for outage information and store it within a database.  There are two versions available.  The ArcGIS version requires for you to have licensed instance of ArcGIS Desktop or Server or Pro.  The scripts there will run against any flavor of ArcGIS that is at least 10.5.1 or higher.  It was tested on the current version of Pro and a 10.7.1 instances of ArcGIS Desktop.  The second version, Agnostic, only requires python to be installed.  I have tested it against python versions 2.7 and 3.6.  You will need to make sure you bring in the additional libraries required by the script, which if my memory serves correctly at the time of writing this is only pyODBC.  You may do so by executing the command pip install pyodbc.  More instructions on how to configure, etc can be found under the specific folders.
