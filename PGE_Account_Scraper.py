#-------------------------------------------------------------------------------
# Name:        PGE Account Scraper
# Purpose:  This script is designed to scrap the PGE PSPS site and extract the 
#           the unique account code for each address along with snagging the
#           account codes for other similar addresses.
#
# Author:      John Spence
#
# Created:     
#
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Define the variables
PGE_premise_lookup = 'https://hiqlvv36ij.cloud.pge.com/Prod/v1/search/premise?address='  #Do not adjust
conn_params = ('Driver={ODBC Driver 17 for SQL Server};'
                      r'Server=GISSQL16SDE\GIS;'
                      'Database=CartaEdit;'
                      #'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      r'UID=john.spence;'  # Comment out if you are using AD authentication.
                      r'PWD=Tal35923!@')
msag_source = 'DBO.MSAG_Listing' #main address table.
data_destination = 'DBO.PGE_Scraped_Accounts' #where all your statuses will get built.  This script will auto create the table if needed.  Do not modify the schema.
city_focus = '' #Place city name if you want to focus script on only 1 city.  Leave '' if you want all.

# Careful with this one...this controls how many workers you have.
workers = 5 # Maximum number of workers. 

# Rebuild Search Table
rebuild = 1  # False to not, true to rebuild.

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import time
import concurrent.futures
import requests, json, collections, string
import pyodbc

conn = pyodbc.connect(conn_params)
#cursor = conn.cursor()
#cursor.execute('SELECT street_name FROM dbo.MSAG_LISTING')
#for row in cursor:
#    test = row[0]
#    print (test)
#cursor.close()
#conn.close()

def prep_data():
    # Build Results Table
    cursor = conn.cursor()
    try:
        string = ('''CREATE TABLE {0}([OBJECTID] [int]
                    , [prefix_typ] [varchar](4)
                    , [prefix_dir] [varchar](4)
                    , [street_nam] [varchar](50)
                    , [street_typ] [varchar](6)
                    , [suffix_dir] [varchar](4)
                    , [unit_numbe] [varchar](10)
                    , [city] [varchar](50)
                    , [state] [varchar](2)
                    , [zip_code] [varchar](20)
                    , [street_num] [varchar](10)
                    , [full_addre] [varchar](254)
                    , [full_address_to_PGE] [varchar](254)
                    , [latitude] [numeric](38,8)
                    , [longitude] [numeric](38,8)
                    , [PGE_status] [varchar](1000)
                    , [SysChangeDate] [datetime2](7))'''.format(data_destination))
        cursor.execute(string)
        conn.commit()
        cursor.close()   
    except Exception as table_exists_err:
        print ('Table likely exists.  Continuing to the next step.')

    # Build Address List      
    cursor = conn.cursor()
    string = ('''
    insert into {1}
    select
	    ROW_NUMBER() OVER(ORDER BY full_address ASC) as OjectID
	    ,prefix_type
        ,prefix_direction
        ,street_name
        ,street_type
        ,suffix_direction
        ,unit_number
        ,city
        ,state
        ,zip_code
        ,street_number
        ,full_address
	    , case
		    when prefix_type = '' and prefix_direction = '' and street_type = '' and suffix_direction = '' then street_number + ' ' + street_name
		    when prefix_type = '' and prefix_direction = '' and street_type <> '' and suffix_direction ='' then street_number + ' ' + street_name + ' ' + street_type
		    when prefix_type = '' and prefix_direction = '' and street_type = '' and suffix_direction <>'' then street_number + ' ' + street_name + ' ' + suffix_direction
		    when prefix_type = '' and prefix_direction = '' and street_type <> '' and suffix_direction <>'' then street_number + ' ' + street_name + ' ' + street_type + ' ' + suffix_direction
		    when prefix_type <> '' and prefix_direction = '' and street_type = '' and suffix_direction = '' then street_number + ' ' + street_name  + ' ' + prefix_type
		    when prefix_type = '' and prefix_direction <> '' and street_type = '' and suffix_direction = '' then street_number + ' ' + prefix_direction + ' ' + street_name
		    when prefix_type = '' and prefix_direction <> '' and street_type <> '' and suffix_direction = '' then street_number + ' ' + prefix_direction + ' ' + street_name + ' ' + street_type
	    end as full_address_to_PGE
        , latitude
        , longitude
	    , ''
        , getdate()
      FROM {0}''').format(msag_source, data_destination)
    cursor.execute(string)
    conn.commit()
    cursor.close()      
    conn.close()




prep_data()





    #item_check = data_destination

    #arcpy.env.workspace = db_connection

    #if arcpy.Exists(item_check):
    #    try:
    #        clear_results_SQL = ('''
    #        truncate table {0}
    #        '''.format(data_destination))
    #        arcpy.ArcSDESQLExecute(db_connection).execute(clear_results_SQL)

    #    except Exception as error_check_for_existance:
    #        print ("Status:  Failure!")
    #        print(error_check_for_existance.args[0])

    #else:
    #    create_results_SQL = ('''
    #    Create Table {0} (
    #    [OBJECTID] [int]
    #    , [prefix_typ] [varchar](4)
    #    , [prefix_dir] [varchar](4)
    #    , [street_nam] [varchar](50)
    #    , [street_typ] [varchar](6)
    #    , [suffix_dir] [varchar](4)
    #    , [unit_numbe] [varchar](10)
    #    , [city] [varchar](50)
    #    , [state] [varchar](2)
    #    , [zip_code] [varchar](20)
    #    , [street_num] [varchar](10)
    #    , [full_addre] [varchar](254)
    #    , [full_address_to_PGE] [varchar](254)
    #    , [latitude] [numeric]
    #    , [longitude] [numeric]
    #    , [PGE_status] [varchar](1000)
    #    , [SysChangeDate] [datetime2](7)
    #    )
    #    '''.format(data_destination))

    #    try:
    #        arcpy.ArcSDESQLExecute(db_connection).execute(create_results_SQL)
    #    except Exception as error_check:
    #        print(error_check.args[0])

    ## Build Address List
    #pull_msag_SQL = ('''
    #insert into {1}
    #select
	   # ROW_NUMBER() OVER(ORDER BY full_address ASC) as OjectID
	   # ,prefix_type
    #    ,prefix_direction
    #    ,street_name
    #    ,street_type
    #    ,suffix_direction
    #    ,unit_number
    #    ,city
    #    ,state
    #    ,zip_code
    #    ,street_number
    #    ,full_address
	   # , case
		  #  when prefix_type = '' and prefix_direction = '' and street_type = '' and suffix_direction = '' then street_number + ' ' + street_name
		  #  when prefix_type = '' and prefix_direction = '' and street_type <> '' and suffix_direction ='' then street_number + ' ' + street_name + ' ' + street_type
		  #  when prefix_type = '' and prefix_direction = '' and street_type = '' and suffix_direction <>'' then street_number + ' ' + street_name + ' ' + suffix_direction
		  #  when prefix_type = '' and prefix_direction = '' and street_type <> '' and suffix_direction <>'' then street_number + ' ' + street_name + ' ' + street_type + ' ' + suffix_direction
		  #  when prefix_type <> '' and prefix_direction = '' and street_type = '' and suffix_direction = '' then street_number + ' ' + street_name  + ' ' + prefix_type
		  #  when prefix_type = '' and prefix_direction <> '' and street_type = '' and suffix_direction = '' then street_number + ' ' + prefix_direction + ' ' + street_name
		  #  when prefix_type = '' and prefix_direction <> '' and street_type <> '' and suffix_direction = '' then street_number + ' ' + prefix_direction + ' ' + street_name + ' ' + street_type
	   # end as full_address_to_PGE
    #    , latitude
    #    , longitude
	   # , ''
    #    , getdate()
    #  FROM {0}''').format(msag_source, data_destination)

    #try:
    #    msag_results = arcpy.ArcSDESQLExecute(db_connection).execute(pull_msag_SQL)
    #except Exception as error_check:
    #    print(error_check.args[0])




    