#-------------------------------------------------------------------------------
# Name:        PGE Power Outage Status Scraper 
# Purpose:  This script, while not the most elegant, scrapes the PG&E back end
#           for data about specific addresses.  As there are sometimes Apt#'s etc
#           within their data, once a match is found, it only compares City and Zip.
#           Some data cleanup would be better, but this is BETA and works semi-decently.
#
# Author:      John Spence
#
# Created:     10/27/2019
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
PGE_status_lookup = 'https://hiqlvv36ij.cloud.pge.com/Prod/v1/search/message?premise_id=' #Do not adjust
db_connection = r'Database Connections\\Connection to CartaEdit GISSQL16SDE.sde'  #This is your database connection.
msag_source = 'dbo.DoIT_Address_10292019_PSPSv2' #main address table.
data_destination = 'DBO.CCC_PGE_Status' #where all your statuses will get built.  This script will auto create the table if needed.  Do not modify the schema.
city_focus = '' #Place city name if you want to focus script on only 1 city.  Leave '' if you want all.

# Careful with this one...this controls how many workers you have.
workers = 5 # Maximum number of workers. 

# Rebuild Search Table
rebuild = 1  # False to not, true to rebuild.

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import arcpy
import time
import concurrent.futures
import requests, json, collections, string

def prep_data():
    # Build Results Table
    item_check = data_destination

    arcpy.env.workspace = db_connection

    if arcpy.Exists(item_check):
        try:
            clear_results_SQL = ('''
            truncate table {0}
            '''.format(data_destination))
            arcpy.ArcSDESQLExecute(db_connection).execute(clear_results_SQL)

        except Exception as error_check_for_existance:
            print ("Status:  Failure!")
            print(error_check_for_existance.args[0])

    else:
        create_results_SQL = ('''
        Create Table {0} (
        [OBJECTID] [int]
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
        , [latitude] [numeric]
        , [longitude] [numeric]
        , [PGE_status] [varchar](1000)
        , [SysChangeDate] [datetime2](7)
        )
        '''.format(data_destination))

        try:
            arcpy.ArcSDESQLExecute(db_connection).execute(create_results_SQL)
        except Exception as error_check:
            print(error_check.args[0])

    # Build Address List
    pull_msag_SQL = ('''
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

    try:
        msag_results = arcpy.ArcSDESQLExecute(db_connection).execute(pull_msag_SQL)
    except Exception as error_check:
        print(error_check.args[0])


def city_list():
    city_list_SQL = '''
    select 
        distinct(city)
        , count(*) as points
    from {0} where city <> ''
    group by city
    order by city asc
    '''.format (data_destination)
    city_return = arcpy.ArcSDESQLExecute(db_connection).execute(city_list_SQL)
    global city_listing
    city_listing = []
    for city in city_return:
        target = city[0]
        city_listing.append(target)


def process_city(city):
    # Begin Status Update
    if rebuild == 1:
        pull_from_PGE_SQL = '''
        select * from {0}
        where city = '{1}'
        '''.format(data_destination, city)
        pge_status_search_return = arcpy.ArcSDESQLExecute(db_connection).execute(pull_from_PGE_SQL)
    else:
        pull_from_PGE_SQL = '''
        select * from {0}
        where city = '{1}' and PGE_status not like 'Error%' and PGE_status = ''
        '''.format(data_destination, city)
        pge_status_search_return = arcpy.ArcSDESQLExecute(db_connection).execute(pull_from_PGE_SQL)

    hitcount = 0

    for row in pge_status_search_return:
        objectID = row[0]
        address = row[12]
        zipcode = row[9]
        city = row[7]

        hitcount += 1
        updated = 0

        print ("Records reviewed:  {0}\n\n".format(hitcount))

        PGE_premise_search = PGE_premise_lookup + '{0}'.format(address)

        print ("Looking up {0}, {1} {2}".format(address, city, zipcode))

        while True:
            try:
                response = requests.get (PGE_premise_search)
                data = response.json()

                payload = data['body']['Items']
                retry = 0
            except Exception as payload_error:
                retry = 1
                time.sleep(60)
            if retry == 0:
                break

        for item in payload:
            location = item
            city_PGE = location['city']
            zipcode_PGE = location['zip']
            pId_PGE = location['pId']
            streetNumber_PGE = location['streetNumber']
            address_PGE = location['address']

            print ("\tFound {0}, {1} {2}".format (address_PGE, city_PGE, zipcode_PGE))
            print ("\tPGE pID:  {0}".format(pId_PGE))

            if city.upper() == city_PGE.upper() and zipcode == zipcode_PGE and updated == 0:

                PGE_pId_status = PGE_status_lookup + '{0}'.format(pId_PGE)

                print ("\tLooking up ID {0}").format(pId_PGE)

                while True:
                    try:
                        status_response = requests.get (PGE_pId_status)
                        status_data = status_response.json()

                        print ("\tChecked.\n")

                        try:
                            status_payload = status_data['Items']
                            for item in status_payload:
                                status_message = item['message']
                                status_message = status_message.replace(r'\u00a0', ' ')
                                printable = set(string.printable)
                                status_message = filter(lambda x: x in printable, status_message)

                                print ('\t***')
                                print ('\tMessage Found')
                                print ('\t***\n')

                                update_status_SQL = '''
                                update {2}
                                set PGE_status = '{0}', SysChangeDate = getdate()
                                where ObjectID = '{1}'
                                '''.format(status_message, objectID, data_destination)

                                while True:
                                    try:
                                        arcpy.ArcSDESQLExecute(db_connection).execute(update_status_SQL)
                                    except Exception as write_error:
                                        retry = 1
                                        time.sleep(1)
                                    if retry == 0:
                                        break
                                updated = 1
                                retry = 0
                        except Exception as status_payload_check:
                            print ('Something weird here.')
                            print(status_payload_check.args[0])
                            status_message = 'ERROR Returned from PG&E.  Run script again later.  May be due to congestion on the site.'
                            update_status_SQL = '''
                            update {2}
                            set PGE_status = '{0}', SysChangeDate = getdate()
                            where ObjectID = '{1}'
                            '''.format(status_message, objectID, data_destination)

                            while True:
                                try:
                                    arcpy.ArcSDESQLExecute(db_connection).execute(update_status_SQL)
                                except Exception as write_error:
                                    retry = 1
                                    time.sleep(1)
                                if retry == 0:
                                    break
                                retry = 1
                                time.sleep(5)
                    except Exception as payload_error:
                        retry = 1
                        time.sleep(60)
                    if retry == 0:
                        break

            elif city.upper() == city_PGE.upper() and zipcode == zipcode_PGE and updated ==1:
                print ("\tPreviously updated!\n")

            else:
                print ("\tNo address match.\n")

    print ("{0} has been processed.".format(city))

# ------------ Main ------------
start_time = time.time()
print ('Process started:  {0}'.format(start_time))
if rebuild == 1:
    prep_data()

if city_focus == '':
    city_list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(process_city, city_listing)

else:
    target = '{0}'.format (city_focus)
    process_city(target)

finished_time = time.time()
total_time = finished_time - start_time
total_time = total_time / 60
print ('Process finished:  {0}'.format(start_time))
print ('Time time required:  {0}'.format(total_time))
