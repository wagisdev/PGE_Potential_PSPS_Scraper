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
# Modified:    
# Modification Purpose:  
#
#
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Define the variables
PGE_premise_lookup = 'https://hiqlvv36ij.cloud.pge.com/Prod/v1/search/premise?address='
PGE_status_lookup = 'https://hiqlvv36ij.cloud.pge.com/Prod/v1/search/message?premise_id='
db_connection = r'Database Connections\\Connection to CartaEdit GISSQL16SDE.sde'
msag_source = 'DBO.CCC_ADDRESS_POINTS'
data_destination = 'DBO.CCC_PGE_Status'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import arcpy
import requests, json, collections

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
        print "Status:  Failure!"
        print(error_check_for_existance.args[0])

else:
    create_results_SQL = ('''
    Create Table {0} (
    [OBJECTID] [int]
    , [prefix_typ] [nvarchar](4)
    , [prefix_dir] [nvarchar](4)
    , [street_nam] [nvarchar](50)
    , [street_typ] [nvarchar](6)
    , [suffix_dir] [nvarchar](4)
    , [unit_numbe] [nvarchar](10)
    , [city] [nvarchar](50)
    , [state] [nvarchar](2)
    , [zip_code] [nvarchar](20)
    , [street_num] [nvarchar](10)
    , [full_addre] [nvarchar](254)
    , [full_address_to_PGE] [nvarchar](254)
    , [PGE_status] [nvarchar](254)
    , [SysChangeDate] [datetime2](7)
    )
    '''.format(data_destination))

    arcpy.ArcSDESQLExecute(db_connection).execute(create_results_SQL)

# Build Address List
pull_msag_SQL = ('''
insert into {1}
select 
	ROW_NUMBER() OVER(ORDER BY full_addre ASC) as OjectID
	,prefix_typ
    ,prefix_dir
    ,street_nam
    ,street_typ
    ,suffix_dir
    ,unit_numbe
    ,city
    ,state
    ,zip_code
    ,street_num
    ,full_addre
	, case
		when prefix_typ = '' and prefix_dir = '' and street_typ = '' and suffix_dir = '' then street_num + ' ' + street_nam
		when prefix_typ = '' and prefix_dir = '' and street_typ <> '' and suffix_dir ='' then street_num + ' ' + street_nam + ' ' + street_typ
		when prefix_typ = '' and prefix_dir = '' and street_typ = '' and suffix_dir <>'' then street_num + ' ' + street_nam + ' ' + suffix_dir
		when prefix_typ = '' and prefix_dir = '' and street_typ <> '' and suffix_dir <>'' then street_num + ' ' + street_nam + ' ' + street_typ + ' ' + suffix_dir
		when prefix_typ <> '' and prefix_dir = '' and street_typ = '' and suffix_dir = '' then street_num + ' ' + street_nam  + ' ' + prefix_typ
		when prefix_typ = '' and prefix_dir <> '' and street_typ = '' and suffix_dir = '' then street_num + ' ' + prefix_dir + ' ' + street_nam
		when prefix_typ = '' and prefix_dir <> '' and street_typ <> '' and suffix_dir = '' then street_num + ' ' + prefix_dir + ' ' + street_nam + ' ' + street_typ
	end as full_address_to_PGE
	, ''
    , getdate()
  FROM {0}''').format(msag_source, data_destination)

try:
    msag_results = arcpy.ArcSDESQLExecute(db_connection).execute(pull_msag_SQL)
except Exception as error_check:
    print(error_check.args[0])

# Begin Status Update
pull_from_PGE_SQL = '''
select top 100 * from {0}
'''.format(data_destination)
pge_status_search_return = arcpy.ArcSDESQLExecute(db_connection).execute(pull_from_PGE_SQL)

hitcount = 0

for row in pge_status_search_return:
    objectID = row[0]
    address = row[12]
    zipcode = row[9]
    city = row[7]

    hitcount += 1

    print ("Records reviewed:  {0}\n\n".format(hitcount))

    PGE_premise_search = PGE_premise_lookup + '{0}'.format(address)

    print ("Looking up {0}, {1} {2}".format(address, city, zipcode))

    response = requests.get (PGE_premise_search)
    data = response.json()

    payload = data['body']['Items']

    for item in payload:
        location = item
        city_PGE = location['city']
        zipcode_PGE = location['zip']
        pId_PGE = location['pId']
        streetNumber_PGE = location['streetNumber']
        address_PGE = location['address']

        print ("Found {0}, {1} {2}".format (address_PGE, city_PGE, zipcode_PGE))
        print ("PGE pID:  {0}".format(pId_PGE))

        if city.upper() == city_PGE.upper() and zipcode == zipcode_PGE:

            PGE_pId_status = PGE_status_lookup + '{0}'.format(pId_PGE)

            print ("Looking up ID {0}").format(pId_PGE)

            status_response = requests.get (PGE_pId_status)
            status_data = status_response.json()

            print (status_data)

            print ("Status Found!  Writing to DB.\n")

            try:
                status_payload = status_data['Items']
                for item in status_payload:
                    status_message = item['message']

                    print ('***')
                    print (status_message)
                    print ('***')

                    #if status_message == None:
                    #    update_status_SQL = '''
                    #    update {2}
                    #    set PGE_status = '{0}', SysChangeDate = getdate()
                    #    where ObjectID = '{1}'
                    #    '''.format(status_message, objectID, data_destination)
                    #else:
            
                    update_status_SQL = '''
                    update {2}
                    set PGE_status = '{0}', SysChangeDate = getdate()
                    where ObjectID = '{1}'
                    '''.format(status_message, objectID, data_destination)
            
                    arcpy.ArcSDESQLExecute(db_connection).execute(update_status_SQL)

            except Exception as status_payload_check:
                print ('Something weird here.')
                print(status_payload_check.args[0])
                status_message = 'ERROR Returned from PG&E.  Check this property from the official source.'
                update_status_SQL = '''
                update {2}
                set PGE_status = '{0}', SysChangeDate = getdate()
                where ObjectID = '{1}'
                '''.format(status_message, objectID, data_destination)
            
                arcpy.ArcSDESQLExecute(db_connection).execute(update_status_SQL)

        else:
            print ("Not an address match.\n")

