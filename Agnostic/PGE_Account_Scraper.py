#-------------------------------------------------------------------------------
# Name:        PGE Account Scraper
# Purpose:  This script is designed to scrap the PGE PSPS site and extract the
#           the unique account code for each address along with snagging the
#           account codes for other similar addresses.  It also snags the status.
#           You can run this on pretty much any version of Python that is 2.7+.
#           It has been tested to 3.6.  You need to install PyODBC to make it work.
#           The reference material for pyODBC, along with different connection types
#           can be found here.  https://github.com/mkleehammer/pyodbc/wiki
#           I recommend performing installation of pyodbc via pip.  If you don't know
#           how, the python command is:  pip install pyodbc
#           You may need additional libraries, but you'll find that out as you go.
#           If you are using the stock library though, I think everything you should
#           have out of the box.
#           !!! HOT NOTE:  It would be best if you run this script all the way through
#           one time with the rebuild set to 1.  After you have built your list, set to 0.
#           Setting to 0 forces a different loop where you only update account status.
#           During testing the initial pull for about 350K seed addresses took about 12 hours.
#           It took that long because seed addresses let you discover other addresses such as
#           in appartment complexes, etc.
#           The update of the status took
#
# Author:      John Spence
#
# Created:     11/2/2019
#
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Define the variables
PGE_premise_lookup = 'https://hiqlvv36ij.cloud.pge.com/Prod/v1/search/premise?address='  #Do not adjust unless you know differently.
PGE_status_lookup = 'https://hiqlvv36ij.cloud.pge.com/Prod/v1/search/message?premise_id=' #Do not adjust unless you know differently.

conn_params = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=YourServer;'
                      'Database=YourDatabase;'
                      #'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      r'UID=YourUserName;'  # Comment out if you are using AD authentication.
                      r'PWD=YourPassword'     # Comment out if you are using AD authentication.
                      )

msag_source = 'DBO.MSAG_Listing' #main address table.
data_destination = 'DBO.PGE_Status' #where all your statuses will get built.  This script will auto create the table if needed.  Do not modify the schema.
account_destination = 'DBO.PGE_Cached_Accounts'
city_focus = '' #Place city name if you want to focus script on only 1 city.  Leave '' if you want all.

# Careful with this one...this controls how many workers you have.
workers = 15 # Maximum number of workers.

# Rebuild Search Table
rebuild = 1  # False to not, true to rebuild.

# Send confirmation of rebuild to
email_target = 'yours@yourdomain.com'

# Configure the e-mail server and other info here.
mail_server = 'smtp-relay.gmail.com:587'
mail_from = 'Account Scraper<noreply@yourdomain.com>'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

import time
import re
import concurrent.futures
import requests, json, collections, string
import smtplib
import pyodbc

def prep_data():
    conn = pyodbc.connect(conn_params)

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
        string = ('''truncate table {0}'''.format(data_destination))
        cursor.execute(string)
        conn.commit()
        cursor.close()

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

def prep_4accounts():
    conn = pyodbc.connect(conn_params)

    # Build Results Table
    cursor = conn.cursor()
    try:
        string = ('''CREATE TABLE {0}([OBJECTID] [INT] IDENTITY(1,1)
                    , [address] [varchar](254)
                    , [streetnum] [varchar](20)
                    , [city] [varchar](50)
                    , [zip] [varchar](20)
                    , [pId] [varchar](20)
                    , [serviceType] [varchar](10)
                    , [PGE_status] [varchar](1000)
                    , [SysChangeDate] [datetime2](7))'''.format(account_destination))
        cursor.execute(string)
        conn.commit()

    except Exception as table_exists_err:
        print ('Table likely exists.  Continuing to the next step.')
        if rebuild == 1:
            string = ('''truncate table {0}'''.format(account_destination))
            cursor.execute(string)
            conn.commit()
        else:
            print ('No rebuild needed.')

    cursor.close()
    conn.close()

def city_list():
    conn = pyodbc.connect(conn_params)

    # Build the city list for search.
    cursor = conn.cursor()
    if rebuild == 1:
        string = ('''
        select
            distinct(city)
            , count(*) as points
        from {0} where city <> ''
        group by city
        order by points desc
        ''').format (data_destination)
    else:
        string = ('''
        select
            distinct(city)
            , count(*) as points
        from {0} where city <> ''
        group by city
        order by points desc
        ''').format (account_destination)

    cursor.execute(string)
    global city_listing
    city_listing = []
    for city in cursor.fetchall():
        target = city[0]
        city_listing.append(target)
    cursor.close()
    conn.close()

def process_city(city):
    conn = pyodbc.connect(conn_params)
   #Begin Account Search
    cursor = conn.cursor()
    if rebuild ==1:
        city_string = ('''
        select * from {0}
        where city = '{1}'
        ''').format(data_destination, city)
        cursor.execute(city_string)
    else:
        city_string = ('''
        select * from {0}
        where city = '{1}'
        ''').format(account_destination, city)
        cursor.execute(city_string)

    hitcount = 0

    if rebuild == 1:
        for row in cursor.fetchall():
            address = row[12]
            zipcode = row[9]
            city = row[7]

            hitcount += 1

            print ("\n\n***Records reviewed:  {0}\n\n".format(hitcount))
            PGE_premise_search = PGE_premise_lookup + '{0}'.format(address)
            print ("Looking up {0}, {1} {2}".format(address, city, zipcode))

            # Get addresses like the seed values.
            while True:
                try:
                    response = requests.get (PGE_premise_search)
                    data = response.json()
                    payload = data['body']['Items']
                    retry = 0
                except Exception as payload_error:
                    retry = 1
                    time.sleep(10)
                if retry == 0:
                    break

            for item in payload:
                location = item
                city_PGE = location['city']
                zipcode_PGE = location['zip']
                pId_PGE = location['pId']
                streetNumber_PGE = location['streetNumber']
                # Cleanup on aisle 6 required as some special charcters sneak in from time to time.
                address_PGE = re.sub('[^a-zA-Z0-9 \n\.]', '', location['address'])
                servicetype_PGE = location ['serviceType']
                print ("\tFound {0}, {1} {2}".format (address_PGE, city_PGE, zipcode_PGE))
                print ("\tPGE pID:  {0}".format(pId_PGE))
                PGE_pId_status = PGE_status_lookup + '{0}'.format(pId_PGE)
                print ("\tLooking up {0}, {1} {2}".format(address_PGE, city_PGE, zipcode_PGE))

                # Get the status of the account for the address in question.
                if city_PGE.upper() == city.upper():

                    halt = 0

                    while True:
                        try:
                            status_response = requests.get (PGE_pId_status)
                            status_data = status_response.json()
                            print ('\tPG&E payload response: {0}'.format(status_data))
                            halt += 1
                            retry = 0
                            if halt == 10:
                                time.sleep(60)
                                break
                        except Exception as account_pull_error:
                            retry = 1
                            time.sleep(60)
                        if retry == 0:
                            break
                    print ("\tChecked.\n")

                    try:
                        if status_data['Items'] == []:
                            status_message = 'No Update Available'

                        else:
                            status_payload = status_data['Items']
                            for item in status_payload:
                                status_message = item['message']
                                status_message = status_message.replace(r'\u00a0', ' ')
                                printable = set(string.printable)
                                status_message = filter(lambda x: x in printable, status_message)
                    except:
                            status_message = 'Status Retrieval Error'

                    #Insert the address and status into the database.
                    update_conn = pyodbc.connect(conn_params)
                    update_cursor = update_conn.cursor()
                    update_string = ('''
                        insert into {0} (
                            [address]
                            , [streetnum]
                            , [city]
                            , [zip]
                            , [pId]
                            , [serviceType]
                            , [PGE_status]
                            , [SysChangeDate])
                        values ('{1}'
                            , '{2}'
                            , '{3}'
                            , '{4}'
                            , '{5}'
                            , '{6}'
                            , '{7}'
                            , getdate())''').format(account_destination, address_PGE, streetNumber_PGE, city_PGE, zipcode_PGE, pId_PGE, servicetype_PGE, status_message)
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()

                else:
                    print ('\n\n*****{0} is outside of search scope*****\n\n'.format(city_PGE))
    else:
        for row in cursor.fetchall():
            pId_PGE = row[5]

            hitcount += 1
            print ("\n\n***Records reviewed:  {0}\n\n".format(hitcount))
            PGE_pId_status = PGE_status_lookup + '{0}'.format(pId_PGE)
            halt = 0
            while True:
                try:
                    status_response = requests.get (PGE_pId_status)
                    status_data = status_response.json()
                    print ('Attempted Account Number:  {0}'.format(pId_PGE))
                    print ('\tPG&E payload response: {0}'.format(status_data))
                    halt += 1
                    retry = 0
                    if halt == 10:
                        time.sleep(60)
                        break
                except Exception as account_pull_error:
                    retry = 1
                    print ('Retrying...')
                    time.sleep(60)
                if retry == 0:
                    break
            print ("\tChecked.\n")

            try:
                if status_data['Items'] == []:
                    status_message = 'No Update Available'
                else:
                    status_payload = status_data['Items']
                    for item in status_payload:
                        status_message = item['message']
                        status_message = status_message.replace(r'\u00a0', ' ')
                        printable = set(string.printable)
                        status_message = filter(lambda x: x in printable, status_message)
            except:
                    status_message = 'Status Retrieval Error'

            #Insert the address and status into the database.
            update_conn = pyodbc.connect(conn_params)
            update_cursor = update_conn.cursor()
            update_string = ('''
                update {0}
                set [PGE_Status] = '{2}'
                , [SysChangeDate] = getdate()
                where [pId] = '{1}'
                ''').format(account_destination, pId_PGE, status_message)
            update_cursor.execute(update_string)
            update_conn.commit()
            update_cursor.close()
            update_conn.close()

    cursor.close()
    conn.close()

def prep_update():
    update_conn = pyodbc.connect(conn_params)
    update_cursor = update_conn.cursor()
    update_string = ('''
        update {0}
        set [PGE_Status] = NULL
        , [SysChangeDate] = getdate()
        ''').format(account_destination)
    update_cursor.execute(update_string)
    update_conn.commit()
    update_cursor.close()
    update_conn.close()


def remove_dupes():
    # Removes duplicate accounts from the database.  Not sure if each address has a unique account number, etc.
    conn = pyodbc.connect(conn_params)
    cursor = conn.cursor()
    dupe_string = ('''
            WITH cte AS (
            SELECT
                piD
                ,ROW_NUMBER() OVER (
                    PARTITION BY
                        piD
                    ORDER BY
                        piD
                ) row_num
             FROM
                {0}
            )
            DELETE FROM cte
            WHERE row_num > 1
    ''').format(account_destination)
    cursor.execute(dupe_string)
    conn.commit()
    cursor.close()
    conn.close()

def sendcompletetion(email_target, mail_server, mail_from,total_time):
    conn = pyodbc.connect(conn_params)
   #Begin Account Search
    cursor = conn.cursor()
    status_check = ('''
    select count(*) from {0}
    ''').format(account_destination)
    cursor.execute(status_check)
    for row in cursor.fetchall():
        status_response = row[0]
    #cursor.close()
    status_check = ('''
    select count(distinct(city)) from {0}
    ''').format(account_destination)
    cursor.execute(status_check)
    for row in cursor.fetchall():
        city_response = row[0]
    cursor.close()
    conn.close()

    mail_priority = '5'
    mail_subject = 'Success:  PG&E Account Scraper Has Finished.'
    mail_msg = '{0} records have been logged spanning {1} cities.  The process took {2} minutes.\n\n[SYSTEM GENERATED MESSAGE]'.format(
    status_response, city_response, total_time)

    # Set SMTP Server and configuration of message.
    server = smtplib.SMTP(mail_server)
    server.ehlo()
    server.starttls()
    #server.set_debuglevel(1)
    email_target = email_target
    mail_priority = mail_priority
    mail_subject =  mail_subject
    mail_msg =  mail_msg

    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_msg)
    # Double commented out code hides how to send a BCC as well.
    ##send_mail = 'To: {0}\nFrom: {1}\nBCC: {2}\nX-Priority: {3}\nSubject: {4}\n\n{5}'.format(email_target, mail_from, mail_bcc, mail_priority, mail_subject, mail_msg)

    server.sendmail(mail_from, email_target, send_mail)
    # Double commented out code hides how to send a BCC as well.
    ##server.sendmail(mail_from, [email_target, mail_bcc], send_mail)

    server.quit()

# ------------ Main ------------
start_time = time.time()
print ('Process started:  {0}'.format(start_time))
if rebuild == 1:
    prep_data()
    prep_4accounts()
else:
    prep_update()

if city_focus == '':
    city_list()
    if rebuild == 0:
        for target in city_listing:
            process_city(target)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(process_city, city_listing)

else:
    target = '{0}'.format (city_focus)
    process_city(target)

#remove_dupes()  #Keep all data captured.

finished_time = time.time()
total_time = finished_time - start_time
total_time = total_time / 60
print ('\n\nProcess finished:  {0}'.format(start_time))
print ('Time time required:  {0}'.format(total_time))

sendcompletetion(email_target, mail_server, mail_from, total_time)

# If python 2.7 comment out input below.
input("Press Enter to continue...")

# If pythong 3+ comments out input blow.
#raw_input("Press Enter to continue...")
