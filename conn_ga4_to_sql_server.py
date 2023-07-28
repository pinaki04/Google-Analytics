import pandas as pd
import os
import pyodbc as sql
import logging
from configparser import ConfigParser
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

#Fetch Subaru data from GA4
def fetch_data_ga4(property_id, input_start_date, input_end_date):

    client = BetaAnalyticsDataClient()

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date"),
                   Dimension(name="pageTitle"),
                   Dimension(name="mobileDeviceBranding"),
                   Dimension(name='country'),
                   Dimension(name='region'),
                   Dimension(name="city")],
        metrics=[Metric(name="screenPageViews")],
        date_ranges=[DateRange(start_date = input_start_date, end_date = input_end_date)],
    )
    response = client.run_report(request)
    return response

#Create table out of Subaru data
def dataFrame(response):
    output = []
    print("Report result:")
    for row in response.rows:
      output.append({"Date":row.dimension_values[0].value,
                     "Page_title":row.dimension_values[1].value,
                     "Device_brand":row.dimension_values[2].value,
                     "Country":row.dimension_values[3].value,
                     "State":row.dimension_values[4].value,
                     "City":row.dimension_values[5].value,
                     "Views": row.metric_values[0].value})
    df = pd.DataFrame(output)
    return df

#Connect to SQL Server and Load dataFrame to SQL Server table
def sql_connect(table,server,db):
	cnxn = sql.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server +f';DATABASE=' + db + ';Trusted_Connection=yes')
	cursor = cnxn.cursor()
	# Insert Dataframe into SQL Server:
	for index, row in table.iterrows():
		cursor.execute("INSERT INTO [analyticssql01].[Test].[Subaru].[ga4_qr_codes] (Date,Page_title,Device_brand,Country,State,City,Views) values(?,?,?,?,?,?,?)", row.Date, row.Page_title, row.Device_brand, row.Country, row.State, row.City, row.Views)
	cnxn.commit()
	cursor.close()

def main():
    #Initializing logging
    logging.basicConfig(filename='C:\\Packages\\PythonETL\\GA4_Analytics\\logfile.log',filemode='w',level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # initialize config file
    cfg = r"C:\Packages\PythonETL\GA4_Analytics\config.ini"
    cfg_parse = ConfigParser()
    cfg_parse.read(cfg)

    # set credentials
    creds_info = cfg_parse["DIRECTORIES"]
    file_name = creds_info["FILENAME"]
    property_id = creds_info["PROPERTY_ID"]

    # set dates
    date_info = cfg_parse["DATES"]
    start_date = date_info["START_DATE"]
    end_date = date_info["END_DATE"]

    #set database info
    db_info=cfg_parse["SQL"]
    db_server=db_info["SERVER"]
    db=db_info["DATABASE"]

    #Initialize Subaru GA4 json file credentials
    logging.info("Initializing Subaru GA4 credentials...")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file_name
    logging.info("Initializing Subaru GA4 credentials done.")

    #Fetch Subaru data from GA4
    logging.info("Fetching Subaru data from GA4...")
    response_api = fetch_data_ga4(property_id,start_date,end_date)
    logging.info("Fetching Subaru data complete.")

    #Create table out of Subaru data
    logging.info("Creating Subaru dataFrame...")
    dataFrame_table = dataFrame(response_api)
    logging.info("Created Subaru dataFrame.")

	#Connect to SQL Server and Load dataFrame to SQL Server table
    logging.info("Creating and  loading dataFrame to SQL table...")
    sql_connect(dataFrame_table, db_server, db)
    logging.info("SQL table loaded.")

if __name__ == "__main__":
    main()