from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.customaudience import CustomAudience
from google.cloud import bigquery
from datetime import datetime

# Set up your environment
app_id = '*****************************'
app_secret = '*************************'
access_token = '************************'

ad_account_id = '**********************'
project_id = '***********************'
dataset_id = '***********************'

# Create a BigQuery client and specify your project ID
client = bigquery.Client(project=project_id)

# Authenticate your app
FacebookAdsApi.init(app_id=app_id, app_secret=app_secret, access_token=access_token)

# Find or Create the Custom Audience
account = AdAccount(ad_account_id)

# Define a function called get_audience_tables that takes 'dataset_id' as an argument.
def get_audience_tables(dataset_id):
    """
    Get a dictionary of BigQuery tables from a dataset where the row count is greater than or equal to 1000.
    Parameters:
        dataset_id (str): The ID of the dataset in BigQuery.
    Returns:
        dict: A dictionary containing table IDs as keys and formatted table names as values.
              The formatted table names include modifications to the table ID for display or description purposes.
              If the row count is greater than or equal to 1000, the table ID is included in the dictionary.
    """

    # Create a reference to the BigQuery dataset with the given 'dataset_id'.
    dataset_ref = client.dataset(dataset_id)

    # Get a list of all the tables in the specified dataset using the client's list_tables method.
    tables = client.list_tables(dataset_ref)

    # Create an empty dictionary to store the table names and their corresponding formatted names.
    audiences_grt_1000 = {}

    # Loop through each table in the 'tables' list.
    for table in tables:

        # Create a reference to the specific table using the table_id obtained from 'tables'.
        table_ref = dataset_ref.table(table.table_id)

        # Retrieve the table schema and metadata using the client's get_table method.
        table = client.get_table(table_ref)

        # Get the number of rows (row_count) in the current table.
        row_count = table.num_rows

        # Check if the row_count of the table is greater than or equal to 1000.
        if row_count >= 1000:

            # If the condition is met, format the table name to be more readable and descriptive.
            # Replace underscores (_) with spaces, and add specific prefixes to certain table names.
            # Convert the table name to uppercase and append " O2O" at the end. O2O means offline to online
            table_format = table.table_id.replace('_', " ").replace('cat', 'vehicle category:').replace('event', 'event name:').replace('channel', 'channel:').upper() + " O2O"

            # Store the formatted table name (table_format) as the value and the original table_id as the key in the dictionary.
            audiences_grt_1000[table.table_id] = table_format

    # After processing all the tables, return the dictionary containing the formatted table names for tables with row_count >= 1000.
    return audiences_grt_1000

def get_existing_audience_names():
    """
    Get the names and IDs of custom audiences created after a specified date.

    Returns:
        dict: A dictionary containing custom audience names as keys and their corresponding IDs as values.
    """
    # Define the date to filter audiences after ('YYYY-MM-DD')
    date_to_filter_after = '2023-07-01'

    # Convert the 'date_to_filter_after' string to a datetime object
    date_to_filter_after_dt = datetime.strptime(date_to_filter_after, '%Y-%m-%d')

    # Fetch existing custom audiences from the account with their names, IDs, and creation dates
    existing_audiences = account.get_custom_audiences(fields=[CustomAudience.Field.name, CustomAudience.Field.id, CustomAudience.Field.time_created])

    # Filter audiences based on the specified date
    filtered_audiences = [existing_audience for existing_audience in existing_audiences if datetime.utcfromtimestamp(existing_audience[CustomAudience.Field.time_created]) > date_to_filter_after_dt]

    # Extract names and IDs of the filtered audiences into a dictionary
    filtered_audiences_names_id = {}
    for audience in filtered_audiences:
        filtered_audiences_names_id[audience[CustomAudience.Field.name]] = audience[CustomAudience.Field.id]

    # Return the dictionary containing custom audience names as keys and their corresponding IDs as values
    return filtered_audiences_names_id

def create_audience(audience_name, filtered_audiences_names):
    """
    Create a custom audience on Facebook Ads Manager.
    If the audience name already exists in the list of filtered audiences,
    the existing audience with the same name will be returned.
    Otherwise, a new custom audience with the given name will be created.
    Parameters:
        audience_name (str): The name of the custom audience to be created.
        filtered_audiences_names (dict): A dictionary containing existing custom audience names as keys
                                         and their corresponding IDs as values.
    Returns:
        CustomAudience: The custom audience object that has been created or retrieved.
    """
    # Define the audience description for the custom audience
    audience_description = "This audience list is extracted from the O2O master table based on the segmentation rule provided"

    # Check if the audience name already exists in the list of filtered audiences
    if audience_name in filtered_audiences_names.keys():
        # If the audience name exists, retrieve the existing audience based on its ID
        audience = CustomAudience(filtered_audiences_names[audience_name])
    else:
        # If the audience name doesn't exist, create a new custom audience
        audience = account.create_custom_audience(
            fields=[],
            params={
                CustomAudience.Field.name: audience_name,
                CustomAudience.Field.description: audience_description,
                CustomAudience.Field.subtype: CustomAudience.Subtype.custom,
                CustomAudience.Field.customer_file_source: CustomAudience.CustomerFileSource.user_provided_only,
            },
        )

    # Return the custom audience object that has been created or retrieved
    return audience

def get_users_data(table_id):
    """
    Get user data from a specified BigQuery table.
    The function executes a BigQuery SQL query to retrieve user data from the provided table.
    The user data includes mailing address first name, email, mailing address last name, and phone number.
    Parameters:
        table_id (str): The ID of the BigQuery table containing the user data.
    Returns:
        list: A list of lists, where each inner list contains user data for an individual user.
              Each inner list has the following format: [mailing_address_first_name, email, mailing_address_last_name, phone].
    """
    # Define your BigQuery SQL query to retrieve the data from the specified table
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    # Execute the query and fetch the results
    results = client.query(query).result()

    # Initialize an empty list to store the user data
    users_data = []

    # Loop through the query results and extract user data into a list of lists
    for row in results:
        user_data = [
            row['mailing_address_first_name'],
            row['email'],
            row['mailing_address_last_name'],
            row['phone']
        ]
        users_data.append(user_data)

    # Return the list of user data
    return users_data

def upload_audience_users(audience, users_data):
    """
    Upload user data to a custom audience on Facebook Ads Manager.
    The function uploads user data provided in the 'users_data' parameter to the specified custom audience.
    Parameters:
        audience (CustomAudience): The custom audience object where the user data will be uploaded.
        users_data (list): A list of lists containing user data to be uploaded.
                           Each inner list must contain user attributes in the specified order.
    Returns:
        dict: A dictionary containing information about the upload status and number of successful and failed entries.
    """
    # Define the schema for the audience attributes
    schema = ['FN', 'EMAIL', 'LN', 'PHONE']

    # Upload user data to the specified custom audience
    audience_users = audience.create_user(
        fields=[],
        params={
            'payload': {
                'schema': schema,
                'data': users_data
            }
        }
    )
    
    # Return the result of the audience data upload
    return audience_users

def main(request):
    """
    Main function to create custom audiences, fetch user data, and upload them to Facebook Ads Manager.

    This function performs the following steps:
    1. Retrieves a dictionary of BigQuery tables with row count >= 1000 as 'audiences_grt_1000'.
       (Note: A test dictionary is provided for demonstration purposes.)
    2. Fetches existing custom audience names and IDs from Facebook Ads Manager as 'filtered_audiences_names'.
    3. For each table in 'audiences_grt_1000':
       - Creates or retrieves a custom audience in Facebook Ads Manager with the provided audience name.
       - Fetches user data from the BigQuery table.
       - Uploads the user data to the corresponding custom audience.

    Returns:
        str: A success message indicating that the audiences were created successfully.
    """
    # Step 1: Retrieve a dictionary of BigQuery tables with row count >= 1000 (For demonstration, a test dictionary is provided)
    audiences_grt_1000 = get_audience_tables(dataset_id)

    # Step 2: Fetch existing custom audience names and IDs from Facebook Ads Manager
    filtered_audiences_names = get_existing_audience_names()

    # Step 3: Process each table and create audiences, fetch user data, and upload to Facebook Ads Manager
    for table_id, audience_name in audiences_grt_1000.items():
        # Create or retrieve a custom audience in Facebook Ads Manager with the provided audience name
        audience = create_audience(audience_name, filtered_audiences_names)

        # Fetch user data from the BigQuery table with the specified 'table_id'
        users_data = get_users_data(table_id)

        # Upload the user data to the corresponding custom audience in Facebook Ads Manager
        audience_users = upload_audience_users(audience, users_data)

    # Return a success message indicating that the audiences were created successfully
    return "Audience created successfully!"
