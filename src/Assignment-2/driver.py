import requests
from util import process_data, write_to_delta

# 1. Fetch data from the API
api_link = 'https://reqres.in/api/users?page=2'
response = requests.get(api_link)

# 2. Process data using the utility function
site_info_df = process_data(response.text)

# 3. Write DataFrame to Delta table
write_to_delta(site_info_df, "/site_info/persons", "site_info.persons")
