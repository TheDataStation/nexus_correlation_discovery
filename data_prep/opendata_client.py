import requests
import pandas as pd

class OpenDataClient:
    def __init__(self, domain, base_url, app_token):
        self.domain = domain
        self.base_url = base_url
        self.app_token = app_token

    def get_data(self, id, query=None, limit=1_000_000):
        api_url = f"{self.base_url}{id}.json"
        if query:
            params = {
                '$$app_token': self.app_token,
                '$query': f"{query} LIMIT {limit}"
            }
        else:
            params = {
                '$$app_token': self.app_token,
                '$limit': limit
            }
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Failed to retrieve data: {response.status_code} - {response.text}")
            return None
    
    def get_metadata_by_id(self, id):
        api_url = "http://api.us.socrata.com/api/catalog/v1"
        params = {"ids": id}
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            metadata = response.json()
            return metadata['results'][0]['resource']
        else:
            print(f"Failed to retrieve data: {response.status_code} - {response.text}")
            return None
    
    @staticmethod
    def get_all_domains():
        metadata_api_url = "https://api.us.socrata.com/api/catalog/v1"

        # Perform a metadata search query to retrieve datasets
        response = requests.get(metadata_api_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            
            # Extract the list of domains from the response
            domains = set()
            for dataset in data['results']:
                domains.add(dataset['resource']['domain'])
            
            # Print the list of open data portal domains
            for domain in domains:
                print(domain)
        else:
            print("Error:", response.status_code)

    def datasets(self, domain):
        api_url = "http://api.us.socrata.com/api/catalog/v1"
        params = {"domains": domain, "only": "dataset", "limit": 5000}
        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            metadata = response.json()
            return metadata['results']
        else:
            print(f"Failed to retrieve data: {response.status_code} - {response.text}")
            return None


if __name__ == "__main__":
    domain = "data.cityofchicago.org"
    client = OpenDataClient(domain, "https://data.cityofchicago.org/resource/", "voGyQiv0JulAwW8e2YwTCUlBS")
    id = '22u3-xenr'
    # metadata = client.get_all_ids(domain)
    # print(metadata[0])
    # metadata = client.get_metadata_by_id(id)
    # print(metadata)
    data = client.get_data(id, query="SELECT * WHERE violation_date between '2020-01-01T00:00:00' and '2020-01-31T23:59:59'")
    print(data.columns)