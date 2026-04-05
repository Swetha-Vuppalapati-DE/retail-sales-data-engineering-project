from azure.storage.filedatalake import DataLakeServiceClient
import os

# -------------------------------
# CONFIGURATION
# -------------------------------

account_name = "adlsretailsales"
account_key = os.getenv("AZURE_STORAGE_KEY")
file_system_name = "retailsales-data"

local_folder = "data"

# -------------------------------
# CONNECT TO ADLS
# -------------------------------

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

file_system_client = service_client.get_file_system_client(file_system=file_system_name)

# -------------------------------
# FUNCTION TO UPLOAD FILE
# -------------------------------

def upload_file(local_file_path, adls_path):
    try:
        file_client = file_system_client.get_file_client(adls_path)

        with open(local_file_path, "rb") as f:
            data = f.read()

        file_client.upload_data(data, overwrite=True)

        print(f"Uploaded: {adls_path}")

    except Exception as e:
        print(f"Error: {str(e)}")

# -------------------------------
# FILE MAPPING
# -------------------------------

files_map = {
    "customers.csv": "raw/customers/customers.csv",
    "products.csv": "raw/products/products.csv",
    "orders.csv": "raw/orders/orders.csv"
}

# -------------------------------
# EXECUTION
# -------------------------------

for file_name, adls_path in files_map.items():
    local_path = os.path.join(local_folder, file_name)

    if os.path.exists(local_path):
        upload_file(local_path, adls_path)
    else:
        print(f"File not found: {local_path}")

print("Upload completed!")