from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.logic import LogicManagementClient
from azure.storage.blob import BlobServiceClient
from ftplib import FTP
import os
import random

credential = AzureCliCredential()
subscription_id = os.environ["Azure_Subscription_ID"] 

resource_client = ResourceManagementClient(credential, subscription_id)
RESOURCE_GROUP_NAME = "ftp-task-rg"
LOCATION = "centralus"
#resource group
rg_result = resource_client.resource_groups.create_or_update(RESOURCE_GROUP_NAME,
   {
       "location" : LOCATION
   }
)

#storage account
storage_client = StorageManagementClient(credential, subscription_id)
STORAGE_ACCOUNT_NAME = f"ftp-task-storage-account{random.randint(1,100000):05}"
poller = storage_client.storage_accounts.begin_create ( RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME,  
   { "location": LOCATION,
     "kind": "StorageV2",
   "sku": {"name": "Standard_LRS"}
  }
)
storage_account = poller.result()
CONTAINER_NAME = "ftp-task-blob-container"
container = storage_client.blob_containers.create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME, CONTAINER_NAME, {})

LOGIC_APP_NAME = "ftp-task-logic-app"
logic_client = LogicManagementClient(credential, subscription_id, base_url = 'https://management.azure.com')
poller = logic_client.logic_apps.create_or_update(RESOURCE_GROUP_NAME, LOGIC_APP_NAME,
   {
    "location" : LOCATION
   }
)
logic_app_result = poller.result()

#ftp to azure storage account
temp_list =[]

def get_file(x):
   temp_list.append(x)

def files_list(folder):
   FTP.cwd(folder)
   FTP.retrlines('LIST', get_file)

def download_file(filename):
   FTP.retrbinary('RETR'+ filename, open(filename, "wb").write)

store_files = BlobServiceClient(STORAGE_ACCOUNT_NAME, account_key= '')
FTP.login(user= '', passwd= '')
files_list(folder = '' )

for file in temp_list:
   i = 0
   exists = False
   while i<3 and (not exists):
      try: 
         properties = store_files.get_blob_client(CONTAINER_NAME, file)
         break
      except Exception as e:
         pass

      download_file(file)
      print('Downloaded' + file)
      os.system('zip-f'+ file)
      store_files.put_block_blob_from_path(CONTAINER_NAME, file)
      print('Uploaded' + file)
      os.system('rm' + file)

      if exists:
         break

