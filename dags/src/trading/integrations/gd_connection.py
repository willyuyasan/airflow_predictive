import os
import io
import logging
import pandas as pd
#!pip install openpyxl

#!pip install google-auth
import google.auth
#!pip install google-api-python-client
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account

from helpers.additional_functionalities import try_execution

log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)


class gdConnection:

    def __init__(self, **kwargs):

        # Remember, the google credentials if for APP in google clouds with (service accounts credential)
        # The drive enable conections is after granting access permision to the service account direction to the target folders

        self.credential_filename = kwargs.get('credential_filename',None)
        self.credential_location = './integrations' #Here change depending of your app

    @try_execution
    def run(self):

        self.service = self.get_service(self.credential_location, self.credential_filename)

    @try_execution  
    def get_service(self, credential_location, credential_filename):

        creds = service_account.Credentials.from_service_account_file(
            f'{credential_location}/{credential_filename}', 
            scopes=['https://www.googleapis.com/auth/drive']
        )

        service = build("drive", "v3", credentials=creds)

        return service
    
    @try_execution
    def download_file(self, real_file_id, **kwargs):

        service = kwargs.get('service',None)
        if not service:
            service = self.service

        try:
            file_id = real_file_id

            # pylint: disable=maybe-no-member
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                logger.info(f"Download {int(status.progress() * 100)}.")

        except HttpError as error:
            logger.info(f"An error occurred: {error}")
            file = None

        return file.getvalue()
    
    @try_execution
    def upload_file(self, folder_url, binary_file, file_name, **kwargs):

        service = kwargs.get('service',None)
        if not service:
            service = self.service

        folder_id = url_to_id(folder_url).split('?')[0]

        mimetype = kwargs.get('mimetype', None)
        if not mimetype:
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        #upload file inside the folder
        file_metadata = {
            'name': f'{file_name}', 
            'parents': [folder_id]
            }
        media = MediaIoBaseUpload(binary_file, mimetype=mimetype, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media).execute()

        return file
    
    @try_execution
    def list_folder_content(self, folder_url, **kwargs):

        service = kwargs.get('service',None)
        if not service:
            service = self.service

        folder_id = url_to_id(folder_url).split('?')[0]

        results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name, createdTime, mimeType)").execute()
        items = results.get('files', [])
        filedfs_ls = []
        folder_content_df = None

        if len(items)>0:
            logger.info(f"WU -> Files inside the folder:")
            for l in items:
                name = l['name']
                id = l['id']
                creation_time= l['createdTime']
                mimetype = l['mimeType']

                data_ls = [name, id, creation_time, mimetype]
                cols_ls = ['name', 'id', 'creation_time', 'mimetype']
                data_df = pd.DataFrame([data_ls],columns=cols_ls)

                filedfs_ls.append(data_df)
                logger.info(f"{name}")
        
            folder_content_df = pd.concat(filedfs_ls, axis=0).copy()

        return folder_content_df
    
    @try_execution
    def create_folder(self, parent_url, folder_name, **kwargs):

        service = kwargs.get('service',None)
        if not service:
            service = self.service

        parentfolder_id = url_to_id(parent_url).split('?')[0]

        folder_metadata = {
            'name': f'{folder_name}',
            "parents": [f'{parentfolder_id}'],
            'mimeType': 'application/vnd.google-apps.folder'
        }

        # create folder 
        new_folder = service.files().create(body=folder_metadata).execute()

        if new_folder['name']==folder_name:
            logger.info(f'WU -> Folder correctly created: ({folder_name})')
        else:
            logger.info('WU -> Error creating the folder')

        return new_folder
    
    @try_execution
    def download_excel_file(self, downloadfile_url, sheets_name_ls, **kwargs):

        service = kwargs.get('service',None)
        if not service:
            service = self.service

        skiprows = kwargs.get('skiprows',None)

        file_id = url_to_id(downloadfile_url)
        binary_file = self.download_file(file_id, service=service)

        files_dict = {}
        for sheet_name in sheets_name_ls:
            output_df = pd.read_excel(binary_file, sheet_name = sheet_name, skiprows=skiprows, engine='openpyxl')
            files_dict[sheet_name] = output_df.copy()

            v = output_df.shape
            logger.info(f'##### DOWNLOADED EXCEL FILE WITH SHEET {sheet_name}: {v} #####')
        
        return files_dict



# Additional functionalities
def url_to_id(downloadfile_url):
    x = downloadfile_url.split("/")
    return x[5]

        

