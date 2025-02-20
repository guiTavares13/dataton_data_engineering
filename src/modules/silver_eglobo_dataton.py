from botocore.exceptions import NoCredentialsError
from connector.connect_to_s3 import S3Client
import pandas as pd
import os 


class SilverEgloboDataton:
    
    def __init__(self):
        self.__s3_client = S3Client('eglobo-dataton')

    def list_objects_in_raw_folder(self):
        
        all_objects = self.__s3_client.list_objects_in_bucket()
        return [obj for obj in all_objects if obj.startswith('bronze/')]

    def download_object(self, object_key, local_path):
        self.__s3_client.download_object(object_key, local_path)

    def read_raw_data(self, file_path):
        return pd.read_parquet(file_path) 

    def transform_data(self, df):

        df.columns = df.columns.str.lower()
        
        df = df.rename(columns={'userid': 'user_id',
                                'usertype': 'user_type',
                                'historysize': 'history_size',
                                'history': 'history',
                                'timestamphistory': 'timestamp_history',
                                'numberofclickshistory': 'number_of_clicks_history',
                                'timeonpagehistory': 'time_on_page_history',
                                'scrollpercentagehistory': 'scroll_percentage_history',
                                'pagevisitscounthistory': 'page_visits_count_history',
                                'timestamphistory_new': 'timestamp_history_new'})
        

        df['user_id'] = df['user_id'].astype(str)
        df['user_type'] = df['user_type'].replace({'Non-Logged': 0, 'Logged': 1}).astype(int)
        df['history_size'] = df['history_size'].astype(int)
        df['history'] = df['history'].astype(str)
        df['timestamp_history'] = df['timestamp_history'].astype(str)
        df['number_of_clicks_history'] = df['number_of_clicks_history'].astype(str)
        df['time_on_page_history'] = df['time_on_page_history'].astype(str)
        df['scroll_percentage_history'] = df['scroll_percentage_history'].astype(str)
        df['page_visits_count_history'] = df['page_visits_count_history'].astype(str)
        df['timestamp_history_new'] = df['timestamp_history_new'].astype(str)

        df = df.dropna()

        return df

    def save_to_bronze_layer(self, df, output_path):
        df.to_parquet(output_path, index=False)
        print(f"Dados salvos em {output_path}")

    def process_raw_data(self, object_key, local_path, output_path):
        self.download_object(object_key, local_path)
        df = self.read_raw_data(local_path)
        transformed_df = self.transform_data(df)
        self.save_to_bronze_layer(transformed_df, output_path)

    def upload_processed_data(self, output_path, s3_key):
        self.__s3_client.upload_object(output_path, s3_key)

    def delete_local_files(self, *file_paths):
        """
        Exclui os arquivos locais após o processamento.
        """
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Arquivo {file_path} excluído com sucesso.")
            else:
                print(f"Arquivo {file_path} não encontrado.")

    def main(self):
        object_keys = self.list_objects_in_raw_folder()
        if object_keys:
            for key in object_keys:
                print(f"Processando o arquivo: {key}")
                local_path = f"../io_files/files/{key.split('/')[-1]}" 
                output_path = f"../io_files/output_path/bronze/{key.split('/')[-1].split('.')[0]}.parquet"
                
                self.process_raw_data(key, local_path, output_path)
                
                s3_key = f"silver/{key.split('/')[-1].split('.')[0]}.parquet"
                self.upload_processed_data(output_path, s3_key)
                print(f"Arquivo {output_path} enviado para o S3 com a chave {s3_key}")

                self.delete_local_files(local_path, output_path)

        else:
            print("Nenhum arquivo encontrado na pasta 'bronze' do bucket.")

if __name__ == '__main__':

    try:
        silverEgloboDataton = SilverEgloboDataton()
        silverEgloboDataton.main()
    except Exception as e:
        print(f"Erro inesperado: {e}")