from botocore.exceptions import NoCredentialsError
from connect_to_s3 import S3Client
import pandas as pd


class BronzeEgloboDataton:
    
    def __init__(self):
        self.__s3_client = S3Client('eglobo-dataton')

    def list_objects_in_raw_folder(self):
        # Listar apenas objetos na pasta 'raw' no bucket
        all_objects = self.__s3_client.list_objects_in_bucket()
        return [obj for obj in all_objects if obj.startswith('raw/')]

    def download_object(self, object_key, local_path):
        self.__s3_client.download_object(object_key, local_path)

    def read_raw_data(self, file_path):
        return pd.read_csv(file_path)

    def transform_data(self, df):
        # Exemplo de transformação de dados
        df['userId'] = df['userId'].astype(str)
        df['userType'] = df['userType'].astype(str)
        df['history'] = df['history'].astype(str)
        df['timestampHistory'] = df['timestampHistory'].astype(str)
        df['numberOfClicksHistory'] = df['numberOfClicksHistory'].astype(str)
        df['timeOnPageHistory'] = df['timeOnPageHistory'].astype(str)
        df['scrollPercentageHistory'] = df['scrollPercentageHistory'].astype(str)
        df['pageVisitsCountHistory'] = df['pageVisitsCountHistory'].astype(str)
        df['timestampHistory_new'] = df['timestampHistory_new'].astype(str)

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

    def main(self):
        object_keys = self.list_objects_in_raw_folder()
        if object_keys:
            for key in object_keys:
                print(f"Processando o arquivo: {key}")
                local_path = f"./files/{key.split('/')[-1]}"  # Local path para o arquivo a ser baixado
                output_path = f"./output_path/bronze/{key.split('/')[-1].split('.')[0]}.parquet"
                
                # Processa os dados
                self.process_raw_data(key, local_path, output_path)
                
                # Definindo o caminho do arquivo na pasta bronze do S3
                s3_key = f"bronze/{key.split('/')[-1].split('.')[0]}.parquet"
                self.upload_processed_data(output_path, s3_key)
                print(f"Arquivo {output_path} enviado para o S3 com a chave {s3_key}")

        else:
            print("Nenhum arquivo encontrado na pasta 'raw' do bucket.")


# Executando o processamento
bronzeEgloboDataton = BronzeEgloboDataton()
bronzeEgloboDataton.main()
