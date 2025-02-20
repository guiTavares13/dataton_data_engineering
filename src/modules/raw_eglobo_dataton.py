from botocore.exceptions import NoCredentialsError
from connector.connect_to_s3 import S3Client 
import os


class RawEgloboDataton:
    def __init__(self):
        self.__s3_client = S3Client('eglobo-dataton')

    def upload_csv_to_s3(self, file_path):
        """
        Faz o upload de um arquivo CSV para o S3 na pasta 'raw'.
        """
        file_name = os.path.basename(file_path)
        s3_key = f"raw/{file_name}"  

        try:
            self.__s3_client.upload_object(file_path, s3_key)
            print(f"Arquivo {file_name} enviado com sucesso para o S3 com a chave {s3_key}")
        except NoCredentialsError:
            print('Credenciais não disponíveis')
        except FileNotFoundError:
            print(f"Arquivo {file_name} não encontrado")
        except Exception as e:
            print(f"Erro ao enviar o arquivo {file_name}: {e}")

    def main(self):
        """
        Método principal para processar e enviar os arquivos CSV para o S3.
        """
        path_file = '../io_files/input_data/treino_parte'
        csv_files = []

        for i in range(1, 7):
            name_file = f"{path_file}{i}.csv"
            if os.path.exists(name_file):
                csv_files.append(name_file)
                print(f"Arquivo {name_file} encontrado e adicionado à lista")
            else:
                print(f"Arquivo não encontrado: {name_file}")

        for file_path in csv_files:
            self.upload_csv_to_s3(file_path)


if __name__ == '__main__':

    try:
        rawEgloboDataton = RawEgloboDataton()
        rawEgloboDataton.main()
    except Exception as e:
        print(f"Erro inesperado: {e}")