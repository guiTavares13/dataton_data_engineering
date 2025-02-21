import sys
import os
from botocore.exceptions import NoCredentialsError

# Adiciona o diretório src ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from connector.connect_to_s3 import S3Client

class RawEgloboDataton:
    def __init__(self):
        self.__s3_client = S3Client('dataton-eglobo')

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
        # Obtém o caminho absoluto do diretório atual do script
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../io_files/input_data"))
        
        csv_files = []

        for i in range(1, 7):
            name_file = os.path.join(base_path, f"treino_parte{i}.csv")
            
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