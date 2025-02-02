import pandas as pd
import requests
from datetime import datetime
import json
from pathlib import Path
import signal
import sys
import time

class GracefulExit:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        print("\nRecebido sinal de interrupção. Finalizando graciosamente...")
        self.kill_now = True

def read_excel_file(file_path):
    """
    Lê o arquivo Excel e retorna um DataFrame
    """
    try:
        df = pd.read_excel(file_path)
        return df
    except Exception as e:
        print(f"Erro ao ler arquivo Excel: {str(e)}")
        return None

def create_log_files():
    """
    Cria arquivos de log para sucessos e falhas
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    success_file = f"successful_requests_{timestamp}.txt"
    failure_file = f"failed_requests_{timestamp}.txt"
    return success_file, failure_file

def log_result(file_path, data, response=None, error=None):
    """
    Registra o resultado da requisição no arquivo apropriado
    """
    with open(file_path, 'a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if response:
            f.write(f"[{timestamp}] Sucesso - Dados: {json.dumps(data)} - Status: {response.status_code}\n")
        else:
            f.write(f"[{timestamp}] Falha - Dados: {json.dumps(data)} - Erro: {str(error)}\n")

def prepare_payload(row):
    """
    Prepara o payload JSON a partir de uma linha do DataFrame
    """
    try:
        return {
            "name": str(row['Name']),
            "contact_name": str(row['Company Name']),
            "x_studio_tese": str(row['x_studio_tese']),
            "user_id": int(row['user_id']),
            "team_id": int(row['team_id']),
            "tag_ids": row['tag_ids'].split(',') if isinstance(row['tag_ids'], str) else [],
            "stage_id": int(row['stage_id']) if pd.notna(row['stage_id']) else 10
        }
    except Exception as e:
        print(f"Erro ao preparar payload: {str(e)}")
        return None

def make_request(url, payload, timeout=45, max_retries=3):
    """
    Faz a requisição POST com retry e timeout
    """
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=timeout
            )
            return response
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                raise
            print(f"Timeout na tentativa {attempt + 1}/{max_retries}. Tentando novamente...")
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            print(f"Erro na tentativa {attempt + 1}/{max_retries}: {str(e)}. Tentando novamente...")
            time.sleep(1)

def main():
    # Configurações
    API_URL = "http://127.0.0.1:8000/opportunities/"
    EXCEL_FILE = "crm.xlsx"  # Ajuste o nome do arquivo conforme necessário
    
    # Inicializar handler de interrupção gracioso
    graceful_exit = GracefulExit()

    # Criar arquivos de log
    success_file, failure_file = create_log_files()
    print(f"Arquivos de log criados: {success_file} e {failure_file}")

    # Ler planilha
    df = read_excel_file(EXCEL_FILE)
    if df is None:
        return

    total_rows = len(df)
    print(f"Total de linhas a processar: {total_rows}")

    # Processar cada linha
    for index, row in df.iterrows():
        if graceful_exit.kill_now:
            print("Interrompendo processamento...")
            break

        print(f"\nProcessando linha {index + 1}/{total_rows}")
        
        try:
            # Preparar dados
            payload = prepare_payload(row)
            if payload is None:
                continue

            # Fazer requisição POST
            response = make_request(API_URL, payload)
            
            # Verificar resposta
            if response.status_code in (200, 201):
                log_result(success_file, payload, response=response)
                print(f"✓ Sucesso ao processar linha {index + 1}")
            else:
                error_msg = f"Status code: {response.status_code}"
                log_result(failure_file, payload, error=error_msg)
                print(f"✗ Falha ao processar linha {index + 1}: {error_msg}")
                
        except requests.exceptions.Timeout:
            error_msg = "Timeout na requisição"
            log_result(failure_file, payload, error=error_msg)
            print(f"✗ Erro ao processar linha {index + 1}: {error_msg}")
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Erro na requisição: {str(e)}"
            log_result(failure_file, payload, error=error_msg)
            print(f"✗ Erro ao processar linha {index + 1}: {error_msg}")
            
        except Exception as e:
            error_msg = f"Erro inesperado: {str(e)}"
            log_result(failure_file, payload, error=error_msg)
            print(f"✗ Erro ao processar linha {index + 1}: {error_msg}")

    print("\nProcessamento finalizado!")

if __name__ == "__main__":
    main()
