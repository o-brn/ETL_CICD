import json
import os
import sys
from datetime import datetime

import pandas as pd
import requests

# Configs
# API que retorna usuários
API_URL = "https://randomuser.me/api/?results=10"
CSV_PATH = "data/novos_leads.csv"
LOG_PATH = "logs/historico_execucao.json"
MAX_DAYS = 10


def load_logs():
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []


def save_logs(logs):
    # Garante pasta log
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(logs, f, indent=4)


def final_report(logs):
    print("\n")
    print(" Relatorio final")
    print(" ")

    df_logs = pd.DataFrame(logs)
    dias_sucesso = df_logs[df_logs["status"] == "sucesso"].shape[0]
    dias_erro = df_logs[df_logs["status"] == "erro"].shape[0]

    total_linhas = 0
    if os.path.exists(CSV_PATH):
        total_linhas = pd.read_csv(CSV_PATH).shape[0]

    print(f"Total de dias executados: {len(logs)}")
    print(f"Sucessos: {dias_sucesso}")
    print(f"Falhas: {dias_erro}")
    print(f"Total de linhas coletados (CSV): {total_linhas}")
    print(" " + "\n")


def main():
    logs = load_logs()

    # Filtra apenas execuções de sucesso para contar
    execucoes_sucesso = [l for l in logs if l["status"] == "sucesso"]

    if len(execucoes_sucesso) >= MAX_DAYS:
        print("Meta de 10 dias atingida. Processo finalizado.")
        final_report(logs)
        return

    run_data = {
        "data": datetime.now().isoformat(),
        "status": "pendente",
        "mensagem": "",
    }

    print(f"Iniciando coleta do dia {len(execucoes_sucesso) + 1}...")

    try:
        # 1. Requisição a API
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()  # Garante que erros 404/500 parem o script

        # 2. Processamento dos Dados
        dados_json = response.json()["results"]

        # Extraindo dados dos 5 primeiros usuarios para uma tabela
        lista_processada = []
        for user in dados_json[:5]:
            lista_processada.append(
                {
                    "nome": f"{user['name']['first']} {user['name']['last']}",
                    "email": user["email"],
                    "pais": user["location"]["country"],
                    "genero": user["gender"],
                    "data_coleta": datetime.now().strftime("%Y-%m-%d"),
                }
            )

        df_novo = pd.DataFrame(lista_processada)

        # 3. Salvamento Incremental (Append)
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

        if os.path.exists(CSV_PATH):
            df_novo.to_csv(CSV_PATH, mode="a", header=False, index=False)
        else:
            df_novo.to_csv(CSV_PATH, mode="w", header=True, index=False)

        run_data["status"] = "sucesso"
        run_data["mensagem"] = f"{len(df_novo)} linhas adicionadas."
        print("Sucesso")

    except Exception as e:
        run_data["status"] = "erro"
        run_data["mensagem"] = str(e)
        print(f"Erro: {e}")

        logs.append(run_data)
        save_logs(logs)
        # Sair com erro (exit 1) para o GitHub Actions saber que falhou
        sys.exit(1)

    # Salva log de sucesso
    logs.append(run_data)
    save_logs(logs)


if __name__ == "__main__":
    main()
