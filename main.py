import csv
import os
import time
import requests
from urllib.parse import urlparse

BASE_URL = (
    "https://www.sped.fazenda.gov.br/"
    "spedtabelas/appconsulta/obterTabelaExterna.aspx"
    "?idPacote=84&idTabela={valor}"
)

def baixar_tabelas(caminho_csv, diretorio_saida="downloads"):
    os.makedirs(diretorio_saida, exist_ok=True)

    with open(caminho_csv, newline="", encoding="utf-8") as arquivo_csv:
        leitor = csv.DictReader(arquivo_csv, delimiter=";")

        for linha in leitor:
            valor = linha["valor"].strip()
            # titulo = linha["titulo"].strip()

            url = BASE_URL.format(valor=valor)
            print(f"Baixando: {url}")

            resposta = requests.get(url, timeout=60)
            resposta.raise_for_status()

            # # Tenta obter a extensão do arquivo pelo Content-Disposition
            # extensao = ""
            # content_disposition = resposta.headers.get("Content-Disposition")
            # if content_disposition and "filename=" in content_disposition:
            #     filename = content_disposition.split("filename=")[1].strip("\"")
            #     _, extensao = os.path.splitext(filename)

            # # Se não houver extensão, assume .csv
            # if not extensao:
            #     extensao = ".csv"

            nome_arquivo = f"Tabela_{valor}.txt"
            caminho_saida = os.path.join(diretorio_saida, nome_arquivo)

            conteudo_texto = resposta.content.decode("iso-8859-1")

            with open(caminho_saida, "w", encoding="utf-8", newline="") as f:
                f.write(conteudo_texto)

            print(f"Salvo em: {caminho_saida}")

if __name__ == "__main__":
    baixar_tabelas("tabelas.csv")
