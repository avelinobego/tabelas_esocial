import csv
import os
import requests
from urllib.parse import urlparse
import re

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
            titulo =  re.sub(string= linha["titulo"].strip(), pattern=r"[\\/]", repl="_").lower()

            url = BASE_URL.format(valor=valor)
            print(f"Baixando: {url}")

            resposta = requests.get(url, timeout=60)
            resposta.raise_for_status()

            nome_arquivo = f"{titulo}.txt"
            caminho_saida = os.path.join(diretorio_saida, nome_arquivo)

            conteudo_texto = resposta.content.decode("iso-8859-1")

            with open(caminho_saida, "w", encoding="utf-8") as f:
                f.write(conteudo_texto)

            # print(f"Salvo em: {caminho_saida}")

if __name__ == "__main__":
    baixar_tabelas("tabelas.csv")
