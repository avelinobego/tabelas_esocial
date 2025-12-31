if __name__ == "__main__":
    from pathlib import Path
    from datetime import datetime
    import psycopg
    import re
    from psycopg.rows import tuple_row

    PASTA = Path("./downloads")

    DB_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "dbname": "sped",
        "user": "postgres",
        "password": "postgres",
    }


    def inferir_tipo(valores):
        tipos = set()

        for v in valores:
            if v == "":
                continue

            try:
                int(v)
                tipos.add("int")
                continue
            except ValueError:
                pass

            try:
                float(v.replace(",", "."))
                tipos.add("float")
                continue
            except ValueError:
                pass

            try:
                datetime.strptime(v, "%d%m%Y")
                tipos.add("date")
                continue
            except ValueError:
                pass

            tipos.add("text")

        if not tipos or tipos == {"int"}:
            return "BIGINT"
        if tipos <= {"int", "float"}:
            return "DOUBLE PRECISION"
        if tipos == {"date"}:
            return "DATE"
        return "TEXT"


    def converter(valor, tipo):
        if valor == "":
            return None
        if tipo == "INTEGER":
            return int(valor)
        if tipo == "DOUBLE PRECISION":
            return float(valor.replace(",", "."))
        if tipo == "DATE":
            return datetime.strptime(valor, "%d%m%Y").date()
        return valor


    with psycopg.connect(**DB_CONFIG, row_factory=tuple_row) as conn:
        with conn.cursor() as cur:

            for arquivo in PASTA.glob("*.txt"):
                tabela = arquivo.stem.lower()

                with arquivo.open(encoding="utf-8") as f:
                    linhas = f.read().splitlines()

                cabecalho = re.sub(r'^versão=\d+\s+', '', linhas[0])

                cabecalho = [c.strip() for c in cabecalho.split(",")]
                dados = [l.split("|") for l in linhas[1:]]

                colunas = list(zip(*dados))
                tipos = [inferir_tipo(col) for col in colunas]

                cur.execute(f'DROP TABLE IF EXISTS "{tabela}" CASCADE')

                ddl = ", ".join(
                    f'"{col}" {tp}'
                    for col, tp in zip(cabecalho, tipos)
                )

                cur.execute(f'CREATE TABLE "{tabela}" ({ddl})')

                sql_insert = f'''
                    INSERT INTO "{tabela}"
                    ({", ".join(f'"{c}"' for c in cabecalho)})
                    VALUES ({", ".join("%s" for _ in cabecalho)})
                '''

                # print(f"------------\n{sql_insert}\n------------------")


                registros = [
                    tuple(converter(v, t) for v, t in zip(linha, tipos))
                    for linha in dados
                ]

                cur.executemany(sql_insert, registros)

                for coluna in cabecalho:
                    idx = f'idx_{tabela}_{coluna}'.lower()
                    cur.execute(
                        f'CREATE INDEX "{idx}" ON "{tabela}" ("{coluna}")'
                    )

        conn.commit()
