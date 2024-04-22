import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import json


DATA_FILE_PATH = "data/json_for_case.json"
LOG_FILE_PATH = "logs/data_insertion.log"

POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')


def save_log(log):
    current_date_hour = datetime.now()
    with open(LOG_FILE_PATH, 'a') as f:
        f.write(f'[{current_date_hour}] {log}\n')


def data_handling(data):
    # Para o tratamento dos dados de entrada, foi optado por usar o pandas para remoção de registros duplicados.
    # Com o json_normalize, cada chave do dicionário na coluna "endereco" se torna uma coluna diferente, no formato endereco_[chave].
    df = pd.json_normalize(data, sep='_')

    num_rows_before = len(df)
    df = df.drop_duplicates()
    num_rows_after = len(df)

    if num_rows_after < num_rows_before:
        dif = num_rows_before - num_rows_after
        log = f'Quantidade de registros duplicados e removidos do JSON: {dif}'
        save_log(log)

    # Mais transformações aqui...

    return df


def data_extraction(file_path):
    with open(file_path) as file:
        data = json.load(file)
        
    return data
    

def send_to_postgres(df, table_name):
    try:
        # Criando conexão com o Postgres
        engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        upsert_data = df.to_dict(orient='records')

        # Criando a tabela se não existir
        with engine.connect() as connection:
            connection.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    nome VARCHAR(255),
                    idade INTEGER,
                    email VARCHAR(255),
                    telefone VARCHAR(20),
                    endereco_logradouro VARCHAR(255),
                    endereco_numero INTEGER,
                    endereco_bairro VARCHAR(255),
                    endereco_cidade VARCHAR(255),
                    endereco_estado CHAR(2),
                    endereco_cep VARCHAR(10)
                )
            ''')

            # Checando se a constraint de primary key já existe
            existing_constraints = engine.execute(f"""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name='{table_name}' AND constraint_name='pk_{table_name}_id'
            """).fetchall()

            # Caso não existir, criar
            if not existing_constraints:
                connection.execute(f'''
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT pk_{table_name}_id PRIMARY KEY (id);
                ''')

            metadata = MetaData(bind=engine)
            table = Table(table_name, metadata, autoload=True, autoload_with=engine)

            # Enviando os dados para o Postgres (modo de upsert --> sem duplicação dos dados)
            for data in upsert_data:
                stmt = insert(table).values(data)
                stmt = stmt.on_conflict_do_update(
                    constraint=f"pk_{table_name}_id",
                    set_=data
                )
                connection.execute(stmt)
                
        log = f'Dados enviados para o banco Postgres com sucesso!'

    except Exception as e:
        log = f'Erro ao tentar enviar dados para o banco Postgres: {e}'

    print(log)
    save_log(log)    


def main():
    data = data_extraction(DATA_FILE_PATH)
    df_prep = data_handling(data)
    send_to_postgres(df_prep, table_name="pessoas")


if __name__ == "__main__":
    main()
