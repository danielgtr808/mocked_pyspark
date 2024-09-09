from sqlalchemy import create_engine, Engine



def get_sqlalchemy_engine() -> Engine:
    return create_engine(
        "mssql+pyodbc://localhost/DB_IGE?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
    )












from sqlalchemy import Integer, MetaData, Table, Column, Boolean, String, select, Engine, update
from typing import Dict, List

import os
import utils as u



def add_new_files(files_dict: List[Dict], tb_rco_arqv_ctrl: Table) -> None:
    if (len(files_dict) == 0):
        return
    
    with engine.begin() as connection:
        connection.execute(tb_rco_arqv_ctrl.insert(), files_dict)

def get_all_inserted_files(engine: Engine, tb_rco_arqv_ctrl: Table) -> List:
    with engine.connect() as connection:
        return connection.execute(
            select(
                tb_rco_arqv_ctrl
            )
        ).fetchall()

def get_non_uploaded_files(engine: Engine, tb_rco_arqv_ctrl: Table) -> List:
    with engine.connect() as connection:
        return connection.execute(
            select(
                tb_rco_arqv_ctrl
            ).where(
                tb_rco_arqv_ctrl.c.UPLOADED == False
            )
        ).fetchall()
   
def list_all_files_paths(directory: str) -> List[str]:
    all_files = []
    for root, dirs, files in os.walk(directory):
        all_files += [os.path.join(root, file) for file in files]
    return all_files

def mark_file_as_uploaded(engine: Engine, tb_rco_arqv_ctrl: Table, file_name: str) -> None:
    print("file_name: ", file_name)
    with engine.begin() as connection:
        connection.execute((
            update(tb_rco_arqv_ctrl)
            .where(tb_rco_arqv_ctrl.c.NOME_ARQV == file_name)
            .values(UPLOADED = True)
        ))

engine = u.get_sqlalchemy_engine()
metadata = MetaData()

tb_rco_arqv = Table(
    "TB_RCO_ARQV",
    metadata,
    Column("NMR_LINH", Integer(), primary_key = True),
    Column("LINH", String(255), nullable = False),
    Column("NOME_ARQV", String(127), primary_key = True),
    schema = "DB_IGE.dbo"
)

tb_rco_arqv_ctrl = Table(
    "TB_RCO_ARQV_CTRL",
    metadata,
    Column("NOME_ARQV", String(127), primary_key = True),
    Column("CAMINHO_ARQV", String(255), primary_key = True),
    Column("UPLOADED", Boolean, nullable = False),
    schema = "DB_IGE.dbo"
)

metadata.create_all(engine)



rco_files_avaliable: List[Dict] = list(map(
    lambda x:
    {
        "NOME_ARQV": x.split("\\")[-1],
        "CAMINHO_ARQV": "\\".join(x.split("\\")[:-1]),
        "UPLOADED": False
    },
    list_all_files_paths(r"C:\Users\danie\OneDrive\Documentos\my_files")
))
inserted_files: List[str] = list(map(lambda x: x[0], get_all_inserted_files(engine, tb_rco_arqv_ctrl)))
add_new_files(
    list(
        filter(
            lambda x:
            ((x["NOME_ARQV"] not in inserted_files) and (x["NOME_ARQV"].lower().startswith("rco"))),
            rco_files_avaliable
        )
    ),
    tb_rco_arqv_ctrl
)

for non_uploaded_file in get_non_uploaded_files(engine, tb_rco_arqv_ctrl):
    batch_size = 1_000_000
    if (non_uploaded_file[1] == ""):
        continue

    with open(os.path.join(non_uploaded_file[1], non_uploaded_file[0]), 'r', encoding='utf-8') as f:        
        batch = []

        for idx, line in enumerate(f):
            batch.append({
                "NMR_LINH": idx + 1,
                "LINH": line.strip(),
                "NOME_ARQV": non_uploaded_file[0]
            })

            if len(batch) >= batch_size:
                with engine.begin() as connection:
                    connection.execute(
                        tb_rco_arqv.insert(),
                        batch
                    )
                print(f"Inserted {len(batch)} rows")
                batch = [] 

        # Insert any remaining lines
        if batch:
            with engine.begin() as connection:
                connection.execute(
                    tb_rco_arqv.insert(),
                    batch
                )
            print(f"Inserted {len(batch)} remaining rows")

    mark_file_as_uploaded(engine, tb_rco_arqv_ctrl, non_uploaded_file[0])
