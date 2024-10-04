import sys
sys.path.append("D:\\10_outros\\")

import sqlalchemy as sa
import subprocess
import time
import utils as u



def run_mesh(mesh_id: str, sleep_minutes_after_not_success: int = 60) -> None:
    u.log("Estabelecendo conexão com o SQL")
    engine, metadata = u.get_sql_alchemy_engine()
    u.log("Conexão estabelecida com sucesso.")
    print()

    u.log(f"Obtendo jobs para a malha: {mesh_id}")
    vw_liws_jobs = sa.Table("VW_LIWS_JOBS", metadata, autoload_with=engine)

    while True:
        with engine.begin() as connection:
            select_result = connection.execute(sa.select(
                vw_liws_jobs.c.REF,
                vw_liws_jobs.c.JOB_ORDER,
                vw_liws_jobs.c.JOB_PATH,
                vw_liws_jobs.c.MESH_ID
            ).where(
                vw_liws_jobs.c.MESH_ID == mesh_id
            )).fetchall()

            if (len(select_result) == 0):
                u.log(f"Nenhum script a ser executado para a malha \"{mesh_id}\".")
                return

            select_result = select_result[0]
            u.log(f"Será feita a execução do script: {select_result[2]}")
            result = subprocess.run(['python', select_result[2]], capture_output=True, text=True)
            u.log_script_return(
                engine,
                metadata,
                select_result[0],
                select_result[1],
                select_result[3],
                result.returncode
            )

            if (result.returncode != 0):
                u.log(f"A execução do script retornou um código \"{result.returncode}\", portanto, será executado após: {sleep_minutes_after_not_success} minutos.")
                time.sleep(60*sleep_minutes_after_not_success)
            else:
                u.log("Execução bem sucedida. Avançando para o próximo script.")
            print()





from datetime import date, datetime

import sqlalchemy as sa



def log_script_return(engine: sa.Engine, metadata: sa.MetaData, ref: date, job_order: int, mesh_id: str, job_return: int) -> None:
    tb_liws_logs = sa.Table("TB_LIWS_LOGS", metadata, autoload_with = engine)
    with engine.begin() as connection:
        connection.execute(sa.insert(tb_liws_logs).values(
            REF = ref,
            JOB_ORDER = job_order,
            MESH_ID = mesh_id,
            JOB_RETURN = job_return,
            EXECUTION_TIME = datetime.now()
        ))
    



















CREATE TABLE [DB_IGE].[dbo].[TB_LIWS_JOBS] (
	[JOB_ORDER] [int] NOT NULL,
	[JOB_NAME] [varchar](128) NOT NULL,
	[JOB_PATH] [varchar](256) NOT NULL,
	[MESH_ID] [varchar](128) NOT NULL,
	[ATIVO] [bit] NOT NULL,
	PRIMARY KEY ([JOB_ORDER], [MESH_ID])
)

GO

INSERT INTO
	[DB_IGE].[dbo].[TB_LIWS_JOBS]
VALUES
	(1, 'run_pp_aaig_tb_5000_iv_invt_cadt', 'D:\10_outros\02_local_iws\meshs\pp_aaig_5000\run_pp_aaig_tb_5000_iv_invt_cadt.py', 'pp_aaig_5000', 1),
	(2, 'run_pp_aaig_exp_tb_5000_iv_invt_cadt', 'D:\10_outros\02_local_iws\meshs\pp_aaig_5000\run_pp_aaig_exp_tb_5000_iv_invt_cadt.py', 'pp_aaig_5000', 0),
	(3, 'run_pp_aaig_tb_5001_iv_invt_enc', 'D:\10_outros\02_local_iws\meshs\pp_aaig_5000\run_pp_aaig_tb_5001_iv_invt_enc.py', 'pp_aaig_5000', 1),
	(4, 'run_pp_aaig_exp_tb_5001_iv_invt_enc', 'D:\10_outros\02_local_iws\meshs\pp_aaig_5000\run_pp_aaig_exp_tb_5001_iv_invt_enc.py', 'pp_aaig_5000', 1)

GO

CREATE TABLE [DB_IGE].[dbo].[TB_LIWS_LOGS] (
	[REF] [date] NOT NULL,
	[JOB_ORDER] [int] NOT NULL,
	[MESH_ID] [varchar](128) NOT NULL,
	[JOB_RETURN] [int] NOT NULL,
	[EXECUTION_TIME] [datetime] NOT NULL
)

GO

INSERT INTO
	[DB_IGE].[dbo].[TB_LIWS_LOGS]
VALUES
	(
		DATEADD([day], -1, CAST(GETDATE() AS [date])),
		1,
		'pp_aaig_5000',
		0,
		GETDATE()
	),
	(
		DATEADD([day], -1, CAST(GETDATE() AS [date])),
		3,
		'pp_aaig_5000',
		0,
		GETDATE()
	),
	(
		DATEADD([day], -1, CAST(GETDATE() AS [date])),
		4,
		'pp_aaig_5000',
		0,
		GETDATE()
	),
	(
		CAST(GETDATE() AS [date]),
		1,
		'pp_aaig_5000',
		1,
		GETDATE()		
	)

GO

CREATE TABLE [DB_IGE].[dbo].[TB_LIWS_RETURN] (
	[ID] [int] NOT NULL PRIMARY KEY,
	[DESC] [varchar](256) NOT NULL,
	[CATEG] [varchar](256) NOT NULL
)

GO

INSERT INTO
	[DB_IGE].[dbo].[TB_LIWS_RETURN]
VALUES
	(0, 'Sucesso', 'Sucesso'),
	(1, 'Sem alteração de estado', 'Alerta'),
	(100, 'Erro genérico', 'Erro')

GO

CREATE VIEW [dbo].[VW_LIWS_JOBS] AS (

	SELECT
		SS0.[REF],
		SS0.[JOB_ORDER],
		SS0.[JOB_PATH],
		SS0.[MESH_ID]
	FROM (
		SELECT
			CAST(GETDATE() AS [date]) AS 'REF',
			ROW_NUMBER() OVER(
				PARTITION BY T0.[MESH_ID]
				ORDER BY T0.[JOB_ORDER]
			) AS 'ROW_NMBR',
			T0.[JOB_ORDER],
			T0.[JOB_PATH],
			T0.[MESH_ID]
		FROM
			[DB_IGE].[dbo].[TB_LIWS_JOBS] T0
		LEFT JOIN
			[DB_IGE].[dbo].[TB_LIWS_LOGS] T1 ON
			T0.[JOB_ORDER] = T1.[JOB_ORDER] AND
			T0.[MESH_ID] = T1.[MESH_ID] AND
			T1.[JOB_RETURN] = 0 AND
			T1.[REF] = CAST(GETDATE() AS [date])
		WHERE
			T0.[ATIVO] = 1 AND
			T1.[JOB_ORDER] IS NULL
	) SS0
	WHERE
		SS0.[ROW_NMBR] = 1

)

GO
