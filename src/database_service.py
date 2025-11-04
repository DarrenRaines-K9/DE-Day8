# Database Service - SQL interactions for MSSQL (source) and PostgreSQL (target)

from sqlalchemy import create_engine, select
import models as m
import pandas as pd

# MSSQL (Azure SQL Database) Configuration - SOURCE database
USERNAME = "devadmin"
PASSWORD = "Bootcamp1433!"
HOST = "de-nss-bootcamp.database.windows.net"
PORT = 1433
DATABASE = "de_bootcamp"
DRIVER = "pymssql"


def get_student_score_info():
    # Extracts and joins student scores and departments from Azure SQL (EXTRACT phase)
    # Create database connection engine
    engine = create_engine(
        f"mssql+pymssql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )

    query = select(
        m.StudentScore.student_id,
        m.StudentScore.score,
        m.StudentDepartment.home_department,
    ).join(
        m.StudentDepartment, m.StudentScore.student_id == m.StudentDepartment.student_id
    )

    # Execute query and load results into DataFrame
    results = pd.read_sql(query, engine)
    return results


# PostgreSQL (Local Database) Configuration - TARGET database
DB_USER = "myuser"
DB_PASSWORD = "mypassword"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "de"


def push_student_info(student_data):
    # Loads transformed student data to PostgreSQL silver.students table (LOAD phase)
    # Create PostgreSQL connection engine
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    table_name = "students"
    schema_name = "silver"

    student_data.to_sql(
        table_name,
        engine,
        schema=schema_name,
        if_exists="replace",
        index=False
    )
