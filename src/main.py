# Main ETL Pipeline Orchestrator - Extracts from Azure SQL/MinIO, transforms, and loads to PostgreSQL

import database_service as ds
import storage_service as ss
import data_cleaning as dc


def main():
    # Main ETL pipeline - extracts, transforms, and loads student data
    # EXTRACT PHASE
    student_data = ds.get_student_score_info()
    student_demographics = ss.get_students()

    # TRANSFORM PHASE
    students = dc.combine_student_data(student_data, student_demographics)
    students_transformed = dc.process_student_data(students)

    # LOAD PHASE
    ds.push_student_info(students_transformed)


# Entry point
if __name__ == "__main__":
    main()
