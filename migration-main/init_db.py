from app.config import get_connection, get_mapping_rule_table, get_result_table
from app.logger import logger


def init_db():
    """
    Validate Oracle connectivity and confirm both input/result tables are reachable.

    This project assumes the Oracle schema already exists.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        mapping_table = get_mapping_rule_table()
        result_table = get_result_table()

        cursor.execute(f"SELECT COUNT(*) FROM {mapping_table}")
        mapping_count = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM {result_table}")
        result_count = cursor.fetchone()[0]

        logger.info(f"{mapping_table} is reachable. Current row count: {mapping_count}")
        logger.info(f"{result_table} is reachable. Current row count: {result_count}")


if __name__ == "__main__":
    init_db()
