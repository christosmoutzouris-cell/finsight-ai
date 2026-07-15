"""
Κοινές database utilities για όλα τα AI modules.
Διαβάζει symbols από watched_symbols αντί για hardcoded λίστες.
"""

import psycopg2

PG_CONN = {
    "host":     "finsight-postgres",
    "port":     5432,
    "user":     "finsight",
    "password": "finsight123",
    "dbname":   "finsight_db",
}


def get_connection():
    return psycopg2.connect(**PG_CONN)


def get_active_symbols() -> list[str]:
    """
    Επιστρέφει τα active symbols από τη βάση.
    Αν προσθέσεις νέο symbol στον πίνακα watched_symbols,
    όλο το pipeline το παίρνει αυτόματα.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol FROM watched_symbols
        WHERE is_active = TRUE
        ORDER BY symbol
    """)
    symbols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return symbols