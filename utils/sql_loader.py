import os

def load_sql(filename: str) -> str:
    """
    Charge le contenu d'un fichier SQL situé dans le dossier oeci/sql/
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(base_dir, "..", "sql", filename)

    # Correction ici : si on est dans omop/utils/, alors base_dir doit pointer sur oeci/
    if not os.path.exists(sql_path):
        sql_path = os.path.join(base_dir, "..", "..", "oeci", "sql", filename)

    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()
