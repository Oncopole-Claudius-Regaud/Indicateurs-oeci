import hashlib
from datetime import datetime, date, time

# Convertir les objets datetime/date en texte pour qu’ils soient
# exploitables plus tard

def serialize(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    return obj

# Applique la fonction serialize sur toute une liste de dictionnaires


def make_serializable(data_list):
    return [
        {k: serialize(v) for k, v in item.items()}
        for item in data_list
    ]





def none_if_empty(v):
    """Transforme '' en None, laisse les autres valeurs intactes."""
    return None if (v is None or (isinstance(v, str) and v.strip() == "")) else v


def to_bool_or_none(v):
    """Convertit différentes représentations en bool ou None."""
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "t", "1", "y", "yes", "oui"}:
        return True
    if s in {"false", "f", "0", "n", "no", "non"}:
        return False
    return None
