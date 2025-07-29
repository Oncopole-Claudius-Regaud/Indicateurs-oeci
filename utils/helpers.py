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
