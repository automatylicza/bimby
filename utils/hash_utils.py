# utils/hash_utils.py

import hashlib

def calculate_hash(data: bytes, algorithm: str = 'sha256') -> str:
    """
    Oblicza hash danych przy użyciu wybranego algorytmu.
    Domyślnie używa SHA-256.
    """
    hash_func = hashlib.new(algorithm)
    hash_func.update(data)
    return hash_func.hexdigest()
