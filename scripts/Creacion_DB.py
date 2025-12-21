# Archivo donde se crea la DB de PIXQUI-SENTINEL
import pandas as pd
# scripts/setup_db.py
from src.utils import inicializar_db

if __name__ == "__main__":
    print("🚀 Iniciando configuración de la base de datos nacional...")
    inicializar_db()
    print("✅ Base de datos nacional creada exitosamente como 'pixqui_sentinel.db'")