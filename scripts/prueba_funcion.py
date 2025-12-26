# Prueba de concepto
from src.database.connection import db

def prueba_1():
    print("--- Test 1: Crear Tabla ---")
    db.ejecutar_query("CREATE TABLE IF NOT EXISTS prueba_x (id INTEGER, dato VARCHAR)")
    print("Tabla creada.")

def prueba_2():
    print("--- Test 2: Insertar Dato ---")
    # Fíjate que NO creamos una nueva conexión. Usamos la misma 'db'.
    db.ejecutar_query("INSERT INTO prueba_x VALUES (1, 'Funcionó el Singleton')")
    print("Dato insertado.")

def prueba_3():
    print("--- Test 3: Leer Dato ---")
    # .df() convierte el resultado a Pandas
    df = db.ejecutar_query("SELECT * FROM prueba_x").df()
    print(df)

if __name__ == "__main__":
    prueba_1()
    prueba_2()
    prueba_3()
    db.cerrar()