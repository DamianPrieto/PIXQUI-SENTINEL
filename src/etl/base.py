from abc import ABC, abstractmethod
import pandas as pd

class CargadorBase(ABC):
    """
    Clase Abstracta (Plantilla).
    Define la estructura que deben tener TODOS los cargadores de datos.
    """
    
    def __init__(self, anio):
        self.anio = anio
        # Inicializamos el df vacío para evitar errores de "variable no definida"
        self.df = pd.DataFrame()

    @abstractmethod
    def extraer(self):
        """
        MÉTODO ABSTRACTO: No tiene código aquí.
        Obliga a las clases hijas a definir de dónde sacan el archivo (CSV, Excel, etc).
        """
        pass

    @abstractmethod
    def transformar(self):
        """
        MÉTODO ABSTRACTO:
        Obliga a las clases hijas a definir su propia limpieza específica.
        """
        pass

    def procesar(self):
        """
        MÉTODO MAESTRO (Template Method Pattern):
        Este es el único método que llamará el main.py.
        Orquesta los pasos en orden lógico.
        """
        print(f"[ETL] Iniciando proceso para {self.anio}...")
        
        # 1. Ejecutar la extracción definida por el hijo
        self.extraer()
        
        # 2. Si hay datos, ejecutar la transformación definida por el hijo
        if not self.df.empty:
            self.transformar()
            print(f"Transformación terminada. Filas listas: {len(self.df)}")
            return self.df
        else:
            print(f"No se encontraron datos para procesar en {self.anio}.")
            return pd.DataFrame() # Retorna vacío para no romper el programa