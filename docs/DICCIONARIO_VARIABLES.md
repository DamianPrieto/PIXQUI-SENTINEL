# Diccionario de Variables - Proyecto PIXQUI
**Sistema de Vigilancia Epidemiológica y Modelado Predictivo**
**Autor:** Angel Damian Prieto López | UPIBI - IPN

Este documento define las variables críticas procesadas por el motor de PIXQUI para el análisis de egresos hospitalarios y detección de brotes epidemiológicos.

---

## 1. Variables de Identificación y Perfilamiento
| Variable | Tipo | Definición Técnica | Relevancia Analítica |
| :--- | :--- | :--- | :--- |
| **ID** | Number | Identificador único de registro | Evita duplicidad y garantiza integridad referencial. |
| **EDAD** | Number | Edad cronológica del paciente | Factor de riesgo primario y ajuste de tasas de mortalidad. |
| **SEXO** | Text | Identidad biológica (1-M, 2-F, 3-I, 4-NE) | Análisis de disparidad epidemiológica por género. |

---

## 2. Variables Clínicas y Antropométricas
| Variable | Tipo | Definición Técnica | Relevancia Analítica |
| :--- | :--- | :--- | :--- |
| **AFEC** | Text | Código CIE-10 (Diagnóstico principal) | Eje central para el filtrado de patologías (ej. ECV). |
| **PESO** | Float | Masa corporal en kilogramos | Dato base para evaluación metabólica. |
| **TALLA** | Number | Estatura en centímetros | Dato base para evaluación de desarrollo y nutrición. |
| **IMC** | Float | Índice de Masa Corporal (Calculada) | Predictor de riesgo cardiovascular. $IMC = \frac{peso(kg)}{talla(m)^2}$ |

---

## 3. Inteligencia Geográfica y Operativa
| Variable | Tipo | Definición Técnica | Relevancia Analítica |
| :--- | :--- | :--- | :--- |
| **CLUES** | Text | Clave de Establecimiento de Salud | Mapeo de capacidad instalada y carga hospitalaria. |
| **GEO_CODE** | Text | Código único (Entidad + Mun + Loc) | Habilita el análisis de clústeres y georreferenciación. |
| **PROMED** | Text | Código de procedimiento (4 dígitos) | Clasificación de la complejidad de la intervención. |
| **TIPO** | Text | Procedencia del ingreso (1 al 10) | Identifica urgencias vs. procedimientos programados. |

---

## 4. Indicadores de Resultado
| Variable | Tipo | Definición Técnica | Relevancia Analítica |
| :--- | :--- | :--- | :--- |
| **EGRESO** | Date | Fecha de alta hospitalaria | Análisis de temporalidad, estacionalidad y tendencias. |
| **MOTEGRE** | Text | Motivo de egreso (6 = Defunción) | Variable dependiente para modelos de riesgo de mortalidad. |

---
*Documento generado para el protocolo de investigación PIXQUI - Diciembre 2025.*