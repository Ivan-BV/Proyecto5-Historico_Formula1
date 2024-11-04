
# 📊 Proyecto5: Análisis historico sobre la Formula 1

## 📖 Descripción del Proyecto

Este proyecto tiene como objetivo realizar un análisis profundo sobre la historia de la Fórmula 1, centrándose en dos aspectos principales: la evolución de los equipos históricos y la comparación de los considerados mejores pilotos de la historia. Usando datos de diversas fuentes, hemos recopilado información sobre los equipos y pilotos para entender mejor la evolución de la competición y los factores que han influido en el éxito de los distintos participantes.

## 🎯 Objetivos del proyecto

Comparación de los Mejores Pilotos de la Historia: Evaluar a los pilotos que son considerados los mejores de todos los tiempos en la Fórmula 1, basándonos en diversas métricas, tales como títulos de campeonatos, número de victorias, podios, poles, puntos acumulados, entre otros.

Evolución de los Equipos Históricos: Analizar la trayectoria de los equipos que han tenido un impacto significativo en la historia de la Fórmula 1, desde sus inicios hasta la temporada actual. Esto incluye estudiar las victorias, campeonatos, desempeño en distintas épocas, y los cambios de escudería a lo largo de los años.

## 🗂️ Estructura del Proyecto

El proyecto está organizado de la siguiente manera:

```bash
├── datos/                # Conjuntos de datos sin procesar y ya procesados
│   ├── output/           # Datos procesados y resultados finales
│   └── raw/              # Datos en bruto (sin procesar)
│
├── notebooks/            # Notebooks con el contenido y análisis de datos
│
├── src/                  # Scripts para la limpieza y procesamiento de datos
│
├── README.md             # Descripción general del proyecto e instrucciones
└── requirements.txt      # Lista de dependencias del proyecto
```

## 🛠️ Instalación y Requisitos

Este proyecto utiliza [Python 3.12.7](https://docs.python.org/3.12/) y requiere las siguientes bibliotecas para la ejecución y análisis:

- [pandas 2.2.3](https://pandas.pydata.org/docs/)
- [matplotlib 3.9.2](https://matplotlib.org/stable/index.html)
- [seaborn 0.13.2](https://seaborn.pydata.org/tutorial.html)
- [beautifulsoup4 4.12.3](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [selenium 4.25.0](https://www.selenium.dev/documentation/)
- [psycopg2 2.9.10](https://www.psycopg.org/docs/)

Para instalar las dependencias, puedes ejecutar el siguiente comando dentro de un entorno virtual:

```bash
pip install -r requirements.txt
```

## 🔄 Próximos Pasos

- Implementar análisis de la evolución historica de las temporadas en la formula 1.
- Implentación de los datos almacenados en la bbdd.
- Implementar visualización.

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Si deseas colaborar en este proyecto, por favor abre un pull request o una issue en este repositorio.

## ✒️ Autores

Iván Bravo - Autor principal del proyecto.