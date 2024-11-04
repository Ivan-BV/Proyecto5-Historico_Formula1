
import pandas as pd
import matplotlib.pyplot as plt
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def obtener_historicos(pilotos):
    """
    Obtiene el rendimiento histórico de una lista de pilotos de Fórmula 1 utilizando la API de Ergast.

    Parámetros:
    - pilotos (list): Lista de identificadores de los pilotos (str) para los cuales se desea obtener el rendimiento histórico.

    Para cada piloto en la lista, la función realiza lo siguiente:
    - Obtiene todas las temporadas en las que participó.
    - Para cada temporada, obtiene los resultados de las carreras, incluyendo:
        - Número de victorias, podios, poles, y retiros (DNF).
        - Posiciones promedio en carrera y en clasificación.
        - Puntos totales obtenidos y promedio de puntos por carrera.
        - Información del equipo constructor principal y si el piloto ganó el campeonato.

    Retorna:
    - pd.DataFrame: Un DataFrame con el rendimiento de los pilotos. Las columnas incluyen:
        - "piloto": Nombre del piloto.
        - "temporada": Temporada específica (año).
        - "equipo": Nombre del equipo constructor principal.
        - "puntos_totales_constructor": Puntos totales del equipo constructor.
        - "total_carreras": Número total de carreras disputadas.
        - "victorias": Número de victorias.
        - "podios": Número de podios.
        - "puntos": Puntos totales obtenidos por el piloto.
        - "promedio_posicion_carrera": Posición promedio en las carreras.
        - "promedio_posicion_clasificacion": Posición promedio en clasificación.
        - "poles": Número de poles.
        - "cantidad_dnf": Cantidad de retiros (DNF).
        - "promedio_puntos": Promedio de puntos por carrera.
        - "titulo": Indica si el piloto ganó el campeonato en esa temporada (True/False).

    Notas:
    - La función utiliza múltiples llamadas a la API de Ergast para obtener los datos de cada piloto y temporada.
    - Utiliza `tqdm` para mostrar el progreso de la descarga de datos.
    - Requiere las funciones auxiliares `obtener_campeon` y `obtener_puntos_constructor` para determinar si un piloto fue campeón y los puntos del constructor.

    """

    rendimiento_pilotos = []
    
    for piloto in tqdm(pilotos):
        url = f"https://ergast.com/api/f1/drivers/{piloto}/seasons.json"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            for season in data["MRData"]["SeasonTable"]["Seasons"]:
                temporada = season["season"]
                url_stats = f"https://ergast.com/api/f1/{temporada}/drivers/{piloto}/results.json"
                response_stats = requests.get(url_stats)
                if response_stats.status_code == 200:
                    data_stats = response_stats.json()
                    victorias = 0
                    podios = 0
                    puntos = 0
                    retiros = 0
                    pole_positions = 0
                    posicion_media_clasificacion = []
                    posicion_media_carrera = []
                    total_carreras = len(data_stats["MRData"]["RaceTable"]["Races"])
                    constructor_principal = None
                    for race in data_stats["MRData"]["RaceTable"]["Races"]:
                        if "Results" in race and len(race["Results"]) > 0:
                            resultado = race["Results"][0]
                            posicion = int(resultado["position"])
                            posicion_clasificacion = int(resultado.get("grid", 0))
                            if resultado["position"] == "1":
                                victorias += 1
                            if int(resultado["position"]) <= 3:
                                podios += 1
                            if posicion_clasificacion == 1:
                                pole_positions += 1
                            if resultado.get("positionText", "") == "R":
                                retiros += 1
                            puntos += float(resultado["points"])
                            posicion_media_carrera.append(posicion)
                            posicion_media_clasificacion.append(posicion_clasificacion)
                            if constructor_principal is None:
                                constructor_principal = resultado["Constructor"]["name"]
                    promedio_posicion_carrera = round(sum(posicion_media_carrera) / len(posicion_media_carrera), 2) if posicion_media_carrera else 0
                    promedio_posicion_clasificacion = round(sum(posicion_media_clasificacion) / len(posicion_media_clasificacion), 2) if posicion_media_clasificacion else 0
                    promedio_puntos = round(puntos / total_carreras, 2) if total_carreras > 0 else 0

                    url_campeon = f"https://ergast.com/api/f1/{temporada}/driverStandings/1.json"
                    es_campeon = obtener_campeon(url_campeon, piloto)

                    url_constructor = f"https://ergast.com/api/f1/{temporada}/constructors/{constructor_principal.lower().replace(' ', '_')}/constructorStandings.json"
                    puntos_totales_constructor = obtener_puntos_constructor(url_constructor)

                    rendimiento_pilotos.append([
                        piloto,
                        temporada,
                        constructor_principal,
                        puntos_totales_constructor,
                        total_carreras,
                        victorias,
                        podios,
                        puntos,
                        promedio_posicion_carrera,
                        promedio_posicion_clasificacion,
                        pole_positions,
                        retiros,
                        promedio_puntos,
                        es_campeon
                    ])

    return pd.DataFrame(rendimiento_pilotos, columns=["piloto", "temporada", "equipo", "puntos_totales_constructor","total_carreras", "victorias", "podios", "puntos", "promedio_posicion_carrera", "promedio_posicion_clasificacion", "poles", "cantidad_dnf", "promedio_puntos", "titulo"])


def obtener_campeon(url_campeon, piloto):
    """
    Verifica si el piloto especificado fue el campeón de la temporada correspondiente.

    Parámetros:
    - url_campeon (str): URL de la API de Ergast para obtener la información del campeón de la temporada.
    - piloto (str): Identificador del piloto que se desea verificar si ganó el campeonato.

    La función realiza una petición HTTP a la URL proporcionada y verifica si el `driverId` del campeón coincide con el `piloto` especificado.

    Retorna:
    - bool: Retorna `True` si el piloto fue el campeón de la temporada, de lo contrario `False`.

    Notas:
    - La función asume que la respuesta de la API contiene información sobre la clasificación del piloto en el primer lugar.
    - Si no hay información de la clasificación, la función retorna `False`.
    """
    response_champion = requests.get(url_campeon)
    if response_champion.status_code == 200:
        data_champion = response_champion.json()
        if data_champion["MRData"]["StandingsTable"]["StandingsLists"]:
            champion = data_champion["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"][0]["Driver"]["driverId"]
            if champion == piloto:
                return True
            else:
                return False

def obtener_puntos_constructor(url_constructor):
    """
    Obtiene los puntos totales del constructor para una temporada específica utilizando la API de Ergast.

    Parámetros:
    - url_constructor (str): URL de la API de Ergast para obtener la clasificación del constructor de la temporada correspondiente.

    La función realiza una petición HTTP a la URL proporcionada y extrae los puntos totales obtenidos por el constructor en esa temporada.

    Retorna:
    - float: Puntos totales del constructor en la temporada correspondiente. Si no se encuentran datos, retorna 0.

    Notas:
    - La función asume que la respuesta de la API contiene la clasificación del constructor en la temporada.
    - Si no hay información sobre la clasificación, la función retornará 0.
    """
    response_constructor = requests.get(url_constructor)
    puntos_totales_constructor = 0
    if response_constructor.status_code == 200:
        data_constructor = response_constructor.json()
        if data_constructor['MRData']['StandingsTable']['StandingsLists']:
            puntos_totales_constructor = float(data_constructor['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings'][0]['points'])
    return puntos_totales_constructor

def formatear_datos_historicos(df_rendimiento: pd.DataFrame, pilotos_historicos: list):
    """
    Formatea los nombres de los pilotos en el DataFrame de rendimiento histórico, utilizando una lista de nombres históricos para hacer la correspondencia.

    Parámetros:
    - df_rendimiento (pd.DataFrame): DataFrame que contiene información sobre el rendimiento de los pilotos.
      Debe incluir una columna llamada "piloto" con los identificadores de los pilotos.
    - pilotos_historicos (list): Lista de nombres completos de los pilotos históricos.

    La función realiza un mapeo de los apellidos de los pilotos a sus nombres completos utilizando la lista de `pilotos_historicos`.
    Reemplaza los identificadores en la columna "piloto" del DataFrame con el nombre completo correspondiente.

    Retorna:
    - pd.DataFrame: DataFrame actualizado con los nombres completos de los pilotos en la columna "piloto".

    Notas:
    - La función utiliza el apellido del piloto como clave para buscar su nombre completo en `pilotos_historicos`.
    - Si no se encuentra una correspondencia, se mantiene el valor original del identificador del piloto.
    """
    nombre_mapeo = {piloto.split()[-1].lower(): piloto for piloto in pilotos_historicos}
    df_rendimiento["piloto"] = df_rendimiento["piloto"].apply(lambda x: nombre_mapeo.get(str(x).split("_")[-1], x))
    return df_rendimiento

# API 2
def obtener_historial_escuderias(url):
    """
    Obtiene el historial de escuderías de Fórmula 1 desde una API y devuelve la información en un DataFrame.

    Parámetros:
    - url (str): URL de la API desde la cual se desea obtener el historial de escuderías.

    La función realiza una petición HTTP a la URL proporcionada y obtiene información sobre las escuderías,
    incluyendo el identificador del constructor, nombre y nacionalidad.

    Retorna:
    - pd.DataFrame: DataFrame con la información de las escuderías. Las columnas incluyen:
        - "constructor_id": Identificador del constructor.
        - "nombre": Nombre de la escudería.
        - "nacionalidad": País de origen de la escudería.

    Notas:
    - La función envía una cabecera con la autorización que incluye una API key. Debes reemplazar `"YOUR_API_KEY"` con una clave válida.
    - Si la respuesta de la API no tiene un código de estado 200, se retorna un DataFrame vacío.
    """
    historial_escuderias = []
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer YOUR_API_KEY"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        for team in data.get("teams", []):
            constructor_id = team.get("id")
            nombre = team.get("name")
            nacionalidad = team.get("country")

            historial_escuderias.append([
                constructor_id,
                nombre,
                nacionalidad
            ])

    df_escuderias = pd.DataFrame(historial_escuderias, columns=["constructor_id", "nombre", "nacionalidad"])
    return df_escuderias

# Web scraping 1
def obtener_equipos_historicos_wikipedia(url):
    """
    Obtiene información sobre equipos históricos de Fórmula 1 desde una tabla de Wikipedia y la devuelve en un DataFrame.

    Parámetros:
    - url (str): URL de la página de Wikipedia desde la cual se desea extraer la información sobre los equipos históricos.

    La función realiza una petición HTTP a la URL proporcionada y analiza el contenido HTML utilizando BeautifulSoup para extraer 
    una tabla con información sobre equipos históricos. Luego, limpia los datos eliminando referencias y ajusta ciertos valores 
    para crear un DataFrame con la información obtenida.

    Retorna:
    - pd.DataFrame: DataFrame con la información de los equipos históricos. Las columnas incluyen:
        - "nombre": Nombre del equipo.
        - "nacionalidad": País de origen del equipo.
        - "duracion": Duración de participación en la Fórmula 1.
        - "carreras_inscritas": Número de carreras en las que el equipo se inscribió.
        - "carreras_empezadas": Número de carreras en las que el equipo empezó.
        - "pilotos_totales": Número total de pilotos que compitieron con el equipo.
        - "total_inscripciones": Número total de inscripciones.
        - "victorias": Número de victorias.
        - "puntos_totales": Puntos totales acumulados.
        - "cantidad_poles": Número de poles conseguidas.
        - "vueltas_rapidas": Número de vueltas rápidas.
        - "podios": Número de podios conseguidos.
        - "titulos_constructores": Número de títulos de constructores ganados.
        - "titulos_pilotos": Número de títulos de pilotos ganados.

    Notas:
    - La función asume que la tabla relevante en la página de Wikipedia es la segunda tabla de clase `"wikitable"`.
    - Utiliza expresiones regulares para limpiar las referencias de texto (por ejemplo, "[1]").
    - Si la página no se encuentra o no tiene la estructura esperada, se retorna un DataFrame vacío.
    """
    response = requests.get(url)
    equipos_historicos = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        tables = soup.find_all("table", class_="wikitable")
        patron_regex = r"\[.*?\]"
        if len(tables) > 1:
            rows = tables[1].find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                resultados = []
                if len(cols) > 1:
                    for n in range(len(cols)):
                        elemento = re.sub(patron_regex, "", cols[n].get_text(strip=True))
                        if elemento == "n/a":
                            elemento = pd.NA
                        resultados.append(elemento)
                    equipos_historicos.append(resultados)

    df_equipos_historicos = pd.DataFrame(equipos_historicos, columns=[
        "nombre", "nacionalidad", "duracion", "carreras_inscritas", "carreras_empezadas", "pilotos_totales", "total_inscripciones",
        "victorias", "puntos_totales", "cantidad_poles", "vueltas_rapidas", "podios", "titulos_constructores", "titulos_pilotos"])
    return df_equipos_historicos

def obtener_equipos_presentes_wikipedia(url):
    """
    Obtiene información sobre los equipos presentes de Fórmula 1 desde una tabla de Wikipedia y la devuelve en un DataFrame.

    Parámetros:
    - url (str): URL de la página de Wikipedia desde la cual se desea extraer la información sobre los equipos presentes.

    La función realiza una petición HTTP a la URL proporcionada y analiza el contenido HTML utilizando BeautifulSoup para extraer 
    una tabla con información sobre los equipos presentes. Luego, limpia los datos eliminando referencias, caracteres especiales y 
    ajusta ciertos valores para crear un DataFrame con la información obtenida.

    Retorna:
    - pd.DataFrame: DataFrame con la información de los equipos presentes. Las columnas incluyen:
        - "nombre": Nombre del equipo.
        - "motor": Motor utilizado por el equipo.
        - "nacionalidad": País de origen del equipo.
        - "base": Base de operaciones del equipo.
        - "duracion": Duración de participación en la Fórmula 1.
        - "carreras_ingresadas": Número de carreras en las que el equipo se inscribió.
        - "carreras_empezadas": Número de carreras en las que el equipo empezó.
        - "pilotos_totales": Número total de pilotos que compitieron con el equipo.
        - "total_inscripciones": Número total de inscripciones.
        - "victorias": Número de victorias.
        - "puntos_totales": Puntos totales acumulados.
        - "cantidad_poles": Número de poles conseguidas.
        - "vueltas_rapidas": Número de vueltas rápidas.
        - "cantidad_podiums": Número de podios conseguidos.
        - "titulos_constructores": Número de títulos de constructores ganados.
        - "titulos_pilotos": Número de títulos de pilotos ganados.
        - "anteriores_equipos": Equipos anteriores relacionados con el equipo actual.

    Notas:
    - La función asume que la tabla relevante en la página de Wikipedia es la primera tabla de clase `"wikitable"`.
    - Utiliza expresiones regulares para limpiar las referencias de texto (por ejemplo, "[1]").
    - Reemplaza "—" con "Sin equipos antecedentes" en la columna de equipos anteriores.
    - Si la página no se encuentra o no tiene la estructura esperada, se retorna un DataFrame vacío.
    """
    response = requests.get(url)
    equipos_presentes = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        tables = soup.find_all("table", class_="wikitable")
        patron_regex = r"\[.*?\]"
        if len(tables) > 1:
            rows = tables[0].find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                resultados = []
                if len(cols) > 1:
                    for n in range(len(cols)):
                        elemento = re.sub(patron_regex, "", cols[n].get_text(strip=True))
                        elemento = elemento.replace("/","")
                        if elemento == "—":
                            elemento = "Sin equipos antecedentes"
                        resultados.append(elemento)
                    equipos_presentes.append(resultados)

    df_equipos_presentes = pd.DataFrame(equipos_presentes, columns=[
        "nombre", "motor", "nacionalidad", "base", "duracion", "carreras_ingresadas", "carreras_empezadas", "pilotos_totales",
        "total_inscripciones", "victorias", "puntos_totales", "cantidad_poles", "vueltas_rapidas", "cantidad_podiums", "titulos_constructores", "titulos_pilotos", "anteriores_equipos"
    ])
    return df_equipos_presentes

# Web scraping 2
def obtener_datos_audiencia_liberty_media():
    chrome_options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": r"C:/Bravo/Hackio/Modulo5/Proyecto/Proyecto5-Historico_Formula1/datos/raw",
        "download.prompt_for_download": False,
        "directory_upgrade": True,
    }

    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=chrome_options)
    url = "https://www.libertymedia.com/investors/news-events/press-releases"
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "press-release-item")))
        enlaces = driver.find_elements(By.CLASS_NAME, "press-release-item")
        audiencia_data = []

        for enlace in enlaces:
            try:
                enlace.click()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))
                parrafos = driver.find_elements(By.TAG_NAME, "p")
                for parrafo in parrafos:
                    texto = parrafo.text
                    if "audience" in texto.lower() or "viewership" in texto.lower() or "rating" in texto.lower():
                        audiencia_data.append([texto])
                driver.back()
            except Exception as e:
                print(f"Error al procesar un enlace: {e}")
                driver.back()

        df_audiencia = pd.DataFrame(audiencia_data, columns=["descripcion"])
    finally:
        driver.quit()

    return df_audiencia
