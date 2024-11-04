
import psycopg2
import pandas as pd

def establecer_conexion(database_name, postgres_pass, usuario, host="localhost", autocommit=False):
    """
    Establece una conexión a la base de datos PostgreSQL.

    Parámetros:
    - database_name (str): Nombre de la base de datos a la que se va a conectar.
    - postgres_pass (str): Contraseña del usuario de PostgreSQL.
    - usuario (str): Nombre de usuario de PostgreSQL.
    - host (str, opcional): Dirección del host donde se encuentra la base de datos. Por defecto es "localhost".
    - autocommit (bool, opcional): Si se debe habilitar el autocommit para la conexión. Por defecto es False.

    Retorna:
    - conn (psycopg2.connection): Objeto de conexión a la base de datos.
    """
    conn = psycopg2.connect(
        host=host,
        user=usuario,
        password=postgres_pass,
        database=database_name
    )
    conn.autocommit = autocommit
    return conn

def crear_bbdd(database_name):
    """
    Crea una base de datos en PostgreSQL si no existe.

    Parámetros:
    - database_name (str): Nombre de la base de datos que se desea crear.

    Este método establece una conexión a la base de datos `postgres`, verifica si la base de datos especificada ya existe, 
    y, si no es así, la crea. Utiliza la función `establecer_conexion` para establecer la conexión a la base de datos.

    Retorna:
    - None
    
    Notas:
    - Requiere privilegios de superusuario para crear la base de datos.
    - En caso de error, imprime el mensaje de error correspondiente.
    """
    try:
        conn = establecer_conexion("postgres", "admin", "postgres", autocommit=True)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
        bbdd_existe = cursor.fetchone()
    
        if not bbdd_existe:
            cursor.execute(f"CREATE DATABASE {database_name};")
            print(f"Base de datos {database_name} creada con éxito")
        else:
            print(f"La base de datos ya existe")
            
        cursor.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al crear la base de datos: {error}")

def insertar_datos(conn, df: pd.DataFrame, nombre_tabla):
    """
    Inserta los datos de un DataFrame en la tabla especificada de la base de datos PostgreSQL.

    Parámetros:
    - conn (psycopg2.connection): Objeto de conexión a la base de datos.
    - df (pd.DataFrame): DataFrame que contiene los datos que se desean insertar.
    - nombre_tabla (str): Nombre de la tabla en la que se insertarán los datos.

    La función inserta datos en varias tablas según el valor del parámetro `nombre_tabla`. Las tablas soportadas son:
    - `escuderias_historicas`: Inserta información sobre escuderías históricas.
    - `equipos_presente`: Inserta información sobre los equipos presentes, incluyendo datos de desempeño y antecedentes.
    - `datos_historicos`: Inserta información histórica relacionada con carreras y desempeño.
    - `mejores_pilotos`: Inserta información sobre los mejores pilotos de la historia.
    - `equipos_antecedentes`: Inserta relaciones entre equipos presentes y sus antecedentes históricos.

    La función utiliza un cursor para ejecutar las sentencias `INSERT` y cierra el cursor después de realizar las inserciones.

    Notas:
    - Se asume que las columnas del DataFrame coinciden con los campos de la tabla correspondiente.
    - En caso de que `nombre_tabla` no coincida con ninguna tabla soportada, no se realiza ninguna inserción.
    """
    cursor = conn.cursor()
    for index, row in df.iterrows():
        if nombre_tabla == "escuderias_historicas":
            cursor.execute("""
                INSERT INTO escuderias_historicas (nombre, nacionalidad, duracion)
                VALUES (%s, %s, %s)
            """, (row["nombre"], row["nacionalidad"], row["duracion"]))
        elif nombre_tabla == "equipos_presente":
            query = """
                INSERT INTO equipos_presente (nombre,
                motor,
                base,
                nacionalidad,
                duracion,
                carreras_ingresadas,
                carreras_empezadas,
                pilotos_totales,
                total_inscripciones,
                victorias,
                puntos_totales,
                poles,
                vueltas_rapidas,
                podios,
                titulos_constructores,
                titulos_pilotos,
                escuderias_antecedentes
            )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row["nombre"], row["motor"], row["base"], row["nacionalidad"], row["duracion"],
                row["carreras_ingresadas"], row["carreras_empezadas"], row["pilotos_totales"],
                row["total_inscripciones"], row["victorias"], row["puntos_totales"], row["cantidad_poles"],
                row["vueltas_rapidas"], row["cantidad_podiums"], row["titulos_constructores"], row["titulos_pilotos"],
                row["anteriores_equipos"]
            ))
        elif nombre_tabla == "datos_historicos":
            query = """
                INSERT INTO datos_historicos (carreras_inscritas,
                carreras_empezadas,
                pilotos_totales,
                total_inscripciones,
                victorias,
                puntos_totales,
                cantidad_poles,
                vueltas_rapidas,
                podios,
                titulos_constructores,
                titulos_pilotos)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row["carreras_inscritas"], row["carreras_empezadas"], row["pilotos_totales"],
                row["total_inscripciones"], row["victorias"], row["puntos_totales"], row["cantidad_poles"],
                row["vueltas_rapidas"], row["podios"], row["titulos_constructores"], row["titulos_pilotos"]
            ))
        elif nombre_tabla == "mejores_pilotos":
            query = """
                INSERT INTO mejores_pilotos (nombre,
                temporada,
                equipo,
                puntos_totales_constructor,
                total_carreras,
                victorias,
                podios,
                puntos,
                promedio_posicion_carrera,
                promedio_posicion_clasificacion,
                poles,
                cantidad_dnf,
                promedio_puntos,
                titulo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row["nombre"], row["temporada"], row["equipo"], row["puntos_totales_constructor"],
                row["total_carreras"], row["victorias"], row["cantidad_podiums"], row["puntos"],
                row["promedio_posicion_carrera"], row["promedio_posicion_clasificacion"],
                row["poles"], row["cantidad_dnf"], row["promedio_puntos"], row["titulo"]
            ))
        elif nombre_tabla == "equipos_antecedentes":
                query = """
                    INSERT INTO equipos_antecedentes (id_equipo_presente, id_escuderia_historica)
                    VALUES (%s, %s)
                """
                cursor.execute(query, (row["id_equipo_presente"], row["id_escuderia_historica"]))
    
    cursor.close()

        
def crear_tablas(database_name):
    """
    Crea las tablas necesarias en la base de datos PostgreSQL para el proyecto de análisis de Fórmula 1.

    Parámetros:
    - database_name (str): Nombre de la base de datos en la que se desean crear las tablas.

    Las tablas que se crean son:
    - **escuderias_historicas**: Almacena información sobre escuderías históricas, incluyendo nombre, nacionalidad y duración.
    - **equipos_presente**: Almacena datos sobre los equipos presentes en la Fórmula 1, incluyendo información sobre motor, base, nacionalidad y estadísticas de carreras.
    - **datos_historicos**: Almacena información histórica relacionada con carreras, victorias, poles y campeonatos de los equipos históricos.
    - **mejores_pilotos**: Almacena datos sobre los pilotos más destacados de la historia de la Fórmula 1, incluyendo información sobre temporadas, victorias, podios y estadísticas de desempeño.

    La función establece una conexión a la base de datos, ejecuta las consultas SQL necesarias para crear las tablas, y luego cierra la conexión.

    Notas:
    - Si alguna de las tablas ya existe, la consulta `CREATE TABLE IF NOT EXISTS` se asegura de no duplicarla.
    - En caso de error, se imprime un mensaje descriptivo del error.
    """
    try:
        conn = establecer_conexion(database_name, "admin", "postgres")
        cursor = conn.cursor()
        
        query_escuderias_historicas = """
            CREATE TABLE IF NOT EXISTS escuderias_historicas (
                id_escuderia SERIAL PRIMARY KEY,
                nombre VARCHAR(200),
                nacionalidad VARCHAR(200),
                duracion VARCHAR(200) 
            );
        """
        cursor.execute(query_escuderias_historicas)

        query_escuderias_presente = """
            CREATE TABLE IF NOT EXISTS equipos_presente (
                id_equipo_presente SERIAL PRIMARY KEY,
                nombre VARCHAR(200),
                motor VARCHAR(200),
                base VARCHAR(200),
                nacionalidad VARCHAR(200),
                duracion VARCHAR(200),
                carreras_ingresadas INT,
                carreras_empezadas INT,
                pilotos_totales INT,
                total_inscripciones INT,
                victorias INT,
                puntos_totales INT,
                poles INT,
                vueltas_rapidas INT,
                podios INT,
                titulos_constructores INT,
                titulos_pilotos INT
            );
        """
        cursor.execute(query_escuderias_presente)

        query_datos_historicos = """
            CREATE TABLE IF NOT EXISTS datos_historicos (
                id_registro SERIAL PRIMARY KEY,
                carreras_inscritas INT,
                carreras_empezadas INT,
                pilotos_totales INT,
                total_inscripciones INT,
                victorias INT,
                puntos_totales DECIMAL,
                cantidad_poles INT,
                vueltas_rapidas INT,
                podios INT,
                titulos_constructores DECIMAL,
                titulos_pilotos DECIMAL,
                escuderias_antecedentes VARCHAR(200)
            );
        """
        cursor.execute(query_datos_historicos)

        query_mejores_pilotos = """
            CREATE TABLE IF NOT EXISTS mejores_pilotos (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(200),
                temporada INT,
                equipo VARCHAR(200),
                puntos_totales_constructor DECIMAL,
                total_carreras INT,
                victorias INT,
                podios INT,
                puntos DECIMAL,
                promedio_posicion_carrera DECIMAL,
                promedio_posicion_clasificacion DECIMAL,
                poles DECIMAL,
                cantidad_dnf INT,
                promedio_puntos DECIMAL,
                titulo BOOLEAN
            );
        """
        cursor.execute(query_mejores_pilotos)

        conn.commit()
        print("Tables created successfully.")

        cursor.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al crear las tablas: {error}")

