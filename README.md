# PruebaIndiretail_GoldsteinAlex

Main:
program.py: Programa principal. Instancia las clases y levanta el archivo de propiedades con las configuraciones. Por cada tipo de accción posible, se ejecuta un método individual.
program_functions.py: Contiene las funciones propias del programa requerido.

Resources:
files.json: Este archivo tiene como objetivo ser la interfaz del programa con el usuario. Dicha persona puede agregar o eliminar acciones a realizar o puede agregar su propio archivo de propiedades y el programa al ser genérico va a realizar las acciones definidas.

Utils:
actions.py: Funciones de Spark para realizar transformaciones en Dataframes. 
spark_util.py: Funciones básicas de Spark para leer o escribir datos de distintas fuentes e instanciar el cliente Spark.
