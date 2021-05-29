
import statistics
import sys

#LIBRERIAS
sys.path.append('/usr/lib/python3/dist-packages')
import requests
import json
from datetime import date
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Proyecto final AlmacenamientoParalelo').getOrCreate()
sc = spark.sparkContext

def main():
    run = True
    while run:
        print("Obtener datos de sensores") #OBTIENE DATOS DE LOS SENSORES
        url = "http://45.55.52.222:8080/api/data" ??''
        response = requests.get(url)
        data = response.text
        #convierte a tipo json
        parsed = json.loads(data)
        rdd = sc.parallelize(parsed)
        rdd = spark.read.json(rdd)
        rdd.show()
        rdd.printSchema()

        #BD TABLA DE INFORMACION#
        rdd.createOrReplaceTempView("Informacion")
        temp = spark.sql("SELECT AVG(temperatura) AS Prom_STemperatura FROM informacion WHERE fecha BETWEEN '2021-05-28' AND '2021-05-28'")
        humedad = spark.sql("SELECT AVG(humedad) AS Prom_SHumedad FROM informacion WHERE fecha BETWEEN '2021-05-28' AND '2021-05-28'")
        sonico = spark.sql("SELECT AVG(sonico) AS Prom_SUltrasonico FROM informacion WHERE fecha BETWEEN '2021-05-28' AND '2021-05-28'")
        temp.show()
        humedad.show()
        sonico.show()
        R = input("Fin")
        if R != 1:
            run = False


if __name__ == '__main__':
    main()
#FIN DEL PROGRAMA