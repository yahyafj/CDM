from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Calcul de moyenne") \
    .getOrCreate()

# Créer un DataFrame à partir d'une liste de tuples
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["Name", "Value"])

# Afficher le DataFrame
df.show()

# Calculer la moyenne de la colonne "Value"
average_value = df.select(avg("Value")).collect()[0][0]
print("Moyenne des valeurs:", average_value)

# Arrêter la session Spark
spark.stop()
