from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

def Get_SparkSession():
    spark = (
        SparkSession.builder
        .appName("KafkaConsumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
        )
        .getOrCreate()
    )
    return spark

def Obtener_Datos_Stream(spark, topic: str) :
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    return df

def Desbinarizar_Datos(df):
    df = df.select(
        df["value"].cast("STRING")
    )
    return df

def Generacion_esquema():

    schema = StructType([
        StructField("usuario_id", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("email", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("fecha_registro", StringType(), True),
        StructField("evento", StringType(), True)
    ])
    return schema

def Transformar_Datos_Byn_Json(df):
    schema = Generacion_esquema()
    df = df.withColumn("json_data", F.from_json(F.col("value"), schema))
    df = df.select("json_data.*")
    return df

def Modelamiento_tipo_Datos(df):
    df_modelado = df.select(
        df["usuario_id"].cast("string").alias("usuario_id"),
        df["nombre"].cast("string").alias("nombre"),
        df["email"].cast("string").alias("email"),
        df["pais"].cast("string").alias("pais"),
        F.to_timestamp(df["fecha_registro"], "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("fecha_registro"),
        df["evento"].cast("string").alias("evento")
    )
    return df_modelado

def Almacenamiento_datos(df):
    output_path = "results/output_data"
    checkpoint_path = "results/checkpoint"
    query = (
        df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start(output_path)
    )
    query.awaitTermination()

if __name__ == "__main__":
    spark = Get_SparkSession()
    try:
        esquemas_datos = Obtener_Datos_Stream(spark, "actividad-topic")
        esquemas_datos = Desbinarizar_Datos(esquemas_datos)
        esquemas_datos = Transformar_Datos_Byn_Json(esquemas_datos)
        esquemas_datos = Modelamiento_tipo_Datos(esquemas_datos)
        esquemas_datos.printSchema()
        Almacenamiento_datos(esquemas_datos)
    except Exception as e:
        print(f"No se pudo conectar al topic: {e}")
    finally:
        spark.stop()