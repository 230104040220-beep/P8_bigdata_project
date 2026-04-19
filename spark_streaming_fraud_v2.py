
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit, when, base64
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Inisialisasi Spark Session
# Membangun sesi Spark dengan nama aplikasi 'Fraud Detection'
spark = SparkSession.builder \
    .appName("Fraud Detection") \
    .getOrCreate()

# Mengatur level log agar tidak terlalu ramai di terminal
spark.sparkContext.setLogLevel("WARN")

# 2. Membaca stream data dari Kafka
# Menghubungkan ke broker Kafka lokal pada topic 'bank_topic'
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_topic") \
    .load()

# 3. Definisi Schema Data Transaksi
# Sesuaikan dengan struktur JSON yang dikirim oleh Kafka Producer
schema = StructType([
    StructField("nama", StringType()),
    StructField("rekening", StringType()),
    StructField("jumlah", IntegerType()),
    StructField("lokasi", StringType())
])

# 4. Parsing data JSON dari Kafka
# Mengubah kolom 'value' dari Kafka (binary) menjadi string, lalu parsing ke kolom-kolom schema
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. DATA MASKING (Keamanan Privasi)
# Menyembunyikan nomor rekening dan hanya menampilkan 2 angka terakhir (contoh: ****99)
df = df.withColumn("rekening_masked", 
                   concat(lit("****"), col("rekening").substr(-2, 2)))

# 6. FRAUD DETECTION (Logika Bisnis)
# Menandai transaksi sebagai FRAUD jika jumlah > 50jt atau lokasi di 'Luar Negeri'
df = df.withColumn("status", 
                   when(col("jumlah") > 50000000, "FRAUD")
                   .when(col("lokasi") == "Luar Negeri", "FRAUD")
                   .otherwise("NORMAL"))

# 7. ENCRYPTION (Keamanan Data)
# Mengenkripsi kolom 'jumlah' menggunakan Base64 untuk simulasi keamanan data
df = df.withColumn("jumlah_encrypted", 
                   base64(col("jumlah").cast("string")))

# 8. Menulis Output ke Folder Parquet (Sink)
# Data disimpan secara real-time ke folder 'realtime_output' agar bisa dibaca Streamlit
# Bagian akhir di scripts/spark_streaming_fraud_v2.py
query = df.writeStream \
    .format("parquet") \
    .option("path", "stream_data/realtime_output") \
    .option("checkpointLocation", "data/checkpoints") \
    .outputMode("append") \
    .start()

print("Spark Streaming aktif dan sedang memproses data...")

# Menjaga agar aplikasi streaming tetap berjalan
query.awaitTermination()
