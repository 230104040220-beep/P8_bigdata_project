🏦 Sistem Deteksi Fraud Real-time dengan Apache Kafka & Spark Streaming
Proyek ini mendemonstrasikan pipeline Big Data sederhana untuk mendeteksi transaksi perbankan yang mencurigakan secara real-time. Sistem ini mengintegrasikan pengiriman data, pemrosesan streaming, hingga visualisasi dashboard.
🚀 Arsitektur Sistem
1.	Apache Kafka: Sebagai message broker untuk menampung aliran data transaksi.
2.	Apache Spark Streaming: Sebagai mesin pemroses data untuk melakukan masking, enkripsi, dan deteksi fraud.
3.	Streamlit: Sebagai dashboard interaktif untuk memantau hasil analisis secara visual.
📂 Struktur Folder
🛠️ Cara Menjalankan Sistem
Pastikan Zookeeper dan Kafka Server sudah berjalan di latar belakang sebelum memulai.
Langkah 1: Jalankan Kafka Producer
Mulai pengiriman simulasi data transaksi bank.
Langkah 2: Jalankan Spark Streaming
Proses data dari Kafka, terapkan keamanan data, dan simpan hasilnya.
Langkah 3: Jalankan Dashboard Streamlit
Visualisasikan data transaksi dan deteksi fraud di browser.
🔒 Fitur Keamanan & Logika Bisnis
• Data Masking: Menyembunyikan nomor rekening (hanya menampilkan 2 digit terakhir).
• Data Encryption: Mengenkripsi kolom jumlah transaksi menggunakan Base64.
• Fraud Detection:
• Transaksi > Rp 50.000.000 otomatis dianggap FRAUD.
• Transaksi dengan lokasi Luar Negeri otomatis dianggap FRAUD.
📝 Catatan Penting
• Pastikan library kafka-python, pyspark, streamlit, pandas, dan pyarrow sudah terinstal.
• Jika terjadi error Not a directory, pastikan Spark telah berhasil memproses setidaknya satu data transaksi untuk membuat folder output.
