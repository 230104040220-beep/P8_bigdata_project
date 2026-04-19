
import streamlit as st
import pandas as pd
import os
import time

# Konfigurasi halaman
st.set_page_config(page_title="Sistem Deteksi Fraud Real-time", layout="wide")

st.title("🏦 Dashboard Pemantauan Keamanan Transaksi Bank")
st.markdown("---")

# Jalur folder tempat Spark menyimpan hasil (Parquet)
output_path = "stream_data/realtime_output/"

# Tempat penampung untuk update real-time
placeholder = st.empty()

while True:
    with placeholder.container():
        # Cek apakah folder output ada dan tidak kosong
        if os.path.exists(output_path) and any(f.endswith('.parquet') for f in os.listdir(output_path)):
            try:
                # Membaca data Parquet (Spark Output)
                df = pd.read_parquet(output_path)
                
                # Baris Statistik Utama (Metrik)
                col1, col2, col3 = st.columns(3)
                total_transaksi = len(df)
                total_fraud = len(df[df["status"] == "FRAUD"])
                persentase_fraud = (total_fraud / total_transaksi * 100) if total_transaksi > 0 else 0

                col1.metric("Total Transaksi", f"{total_transaksi}")
                col2.metric("Transaksi Mencurigakan (FRAUD)", f"{total_fraud}", delta_color="inverse")
                col3.metric("Rasio Risiko", f"{persentase_fraud:.2f}%")

                st.markdown("---")

                # Grafik Distribusi Status
                st.subheader("📊 Analisis Status Keamanan")
                status_counts = df["status"].value_counts()
                st.bar_chart(status_counts)

                # Tabel Transaksi Terbaru
                st.subheader("📝 Log Transaksi Terkini (Masked & Encrypted)")
                # Menampilkan 10 data terbaru
                display_df = df.tail(10)[["nama", "rekening_masked", "lokasi", "status", "jumlah_encrypted"]]
                st.dataframe(display_df, use_container_width=True)

            except Exception as e:
                st.info("Sedang menyinkronkan data dengan Spark...")
        else:
            st.warning("Menunggu data masuk dari Spark Streaming... Pastikan Spark sudah berjalan.")
            st.info(f"Mencari folder: {output_path}")

    # Refresh otomatis setiap 3 detik
    time.sleep(3)
