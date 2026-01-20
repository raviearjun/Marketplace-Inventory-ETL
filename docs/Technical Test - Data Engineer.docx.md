**Technical Test: "Marketplace Inventory Pipeline"**

1. Skenario

Anda adalah Data Engineer di sebuah perusahaan yang mengolah data Marketplace. Anda mendapatkan file mentah (.json atau .csv) berisi data produk dan toko. Tugas Anda adalah membangun pipeline ETL yang andal untuk memindahkan data ini ke Data Warehouse (PostgreSQL) agar siap dianalisis.

2. Instruksi Pengerjaan  
1. Sumber Dataset  
   Tautan : [https://drive.google.com/drive/folders/1OaTerOEb6fRHJ4Zys8m3UbxM4vfylsAg?usp=drive\_link](https://drive.google.com/drive/folders/1OaTerOEb6fRHJ4Zys8m3UbxM4vfylsAg?usp=drive_link)   
2. Resources & Tools (Docker)  
   Siapkan lingkungan kerja lokal menggunakan Docker Compose yang berisi:  
* PostgreSQL: Sebagai Data Warehouse.  
* Apache Airflow: Sebagai orkestrator pipeline (Wajib scheduler, disarankan Airflow. Crontab diperbolehkan namun nilai berbeda).  
* Python: Untuk script processing.  
3. ETL Pipeline (Cleaning, Parsing, & Modeling)  
   Buatlah DAG/Pipeline yang memproses dataset yang diberikan dengan ketentuan:  
1. Data Cleaning & Transformation  
1. Normalisasi Database: Pisahkan data menjadi dua tabel utama: stores dan products (terapkan Primary Key dan Foreign Key).  
2. Handling Nulls  
3. Data Integrity: Pastikan kolom sesuai dengan tipe datanya.  
4. Standardization: Ubah data pada kolom menjadi format Uppercase jika diperlukan.  
5. Text Cleaning: Bersihkan karakter newline (\\n), spasi berlebih atau karakter lain yang perlu dibersihkan.  
6. Deduplication: Pastikan tidak ada product\_id atau store\_id yang duplikat masuk ke database.

2. Storage & Modeling  
1. Desainlah skema database (DDL) di PostgreSQL.  
2. Simpan data yang sudah bersih ke dalam tabel tersebut.  
4. Output yang Dikumpulkan  
1. Link Repository (GitHub/Gitlab) yang berisi file docker-compose.yml, kode Python/DAGs, dan README.md.  
2. Video Penjelasan (Maks. 10 Menit) yang mendemokan pipeline berjalan dan menjawab pertanyaan teori di bawah ini.  
3. Pengumpulan video penjelasan beserta tautan repository dilakukan paling lambat 5 (lima) hari setelah interview technical test.  
3. Video Penjelasan & Konsep

Rekam video singkat yang mencakup:

1. Demo: Tunjukkan pipeline berjalan (hijau) di Airflow dan data masuk ke Postgres.  
2. Reasoning: Mengapa Anda memilih memisahkan tabel stores dan products? Apa keuntungan skema ini dibanding membiarkannya dalam satu tabel besar (flat table)?  
3. OLAP vs OLTP: Jika database PostgreSQL yang Anda buat hari ini ingin diubah fungsinya dari OLTP (mendukung aplikasi belanja real-time) menjadi OLAP (mendukung laporan tahunan ribuan toko), perubahan teknis apa yang akan Anda lakukan pada skema tabel dan sistem indexing-nya?  
4. Data Warehouse vs Data Mart: Tabel agregat yang Anda buat (Summary Toko) lebih tepat disebut sebagai bagian dari Data Warehouse atau Data Mart? Jelaskan perbedaannya dalam konteks struktur data marketplace ini.  
5. Data Lake: Dalam pipeline ini, Anda membaca file JSON langsung ke Postgres. Di perusahaan skala besar, kita biasanya menyimpan file JSON tersebut terlebih dahulu ke dalam Data Lake (seperti S3 atau HDFS) sebelum masuk ke database. Mengapa langkah ini penting? Apa resikonya jika kita langsung membuang file JSON asli setelah data masuk ke Postgres?  
6. Big Data: Jika data produk mencapai 1 Milyar baris, strategi apa yang akan Anda ubah dari script Anda saat ini?

*Good luck, and happy coding\!*