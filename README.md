# 🚒 address-firefighter

System geokodowania adresów oparty na danych PRG (Główny Urząd Geodezji i Kartografii) oraz silniku PostGIS.

## 🚀 Kluczowe funkcjonalności
- **Fourth Pass Logic**: Zaawansowane dopasowanie fuzzy (trigramy), korekta błędów formatowania Excela, oraz obsługa homonimów.
- **Docker-native**: Pełna konteneryzacja bazy danych i aplikacji.
- **Automated Setup**: Skrypt automatycznie pobiera i indeksuje punkty adresowe z serwerów GUGiK.

## 🛠️ Szybki start
1. Umieść aktualny plik `terc.csv` w katalogu `data/`.
2. Uruchom system:
   ```bash
   docker-compose up -d