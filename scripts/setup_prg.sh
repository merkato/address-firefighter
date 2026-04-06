#!/bin/bash
set -e

export PGPASSWORD=$DB_PASS
DB_CONN="host=$DB_HOST user=$DB_USER dbname=$DB_NAME password=$DB_PASS"

echo "📥 ROZPOCZYNAM IMPORT DANYCH DO PRZYGOTOWANEGO SCHEMATU..."

# 1. Sprawdzenie czy dane już są (aby nie pobierać co restart)
COUNT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "SELECT count(*) FROM addresses;")
if [ "${COUNT//[[:space:]]/}" -gt 0 ]; then
    echo "✅ Dane adresowe są już w bazie. Pomijam import."
else
    echo "🌐 Pobieranie paczki PRG z GUGiK..."
    wget -O /tmp/prg.zip "https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_shp"
    unzip -o /tmp/prg.zip -d /tmp/prg_raw
    
    SHP_FILE=$(find /tmp/prg_raw -name "NOWE_PRG_PunktyAdresowe_POLSKA.shp")
    
    echo "📥 Wstrzykiwanie punktów do tabeli pośredniej..."
    # Importujemy do tabeli tymczasowej, aby dopasować kolumny do naszego schematu
    ogr2ogr -f "PostgreSQL" PG:"$DB_CONN" "$SHP_FILE" \
        -nln tmp_import -lco GEOMETRY_NAME=geom -overwrite \
        -t_srs EPSG:2180 -gt 65536 --config PG_USE_COPY YES

    echo "🚚 Migracja danych do tabeli addresses..."
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
        INSERT INTO addresses (miejscowosc, ulica, numer, kod_pocztowy, teryt_msc, geom)
        SELECT miejscowos, NULLIF(ulica, ''), numer_porz, kod_poczto, terc, geom FROM tmp_import;
        DROP TABLE tmp_import;"
fi

# 2. Import TERC i Aktualizacja Admin (woj, pow, gmi)
if [ -f "/data/terc.csv" ]; then
    echo "🗺️ Import słownika TERC..."
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "TRUNCATE terc_dict; \copy terc_dict FROM '/data/terc.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true, ENCODING 'WIN1250');"
    
    echo "🔄 Aktualizacja nazw administracyjnych (Second Pass)..."
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME <<EOF
    UPDATE addresses a SET wojewodztwo = t.nazwa FROM terc_dict t WHERE LEFT(a.teryt_msc, 2) = t.woj AND t.pow IS NULL;
    UPDATE addresses a SET powiat = t.nazwa FROM terc_dict t WHERE LEFT(a.teryt_msc, 4) = t.woj || t.pow AND t.gmi IS NULL AND t.pow IS NOT NULL;
EOF
fi

# 3. Indeksy (Budowane po imporcie dla szybkości)
echo "⚡ Budowa indeksów optymalizacyjnych..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME <<EOF
CREATE INDEX IF NOT EXISTS idx_addresses_msc_trgm ON addresses USING GIN (public.immutable_unaccent(miejscowosc) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_addresses_ulc_trgm ON addresses USING GIN (public.immutable_unaccent(COALESCE(ulica, '')) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_addresses_num ON addresses (upper(numer));
CREATE INDEX IF NOT EXISTS idx_addresses_geom ON addresses USING GIST (geom);
ANALYZE addresses;
EOF

echo "✅ PROCES ZAKOŃCZONY."