-- 1. Rozszerzenia (Extensions)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- 2. Funkcja immutable_unaccent (Kluczowa dla wydajności indeksów GIN)
CREATE OR REPLACE FUNCTION public.immutable_unaccent(text) 
RETURNS text AS $$ 
    SELECT public.unaccent('public.unaccent', $1); 
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

-- 3. Struktura tabeli adresowej
DROP TABLE IF EXISTS public.addresses;
CREATE TABLE public.addresses (
    id SERIAL PRIMARY KEY,
    miejscowosc TEXT,
    ulica TEXT,
    numer TEXT,
    kod_pocztowy TEXT,
    teryt_msc TEXT,
    wojewodztwo TEXT,
    powiat TEXT,
    gmina TEXT,
    geom GEOMETRY(Point, 2180)
);

-- 4. Struktura tabeli słownika TERC
DROP TABLE IF EXISTS public.terc_dict;
CREATE TABLE public.terc_dict (
    woj TEXT,
    pow TEXT,
    gmi TEXT,
    rodz TEXT,
    nazwa TEXT,
    nazwa_dod TEXT
);

-- 5. Wstępne uprawnienia (opcjonalne)
ALTER TABLE public.addresses OWNER TO strazak;
ALTER TABLE public.terc_dict OWNER TO strazak;