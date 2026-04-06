import asyncio
import httpx
import asyncpg
import zipfile
import io
import pandas as pd

# Lista URLi do paczek PRG (przykładowo dla województw)
GUGIK_URLS = [
    "https://example.gov.pl/prg/woj_02.zip", # Dolnośląskie
    # ... reszta województw
]

async def download_and_process(url, pool):
    async with httpx.AsyncClient() as client:
        print(f"🚒 Pobieranie: {url}")
        r = await client.get(url)
        
        # Rozpakowanie w locie (in-memory, by nie zaśmiecać kontenera)
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                # Czytamy CSV przez Pandas (łatwa transformacja)
                df = pd.read_csv(f, sep=';', encoding='utf-8')
                
                # Transformacja: przygotowanie krotki do COPY
                rows = [
                    (r['teryt'], r['miejscowosc'], r['ulica'], r['numer'], r['pna'])
                    for _, r in df.iterrows()
                ]

                # Masowe ładowanie przez asyncpg
                async with pool.acquire() as conn:
                    # Używamy binarnych komend COPY dla szybkości
                    result = await conn.copy_records_to_table(
                        'addresses_staging', 
                        records=rows, 
                        columns=['kod_teryt', 'miejscowosc', 'ulica', 'numer', 'kod_pocztowy']
                    )
                    print(f"✅ Załadowano paczkę z {url}")

async def main():
    pool = await asyncpg.create_pool("postgresql://strazak:mocne-haslo-osp@db:5432/prg_database")
    
    # Najpierw tworzymy tabelę tymczasową (Staging)
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE addresses_staging (LIKE addresses INCLUDING ALL);")

    # Pobieramy wszystko równolegle
    await asyncio.gather(*(download_and_process(url, pool) for url in GUGIK_URLS))

if __name__ == "__main__":
    asyncio.run(main())