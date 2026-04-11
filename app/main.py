import os, shutil, zipfile, tempfile
import io
import re
import json
import asyncio
import asyncpg
import string
import random
import html
import pandas as pd
import geopandas as gpd
from datetime import datetime
from typing import List, Optional
from shapely.geometry import Point
from fastapi import FastAPI, APIRouter, UploadFile, File, Form, Request, HTTPException, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from sse_starlette.sse import EventSourceResponse

app = FastAPI(title="System Geokodowania PRG")
templates = Jinja2Templates(directory="templates")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://strazak:mocne-haslo-osp@db:5432/prg_database")
EXPORTS_DIR = "exports"
os.makedirs(EXPORTS_DIR, exist_ok=True)

MAPS_DIR = "/app/data/maps"
os.makedirs(MAPS_DIR, exist_ok=True)

def generate_short_hash(length=5):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

# Rejestr aktywnych procesów
jobs = {}

def split_combined_address(full_address):
    if not full_address or pd.isna(full_address):
        return "", ""
    s_addr = str(full_address).strip()
    match = re.search(r'^(.*?)\s+([\d/]+[a-zA-Z]?)$', s_addr)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return s_addr, ""

def parse_dms(dms_str):
    if pd.isna(dms_str) or dms_str == "":
        return None
    dms_str = str(dms_str).strip().upper()
    parts = re.findall(r"(\d+\.?\d*)", dms_str)
    if len(parts) < 3: 
        return None
    try:
        degrees = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
        dd = degrees + (minutes / 60) + (seconds / 3600)
        if 'S' in dms_str or 'W' in dms_str: 
            dd = -dd
        return dd
    except Exception: 
        return None

def cleanup_temp_file(path: str):
    if os.path.exists(path):
        # Jeśli to plik, usuwamy plik
        if os.path.isfile(path):
            os.remove(path)
        # Jeśli to folder (gdybyśmy przekazali tmpdir)
        elif os.path.isdir(path):
            shutil.rmtree(path)

def create_kml(gdf, category_field='Rodzaj'):
    kml = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<kml>',
        '<Document>'
    ]

    for _, row in gdf.iterrows():
        # Logika wyciągania głównej kategorii (P, MZ, AF)
        rodzaj_raw = str(row.get(category_field, ''))
        # split('/') bierze to co przed znakiem, np. z "MZ/L" zrobi "MZ"
        main_cat = rodzaj_raw.split('/')[0].strip().upper()
        
        # Współrzędne
        lon = f"{row['lon_dd']}".replace(',', '.')
        lat = f"{row['lat_dd']}".replace(',', '.')
        
        # Tytuł punktu - data i godzina
        name = html.escape(str(row.get('Data i godzina przyjęcia zgłoszenia', 'Zdarzenie')))
        
        # Budujemy bogaty opis do popupu
        desc_html = [
            f"<b>Rodzaj:</b> {html.escape(rodzaj_raw)}",
            f"<b>Miejsce:</b> {html.escape(str(row.get('Miejsce zdarzenia', 'Brak adresu')))}",
            f"<b>Jednostka:</b> {html.escape(str(row.get('Jednostka', '-')))}",
            f"<b>Zastępy:</b> {html.escape(str(row.get('Zastępy', '0')))}"
        ]
        
        # Łączymy opis w jeden string (używamy CDATA, żeby Leaflet nie zgłupiał od tagów HTML)
        description = "<![CDATA[" + "<br>".join(desc_html) + "]]>"
        
        kml.append('<Placemark>')
        kml.append(f'<name>{name}</name>')
        kml.append(f'<description>{description}</description>')
        kml.append(f'<ExtendedData><Data name="type"><value>{main_cat}</value></Data></ExtendedData>')
        kml.append('<Point>')
        kml.append(f'<coordinates>{lon},{lat},0</coordinates>')
        kml.append('</Point>')
        kml.append('</Placemark>')

    kml.append('</Document>')
    kml.append('</kml>')
    
    return "".join(kml)

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # Wymagane jawne przekazanie request jako argumentu nazwanego
    return templates.TemplateResponse(
        request=request, 
        name="index.html", 
        context={}
    )

router_konwerter = APIRouter(tags=["Konwerter GIS"])

@router_konwerter.post("/konwerter/analyze")
async def analyze_columns(file: UploadFile = File(...)):
    """Skan pliku XLS w celu wyciągnięcia nazw kolumn dla UI."""
    try:
        content = await file.read()
        filename = file.filename.lower()
        
        if filename.endswith('.xls'):
            df = pd.read_excel(io.BytesIO(content), engine='xlrd', nrows=5)
        else:
            df = pd.read_excel(io.BytesIO(content), engine='openpyxl', nrows=5)
            
        # Czyszczenie nazw kolumn (strip i twarde spacje)
        cols = [str(c).strip().replace('\xa0', ' ') for c in df.columns]
        return {"columns": cols}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Błąd analizy struktury: {str(e)}")

app.mount("/data/maps", StaticFiles(directory=MAPS_DIR), name="maps")

@router_konwerter.post("/konwerter/process")
async def process_conversion(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    category_field: Optional[str] = Form(None),
    export_kml: bool = Form(False),
    encoding: str = Form("utf-8")
):
    all_dfs = []
    temp_gpkg_path = None
    temp_zip_path = None

    try:
        # 1. Wczytywanie plików
        for file in files:
            content = await file.read()
            if file.filename.lower().endswith('.xls'):
                df = pd.read_excel(io.BytesIO(content), engine='xlrd')
            else:
                df = pd.read_excel(io.BytesIO(content), engine='openpyxl')
            
            df.columns = [str(c).strip().replace('\xa0', ' ') for c in df.columns]
            
            lat_col = next((c for c in df.columns if c.lower() == 'szerokość geo.'), None)
            lon_col = next((c for c in df.columns if c.lower() == 'długość geo.'), None)
            
            if lat_col and lon_col:
                df['lat_dd'] = df[lat_col].apply(parse_dms)
                df['lon_dd'] = df[lon_col].apply(parse_dms)
                all_dfs.append(df.dropna(subset=['lat_dd', 'lon_dd']))

        if not all_dfs:
            raise HTTPException(status_code=400, detail="Brak poprawnych danych do konwersji.")

        # 2. Tworzenie pliku tymczasowego dla GPKG
        fd, temp_gpkg_path = tempfile.mkstemp(suffix=".gpkg")
        os.close(fd)

        final_df = pd.concat(all_dfs, ignore_index=True)
        geometry = [Point(xy) for xy in zip(final_df['lon_dd'], final_df['lat_dd'])]
        gdf = gpd.GeoDataFrame(final_df, geometry=geometry, crs="EPSG:4326")

        # 3. Zapis do GPKG (warstwy)
        if category_field and category_field in gdf.columns:
            first = True
            for val, group in gdf.groupby(category_field):
                layer_name = re.sub(r'[^\w]', '_', str(val))[:30]
                mode = "w" if first else "a"
                group.to_file(temp_gpkg_path, driver="GPKG", engine="pyogrio", layer=layer_name, mode=mode)
                first = False
        else:
            gdf.to_file(temp_gpkg_path, driver="GPKG", engine="pyogrio", layer="import_zbiorczy")

        # 4. Obsługa wyjścia (ZIP lub GPKG)
        map_hash = None
        if export_kml:
            # 1. Generujemy hash i treść KML
            map_hash = generate_short_hash()
            kml_text = create_kml(gdf, category_field)
            
            # 2. Zapisujemy KML do folderu stałego (dla widoku Leaflet /m/{hash})
            permanent_kml_path = os.path.join(MAPS_DIR, f"{map_hash}.kml")
            with open(permanent_kml_path, "w", encoding="utf-8") as f:
                f.write(kml_text)

            # 3. Tworzymy tymczasowy ZIP dla użytkownika
            fd_z, temp_zip_path = tempfile.mkstemp(suffix=".zip")
            os.close(fd_z)
            
            with zipfile.ZipFile(temp_zip_path, "w") as zf:
                # Wrzucamy GPKG
                zf.write(temp_gpkg_path, arcname="dane_GIS.gpkg")
                # Wrzucamy wygenerowany KML (konwersja str na bytes)
                zf.writestr("podglad_mobilny.kml", kml_text.encode('utf-8'))

            # 4. Rejestrujemy czyszczenie plików TYMCZASOWYCH
            # (Pliku w MAPS_DIR nie usuwamy, bo link przestałby działać!)
            background_tasks.add_task(cleanup_temp_file, temp_gpkg_path)
            background_tasks.add_task(cleanup_temp_file, temp_zip_path)
            
            # 5. Przygotowanie nagłówków z linkiem do mapy
            headers = {
                "X-Map-URL": f"https://geokoder.giswgorach.pl/m/{map_hash}",
                "Access-Control-Expose-Headers": "X-Map-URL" # Ważne dla frontendu!
            }
            
            return FileResponse(
                temp_zip_path, 
                media_type="application/zip",
                filename="paczka_GIS_SWD.zip",
                headers=headers
            )

        else:
            background_tasks.add_task(cleanup_temp_file, temp_gpkg_path)
            return FileResponse(
                temp_gpkg_path,
                media_type="application/geopackage+sqlite3",
                filename="zestawienie_GIS.gpkg"
            )

    except Exception as e:
        # Sprzątanie w razie błędu na dowolnym etapie
        if temp_gpkg_path: cleanup_temp_file(temp_gpkg_path)
        if temp_zip_path: cleanup_temp_file(temp_zip_path)
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router_konwerter.get("/m/{map_id}", response_class=HTMLResponse)
async def share_map(map_id: str):
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Mapa zdarzeń z zestawienia SWD</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <style>
            #map {{ height: 100vh; width: 100%; background: #f3f4f6; }}
            body {{ margin: 0; padding: 0; }}
            .popup-box {{ font-family: sans-serif; font-size: 13px; line-height: 1.5; }}
            .popup-box b {{ color: #b91c1c; }}
        </style>
    </head>
    <body>
        <div id="map"></div>
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script>
            var map = L.map('map').setView([52.2, 19.2], 6);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png').addTo(map);

            // Funkcja do dobierania koloru ikony
            function getIcon(type) {{
                var color = "blue"; // domyślny
                if (type === "P") color = "red";
                if (type === "MZ") color = "orange";
                if (type === "AF") color = "black";
                
                return new L.Icon({{
                    iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-' + color + '.png',
                    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
                    iconSize: [25, 41],
                    iconAnchor: [12, 41],
                    popupAnchor: [1, -34],
                    shadowSize: [41, 41]
                }});
            }}

            fetch('/data/maps/{map_id}.kml')
                .then(res => res.text())
                .then(kmltext => {{
                    var parser = new DOMParser();
                    var xml = parser.parseFromString(kmltext.trim(), 'text/xml');
                    var placemarks = xml.getElementsByTagName('Placemark');
                    var markers = [];

                    for (var i = 0; i < placemarks.length; i++) {{
                        try {{
                            // Wyciągamy dane
                            var name = placemarks[i].getElementsByTagName('name')[0].textContent;
                            var desc = placemarks[i].getElementsByTagName('description')[0].textContent;
                            
                            // Wyciągamy kategorię z ExtendedData
                            var type = "MZ"; 
                            var extData = placemarks[i].getElementsByTagName('value');
                            if (extData.length > 0) type = extData[0].textContent;

                            var coords = placemarks[i].getElementsByTagName('coordinates')[0].textContent.split(',');
                            var lng = parseFloat(coords[0]);
                            var lat = parseFloat(coords[1]);

                            if (!isNaN(lat) && !isNaN(lng)) {{
                                var m = L.marker([lat, lng], {{ icon: getIcon(type) }})
                                         .addTo(map)
                                         .bindPopup('<div class="popup-box"><b>' + name + '</b><hr>' + desc + '</div>');
                                markers.push(m);
                            }}
                        }} catch(e) {{ console.error("Błąd punktu:", e); }}
                    }

                    if (markers.length > 0) {{
                        var group = new L.featureGroup(markers);
                        map.fitBounds(group.getBounds(), {{ padding: [40, 40] }});
                    }}
                }});
        </script>
    </body>
    </html>
    """
    return html_content

@app.post("/preview")
async def preview(
    file: UploadFile = File(...), 
    sep: str = Form("auto"), 
    quote: str = Form('"'), 
    encoding: str = Form("utf-8")
):
    try:
        contents = await file.read()
        buffer = io.BytesIO(contents)
        fname = file.filename or ""
        if fname.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(buffer)
        else:
            # Parametry CSV
            csv_params = {
                "encoding": encoding, # Tutaj wpada utf-8 lub cp1250
                "quotechar": quote if quote else None,
                "engine": "python",
                "on_bad_lines": "skip" # Zabezpieczenie przed uszkodzonymi wierszami
            }
            
            if sep == "auto":
                csv_params["sep"] = None
            elif sep == "\\t":
                csv_params["sep"] = "\t"
            else:
                csv_params["sep"] = sep

            df = pd.read_csv(buffer, **csv_params)

        # Standaryzacja nazw kolumn
        df.columns = [str(c).strip() for c in df.columns]
        
        return {
        "columns": list(df.columns),  # Użycie list() zamiast .tolist()
        "sample": df.head(5).fillna('').to_dict(orient="records"),
        "total_rows": len(df)
        }

    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Błąd kodowania znaków. Spróbuj zmienić na Windows-1250.")
    except Exception as e:
        print(f"Błąd krytyczny preview: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Błąd odczytu: {str(e)}")

@app.post("/start-geocoding")
async def start_geocoding(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    mapping_type: str = Form(...),
    msc_col: str = Form(...),
    ulc_col: str = Form(...),
    num_col: str = Form(""),
    woj_col: str = Form(""),
    pow_col: str = Form(""),
    gmi_col: str = Form(""),
    encoding: str = Form("utf-8"),
    sep: str = Form("auto"),
    quote: str = Form('"')
):
    job_id = f"job_{datetime.now().strftime('%H%M%S')}"
    contents = await file.read()
    
    # Inicjalizacja statusu
    jobs[job_id] = {
        "progress": 0, "success": 0, "fail": 0, 
        "status": "processing", "total": 0,
        "gpkg_url": None, "csv_url": None
    }
    
    # UWAGA: Tutaj muszą być przekazane WSZYSTKIE argumenty, które przyjmuje run_geocoding_task
    background_tasks.add_task(
        run_geocoding_task, 
        job_id, contents, file.filename, mapping_type, 
        msc_col, ulc_col, num_col, woj_col, pow_col, gmi_col, 
        encoding, sep, quote
    )
    
    return {"job_id": job_id}
@app.post("/cancel-job/{job_id}")
async def cancel_job(job_id: str):
    if job_id in jobs:
        jobs[job_id]["status"] = "cancelled"
        return {"status": "cancelled"}
    return {"status": "error"}

@app.get("/stream-progress/{job_id}")
async def stream_progress(job_id: str):
    async def event_generator():
        while True:
            if job_id in jobs:
                yield {"data": json.dumps(jobs[job_id])}
                if jobs[job_id]["status"] in ["completed", "cancelled", "failed"]:
                    break
            await asyncio.sleep(1)
    return EventSourceResponse(event_generator())

async def run_geocoding_task(job_id, contents, filename, m_type, msc_c, ulc_c, num_c, woj_c, pow_c, gmi_c, enc, sep, quote):
    try:
        buffer = io.BytesIO(contents)
        if filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(buffer)
        else:
            csv_params = {"encoding": enc, "quotechar": quote if quote else None, "engine": "python"}
            if sep == "auto": 
                csv_params["sep"] = None
            elif sep == "\\t": 
                csv_params["sep"] = "\t"
            else: 
                csv_params["sep"] = sep
            df = pd.read_csv(buffer, **csv_params)

        total = len(df)
        jobs[job_id]["total"] = total
        success_list, fail_list = [], []

        # POMOCNIK: Naprawa sabotażu Excela i czyszczenie znaków
        def normalize_entry(val, is_num=False):
            if not val or pd.isna(val): 
                return ""
            
            # Naprawa dat Excela (np. 1900-01-34 -> 34)
            if isinstance(val, (datetime, pd.Timestamp)):
                return str(val.day)
            
            s = str(val).strip()
            
            # Obsługa numerów domów zamienionych na tekstowe daty przez Pandas
            if is_num and '-' in s and s.count('-') == 2:
                try:
                    # Próba wyciągnięcia dnia z formatu RRRR-MM-DD
                    return str(int(s.split('-')[2].split()[0]))
                except (ValueError, IndexError): 
                    pass

            # Usuwanie sufiksów po myślniku w miejscowościach (np. "Jelenia Góra - SIMET")
            if not is_num:
                s = s.split(' - ')[0]

            # Usuwanie znaków interpunkcyjnych (kropki na końcu, przecinki)
            s = re.sub(r'[.,]$', '', s)
            return s.strip()

        async with app.state.pool.acquire() as conn:
            await conn.execute("SET pg_trgm.similarity_threshold = 0.2;")

            for idx, (i, row) in enumerate(df.iterrows()):
                if jobs.get(job_id, {}).get("status") == "cancelled": 
                    return

                # 1. Pobranie i walidacja niezbędnych danych
                msc = normalize_entry(row.get(msc_c, ''))
                num = normalize_entry(row.get(num_c, ''), is_num=True)
                
                # Jeśli brak miejscowości lub numeru - pomijamy (zgodnie z Twoją wytyczną)
                if not msc or not num:
                    fail_list.append(row.to_dict())
                    jobs[job_id]["fail"] += 1
                    continue

                if m_type == 'combined':
                    ulc, _ = split_combined_address(row.get(ulc_c, ''))
                    ulc = normalize_entry(ulc)
                else:
                    ulc = normalize_entry(row.get(ulc_c, ''))

                num = num.upper()

                # Funkcja budująca filtry administracyjne
                def get_admin_params(start_idx):
                    f_sql, f_params, curr = [], [], start_idx
                    for col, field in [(woj_c, 'wojewodztwo'), (pow_c, 'powiat')]:
                        if col and col in df.columns:
                            v = normalize_entry(row.get(col, ''))
                            if v:
                                f_sql.append(f"AND (public.immutable_unaccent({field}) % public.immutable_unaccent(${curr}) OR public.immutable_unaccent({field}) ILIKE '%' || public.immutable_unaccent(${curr}) || '%')")
                                f_params.append(v)
                                curr += 1
                    return " ".join(f_sql), f_params

                # --- PRZEBIEG 1: MSC + ULC + NUM + ADMIN (Pełna precyzja) ---
                f_sql, f_params = get_admin_params(4)
                query_1 = f"""
                    SELECT ST_X(geom) as x, ST_Y(geom) as y FROM addresses
                    WHERE (public.immutable_unaccent(miejscowosc) = public.immutable_unaccent($1) OR public.immutable_unaccent(miejscowosc) % public.immutable_unaccent($1))
                    {f_sql}
                    AND (
                        ($2 = '' AND (ulica IS NULL OR ulica = '')) OR 
                        public.immutable_unaccent(COALESCE(ulica, '')) % public.immutable_unaccent($2) OR
                        public.immutable_unaccent(COALESCE(ulica, '')) ILIKE '%' || public.immutable_unaccent($2) || '%'
                    )
                    AND (upper(numer) = $3) LIMIT 1;
                """
                res = await conn.fetchrow(query_1, msc, ulc, num, *f_params)

                # --- PRZEBIEG 2: MSC + NUM + ADMIN (Ignorujemy ulicę - dla Istebnej/Brennej) ---
                if not res:
                    f_sql, f_params = get_admin_params(3)
                    query_2 = f"""
                        SELECT ST_X(geom) as x, ST_Y(geom) as y FROM addresses
                        WHERE public.immutable_unaccent(miejscowosc) = public.immutable_unaccent($1)
                        {f_sql} AND (upper(numer) = $2)
                        ORDER BY (ulica IS NULL) DESC LIMIT 1;
                    """
                    res = await conn.fetchrow(query_2, msc, num, *f_params)

                # --- PRZEBIEG 3: MSC + NUM (Rozszerzone podobieństwo miejscowości) ---
                if not res:
                    f_sql, f_params = get_admin_params(3)
                    query_3 = f"""
                        SELECT ST_X(geom) as x, ST_Y(geom) as y FROM addresses
                        WHERE public.immutable_unaccent(miejscowosc) % public.immutable_unaccent($1)
                        {f_sql} AND (upper(numer) = $2)
                        ORDER BY similarity(public.immutable_unaccent(miejscowosc), public.immutable_unaccent($1)) DESC LIMIT 1;
                    """
                    res = await conn.fetchrow(query_3, msc, num, *f_params)

                if res:
                    row_dict = row.to_dict()
                    row_dict['geometry'] = Point(res['x'], res['y'])
                    success_list.append(row_dict)
                    jobs[job_id]["success"] += 1
                else:
                    fail_list.append(row.to_dict())
                    jobs[job_id]["fail"] += 1

                if idx % 10 == 0 or idx == total - 1:
                    jobs[job_id]["progress"] = int(((idx + 1) / total) * 100)

        # Zapis wyników
        if success_list:
            gpd.GeoDataFrame(success_list, crs="EPSG:2180").to_file(f"{EXPORTS_DIR}/{job_id}.gpkg", driver="GPKG")
        if fail_list:
            pd.DataFrame(fail_list).to_csv(f"{EXPORTS_DIR}/{job_id}_fail.csv", index=False, encoding="utf-8-sig")

        jobs[job_id]["status"] = "completed"
        jobs[job_id].update({"gpkg_url": f"/download/{job_id}/gpkg", "csv_url": f"/download/{job_id}/csv"})
        
    except Exception as e:
        print(f"BŁĄD: {e}")
        jobs[job_id]["status"] = "failed"

@app.get("/download/{job_id}/{file_type}")
async def download(job_id: str, file_type: str):
    path = f"{EXPORTS_DIR}/{job_id}.gpkg" if file_type == "gpkg" else f"{EXPORTS_DIR}/{job_id}_fail.csv"
    return FileResponse(path, filename=os.path.basename(path))

app.include_router(router_konwerter)