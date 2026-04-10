import os
import io
import re
import json
import asyncio
import asyncpg
import pandas as pd
import geopandas as gpd
from datetime import datetime
from shapely.geometry import Point
from fastapi import FastAPI, APIRouter, UploadFile, File, Form, Request, HTTPException, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, FileResponse
from sse_starlette.sse import EventSourceResponse

app = FastAPI(title="System Geokodowania PRG")
templates = Jinja2Templates(directory="templates")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://strazak:mocne-haslo-osp@db:5432/prg_database")
EXPORTS_DIR = "exports"
os.makedirs(EXPORTS_DIR, exist_ok=True)

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
    if len(parts) < 3: return None
    try:
        degrees = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
        dd = degrees + (minutes / 60) + (seconds / 3600)
        if 'S' in dms_str or 'W' in dms_str: dd = -dd
        return dd
    except Exception: return None

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

@router_konwerter.post("/konwerter/process")
async def process_conversion(
    file: UploadFile = File(...),
    encoding: str = Form("utf-8")
):
    try:
        # Wczytanie pliku (Excel nie wymaga kodowania, ale zostawiamy opcję dla elastyczności)
        content = await file.read()
        df = pd.read_excel(io.BytesIO(content))
        
        lat_col = 'Szerokość geo.'
        lon_col = 'Długość geo.'
        
        if lat_col not in df.columns or lon_col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Brak kolumn {lat_col}/{lon_col}")

        # Konwersja DMS -> DD
        df['lat_dd'] = df[lat_col].apply(parse_dms)
        df['lon_dd'] = df[lon_col].apply(parse_dms)
        
        df_clean = df.dropna(subset=['lat_dd', 'lon_dd']).copy()
        
        if df_clean.empty:
            raise HTTPException(status_code=400, detail="Nie znaleziono poprawnych danych DMS")

        # Tworzenie GeoDataframe
        geometry = [Point(xy) for xy in zip(df_clean['lon_dd'], df_clean['lat_dd'])]
        gdf = gpd.GeoDataFrame(df_clean, geometry=geometry, crs="EPSG:4326")
        
        # Zapis do bufora GPKG
        buffer = io.BytesIO()
        gdf.to_file(buffer, driver="GPKG", engine="pyogrio")
        buffer.seek(0)
        
        output_filename = f"{file.filename.split('.')[0]}_converted.gpkg"
        
        return StreamingResponse(
            buffer,
            media_type="application/geopackage+sqlite3",
            headers={"Content-Disposition": f"attachment; filename={output_filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
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