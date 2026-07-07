import fastf1
import pandas as pd
import numpy as np
import logging
import os
import concurrent.futures

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure FastF1 Cache (This will run inside Docker so we use the container path)
# If running locally without Docker, it will default to d:\ApexPipe\data\cache
CACHE_DIR = os.getenv("FASTF1_CACHE_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "cache"))
os.makedirs(CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)

def process_driver(row, session, race_id):
    driver_id = row['Abbreviation']
    driver_num = int(row['DriverNumber']) if pd.notna(row['DriverNumber']) else 0
    full_name = row['FullName']
    team_name = row['TeamName']
    headshot = row.get('HeadshotUrl')
    if pd.isna(headshot) or not str(headshot).startswith('http'):
        headshot = None

    dim_driver = {
        'driver_id': driver_id,
        'driver_number': driver_num,
        'full_name': full_name,
        'team_name': team_name,
        'headshot_url': headshot
    }

    fct_laps_list = []
    fct_telemetry_list = []

    try:
        driver_laps = session.laps.pick_driver(driver_id)
    except Exception as e:
        logger.warning(f"Could not load laps for {driver_id}: {e}")
        return dim_driver, [], []

    # Get fastest lap
    fastest_lap_num = None
    try:
        fastest_lap = driver_laps.pick_fastest()
        if not pd.isna(fastest_lap['LapNumber']):
            fastest_lap_num = int(fastest_lap['LapNumber'])
    except:
        pass

    for _, lap in driver_laps.iterrows():
        def to_ms(td):
            return int(td.total_seconds() * 1000) if pd.notnull(td) else None

        is_pit_out = pd.notnull(lap['PitOutTime'])
        
        lap_num = int(lap['LapNumber']) if pd.notnull(lap['LapNumber']) else 0
        
        fct_laps_list.append({
            'race_id': race_id,
            'driver_id': driver_id,
            'lap_number': lap_num,
            'lap_time_ms': to_ms(lap['LapTime']),
            'sector1_time_ms': to_ms(lap['Sector1Time']),
            'sector2_time_ms': to_ms(lap['Sector2Time']),
            'sector3_time_ms': to_ms(lap['Sector3Time']),
            'is_pit_out_lap': is_pit_out,
            'compound': str(lap['Compound']) if pd.notnull(lap['Compound']) else 'UNKNOWN',
            'tyre_life': int(lap['TyreLife']) if pd.notnull(lap['TyreLife']) else None
        })

        # TARGETED EXTRACTION: Only process telemetry for the fastest lap
        if fastest_lap_num and lap_num == fastest_lap_num:
            try:
                telemetry = lap.get_telemetry()
                
                if not telemetry.empty:
                    telemetry['race_id'] = race_id
                    telemetry['driver_id'] = driver_id
                    telemetry['lap_number'] = lap_num
                    
                    telemetry['session_time_ms'] = telemetry['SessionTime'].dt.total_seconds() * 1000
                    telemetry['session_time_ms'] = telemetry['session_time_ms'].fillna(0).astype(np.int64)
                    
                    telemetry = telemetry.rename(columns={
                        'Speed': 'speed',
                        'RPM': 'rpm',
                        'nGear': 'gear',
                        'Throttle': 'throttle',
                        'Brake': 'brake',
                        'DRS': 'drs',
                        'X': 'x_position',
                        'Y': 'y_position',
                        'Z': 'z_position',
                        'Distance': 'distance'
                    })

                    cols_to_fill = ['speed', 'rpm', 'gear', 'throttle', 'brake', 'drs', 'x_position', 'y_position', 'z_position', 'distance']
                    telemetry[cols_to_fill] = telemetry[cols_to_fill].ffill().fillna(0)

                    telemetry['speed'] = telemetry['speed'].astype(int)
                    telemetry['rpm'] = telemetry['rpm'].astype(int)
                    telemetry['gear'] = telemetry['gear'].astype(int)
                    telemetry['throttle'] = telemetry['throttle'].astype(int)
                    
                    if telemetry['brake'].dtype == bool:
                        telemetry['brake'] = telemetry['brake'].astype(int) * 100
                    else:
                        telemetry['brake'] = telemetry['brake'].astype(int)
                        
                    telemetry['drs'] = telemetry['drs'].astype(int)
                    telemetry['x_position'] = telemetry['x_position'].astype(float)
                    telemetry['y_position'] = telemetry['y_position'].astype(float)
                    telemetry['z_position'] = telemetry['z_position'].astype(float)
                    telemetry['distance'] = telemetry['distance'].astype(float)

                    telemetry = telemetry[[
                        'race_id', 'driver_id', 'lap_number', 'session_time_ms', 'distance',
                        'speed', 'rpm', 'gear', 'throttle', 'brake', 'drs', 
                        'x_position', 'y_position', 'z_position'
                    ]]
                    
                    fct_telemetry_list.append(telemetry)
            except Exception as e:
                logger.warning(f"Could not load telemetry for {driver_id} Lap {lap_num}: {e}")

    return dim_driver, fct_laps_list, fct_telemetry_list

def extract_and_transform(year: int, race_name: str):
    logger.info(f"Loading session {year} {race_name}...")
    
    session = fastf1.get_session(year, race_name, 'R')
    session.load(telemetry=True, laps=True, weather=False)

    race_id = f"{year}_{race_name.replace(' ', '')}"

    dim_races = pd.DataFrame([{
        'race_id': race_id,
        'year': year,
        'round_number': session.event.RoundNumber,
        'race_name': session.event.EventName,
        'circuit_name': session.event.Location,
        'session_date': session.date.date()
    }])

    results = session.results
    
    dim_drivers_list = []
    fct_laps_list = []
    fct_telemetry_list = []

    # PARALLEL PROCESSING
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for _, row in results.iterrows():
            futures.append(executor.submit(process_driver, row, session, race_id))
            
        for future in concurrent.futures.as_completed(futures):
            dim_driver, l_laps, l_telemetry = future.result()
            dim_drivers_list.append(dim_driver)
            fct_laps_list.extend(l_laps)
            fct_telemetry_list.extend(l_telemetry)

    dim_drivers = pd.DataFrame(dim_drivers_list).drop_duplicates()
    fct_laps = pd.DataFrame(fct_laps_list)
    fct_telemetry = pd.concat(fct_telemetry_list, ignore_index=True) if fct_telemetry_list else pd.DataFrame()

    # Extract highlights
    winner_id = ''
    winner_time = ''
    if not results.empty:
        w_row = results.iloc[0]
        winner_id = w_row.get('Abbreviation', '')
        winner_time = str(w_row.get('Time', ''))
        
    pole_id = ''
    pole_time = ''
    try:
        session_q = fastf1.get_session(year, race_name, 'Q')
        session_q.load(laps=False, telemetry=False, weather=False, messages=False)
        if not session_q.results.empty:
            p_row = session_q.results.iloc[0]
            pole_id = p_row.get('Abbreviation', '')
            for q_col in ['Q3', 'Q2', 'Q1', 'Time']:
                if q_col in p_row and pd.notnull(p_row[q_col]):
                    pole_time = str(p_row[q_col])
                    break
    except Exception as e:
        logger.warning(f"Could not load Qualifying for {race_id}: {e}")
        
    fl_id = ''
    fl_time = ''
    if not fct_laps.empty and 'lap_time_ms' in fct_laps.columns:
        valid_laps = fct_laps[fct_laps['lap_time_ms'] > 0]
        if not valid_laps.empty:
            fl_row = valid_laps.loc[valid_laps['lap_time_ms'].idxmin()]
            fl_id = fl_row['driver_id']
            fl_ms = fl_row['lap_time_ms']
            fl_num = fl_row['lap_number']
            fl_seconds = fl_ms / 1000.0
            fl_min = int(fl_seconds // 60)
            fl_sec = fl_seconds % 60
            fl_time = f"{fl_min:02d}:{fl_sec:06.3f} (Lap {fl_num})"
            
    dim_highlights = pd.DataFrame([{
        'race_id': race_id,
        'winner_driver_id': winner_id,
        'winner_time': winner_time,
        'pole_driver_id': pole_id,
        'pole_time': pole_time,
        'fastest_lap_driver_id': fl_id,
        'fastest_lap_time': fl_time
    }])

    logger.info(f"Extraction complete! Extracted {len(fct_laps)} laps and {len(fct_telemetry)} telemetry points.")
    return dim_races, dim_drivers, fct_laps, fct_telemetry, dim_highlights

if __name__ == "__main__":
    # Test script locally
    races, drivers, laps, telemetry, highlights = extract_and_transform(2023, "Bahrain")
    print(races.head())
    print(drivers.head())
