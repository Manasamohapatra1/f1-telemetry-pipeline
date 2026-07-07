import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import fastf1

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_DIR = os.getenv("FASTF1_CACHE_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "cache"))
os.makedirs(CACHE_DIR, exist_ok=True)
fastf1.Cache.enable_cache(CACHE_DIR)

def main():
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logger.error("DATABASE_URL not found!")
        return
        
    engine = create_engine(db_url)
    
    with engine.begin() as conn:
        logger.info("Executing schema updates...")
        conn.execute(text("ALTER TABLE dim_drivers ADD COLUMN IF NOT EXISTS headshot_url VARCHAR(255);"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_race_highlights (
                race_id VARCHAR(50) PRIMARY KEY REFERENCES dim_races(race_id),
                winner_driver_id VARCHAR(10),
                winner_time VARCHAR(50),
                pole_driver_id VARCHAR(10),
                pole_time VARCHAR(50),
                fastest_lap_driver_id VARCHAR(10),
                fastest_lap_time VARCHAR(50)
            );
        """))
        
    races_df = pd.read_sql("SELECT race_id, year, race_name FROM dim_races ORDER BY round_number", con=engine)
    logger.info(f"Found {len(races_df)} races to process highlights for.")
    
    for _, r_row in races_df.iterrows():
        race_id = r_row['race_id']
        year = int(r_row['year'])
        event = r_row['race_name']
        logger.info(f"Processing highlights for {race_id} ({year} {event})...")
        
        try:
            # 1. Race session for winner and headshots
            session_r = fastf1.get_session(year, event, 'R')
            session_r.load(laps=False, telemetry=False, weather=False, messages=False)
            
            # Update headshot URLs in dim_drivers
            if not session_r.results.empty:
                for _, res in session_r.results.iterrows():
                    drv_id = res.get('Abbreviation')
                    hs = res.get('HeadshotUrl')
                    if pd.notna(drv_id) and pd.notna(hs) and str(hs).startswith('http'):
                        with engine.begin() as conn:
                            conn.execute(text("UPDATE dim_drivers SET headshot_url = :hs WHERE driver_id = :drv"), {"hs": str(hs), "drv": str(drv_id)})
                            
                # Winner info
                winner = session_r.results.iloc[0]
                winner_id = winner.get('Abbreviation', '')
                winner_time = str(winner.get('Time', ''))
            else:
                winner_id = ''
                winner_time = ''
                
            # 2. Qualifying session for pole position
            pole_id = ''
            pole_time = ''
            try:
                session_q = fastf1.get_session(year, event, 'Q')
                session_q.load(laps=False, telemetry=False, weather=False, messages=False)
                if not session_q.results.empty:
                    pole = session_q.results.iloc[0]
                    pole_id = pole.get('Abbreviation', '')
                    for q_col in ['Q3', 'Q2', 'Q1', 'Time']:
                        if q_col in pole and pd.notnull(pole[q_col]):
                            pole_time = str(pole[q_col])
                            break
            except Exception as e:
                logger.warning(f"Could not load Qualifying for {race_id}: {e}")
                
            # 3. Fastest lap from database fct_laps
            fl_id = ''
            fl_time = ''
            fl_query = text("""
                SELECT driver_id, lap_number, lap_time_ms 
                FROM fct_laps 
                WHERE race_id = :rid AND lap_time_ms > 0 
                ORDER BY lap_time_ms ASC LIMIT 1
            """)
            with engine.connect() as conn:
                res_fl = conn.execute(fl_query, {"rid": race_id}).fetchone()
                if res_fl:
                    fl_id = res_fl[0]
                    fl_num = res_fl[1]
                    fl_ms = res_fl[2]
                    fl_seconds = fl_ms / 1000.0
                    fl_min = int(fl_seconds // 60)
                    fl_sec = fl_seconds % 60
                    fl_time = f"{fl_min:02d}:{fl_sec:06.3f} (Lap {fl_num})"
                    
            # 4. Upsert into dim_race_highlights
            upsert_query = text("""
                INSERT INTO dim_race_highlights (
                    race_id, winner_driver_id, winner_time, 
                    pole_driver_id, pole_time, 
                    fastest_lap_driver_id, fastest_lap_time
                ) VALUES (
                    :rid, :wid, :wt, :pid, :pt, :fid, :ft
                )
                ON CONFLICT (race_id) DO UPDATE SET
                    winner_driver_id = EXCLUDED.winner_driver_id,
                    winner_time = EXCLUDED.winner_time,
                    pole_driver_id = EXCLUDED.pole_driver_id,
                    pole_time = EXCLUDED.pole_time,
                    fastest_lap_driver_id = EXCLUDED.fastest_lap_driver_id,
                    fastest_lap_time = EXCLUDED.fastest_lap_time;
            """)
            with engine.begin() as conn:
                conn.execute(upsert_query, {
                    "rid": race_id, "wid": winner_id, "wt": winner_time,
                    "pid": pole_id, "pt": pole_time,
                    "fid": fl_id, "ft": fl_time
                })
            logger.info(f"Successfully saved highlights for {race_id}!")
            
        except Exception as e:
            logger.error(f"Error processing {race_id}: {e}")

if __name__ == "__main__":
    main()
