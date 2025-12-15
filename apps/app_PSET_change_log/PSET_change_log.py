import json
import psycopg2
from io import BytesIO, StringIO
import pandas as pd
from collections import defaultdict
from config.dev import _HOST, _PORT, _UID, _PWD, _DB
from shared.tdm_logging import logger, log_error, class_method_name
from shared.sql import PGSQL
from shared.downloads import excel_format
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side


pgsql = PGSQL()

class PSET_change_log_Backend:
    def _pg_connect(self):
        conn = psycopg2.connect(
            host=_HOST,
            port=_PORT,
            dbname=_DB,
            user=_UID,
            password=_PWD
        )
        conn.autocommit = False
        return conn
    
    def fetch_change_log(self,sql) -> pd.DataFrame:  
        summary_df = pd.DataFrame()
        try:
            summary_df = pgsql.sql_to_df(query=sql, db='PSET', mod='PSET_data')
            latest_revs = []
            for i, row in summary_df.iterrows():
                raw_json = row.get("jsonitem")
                # Case NULL
                if raw_json is None:
                    latest_revs.append({
                        "server time": None,
                        "user": None,
                        "note": None,
                        "rev": None
                    })
                    continue
                
                latest_revs.append({
                    "server time": raw_json.get("timestamp"),
                    "user": raw_json.get("user"),
                    "note": raw_json.get("note"),
                    "rev": raw_json.get("rev")
                })

            latest_df = pd.DataFrame(latest_revs)
            summary_df = pd.concat([summary_df, latest_df], axis=1)
            summary_df = summary_df.drop(columns=["jsonitem"])

            # formata date
            try:
                summary_df["server time"] = pd.to_datetime(
                    summary_df["server time"],
                    utc=True,
                    errors="coerce"
                )
                summary_df["server time"] = summary_df["server time"].dt.tz_localize(None)

                # format as string
                summary_df["server time"] = summary_df["server time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
            
            summary_df.columns = [col.capitalize() for col in summary_df.columns]
            summary_df.columns = [col.replace("_", " ") for col in summary_df.columns]
            summary_df.rename(columns={'Id': 'Log Id'}, inplace=True)

            return summary_df
        
        except Exception as ex:
            logger.error(f"| Exception | {str(ex)}")
            return summary_df

    def update_Jasondata(self, Id, Note, User):
        conn = self._pg_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE dbo.change_log
                SET 
                    jsondata = jsondata || to_jsonb(
                    json_build_object(
                        'rev', (
                            SELECT COALESCE(MAX((item->>'rev')::int), 0)
                            FROM jsonb_array_elements(jsondata) AS item
                        ) + 1,
                        'note', %s,
                        'user', %s,
                        'timestamp', CURRENT_TIMESTAMP
                    )
                )
                WHERE Id = %s;
                """,
                (Note,User,Id)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"| Error updating item {Id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_to_change_log(self, Controller_ID, Station, Model, PSET, User, Note):
        conn = self._pg_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO dbo.change_log
                    (Controller_Id, Station, Model, PSET, JsonData)
                VALUES
                    (
                        %s, %s, %s, %s,
                        jsonb_build_array(
                            jsonb_build_object(
                                'rev', 0,
                                'user', %s,
                                'note', %s,
                                'timestamp', CURRENT_TIMESTAMP
                            )
                        )
                    );
                """,
                (Controller_ID, Station, Model, PSET, User, Note)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"| Error updating item {Controller_ID}: {e}")
        finally:
            cursor.close()
            conn.close()

    def csv_download_callback(self,df):
        try:
            if df is None or df.empty:
                logger.warning("CSV Download: No data available")
                return None
            logger.info(f"CSV Download triggered: {len(df)} rows")

            df_formatted = df.copy()
            csv_data = df_formatted.to_csv(index=False)
            
            return StringIO(csv_data)

        except Exception as e:
            log_error(e)
            return None
        
    def excel_download_callback(self, df):
        # set output
        output = BytesIO()

        # create workbook
        wb = Workbook()
        wb.remove(wb.active)
        ws = wb.create_sheet(title="PSET change log")

        # --------------------------
        # WRITE HEADER
        # --------------------------
        headers = list(df.columns)

        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=col_idx, value=header)

            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.font = Font(size=11, bold=True, color='FFFFFFFF')
            cell.fill = PatternFill(start_color='006EB8', end_color='006EB8', fill_type='solid')

        # --------------------------
        # WRITE DATA ROWS
        # --------------------------
        for r_idx, row in enumerate(df.itertuples(index=False), start=2):
            for c_idx, value in enumerate(row, start=1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        # --------------------------
        # AUTO-WIDTH COLUMN
        # --------------------------
        for column_cells in ws.columns:
            length = max(len(str(cell.value)) if cell.value else 0 for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = length + 2

        # --------------------------
        # SAVE TO BYTES
        # --------------------------
        wb.save(output)
        output.seek(0)

        return output
    
    def filter_by_checkbox(
        self,
        Refresh_while_acquiring_Checkbox,
        Current_Week_Checkbox,
        Previous_Week_Checkbox,
        All_Time_Warning_Checkbox,
        station,
        date
    ):
        base_sql = """
            SELECT 
                c.Id,
                c.Controller_Id,
                c.Station,
                c.Model,
                c.PSET,
                elem.value AS jsonitem
            FROM dbo.change_log c
            CROSS JOIN LATERAL (
                SELECT value
                FROM jsonb_array_elements(c.JsonData)
                ORDER BY (value->>'timestamp')::timestamp DESC
                LIMIT 1
            ) elem
        """

        where_clauses = []

        # All time → no where clause
        if All_Time_Warning_Checkbox:
            pass

        # Current + Previous week
        elif Current_Week_Checkbox and Previous_Week_Checkbox:
            where_clauses.append("""
                (elem.value->>'timestamp')::timestamp 
                    >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'
            """)

        # Current only
        elif Current_Week_Checkbox:
            where_clauses.append("""
                (elem.value->>'timestamp')::timestamp 
                    >= date_trunc('week', CURRENT_DATE)
            """)

        # Previous only
        elif Previous_Week_Checkbox:
            where_clauses.append("""
                (elem.value->>'timestamp')::timestamp 
                    >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'
                AND (elem.value->>'timestamp')::timestamp 
                    <  date_trunc('week', CURRENT_DATE)
            """)

        # station & date
        elif station is not None and date is not None:
            if "All Station" not in station:
                stations = ", ".join(f"'{s}'" for s in station)
                where_clauses.append(f"c.Station IN ({stations})")
            
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp >= '{date[0]}'")
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp <  '{date[1]}'")
        
        # station only
        elif station is not None :
            if "All Station" not in station:
                stations = ", ".join(f"'{s}'" for s in station)
                where_clauses.append(f"c.Station IN ({stations})")

        # date only
        elif date is not None :
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp >= '{date[0]}'")
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp <  '{date[1]}'")
        
        
        # Build where
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        final_sql = f"""
            {base_sql}
            {where_sql}
            ORDER BY c.Id DESC;
        """

        return final_sql

    def get_station_list(self):
        conn = self._pg_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                    SELECT DISTINCT Station
                    FROM dbo.tool_psets_models;
                """
            )
            rows = cursor.fetchall()  

            # save result as a list
            station_list = sorted(row[0] for row in rows)
            station_list.insert(0, "All Station")
            return station_list
        except Exception as ex:
            logger.error(f"| Exception | {str(ex)}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_station_dict(self):
        # EX. output {'F1': ['F1-AA 123456', 'F1-BB 741852'], 'G1': ['G1-TT 951753', 'G1-PP 357159']}
        station_list = self.get_station_list()
        station_list.remove("All Station")
        groups = defaultdict(list)

        # set defalut none to list
        groups[""] = [""]

        for s in station_list:
            prefix = s[:2]   # check the first 2 characters
            groups[prefix].append(s)

        return dict(groups)
    
    def get_model_list(self):
        conn = self._pg_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                    SELECT DISTINCT ModelOfVehicle
                    FROM dbo.tool_psets_models;
                """
            )
            model_list = [row[0] for row in cursor.fetchall()]

            return model_list
        except Exception as ex:
            logger.error(f"| Exception | {str(ex)}")
            return []
        finally:
            cursor.close()
            conn.close()