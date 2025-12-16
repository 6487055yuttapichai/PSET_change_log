import io
import panel as pn
import psycopg2
from io import BytesIO, StringIO
import pandas as pd
from collections import defaultdict
from config.dev import _HOST, _PORT, _UID, _PWD, _DB
from shared.downloads import excel_format
from shared.tdm_logging import logger, log_error, class_method_name
from shared.sql import PGSQL
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



pgsql = PGSQL()

class PSET_change_log_Backend:
    def __init__(self):
        # ======================
        # STATE
        # ======================
        self.selected_row = {"row": None}

        # ======================
        # EDIT SECTION
        # ======================
        self.selected_info = pn.pane.Markdown("", sizing_mode="stretch_width")

        self.edit_name = pn.widgets.TextInput(name="User :", placeholder="Enter User name")
        self.edit_note = pn.widgets.TextAreaInput(name="Note :   ", height=150)

        self.btn_save_edit = pn.widgets.Button(name="Save", button_type="primary")
        self.btn_cancel_edit = pn.widgets.Button(name="Cancel")

        self.pop_up_edit_form = pn.layout.Modal(
            pn.Column(
                pn.pane.Markdown("### Edit Note"),
                self.selected_info,
                self.edit_name,
                self.edit_note,
                pn.Row(self.btn_save_edit, self.btn_cancel_edit)
            ),
            open=False,
            width=1000,
            height=500
        )

        self.btn_cancel_edit.on_click(lambda e: setattr(self.pop_up_edit_form, "open", False))

        # ======================
        # INSERT SECTION
        # ======================
        self.insert_button = pn.widgets.Button(name="New Log ", button_type="success", width=50)

        self.Controller_ID_input = pn.widgets.TextInput(placeholder="Enter ControllerID", width=250)
        self.PSET_input = pn.widgets.TextInput(placeholder="Enter PSET", width=250)

        self.Model_input = pn.widgets.AutocompleteInput(
            options=self.get_model_list(),
            placeholder='Enter Model',
            width=250,
        )

        self.Station_input = pn.widgets.Select(
            groups=self.get_station_dict(),
            width=250
        )

        self.Name_input = pn.widgets.TextInput(placeholder="Enter User name", width=300)
        self.Note_input = pn.widgets.TextAreaInput(name="Note :", height=150)

        self.btn_save_insert = pn.widgets.Button(name="Save", button_type="primary")
        self.btn_cancel_insert = pn.widgets.Button(name="Cancel")

        self.pop_up_insert_form = pn.layout.Modal(
            pn.Column(
                pn.pane.Markdown("### insert form"),
                pn.Row(pn.pane.Markdown("**Controller ID :**", width=80), self.Controller_ID_input),
                pn.Row(pn.pane.Markdown("**Station :**", width=80), self.Station_input),
                pn.Row(pn.pane.Markdown("**Model :**", width=80), self.Model_input),
                pn.Row(pn.pane.Markdown("**PSET :**", width=80), self.PSET_input),
                pn.Row(pn.pane.Markdown("**User :**", width=80), self.Name_input),
                self.Note_input,
                pn.Row(self.btn_save_insert, self.btn_cancel_insert)
            ),
            open=False,
            width=1000,
            height=600
        )

        self.insert_button.on_click(lambda e: setattr(self.pop_up_insert_form, "open", True))
        self.btn_cancel_insert.on_click(lambda e: setattr(self.pop_up_insert_form, "open", False))

        # ======================
        # FILTER SECTION
        # ======================
        self.Station_filter = pn.widgets.MultiChoice(
            name='Select station',
            options=self.get_station_list(),
            visible=False,
            width=500
        )

        self.date_range_picker = pn.widgets.DateRangePicker(
            name='Select date range',
            visible=False,
            width=300
        )

        self.Refresh_while_acquirin_Checkbox = pn.widgets.Checkbox(
            name="Refresh while acquiring data", value=False
        )
        self.Current_Week_Checkbox = pn.widgets.Checkbox(name="Current Week", value=True)
        self.Previous_Week_Checkbox = pn.widgets.Checkbox(name="Previous Week", value=True)
        self.All_Time_Warning_Checkbox = pn.widgets.Checkbox(
            name="All Time and Station (Warning)", value=False
        )

        self.Refresh_button = pn.widgets.Button(name="Refresh ", button_type="primary", width=100)

        # ======================
        # TABLE
        # ======================
        self.table = pn.widgets.Tabulator(
            buttons={"edit": '<button class="btn btn-dark btn-sm">Edit</button>'},
            pagination="local",
            show_index=False,
            disabled=True,
            page_size=20,
            height=400,
        )

        self.table.on_click(self.on_table_edit_click)

        # ======================
        # DOWNLOAD
        # ======================
        self.btn_table_csv_download = pn.widgets.FileDownload(
            callback=lambda: self.csv_download_callback(pd.DataFrame(self.table.value)),
            filename='PSET change log.csv',
            auto=True,
            embed=False,
            button_style='outline',
            button_type='success',
            label='CSV',
            height=32
        )

        self.btn_table_excel_download = pn.widgets.FileDownload(
            callback=lambda: self.excel_download_callback(pd.DataFrame(self.table.value)),
            filename='PSET change log.xlsx',
            embed=False,
            button_style='outline',
            button_type='success',
            label='Excel',
            height=32
        )

        # ======================
        # BIND EVENTS
        # ======================
        self.Refresh_button.on_click(self.Refresh_click)
        self.btn_save_insert.on_click(lambda e: self.save_click("insert"))
        self.btn_save_edit.on_click(lambda e: self.save_click("update"))
        self.All_Time_Warning_Checkbox.param.watch(
            self.on_all_time_change, "value"
        )

        self.Current_Week_Checkbox.param.watch(
            self.on_week_change, "value"
        )

        self.Previous_Week_Checkbox.param.watch(
            self.on_week_change, "value"
        )

        self.All_Time_Warning_Checkbox.param.watch(
            self.on_any_change, "value"
        )
        self.Current_Week_Checkbox.param.watch(
            self.on_any_change, "value"
        )
        self.Previous_Week_Checkbox.param.watch(
            self.on_any_change, "value"
        )

  
    # ======================
    # CALLBACKS
    # ======================
    def Refresh_click(self, event=None):
        sql = self.filter_by_checkbox(
            self.Refresh_while_acquirin_Checkbox.value,
            self.Current_Week_Checkbox.value,
            self.Previous_Week_Checkbox.value,
            self.All_Time_Warning_Checkbox.value,
            self.Station_filter.value,
            self.date_range_picker.value
        )

        rows = self.fetch_change_log(sql)
        self.table.value = pd.DataFrame(rows) if rows is not None else pd.DataFrame()

    def save_click(self, type):
        if type == "update" and self.selected_row.get("row"):
            row = self.selected_row["row"]
            self.update_Jasondata(
                row["Log Id"],
                self.edit_note.value,
                self.edit_name.value
            )
            self.pop_up_edit_form.open = False

        if type == "insert":
            self.insert_to_change_log(
                Controller_ID=self.Controller_ID_input.value,
                Station=self.Station_input.value,
                Model=self.Model_input.value,
                PSET=self.PSET_input.value,
                User=self.Name_input.value,
                Note=self.Note_input.value
            )
            self.pop_up_insert_form.open = False

        self.Refresh_click()

    def on_table_edit_click(self, event):
        if event.column != "edit":
            return

        df = pd.DataFrame(self.table.value)
        row = df.iloc[event.row].to_dict()
        self.selected_row["row"] = row

        self.selected_info.object = (
            f"**Log ID:** {row.get('Id')}  \n"
            f"**Controller ID:** {row.get('Controller id')}  \n"
            f"**Station:** {row.get('Station')}  \n"
            f"**Model:** {row.get('Model')}  \n"
            f"**PSET:** {row.get('Pset')}  \n"
            f"**Server Time:** {row.get('Server time')}"
        )

        self.edit_name.value = row.get("User", "") or ""
        self.edit_note.value = row.get("Note", "") or ""

        self.pop_up_edit_form.open = True

    def on_all_time_change(self, event):
        if event.new:
            self.Current_Week_Checkbox.value = False
            self.Previous_Week_Checkbox.value = False
        self.update_filter_visibility()

    def on_week_change(self, event):
        if event.new:
            self.All_Time_Warning_Checkbox.value = False
        self.update_filter_visibility()

    def update_filter_visibility(self):
        if (
            self.All_Time_Warning_Checkbox.value
            or self.Current_Week_Checkbox.value
            or self.Previous_Week_Checkbox.value
        ):
            self.Station_filter.visible = False
            self.date_range_picker.visible = False
        else:
            self.Station_filter.visible = True
            self.date_range_picker.visible = True

    def on_any_change(self, event):
        self.update_filter_visibility()
    
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
        query = """
        UPDATE dbo.change_log
        SET jsondata = jsondata || to_jsonb(
            json_build_object(
                'rev', (
                    SELECT COALESCE(MAX((item->>'rev')::int), 0)
                    FROM jsonb_array_elements(jsondata) AS item
                ) + 1,
                'note', :note,
                'user', :user,
                'timestamp', CURRENT_TIMESTAMP
            )
        )
        WHERE Id = :id
        """

        params = {
            "note": Note,
            "user": User,
            "id": Id
        }

        engine = create_engine(pgsql.connect_url(db=''), echo=False)

        try:
            with engine.begin() as conn:
                conn.execute(text(query), params)
        except SQLAlchemyError as e:
            logger.error(f"| Error updating item {Id}: {e}")

    def insert_to_change_log(self, Controller_ID, Station, Model, PSET, User, Note):
        query = """
        INSERT INTO dbo.change_log
            (Controller_Id, Station, Model, PSET, JsonData)
        VALUES
            (
                :controller_id,
                :station,
                :model,
                :pset,
                jsonb_build_array(
                    jsonb_build_object(
                        'rev', 0,
                        'user', :user,
                        'note', :note,
                        'timestamp', CURRENT_TIMESTAMP
                    )
                )
            );
        """

        params = {
            "controller_id": Controller_ID,
            "station": Station,
            "model": Model,
            "pset": PSET,
            "user": User,
            "note": Note
        }

        engine = create_engine(pgsql.connect_url(db=None), echo=False)

        try:
            with engine.begin() as conn:
                conn.execute(text(query), params)
        except SQLAlchemyError as e:
            logger.error(f"| Error inserting item {Controller_ID}: {e}")

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
        workbook = excel_format(df, "PSET")
        output = io.BytesIO()
        workbook.save(output)
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
        elif len(station) > 0  and date is not None:
            if "All Station" not in station:
                stations = ", ".join(f"'{s}'" for s in station)
                where_clauses.append(f"c.Station IN ({stations})")
            
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp >= '{date[0]}'")
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp <  '{date[1]}'")
        
        # station only
        elif len(station) > 0 :
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
        query = """
            SELECT DISTINCT "station"
            FROM dbo.tool_psets_models
            ORDER BY "station";
        """

        engine = create_engine(pgsql.connect_url(db=None), echo=False)

        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                station_list = [row[0] for row in result]

            station_list.insert(0, "All Station")
            return station_list

        except SQLAlchemyError as e:
            logger.error(f"| Error get_station_list | {e}")
            return []

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
        query = """
            SELECT DISTINCT "modelofvehicle"
            FROM dbo.tool_psets_models
            ORDER BY "modelofvehicle";
        """

        engine = create_engine(pgsql.connect_url(db=None), echo=False)

        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                model_list = [row[0] for row in result]

            return model_list

        except SQLAlchemyError as e:
            logger.error(f"| Error get_model_list | {e}")
            return []