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
from datetime import datetime
import json


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

        self.edit_name = pn.widgets.TextInput(name="User :", placeholder="Enter User name", width=360)
        self.edit_note = pn.widgets.TextAreaInput(name="Note :   ", height=150, width=360)

        self.btn_save_edit = pn.widgets.Button(name="Save", button_type="primary", width=170)
        self.btn_cancel_edit = pn.widgets.Button(name="Cancel", width=170)

        self.pop_up_edit_form = pn.layout.Modal(
            pn.Column(
                pn.pane.Markdown("### Edit Note"),
                self.selected_info,
                self.edit_name,
                self.edit_note,
                pn.Row(self.btn_save_edit, self.btn_cancel_edit)
            ),
            open=False,
            width=400,
            height=500
        )

        self.btn_cancel_edit.on_click(lambda e: setattr(self.pop_up_edit_form, "open", False))
        
        # ======================
        # REV0 SECTION
        # ======================
        self.rev_compare_body = pn.Column(sizing_mode="stretch_both")

        self.btn_download_Rev = pn.widgets.FileDownload(
            callback=self.download_rev_click,
            auto=True,
            label="Download all revision",
            filename="all_revision.csv",
            button_type="primary",
            width=170
        )
        self.btn_cancel_Rev = pn.widgets.Button(name="Cancel", width=170)

        self.pop_up_Rev = pn.layout.Modal(
            self.rev_compare_body,
            open=False,
            width=900,
            height=680
        )
        self.btn_cancel_Rev.on_click(lambda e: setattr(self.pop_up_Rev, "open", False))
        # ======================
        # FILTER SECTION
        # ======================
        self.Device_name_filter = pn.widgets.MultiChoice(
            name='Select Device name',
            options=self.get_Device_name_list(),
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
            name="All Time and Device", value=False
        )

        self.Refresh_button = pn.widgets.Button(name="Refresh ", button_type="primary", width=100)

        # ======================
        # TABLE
        # ======================
        self.table = pn.widgets.Tabulator(
            buttons={
                "edit": '<button class="btn btn-dark btn-lg">Edit</button>',
                "Rev0": '<button class="btn btn-secondary btn-sm">Compare Rev</button>',
            },
            pagination="local",
            show_index=False,
            disabled=True,
            page_size=20,
            height=800,
            theme = 'bootstrap5',
            header_align='center',
            layout="fit_data_table"
        )

        self.table.text_align = {
            "Log ID": "center",
            "PSET": "center",
            "Rev": "center",
        }


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
        self.Refresh_button.on_click(self.refresh_click)
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
    def refresh_click(self, event=None):
        sql = self.filter_by_checkbox(
            self.Refresh_while_acquirin_Checkbox.value,
            self.Current_Week_Checkbox.value,
            self.Previous_Week_Checkbox.value,
            self.All_Time_Warning_Checkbox.value,
            self.Device_name_filter.value,
            self.date_range_picker.value
        )

        rows = self.fetch_change_log(sql)
        rows = rows[rows["Rev"] != 0] # select only rev > 0

        rows = rows.rename(columns={
            "Id": "Log ID",
            "Controller id": "Controller ID",
            "Device name": "Device name",
            "Pset": "PSET",
            "Server time": "Server time",
            "User": "User",
            "Note": "Note",
            "Rev": "Rev",
            "Last time change": "Last time change",
        })
        self.table.value = pd.DataFrame(rows) if rows is not None else pd.DataFrame()

    def save_click(self, type):
        if type == "update" and self.selected_row.get("row"):
            row = self.selected_row["row"]
            self.update_Jasondata(
                row["Log ID"],
                self.edit_note.value,
                self.edit_name.value
            )
            self.pop_up_edit_form.open = False

        self.refresh_click()

    def download_rev_click(self):
        if not self.selected_row.get("row"):
            return None

        row = self.selected_row["row"]
        id = row["Log ID"]

        all_rev = self.fetch_detail_rec_all_rev(id)
        if all_rev is None or all_rev.empty:
            return None
        
        csv_data = all_rev.to_csv(index=False)

        self.pop_up_Rev.open = False
        return BytesIO(csv_data.encode("utf-8"))

    def on_table_edit_click(self, event):
        df = pd.DataFrame(self.table.value)
        row = df.iloc[event.row].to_dict()
        if event.column == "edit":
            self.selected_row["row"] = row
            self.set_info_for_edit(row)

            self.pop_up_edit_form.open = True

        elif event.column == "Rev0":
            self.selected_row["row"] = row
            self.compare_rev0(row['Log ID'])

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
            self.Device_name_filter.visible = False
            self.date_range_picker.visible = False
        else:
            self.Device_name_filter.visible = True
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
                        "rev": None,
                        "timeLastChange": None
                    })
                    continue
                
                latest_revs.append({
                    "server time": raw_json.get("timestamp"),
                    "user": raw_json.get("user"),
                    "note": raw_json.get("note"),
                    "rev": raw_json.get("rev"),
                    "Last Time Change": raw_json.get("timeLastChange")
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
            
            summary_df["Last Time Change"] = pd.to_datetime(
                summary_df["Last Time Change"],
                format="%Y-%m-%d:%H:%M:%S"
            )
            summary_df["Last Time Change"] = summary_df["Last Time Change"].dt.strftime("%Y-%m-%d %H:%M:%S")
            
            summary_df.columns = [col.capitalize() for col in summary_df.columns]
            summary_df.columns = [col.replace("_", " ") for col in summary_df.columns]
            summary_df['Id'] = summary_df['Id'].astype(str)

            return summary_df
        
        except Exception as ex:
            logger.error(f"| Exception | {str(ex)}")
            return summary_df

    def update_Jasondata(self, Id, Note, User):
        query = """
        UPDATE reporting.change_log cl
        SET jsondata = cl.jsondata || to_jsonb(
            jsonb_build_object(
                'rev', latest.rev + 1,
                'angle', latest.angle,
                'torque', latest.torque,
                'timeLastChange', latest."timeLastChange",
                'note', :note,
                'user', :user,
                'timestamp', CURRENT_TIMESTAMP
            )
        )
        FROM (
            SELECT
                (item->>'rev')::int AS rev,
                item->'angle' AS angle,
                item->'torque' AS torque,
                item->'timeLastChange' AS "timeLastChange"
            FROM reporting.change_log cl2,
                jsonb_array_elements(cl2.jsondata) AS item
            WHERE cl2.id = :id
            ORDER BY (item->>'rev')::int DESC
            LIMIT 1
        ) latest
        WHERE cl.id = :id;
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
        Device_name,
        date
    ):
        base_sql = """
            SELECT 
                c.Id,
                c.Controller_Id,
                c."device_name",
                c.PSET,
                elem.value AS jsonitem
            FROM reporting.change_log c
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
                    <=  date_trunc('week', CURRENT_DATE)
            """)

        # Device name & date
        elif len(Device_name) > 0  and date is not None:
            if "All Device" not in Device_name:
                Device_names = ", ".join(f"'{s}'" for s in Device_name)
                where_clauses.append(f"c.\"device_name\" IN ({Device_names})")
            
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp >= '{date[0]}'")
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp <= '{date[1]}'")
        
        # Device only
        elif len(Device_name) > 0 :
            if "All Device" not in Device_name:
                Device_names = ", ".join(f"'{s}'" for s in Device_name)
                where_clauses.append(f"c.\"device_name\" IN ({Device_names})")

        # date only
        elif date is not None :
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp >= '{date[0]}'")
            where_clauses.append(f"(elem.value->>'timestamp')::timestamp <= '{date[1]}'")
        
        
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

    def get_Device_name_list(self):
        query = """
            SELECT DISTINCT "device_name"
            FROM reporting.change_log
            ORDER BY "device_name";
        """

        engine = create_engine(pgsql.connect_url(db=None), echo=False)

        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                Device_name_list = [row[0] for row in result]

            Device_name_list.insert(0, "All Device")
            return Device_name_list

        except SQLAlchemyError as e:
            logger.error(f"| Error get_Device_name_list | {e}")
            return []

    def get_Device_name_dict(self):
        # EX. output {'F1': ['F1-AA 123456', 'F1-BB 741852'], 'G1': ['G1-TT 951753', 'G1-PP 357159']}
        Device_name_list = self.get_Device_name_list()
        Device_name_list.remove("All Device")
        groups = defaultdict(list)

        # set defalut none to list
        groups[""] = [""]

        for s in Device_name_list:
            prefix = s[:2]   # check the first 2 characters
            groups[prefix].append(s)

        return dict(groups)
    
    # def get_model_list(self):
    #     query = """
    #         SELECT DISTINCT "modelofvehicle"
    #         FROM dbo.tool_psets_models
    #         ORDER BY "modelofvehicle";
    #     """

    #     engine = create_engine(pgsql.connect_url(db=None), echo=False)

    #     try:
    #         with engine.connect() as conn:
    #             result = conn.execute(text(query))
    #             model_list = [row[0] for row in result]

    #         return model_list

    #     except SQLAlchemyError as e:
    #         logger.error(f"| Error get_model_list | {e}")
    #         return []
    
    def set_info_for_edit(self, row):
        self.selected_info.object = (
            f"**Log ID:** {row.get('Log ID')}  \n"
            f"**Controller ID:** {row.get('Controller ID')}  \n"
            f"**Device:** {row.get('Device name')}  \n"
            f"**PSET:** {row.get('PSET')}  \n"
            f"**Last Changed Time:** {row.get('Last time change')}  \n"
            f"**Server Time:** {row.get('Server time')}"
        )

        self.edit_name.value = row.get("User", "") or ""
        self.edit_note.value = row.get("Note", "") or ""

    def compare_rev0(self, id):
        rev = self.fetch_detail_rec_all_rev(id)

        if rev is None or rev.empty:
            return

        rev_indexes = list(range(len(rev)))
        
        self.rev_select_left = pn.widgets.Select(
            options=rev_indexes,
            value=0,                 # default = rev0
            width=160,
            align="start"
        )
        
        
        self.rev_select_right = pn.widgets.Select(
            options=rev_indexes,
            value=rev_indexes[-1],   # default = latest
            width=160,
            align="end"
        )

        def update_view(idx_left, idx_right):
            rev_left = rev.iloc[idx_left]
            rev_right = rev.iloc[idx_right]
            changed_colum = self.compare_2_df(rev_left, rev_right)

            df_left = rev_left.to_frame(name=f"Revision {idx_left}")
            df_right = rev_right.to_frame(name=f"Revision {idx_right}")

            table_border_style = [
                {"selector": "th", "props": [("border", "1px solid #555")]},
                {"selector": "td", "props": [("border", "1px solid #555")]}
            ]

            styled_left = (
                df_left.style
                .set_properties(
                    subset=pd.IndexSlice[changed_colum, :],
                    **{"background-color": "#ff6adf", "font-weight": "bold"}
                )
                .set_table_styles(table_border_style)
            )

            styled_right = (
                df_right.style
                .set_properties(
                    subset=pd.IndexSlice[changed_colum, :],
                    **{"background-color": "#6ae1ff", "font-weight": "bold"}
                )
                .set_table_styles(table_border_style)
            )

            pane_left = pn.pane.DataFrame(styled_left, height=430, sizing_mode="stretch_width")
            pane_right = pn.pane.DataFrame(styled_right, height=430, sizing_mode="stretch_width")

            return pn.Row(
                pn.Column(pane_left, sizing_mode="stretch_width"),
                pn.Column(pane_right, sizing_mode="stretch_width"),
            )
        
        compare_row = update_view(
            self.rev_select_left.value,
            self.rev_select_right.value
        )

        def _on_change(event):
            self.rev_compare_body[1] = update_view(
                self.rev_select_left.value,
                self.rev_select_right.value
            )

        self.rev_select_left.param.watch(_on_change, "value")
        self.rev_select_right.param.watch(_on_change, "value")

        header = pn.Column(
            pn.Row(
                pn.pane.Markdown("## Revision Comparison"),
                sizing_mode="stretch_width"
            ),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown("**Revision base**"),
                    self.rev_select_left,
                    width=220
                ),
                pn.Spacer(sizing_mode="stretch_width"), 
                pn.Column(   
                    pn.pane.Markdown("**Revision latest**", styles={"text-align": "right", "width": "100%"}),
                    self.rev_select_right,
                    width=220,
                    align="end"
                ),
                sizing_mode="stretch_width"
            ),
            sizing_mode="stretch_width"
        )

        self.rev_compare_body[:] = [
            header,
            compare_row,
            pn.Spacer(height=20),
            pn.Row(
                pn.Spacer(),
                self.btn_download_Rev,
                pn.Spacer(width=20),
                self.btn_cancel_Rev,
                pn.Spacer(),
                sizing_mode="stretch_width"
            )
        ]
        
        self.pop_up_Rev.open = True
    
    def compare_2_df(self, df_rev0, df_rev_latest):
        s0 = df_rev0.squeeze()
        s1 = df_rev_latest.squeeze()

        changed_fields = []

        for field in s0.index:
            v0 = s0.get(field)
            v1 = s1.get(field)

            if pd.isna(v0) and pd.isna(v1):
                continue

            if v0 != v1:
                changed_fields.append(field)

        return changed_fields
    
    def fetch_detail_rec_all_rev(self, id):
        Q = '''
        SELECT
            cl.id,
            cl.Controller_Id,
            cl."device_name",
            cl.PSET,
            j->>'rev'              AS rev,
            j->>'user'             AS "user",
            j->>'note'             AS note,
            j->>'timestamp'        AS "timestamp",
            j->>'timeLastChange'   AS "timeLastChange",
            j->'torque'->>'torque min'     AS "torque min",
            j->'torque'->>'torque target'  AS "torque target",
            j->'torque'->>'torque max'     AS "torque max",
            j->'angle'->>'angle min'       AS "angle min",
            j->'angle'->>'angle target'    AS "angle target",
            j->'angle'->>'angle max'       AS "angle max"
        FROM reporting.change_log cl
        JOIN jsonb_array_elements(cl.JsonData) AS j ON true
        WHERE cl.id = :id
        '''
        params = {"id": id}

        rev = pgsql.sql_to_df(query=Q, params=params,db='PSET', mod='PSET_data')
        # format time
        rev["timestamp"] = pd.to_datetime(
            rev["timestamp"],
            errors="coerce", 
            utc=True          
        )
        rev["timeLastChange"] = pd.to_datetime(
            rev["timeLastChange"],
            format="%Y-%m-%d:%H:%M:%S",
            errors="coerce"
        )
        rev["timestamp"] = rev["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        rev["timeLastChange"] = rev["timeLastChange"].dt.strftime("%Y-%m-%d %H:%M:%S")
        rev = rev.rename(columns={'id':'Log ID',
                                  'controller_id':'Controller ID',
                                  'device_name':'Device',
                                  'pset':'PSET',
                                  'rev':'Rev',
                                  'user': 'User',
                                  'note':'Note',
                                  'timestamp':'Server Time',
                                  'timeLastChange':'Time Last Change',
                                  'torque min':'Torque Min',
                                  'torque max':'Torque Max',
                                  'angle min':'Angle Min',
                                  'angle max':'Angle Max',
                                  'torque target':'Torque Target',
                                  'angle target':'Angle Target'})

        return(rev)
    