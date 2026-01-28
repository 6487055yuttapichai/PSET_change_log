import io
import panel as pn
from io import BytesIO, StringIO
import pandas as pd
from collections import defaultdict
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config.prod import _HOST, _PORT, _UID, _PWD, _DB
from shared.downloads import excel_format
from shared.tdm_logging import logger, log_error
from shared.sql import PGSQL


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

        
        self.Current_Week_Checkbox = pn.widgets.Checkbox(name="Current Week", value=True)
        self.Previous_Week_Checkbox = pn.widgets.Checkbox(name="Previous Week", value=True)
        self.All_Time_Warning_Checkbox = pn.widgets.Checkbox(
            name="All Time and Device", value=False
        )

        self.Refresh_button = pn.widgets.Button(name="Refresh ", button_type="primary", width=100)

        # ======================
        # TABLE
        # ======================
        self.title = {"log_id": "Log ID",
            "controller_id": "Controller ID",
            "device": "Device",
            "pset": "PSET",
            "time_last_change": "Time Last Change",
            "rev": "Rev",
            "rev_time": "Revision Time",
            "user": "User",
            "note": "Note",
            "createdat": "Registered Time",
            "torque_max": "Torque Max",
            "torque_target": "Torque Target",
            "torque_min": "Torque Min",
            "angle_max": "Angle Max",
            "angle_target": "Angle Target",
            "angle_min": "Angle Min"}
        text_align = {
            'log_id': 'center',
            'controller_id': 'center',
            'pset': 'center',
            'rev': 'center',
            'user': 'center',
            'note': 'center',
            'torque_min': 'center',
            'torque_target': 'center',
            'torque_max': 'center',
            'angle_min': 'center',
            'angle_target': 'center',
            'angle_max': 'center'
        }
        self.table = pn.widgets.Tabulator(
            buttons={
                "edit": '<button class="btn btn-dark btn-lg">Edit</button>',
                "Rev0": '<button class="btn btn-secondary btn-sm">Compare Rev</button>',
            },
            pagination="local",
            show_index=False,
            disabled=True,
            page_size=50,
            height=730,
            theme = 'bootstrap5',
            header_align='center',
            layout="fit_data_table",
            titles = self.title,
            text_align=text_align
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
            self.Current_Week_Checkbox.value,
            self.Previous_Week_Checkbox.value,
            self.All_Time_Warning_Checkbox.value,
            self.Device_name_filter.value,
            self.date_range_picker.value
        )

        rows = self.fetch_change_log(sql)

        rows["rev_time"] = rows["rev_time"].fillna("")
        self.table.value = pd.DataFrame(rows) if rows is not None else pd.DataFrame()

    def save_click(self, type):
        if type == "update" and self.selected_row.get("row"):
            row = self.selected_row["row"]
            self.edit_rev(
                row["log_id"],
                self.edit_note.value,
                self.edit_name.value
            )
            self.pop_up_edit_form.open = False

        self.refresh_click()

    def download_rev_click(self):
        if not self.selected_row.get("row"):
            return None

        row = self.selected_row["row"]
        id = row["log_id"]

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
            self.compare_rev0(row['log_id'])

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

            # format date time 
            summary_df["time_last_change"] = pd.to_datetime(
                summary_df["time_last_change"],
                format="%Y-%m-%d:%H:%M:%S"
            )
            summary_df["time_last_change"] = summary_df["time_last_change"].dt.strftime("%Y-%m-%d %H:%M:%S")

            summary_df["rev_time"] = pd.to_datetime(
                summary_df["rev_time"],
                format="%Y-%m-%d:%H:%M:%S"
            )
            summary_df["rev_time"] = summary_df["rev_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

            summary_df["createdat"] = pd.to_datetime(
                summary_df["createdat"],
                format="%Y-%m-%d:%H:%M:%S"
            )
            summary_df["createdat"] = summary_df["createdat"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # summary_df["update_at"] = pd.to_datetime(
            #     summary_df["update_at"],
            #     format="%Y-%m-%d:%H:%M:%S"
            # )
            # summary_df["update_at"] = summary_df["update_at"].dt.strftime("%Y-%m-%d %H:%M:%S")


            summary_df['log_id'] = summary_df['log_id'].astype(str)

            return summary_df
        
        except Exception as ex:
            logger.error(f"| Exception | {str(ex)}")
            return summary_df

    def edit_rev(self, id, Note, User):
        query = """
        INSERT INTO reporting.pset_change_log (
            log_id,
            controller_id,
            device,
            pset,
            time_last_change,
            rev,
            rev_time,
            "user",
            note,
            createdat,
            torque_min,
            torque_target,
            torque_max,
            angle_min,
            angle_target,
            angle_max
        )
        SELECT
            l.log_id,
            l.controller_id,
            l.device,
            l.pset,
            l.time_last_change,
            (r.max_rev + 1)::varchar,
            CURRENT_TIMESTAMP,
            NULLIF(TRIM(:user), ''),
            NULLIF(TRIM(:note), ''),
            l.createdat,
            l.torque_min,
            l.torque_target,
            l.torque_max,
            l.angle_min,
            l.angle_target,
            l.angle_max
        FROM reporting.pset_change_log l
        CROSS JOIN (
            SELECT COALESCE(MAX(rev::int), 0) AS max_rev
            FROM reporting.pset_change_log
            WHERE log_id = :log_id
        ) r
        WHERE l.log_id = :log_id
        ORDER BY l.rev::int DESC
        LIMIT 1
        """

        params = {
            "log_id":id,
            "note": Note,
            "user": User,
        }

        engine = create_engine(pgsql.connect_url(db=''), echo=False)

        try:
            with engine.begin() as conn:
                conn.execute(text(query), params)
        except SQLAlchemyError as e:
            logger.error(f"| Error updating item {id}: {e}")

    def csv_download_callback(self,df):
        try:
            if df is None or df.empty:
                logger.warning("CSV Download: No data available")
                return None
            logger.info(f"CSV Download triggered: {len(df)} rows")
            
            df = df.rename(columns=self.title)
            df_formatted = df.copy()
            csv_data = df_formatted.to_csv(index=False)
            
            return StringIO(csv_data)

        except Exception as e:
            log_error(e)
            return None
        
    def excel_download_callback(self, df):
        df = df.rename(columns=self.title)
        workbook = excel_format(df, "PSET")
        output = io.BytesIO()
        workbook.save(output)
        output.seek(0)

        return output
    
    def filter_by_checkbox(
        self,
        Current_Week_Checkbox,
        Previous_Week_Checkbox,
        All_Time_Warning_Checkbox,
        Device_name,
        date
    ):
        base_sql = """
            WITH ranked_logs AS (
                SELECT
                    l.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.log_id
                        ORDER BY l.rev DESC
                    ) AS rn
                FROM reporting.pset_change_log l
            )
            SELECT
                l.*
            FROM ranked_logs l
            WHERE l.rn = 1
        """

        where_clauses = []

        # All time → no extra where
        if All_Time_Warning_Checkbox:
            pass

        # Current + Previous week
        elif Current_Week_Checkbox and Previous_Week_Checkbox:
            where_clauses.append("""
                l.createdat >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'
            """)

        # Current only
        elif Current_Week_Checkbox:
            where_clauses.append("""
                l.createdat >= date_trunc('week', CURRENT_DATE)
            """)

        # Previous only
        elif Previous_Week_Checkbox:
            where_clauses.append("""
                l.createdat >= date_trunc('week', CURRENT_DATE) - INTERVAL '1 week'
                AND l.createdat <  date_trunc('week', CURRENT_DATE)
            """)

        # Device name & date
        elif len(Device_name) > 0 and date is not None:
            if "All Device" not in Device_name:
                device_names = ", ".join(f"'{s}'" for s in Device_name)
                where_clauses.append(f'l."device" IN ({device_names})')

            where_clauses.append(f"l.createdat >= '{date[0]}'")
            where_clauses.append(f"l.createdat <= '{date[1]}'")

        # Device only
        elif len(Device_name) > 0:
            if "All Device" not in Device_name:
                device_names = ", ".join(f"'{s}'" for s in Device_name)
                where_clauses.append(f'l."device" IN ({device_names})')

        # Date only
        elif date is not None:
            where_clauses.append(f"l.createdat >= '{date[0]}'")
            where_clauses.append(f"l.createdat <= '{date[1]}'")

        # Build where clause
        where_sql = ""
        if where_clauses:
            where_sql = " AND " + " AND ".join(where_clauses)

        final_sql = f"""
            {base_sql}
            {where_sql}
            ORDER BY l.log_id ASC;
        """

        return final_sql

    def get_Device_name_list(self):
        query = """
            SELECT DISTINCT "device"
            FROM reporting.pset_change_log
            ORDER BY "device";
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
    
    def set_info_for_edit(self, row):
        self.selected_info.object = (
            f"**Log ID:** {row.get('log_id')}  \n"
            f"**Controller ID:** {row.get('controller_id')}  \n"
            f"**Device:** {row.get('device')}  \n"
            f"**PSET:** {row.get('pset')}  \n"
            f"**Time Last Change:** {row.get('time_last_change')}  \n"
            f"**Registered Time:** {row.get('createdat')}"
        )

        self.edit_name.value = row.get("user", "") or ""
        self.edit_note.value = row.get("note", "") or ""

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
            log_id,
            controller_id,
            device,
            pset,
            time_last_change,
            rev,
            rev_time,
            "user",
            note,
            createdat,
            torque_min,
            torque_target,
            torque_max,
            angle_min,
            angle_target,
            angle_max
        FROM reporting.pset_change_log
        WHERE log_id = :log_id
        ORDER BY rev::int ASC;
        '''
        params = {"log_id": id}

        rev = pgsql.sql_to_df(query=Q, params=params,db='portal', mod='PSET_data')
        
        # format time
        rev["time_last_change"] = pd.to_datetime(
            rev["time_last_change"],
            format="%Y-%m-%d:%H:%M:%S"
        )
        rev["time_last_change"] = rev["time_last_change"].dt.strftime("%Y-%m-%d %H:%M:%S")

        rev["rev_time"] = pd.to_datetime(
            rev["rev_time"],
            format="%Y-%m-%d:%H:%M:%S"
        )
        rev["rev_time"] = rev["rev_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

        rev["createdat"] = pd.to_datetime(
            rev["createdat"],
            format="%Y-%m-%d:%H:%M:%S"
        )
        rev["createdat"] = rev["createdat"].dt.strftime("%Y-%m-%d %H:%M:%S")

        rev['log_id'] = rev['log_id'].astype(str)
        rev['torque_min'] = rev['torque_min'].map("{:.2f}".format)
        rev['torque_target'] = rev['torque_target'].map("{:.2f}".format)
        rev['torque_max'] = rev['torque_max'].map("{:.2f}".format)
        rev['angle_min'] = rev['angle_min'].map("{:.2f}".format)
        rev['angle_target'] = rev['angle_target'].map("{:.2f}".format)
        rev['angle_max'] = rev['angle_max'].map("{:.2f}".format)
        
        rev["rev"] = rev["rev"].fillna("")
        rev["rev_time"] = rev["rev_time"].fillna("")
        rev["user"] = rev["user"].fillna("")
        rev["note"] = rev["note"].fillna("")

        rev = rev.rename(columns={"log_id": "Log ID",    
            "controller_id": "Controller ID",
            "device": "Device name",
            "pset": "PSET",
            "time_last_change": "Time Last Change",
            "rev": "Rev",
            "rev_time": "Revision Time",
            "user": "User",
            "note": "Note",
            "createdat": "Registered Time",
            "torque_max": "Torque Max",
            "torque_target": "Torque Target",
            "torque_min": "Torque Min",
            "angle_max": "Angle Max",
            "angle_target": "Angle Target",
            "angle_min": "Angle Min"})
        
        return(rev)
    