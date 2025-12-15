import panel as pn
import pandas as pd
from pathlib import Path
from apps.app_PSET_change_log.PSET_change_log import PSET_change_log_Backend
from shared.tdm_logging import logger, log_error, class_method_name
from functools import partial
import datetime as dt

raw_css =[
    """
    label {
        position: relative;
        display: inline-block;
        &:before {
            content: '';
            height: ($height - 5) + px;
            position: absolute;
            right: 7px;
            top: 3px;
            width: 22px;

            //background: -webkit-linear-gradient(#fff, #f0f0f0);
            //background: -moz-linear-gradient(#fff, #f0f0f0);
            //background: linear-gradient(#f5f5f5, #e0e0e0);
            background: #fff; //for Firefox in Android

            border-top-right-radius: 3px;
            border-bottom-right-radius: 3px;
            pointer-events: none;
            display: block;
        }
        &:after {
            content: " ";
            position: absolute;
            right: 15px;
            top: 46%;
            margin-top: -3px;
            z-index: 2;
            pointer-events: none;
            width: 0;
            height: 0;
            border-style: solid;
            border-width: 6.9px 4px 0 4px;
            border-color: #aaa transparent transparent transparent;
            pointer-events: none;
        }
        select {
            -webkit-appearance: none;
            -moz-appearance: none;
            appearance: none;
            padding: 0 30px 0 10px;

            border: 1px solid #e0e0e0;
            border-radius: 3px;
            line-height: $height + px;
            height: $height + px;
            //box-shadow: inset 1px 1px 1px 0px rgba(0, 0, 0, 0.2);
            background: #fff;

            //min-width: 200px;
            margin: 0 5px 5px 0;
        }
        }
        //fix for ie 10 later
        select::-ms-expand {
        display: none;
        }
        """
]

pn.extension('tabulator',
             comms='default',
             loading_spinner='arcs',
             css_files=[pn.io.resources.CSS_URLS['font-awesome']],
             notifications=True,
             sizing_mode='stretch_width',
             template='material',
             safe_embed=True,
             dev=False,
             raw_css=raw_css,
             )

backend = PSET_change_log_Backend()

def PSET_change_log_page():
    #---------------------
    #Edit section setup
    selected_row = {"row": None}
    selected_info = pn.pane.Markdown("", sizing_mode="stretch_width")

    edit_name = pn.widgets.TextInput(name="User :", placeholder="Enter User name")
    edit_note = pn.widgets.TextAreaInput(name="Note :   ", height=150)

    btn_save_edit = pn.widgets.Button(name="Save", button_type="primary")
    btn_cancel_edit = pn.widgets.Button(name="Cancel")

    pop_up_edit_form = pn.layout.Modal(
        pn.Column(
            pn.pane.Markdown("### Edit Note"),
            selected_info,
            edit_name,
            edit_note,
            pn.Row(btn_save_edit, btn_cancel_edit)
        ),
        open=False,
        width=1000,
        height=500
    )

    def cancel_edit_click(ev):
        pop_up_edit_form.open = False

    btn_cancel_edit.on_click(cancel_edit_click)

    #---------------------
    #Insert section setup
    insert_button = pn.widgets.Button(name="New Log ", button_type="success",width=50)
    
    Station_insert_list = backend.get_station_dict()
    Controller_ID_input = pn.widgets.TextInput(placeholder="Enter ControllerID", width=250)
    PSET_input = pn.widgets.TextInput(placeholder="Enter PSET", width=250)
    
    model_list = backend.get_model_list()

    Model_input = pn.widgets.AutocompleteInput(
        options=model_list,   
        placeholder='Enter Model',
        width=250,
    )
    Station_input = pn.widgets.Select(groups=Station_insert_list,width=250)

    Name_input = pn.widgets.TextInput(placeholder="Enter User name",width=300)
    Note_input = pn.widgets.TextAreaInput(name="Note :", height=150)

    btn_save_insert = pn.widgets.Button(name="Save", button_type="primary")
    btn_cancel_insert = pn.widgets.Button(name="Cancel")

    pop_up_insert_form = pn.layout.Modal(
        pn.Column(
            pn.pane.Markdown("### insert form"),
            pn.Row(pn.pane.Markdown("**Controller ID :**", width=80),Controller_ID_input),
            pn.Row(pn.pane.Markdown("**Station :**", width=80),Station_input),
            pn.Row(pn.pane.Markdown("**Model :**", width=80),Model_input),
            pn.Row(pn.pane.Markdown("**PSET :**", width=80),PSET_input),
            pn.Row(pn.pane.Markdown("**User :**", width=80),Name_input),
            Note_input,
            pn.Row(btn_save_insert, btn_cancel_insert)
        ),
        open=False,
        width=1000,
        height=600
    )


    #---------------------
    # Insert button & pop up insert form
    def insert_click(event):
        pop_up_insert_form.open = True

    def cancel_insert_click(ev):
        pop_up_insert_form.open = False

    insert_button.on_click(insert_click)
    btn_cancel_insert.on_click(cancel_insert_click)

    
    #---------------------
    # Time check box
    Station_filter_list = backend.get_station_list()
    Station_filter  = pn.widgets.MultiChoice(name='Select station', value=[],
        options=Station_filter_list,visible=False, width=500)
    date_range_picker = pn.widgets.DateRangePicker(name='Select date range',visible=False,width=300)
    Refresh_while_acquirin_Checkbox = pn.widgets.Checkbox(name="Refresh while acquiring data", value=False)
    Current_Week_Checkbox = pn.widgets.Checkbox(name="Current Week", value=True)
    Previous_Week_Checkbox = pn.widgets.Checkbox(name="Previous Week", value=True)
    All_Time_Warning_Checkbox = pn.widgets.Checkbox(name="All Time and Station (Warning)", value=False)
    
    Generate_button = pn.widgets.Button(name="Refresh ", button_type="primary",width=100)

    btn_Confirm = pn.widgets.Button(name="Confirm", button_type="danger")
    btn_cancel_warning = pn.widgets.Button(name="Cancel")

    warning_popup = pn.layout.Modal(
        pn.Column(
            pn.pane.Markdown("### ⚠ WARNING"),
            pn.pane.Markdown("Are you sure to **Generate All Time and Station data**."),
            pn.Row(btn_Confirm, btn_cancel_warning)
        ),
        open=False,
        width=500,
        height=200
    )

    
    def confirm_warning_generate_all(event):
        warning_popup.open = False
        run_generate()

    def cancel_warning_click(ev):
        warning_popup.open = False

    btn_Confirm.on_click(confirm_warning_generate_all)
    btn_cancel_warning.on_click(cancel_warning_click)

    def on_all_time_change(event):
        if event.new:
            Current_Week_Checkbox.value = False
            Previous_Week_Checkbox.value = False
        update_filter_visibility()

    All_Time_Warning_Checkbox.param.watch(on_all_time_change, "value")

    def on_week_change(event):
        if event.new:  
            All_Time_Warning_Checkbox.value = False
        update_filter_visibility()

    Current_Week_Checkbox.param.watch(on_week_change, "value")
    Previous_Week_Checkbox.param.watch(on_week_change, "value")

    def update_filter_visibility():
        if (All_Time_Warning_Checkbox.value or
            Current_Week_Checkbox.value or
            Previous_Week_Checkbox.value):
            Station_filter.visible = False
            date_range_picker.visible = False
        else:
            Station_filter.visible = True
            date_range_picker.visible = True

    def on_any_change(event):
        update_filter_visibility()

    All_Time_Warning_Checkbox.param.watch(on_any_change, "value")
    Current_Week_Checkbox.param.watch(on_any_change, "value")
    Previous_Week_Checkbox.param.watch(on_any_change, "value")

    #---------------------
    # Save button in form
    def save_click(event,type):
        if type == "update":
            if selected_row.get("row") is None:
                pop_up_edit_form.open = False
                return

            logID = selected_row["row"]["Log Id"]

            # save to Database
            backend.update_Jasondata(logID, edit_note.value,edit_name.value)

            pop_up_edit_form.open = False
        if type == "insert":
            backend.insert_to_change_log(
                Controller_ID = Controller_ID_input.value,
                Station = Station_input.value, 
                Model = Model_input.value, 
                PSET = PSET_input.value, 
                User = Name_input.value, 
                Note = Note_input.value)
            
            Controller_ID_input.value = ""
            Station_input.value = None
            Model_input.value = ""
            PSET_input.value = ""
            Name_input.value = ""
            Note_input.value = ""

            pop_up_insert_form.open = False
            

        # auto refresh
        Generate_click()

    btn_save_insert.on_click(partial(save_click, type="insert"))
    btn_save_edit.on_click(partial(save_click, type="update"))


    #------------------------------------------
    ###########################################
    #------------------------------------------


    #---------------------
    # Data & Table
    table = pn.widgets.Tabulator(
        buttons={
            "edit": '<button class="btn btn-dark btn-sm">Edit</button>'
        },
        pagination="local",
        show_index=False,
        disabled=True,
        page_size=20,
        height=400,
    )
    

    def csv_from_table():
        df = pd.DataFrame(table.value)
        return backend.csv_download_callback(df)
    
    def excel_from_table():
        df = pd.DataFrame(table.value)
        return backend.excel_download_callback(df)


    #---------------------
    # Download button
    btn_table_csv_download = pn.widgets.FileDownload(callback=csv_from_table,
                                                     filename='PSET change log.csv',
                                                     auto=True,
                                                     embed=False,
                                                     button_style='outline',
                                                     button_type='success',
                                                     label='CSV',
                                                     height=32,
                                                     disabled=False
                                                     )
    
    btn_table_excel_download = pn.widgets.FileDownload(callback=excel_from_table,
                                                       filename='PSET change log.xlsx',
                                                       label='Excel',
                                                       embed=False,
                                                       button_style='outline',
                                                       button_type='success',
                                                       height=32,
                                                       disabled=False,
                                                       )

    
    # -----------------------
    # Select Callback
    def run_generate():
        sql = backend.filter_by_checkbox(
            Refresh_while_acquirin_Checkbox.value,
            Current_Week_Checkbox.value,
            Previous_Week_Checkbox.value,
            All_Time_Warning_Checkbox.value,
            Station_filter.value,
            date_range_picker.value
        )

        rows = backend.fetch_change_log(sql)
        
        if rows is not None and not rows.empty:
            df = pd.DataFrame(rows)
            table.value = df
        else:
            table.value = pd.DataFrame(columns=["Id", "Controller id", "Station", "Model", "Pset", "Server time", "User", "Note", "Rev"])

    def Generate_click(event=None):
        # show waring pop up for Genarate All Time data
        # if All_Time_Warning_Checkbox.value:
        #     warning_popup.open = True
        #     return  

        # normal case {Current Week,Previous Week}
        run_generate()

    Generate_button.on_click(Generate_click)

    

    #---------------------
    # Edit button & pop up edit form
    def on_table_edit_click(event):
        if event.column == "edit":
            df = pd.DataFrame(table.value)

            # Index of the selected row
            row_index = event.row
            
            if row_index is None or row_index >= len(df):
                return 

            # selected row data
            row = df.iloc[row_index].to_dict()
            # set var for save
            selected_row["row"] = row
            selected_row["index"] = row_index

            # add data to popup
            selected_info.object = (
                f"**Log ID:** {row.get('Id', '')}  \n"
                f"**Controller ID:** {row.get('Controller id', '')}  \n"
                f"**Station:** {row.get('Station', '')}  \n"
                f"**Model:** {row.get('Model', '')}  \n"
                f"**PSET:** {row.get('Pset', '')}  \n"
                f"**Server Time:** {row.get('Server time', '')}"
            )

            if row.get("User", "") is None:
                edit_name.value = ""
                edit_note.name = ""
            else:
                edit_name.value = row.get("User", "")
                edit_note.value = row.get("Note", "")

            pop_up_edit_form.open = True
    
    table.on_click(on_table_edit_click)


    # -----------------------
    # Load custom template
    template_path = Path('apps/app_PSET_change_log/templates/template_PSET_change_log.html')
    template_str = template_path.read_text(encoding="utf-8")
    template = pn.Template(template=template_str)


    # -----------------------
    # Header
    header_html = "<h4 class='page-title mb-0'>PSET change log</h4>"
    template.add_panel('header', pn.Row(pn.pane.HTML(header_html)))


    # -----------------------
    # Extract controls
    controls_column = pn.Column(
        Generate_button,
        Station_filter,
        date_range_picker,
        Refresh_while_acquirin_Checkbox,
        Current_Week_Checkbox,
        Previous_Week_Checkbox,
        All_Time_Warning_Checkbox,
        pop_up_insert_form,
        warning_popup,
        height = 300
    )
    template.add_panel('PSET_change_log', controls_column)
    template.add_panel('xl_download', btn_table_excel_download)
    template.add_panel('csv_download', btn_table_csv_download)

    PSET_change_log_table = pn.Column(
        insert_button,
        table,
        pop_up_edit_form)
    template.add_panel('PSET_change_log_table', PSET_change_log_table)
    # pn.state.onload(lambda: pull_change_log(None))
    return template

# Serve the app
app = PSET_change_log_page()
app.servable()
