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
        # pn.Row(
        #     backend.Refresh_button,
        #     pn.pane.Markdown("### The data is refreshed every 10 minutes.")
        # ),
        backend.Refresh_button,
        backend.Station_filter,
        backend.date_range_picker,
        backend.Refresh_while_acquirin_Checkbox,
        backend.Current_Week_Checkbox,
        backend.Previous_Week_Checkbox,
        backend.All_Time_Warning_Checkbox,
        pn.pane.Markdown("** Monitoring for changes every 10 minutes."),
        # backend.pop_up_insert_form,
        # height=300
        sizing_mode='stretch_width'
    )
    template.add_panel('PSET_change_log', controls_column)
    template.add_panel('xl_download', backend.btn_table_excel_download)
    template.add_panel('csv_download', backend.btn_table_csv_download)

    PSET_change_log_table = pn.Column(
        # backend.insert_button,
        backend.table,
        backend.pop_up_edit_form,
        backend.pop_up_Rev)
    template.add_panel('PSET_change_log_table', PSET_change_log_table)
    return template

# Serve the app
app = PSET_change_log_page()
app.servable()
