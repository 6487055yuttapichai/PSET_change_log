import assets.static.css as css
import config.dev as dev
import config.prod as prod

import panel as pn
import param

# from lib.tdm_logging import log_error

NOTIFY_ERR_MSG = 'Critical error occurred. Try again and contact the Catalyst Technical Support'
LOAD_MSG = 'Gathering and analyzing data. . .'

def db_connection(env: str = 'dev') -> dict:
    conn = {'host': '',
            'uid': '',
            'pwd': '',
            'port': '',
            'db': '',}
    try:
        if env == 'dev':
            conn = {
                'host': dev._HOST,
                'uid': dev._UID,
                'pwd': dev._PWD,
                'port': dev._PORT,
                'db': dev._DB,
            }
        else:
            conn = {
                'host': prod._HOST,
                'uid': prod._UID,
                'pwd': prod._PWD,
                'port': prod._PORT,
                'db': prod._DB,
            }
        return conn
    except (KeyError, ValueError) as err:
        return conn

class Configuration(param.Parameterized):
    theme = param.String()
    site = param.String(default="TDM Report Portal")
    site_url = param.String(default="/torque_data_review")
    favicon = param.String(default="/assets/img/favicon.ico")
    title = param.String()
    url = param.String()
    logo = param.String()
    theme_toggle = param.Boolean(False)
    background_color = param.Color()
    neutral_color = param.Color()
    accent_base_color = param.Color()
    header_color = param.Color()
    header_accent_base_color = param.Color("white")
    header_background = param.Color()
    shadow = param.Boolean(False)
    main_max_width = param.String("100%")
    sidebar_width = param.Integer(310)
    ace_theme = param.String()
    active_header_background = param.String()

    def __init__(self, **params):
        super().__init__(**params)


# def template_config(config: Configuration):
#     template = pn.Template(
#         site=config.site,
#         site_url=config.site_url,
#         title=config.title,
#         favicon=config.favicon,
#         theme_toggle=config.theme_toggle,
#         shadow=config.shadow,
#         main_max_width=config.main_max_width
#     )
#     return template
