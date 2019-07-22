from appdirs import user_data_dir
from pathmagic import Dir
import sqlhandler

appdata = Dir(user_data_dir(appname=sqlhandler.__name__, appauthor="python_module_data"))
