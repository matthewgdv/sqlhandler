from pathmagic import Dir
import sqlhandler

appdata = Dir.from_appdata(appname=sqlhandler.__name__, appauthor="python_module_data")
global_appdata = Dir.from_appdata(appname=sqlhandler.__name__, appauthor="python_module_data", user_specific=False)
