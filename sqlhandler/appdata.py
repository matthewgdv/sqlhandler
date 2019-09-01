from pathmagic import Dir
import sqlhandler

appdata = Dir.from_appdata(app_name=sqlhandler.__name__)
global_appdata = Dir.from_appdata(app_name=sqlhandler.__name__, systemwide=True)
