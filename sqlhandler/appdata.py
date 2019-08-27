from pathmagic import Dir
import sqlhandler

appdata = Dir.from_appdata(appname=sqlhandler.__name__)
global_appdata = Dir.from_appdata(appname=sqlhandler.__name__, user_specific=False)
