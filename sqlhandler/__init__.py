__all__ = ["Alchemy", "TempManager", "Config"]

from pathmagic import File

resources = File(__file__).dir.newdir("localres")

if True:
    from .utils import TempManager
    from .alchemy import Alchemy
    from .config import Config
