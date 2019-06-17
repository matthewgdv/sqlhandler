__all__ = ["Alchemy", "TempManager"]

from .alchemy import Alchemy
from .utils import TempManager

from pathmagic import File

localres = File(__file__).dir.newdir("localres")
