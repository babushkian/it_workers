import sys
from pathlib import Path
# без добавления пути внешней папки этот скрипт не хочет грузить из нее модуль
p = Path(__file__).parent.parent
sys.path.append(str(p.absolute()))
