import sys
import os

path = os.path.dirname(sys.modules[__name__].__file__)
path = os.path.join(path, "..")
sys.path.insert(0, path)

import aw_importer_ios_fitness

aw_importer_ios_fitness.main()
