import configparser
import contextlib
import os
import shutil
import sys

import mib2high as m2high
import mib2tsd as m2tsd
import poifix
from version import VERSION


def resource_path(rpath):
    """ Get the path to a resource relative to this script.
        If using pyinstaller then sys._MEIPASS will be set,
        otherwise just use __file__
    """

    bpath = os.path.dirname(__file__)
    return os.path.join(bpath, rpath)


def create_mypois(config_file):
    config = configparser.ConfigParser()
    config.optionxform = str  # make case sensitive

    config.read_file(open(config_file))

    dest = None
    mib2high = None
    mib2tsd = None
    skipmib2std = False
    skipmib2high = False

    if 'General' in config:
        if 'OutputDirectory' in config['General']:
            dest = config['General']['OutputDirectory']
        skipmib2std = config.getboolean('General', 'SkipMIB2STD', fallback=False)
        skipmib2high = config.getboolean('General', 'SkipMIB2HIGH', fallback=False)

    if dest is None:
        print("Failed to find OutputDirectory in configuration file")
        sys.exit(1)
    else:
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(dest)
    shutil.copytree(resource_path('template'), dest)

    if not skipmib2high:
        mib2high = m2high.MIB2HIGH(os.path.join(dest, 'PersonalPOI', 'MIB2', 'MIB2HIGH'))
        mib2high.open()

    if not skipmib2std:
        mib2tsd = m2tsd.MIB2TSD(os.path.join(dest, 'PersonalPOI', 'MIB2TSD'))
        mib2tsd.open()

    for section in config.sections():
        if section != 'General':
            if 'Disabled' in config[section] and config.getboolean(section, 'Disabled'):
                print("Disabled")
                continue

            if not skipmib2high:
                mib2high.read(config, section)
            if not skipmib2std:
                mib2tsd.read(config, section)

    if not skipmib2high:
        mib2high.close()
        poifix.fix(mib2high.dest)

    if not skipmib2std:
        mib2tsd.close()
        poifix.fix(mib2tsd.dest)


def main():
    cfg = resource_path('config.ini')
    if len(sys.argv) >= 2:
        cfg = sys.argv[1]
    create_mypois(cfg)


if __name__ == "__main__":
    sys.exit(main())
