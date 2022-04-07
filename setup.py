from __future__ import print_function
from setuptools import setup, Command
import os

import sys

version = "0.2.dev0"

sys.path.append(".")
if sys.version_info[0] < 3:
    print("------------------------------")
    print("Must use python 3.0 or greater", file=sys.stderr)
    print("Found python verion ", sys.version_info, file=sys.stderr)
    print("Installation aborted", file=sys.stderr)
    print("------------------------------")
    sys.exit()

setup(
    name="idms",
    version=version,
    packages=["idms", "idms.handlers", "idms.lib", "idms.lib.assertions", "idms.plugins", "idms.lib.utils"],
    data_files=[("idms/public", ["idms/public/index.html", "idms/public/health.html"]),
                ("idms/public/js", ["idms/public/js/droparea.js", "idms/public/js/manager.js",
                                    "idms/public/js/jquery-3.4.1.min.js", "idms/public/js/health.js"]),
                ("idms/public/css", ["idms/public/css/Lato-Regular",
                                     "idms/public/css/Lato-Bold",
                                     "idms/public/css/Lato-Italic",
                                     "idms/public/css/Lato-BoldItalic",
                                     "idms/public/css/main.css"]),
                ("idms/public/semantic", ["idms/public/semantic/fonts.css",
                                          "idms/public/semantic/icons.eot",
                                          "idms/public/semantic/icons.svg",
                                          "idms/public/semantic/icons.ttf",
                                          "idms/public/semantic/icons.woff",
                                          "idms/public/semantic/icons.woff2",
                                          "idms/public/semantic/semantic.min.css",
                                          "idms/public/semantic/semantic.min.js"]),
                ("idms/public/img", ["idms/public/img/favicon.png"])],
    author="Jeremy Musser, Ezra Kissel",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    dependency_links=[
        "git+https://github.com/periscope-ps/lace.git/@master#egg=lace",
        "git+https://github.com/periscope-ps/unisrt.git/@develop#egg=unisrt",
        "git+https://github.com/datalogistics/libdlt.git/@develop#egg=libdlt"
    ],
    install_requires=[
        "falcon>=3.0.0",
        "unisrt",
        "lace",
        "libdlt",
        "shapely"
    ],
    entry_points = {
        'console_scripts': [
            'idms = idms.app:main'
        ]
    }
)
