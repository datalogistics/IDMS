#!/usr/bin/env python

from distutils.core import setup

setup(name        = 'unisdispatcher',
      version     = '0.1.0',
      description = "unisdispatcher provides multiple methods of warming and managing files curated in unis.",
      author      = "Jeremy Musser",
      packages    = ['exnodemanager', 'exnodemanager.policy', 'exnodemanager.protocol', 'exnodemanager.web'],
      entry_points = {
          'console_scripts': [
              'unisdispatcherd = exnodemanager.app:main',
          ]
      },
  )
