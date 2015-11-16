#!/usr/bin/env python

from setuptools import setup

setup(name        = 'unisdispatcher',
      version     = '0.2.1',
      description = "unisdispatcher provides multiple methods of warming and managing files curated in unis.",
      author      = "Jeremy Musser",
      packages    = ['exnodemanager', 'exnodemanager.policy', 'exnodemanager.filter', 'exnodemanager.protocol', 'exnodemanager.protocol.ibp', 'exnodemanager.web'],
      entry_points = {
          'console_scripts': [
              'unisdispatcherd = exnodemanager.app:main',
          ]
      },
  )
