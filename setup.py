#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Setup.py module for the workflow's worker utilities.

All the workflow related code is gathered in a package that will be built as a
source distribution, staged in the staging area for the workflow being run and
then installed in the workers when they start running.

This behavior is triggered by specifying the --setup_file command line option
when running the workflow for remote execution.
"""

import logging
from distutils.command.build import build as _build
import subprocess

import setuptools
import os

logger = logging.getLogger(__name__)

# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name
  """A build command class that will be invoked during package install.

  The package built using the current setup.py will be staged and later
  installed in the worker using `pip install package'. This class will be
  instantiated during install for this specific scenario and will trigger
  running the custom commands specified.
  """
  sub_commands = _build.sub_commands + [('CustomCommands', None)]


# Some custom command to run during setup. The command is not essential for this
# workflow. It is used here as an example. Each command will spawn a child
# process. Typically, these commands will include steps to install non-Python
# packages. For instance, to install a C++-based library libjpeg62 the following
# two commands will have to be added:
#
#     ['apt-get', 'update'],
#     ['apt-get', '--assume-yes', install', 'libjpeg62'],
#
# First, note that there is no need to use the sudo command because the setup
# script runs with appropriate access.
# Second, if apt-get tool is used then the first command needs to be 'apt-get
# update' so the tool refreshes itself and initializes links to download
# repositories.  Without this initial step the other apt-get install commands
# will fail with package not found errors. Note also --assume-yes option which
# shortcuts the interactive confirmation.
#
# The output of custom commands (including failures) will be logged in the
# worker-startup log.

# Installing GDAL with its python bindings:
# GDAL must be installed using apt-get and then the path to it needs to be provided to
# the pip install of GDAL
# half the internet says to do this:
# ['pip3', 'install', '--global-option=build_ext', '--global-option="-I/usr/include/gdal"', 'gdal==2.2.3']
# but it doesn't work with recent pip, so set an environment variable instead
os.environ['CPLUS_INCLUDE_PATH']="/usr/include/gdal"
os.environ['C_INCLUDE_PATH']="/usr/include/gdal"

CUSTOM_COMMANDS = [
    ['apt-get', 'update'],
	['apt-get', 'upgrade'],
    ['apt-get', '--assume-yes', 'install', 'gdal-bin'],
	['apt-get', '--assume-yes', 'install', 'libgdal-dev'],
	['apt-get', '--assume-yes', 'install', 'python3-dev'], # may not be required
	# half the internet also says to do pip install gdal==`gdal-config --version` but at present apt-get
	# installs 2.1.2 and pip only has candidates for 2.1.0 and 2.1.3 so hack it instead
	['pip3', 'install', 'gdal==2.1.0']
]


class CustomCommands(setuptools.Command):
  """A setuptools Command class able to run arbitrary commands."""

  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def RunCustomCommand(self, command_list):
    print ('Running command: %s' % command_list)
    p = subprocess.Popen(
        command_list,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # Can use communicate(input='y\n'.encode()) if the command run requires
    # some confirmation.
    stdout_data, _ = p.communicate()
    print ('Command output: %s' % stdout_data)
    if p.returncode != 0:
      raise RuntimeError(
          'Command %s failed: exit code: %s' % (command_list, p.returncode))

  def run(self):
    for command in CUSTOM_COMMANDS:
      self.RunCustomCommand(command)


# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
    'numpy'
    # maybe we could put gdal here rather than pip installing it in the custom commands
    # but i am not sure which would run first or how the env variables would work and I
    # cba to spend any more time figuring it out
	]

#with open("requirements-worker.txt") as f:
#    worker_requirements = f.read().splitlines()

setuptools.setup(
    name="modis-acquisition-pipeline",
    version="0.1.0",
    description="Download MODIS tiles, mosaic, calculate output data, reproject to WGS84 GeoTiffs",
    author="Harry Gibson",
    author_email="harry.s.gibson@gmail.com",
    install_requires=REQUIRED_PACKAGES, # worker_requirements
    packages=setuptools.find_packages(),
    #py_modules=['dfndvi', 'ndvi'],
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
        }
    )
