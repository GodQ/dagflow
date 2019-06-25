import sys
import setuptools
import re

install_requires = list()
dependency_links = list()
uninstalled_pkgs = list()
with open("requirements.txt", "r", encoding='utf-8') as f:
    for line in f:
        if re.match("^[\w_-]*(==(\d+\.)*\d+){0,1}$", line.strip()):
            install_requires.append(line.strip())
        elif line.strip() and not line.strip().startswith("#"):
            uninstalled_pkgs.append(line.strip())

for i, pkg in enumerate(uninstalled_pkgs):
    if pkg.startswith("-e "):
        dependency_links.append(pkg.lstrip("-e").strip())
        del uninstalled_pkgs[i]

setuptools.setup(name='dagflow',
      version='0.0.2',
      description='DAG Task Schedule Service',
      long_description='DAG Task Schedule Service',
      author='GodQ',
      author_email='quchuanhao@gmail.com',
      url='https://github.com/godq/dagflow',
      keywords=['dag', 'flow', 'mq', 'task', "schedule", "dagflow"],
      classifiers=[
          'Environment :: Console',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Environment :: Web Environment',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Topic :: Utilities',
          'Topic :: System :: Monitoring',
      ],
      packages=setuptools.find_packages(exclude=['tests*']),
      license='Apache License, Version 2.0',
      install_requires=install_requires,
      dependency_links=dependency_links,
      include_package_data=True,
      zip_safe=True,
      entry_points={
        'console_scripts': [
            'dagflow = dagflow.manage:main',
        ],
      },
)
