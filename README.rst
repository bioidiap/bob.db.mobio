================
 MOBIO Database
================

This package contains the access API and descriptions for the `MOBIO
Database <http://www.idiap.ch/dataset/mobio/>`_. The actual raw data for
the database should be downloaded from the original URL. This package only
contains the `Bob <http://www.idiap.ch/software/bob/>`_ accessor methods to use
the DB directly from python, with our certified protocols.

You would normally not install this package unless you are maintaining it. What
you would do instead is to tie it in at the package you need to **use** it.
There are a few ways to achieve this:

1. You can add this package as a requirement at the ``setup.py`` for your own
   `satellite package
   <https://github.com/idiap/bob/wiki/Virtual-Work-Environments-with-Buildout>`_
   or to your Buildout ``.cfg`` file, if you prefer it that way. With this
   method, this package gets automatically downloaded and installed on your
   working environment, or

2. You can manually download and install this package using commands like
   ``easy_install`` or ``pip``.

The package is available in two different distribution formats:

1. You can download it from `PyPI <http://pypi.python.org/pypi/xbob.db.mobio>`_, or

2. You can download it in its source form from `its git repository
   <https://github.com/bioidiap/xbob.db.mobio>`_. When you download the
   version at the git repository, you will need to run a command to recreate
   the backend SQLite file required for its operation. This means that the
   database raw files must be installed somewhere in this case. With option
   ``a`` you can run in `dummy` mode and only download the raw data files for
   the database once you are happy with your setup.

You can mix and match points 1/2 and a/b above based on your requirements. Here
are some examples:

Modify your setup.py and download from PyPI
===========================================

That is the easiest. Edit your ``setup.py`` in your satellite package and add
the following entry in the ``install_requires`` section (note: ``...`` means
`whatever extra stuff you may have in-between`, don't put that on your
script)::

    install_requires=[
      ...
      "xbob.db.mobio",
    ],

Proceed normally with your ``boostrap/buildout`` steps and you should be all
set. That means you can now import the ``xbob.db.mobio`` namespace into your scripts.

Modify your buildout.cfg and download from git
==============================================

You will need to add a dependence to `mr.developer
<http://pypi.python.org/pypi/mr.developer/>`_ to be able to install from our
git repositories. Your ``buildout.cfg`` file should contain the following
lines::

  [buildout]
  ...
  extensions = mr.developer
  auto-checkout = *
  eggs = bob
         ...
         xbob.db.mobio

  [sources]
  xbob.db.mobio = git https://github.com/bioidiap/xbob.db.mobio.git
  ...

MOBIO protocols
===============

There were initially two protocols defined on the Phase 2 of the database,
which were called 'female' and 'male'. Later on, the number of protocols
has increased, considering the additional data recorded using laptops, which
has led to 8 protocols.

The two initial protocols 'female' and 'male' now correspond to the protocols
called 'mobile0-female' and 'mobile0-male', respectively. The training, 
development and evaluation sets are indeed identical.

However, if you want to use the same ZT score normalization files as in this 
publication::

  @article{McCool_IET_BMT_2013,
    title = {Session variability modelling for face authentication},
    author = {McCool, Chris and Wallace, Roy and McLaren, Mitchell and El Shafey, Laurent and Marcel, S{\'{e}}bastien},
    month = sep,
    journal = {IET Biometrics},
    volume = {2},
    number = {3},
    year = {2013},
    pages = {117-129},
    issn = {2047-4938},
    doi = {10.1049/iet-bmt.2012.0059},
  }

You have to specify optional arguments::

  1. `speech_type = 'p'` when calling the `tobjects()` method

  2. `speech_type = ['p','r','l','f']` when calling the `zobjects()` method

