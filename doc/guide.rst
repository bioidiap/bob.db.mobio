.. vim: set fileencoding=utf-8 :
.. @author: Manuel Guenther <Manuel.Guenther@idiap.ch>
.. @date:   Thu Dec  6 12:28:25 CET 2012

==============
 User's Guide
==============

This package contains the access API and descriptions for the `MOBIO`_ database.
It only contains the Bob_ accessor methods to use the DB directly from python, with our certified protocols.
The actual raw data for the database should be downloaded from the original URL.

The Database Interface
----------------------

The :py:class:`bob.db.mobio.Database` can be used in standard biometric verification experiments.


MOBIO Protocols
---------------

There were initially two protocols defined on the Phase 2 of the database, which were called ``'female'`` and ``'male'``.
Later on, the number of protocols has increased, considering the additional data recorded using laptops, which has led to a total of 8 protocols.

The two initial protocols ``'female'`` and ``'male'`` now correspond to the protocols called ``'mobile0-female'`` and ``'mobile0-male'``, respectively.
The training, development and evaluation sets are indeed identical.

However, if you want to use the same ZT score normalization files as in this publication:

.. code-block:: latex

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

you have to specify optional arguments::

  1. `speech_type = 'p'` when calling the :py:meth:`bob.db.mobio.Database.tobjects` method

  2. `speech_type = ['p','r','l','f']` when calling the :py:meth:`bob.db.mobio.Database.zobjects` method


.. todo::
   Explain further particularities of the :py:class:`bob.db.mobio.Database`.


.. _mobio: http://www.idiap.ch/dataset/mobio
.. _bob: https://www.idiap.ch/software/bob
