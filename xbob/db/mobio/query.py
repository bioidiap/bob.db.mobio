#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
# Laurent El Shafey <Laurent.El-Shafey@idiap.ch>

"""This module provides the Dataset interface allowing the user to query the
MOBIO database in the most obvious ways.
"""

import os
from bob.db import utils
from .models import *
from .driver import Interface

INFO = Interface()

SQLITE_FILE = INFO.files()[0]

class Database(object):
  """The dataset class opens and maintains a connection opened to the Database.

  It provides many different ways to probe for the characteristics of the data
  and for the data itself inside the database.
  """

  def __init__(self):
    # opens a session to the database - keep it open until the end
    self.connect()

  def connect(self):
    """Tries connecting or re-connecting to the database"""
    if not os.path.exists(SQLITE_FILE):
      self.session = None

    else:
      self.session = utils.session_try_readonly(INFO.type(), SQLITE_FILE)

  def is_valid(self):
    """Returns if a valid session has been opened for reading the database"""

    return self.session is not None

  def assert_validity(self):
    """Raise a RuntimeError if the database backend is not available"""

    if not self.is_valid():
      raise RuntimeError, "Database '%s' cannot be found at expected location '%s'. Create it and then try re-connecting using Database.connect()" % (INFO.name(), SQLITE_FILE)

  def __check_validity__(self, l, obj, valid, default):
    """Checks validity of user input data against a set of valid values"""
    if not l: return default
    elif not isinstance(l, (tuple,list)):
      return self.__check_validity__((l,), obj, valid, default)
    for k in l:
      if k not in valid:
        raise RuntimeError, 'Invalid %s "%s". Valid values are %s, or lists/tuples of those' % (obj, k, valid)
    return l

  def groups(self):
    """Returns the names of all registered groups"""

    return ProtocolPurpose.group_choices

  def genders(self):
    """Returns the list of genders"""

    return Client.gender_choices

  def subworld_names(self):
    """Returns all registered subworld names"""

    self.assert_validity()
    l = self.subworlds()
    retval = [str(k.name) for k in l]
    return retval

  def subworlds(self):
    """Returns the list of subworlds"""

    self.assert_validity()

    return list(self.session.query(Subworld))

  def has_subworld(self, name):
    """Tells if a certain subworld is available"""

    self.assert_validity()
    return self.session.query(Subworld).filter(Subworld.name==name).count() != 0

  def clients(self, protocol=None, groups=None, subworld=None, gender=None):
    """Returns a list of Clients for the specific query by the user.

    Keyword Parameters:

    protocol
      The protocol to consider ('male', 'female')

    groups
      The groups to which the clients belong ('dev', 'eval', 'world')
      Please note that world data are protocol/gender independent

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      In order to be considered, 'world' should be in groups and only one
      split should be specified.

    gender
      The gender to consider ('male', 'female')

    Returns: A list containing all the clients which have the given properties.
    """

    self.assert_validity()

    VALID_PROTOCOLS = self.protocol_names()
    VALID_GROUPS = self.groups()
    VALID_SUBWORLDS = self.subworld_names()
    VALID_GENDERS = self.genders()
    protocol = self.__check_validity__(protocol, "protocol", VALID_PROTOCOLS, '')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, '')
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, '')
    gender = self.__check_validity__(gender, "gender", VALID_GENDERS, '')

    # List of the clients
    q = self.session.query(Client)
    if protocol:
      q = q.filter(Client.gender.in_(protocol))
    if groups:
      q = q.filter(Client.sgroup.in_(groups))
    if subworld:
      q = q.join(Subworld, Client.subworld).filter(Subworld.name.in_(subworld))
    if gender:
      q = q.filter(Client.gender.in_(gender))
    q = q.order_by(Client.id)
    return list(q)

  def has_client_id(self, id):
    """Returns True if we have a client with a certain integer identifier"""

    self.assert_validity()
    return self.session.query(Client).filter(Client.id==id).count() != 0

  def client(self, id):
    """Returns the Client object in the database given a certain id. Raises
    an error if that does not exist."""

    self.assert_validity()
    return self.session.query(Client).filter(Client.id==id).one()

  def tclients(self, protocol=None, groups=None, subworld='onethird', gender=None):
    """Returns a set of T-Norm clients for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the MOBIO protocols ('male', 'female').

    groups
      The groups to which the clients belong ('dev', 'eval').
      For the MOBIO database, this has no impact as the Z-Norm clients are coming from
      the 'world' set, and are hence the same for both the 'dev' and 'eval' sets.

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      Please note that 'onethird' is the default value.

    gender
      The gender to consider ('male', 'female')

    Returns: A list containing all the T-norm clients belonging to the given group.
    """

    VALID_PROTOCOLS = self.protocol_names()
    VALID_GROUPS = ('dev', 'eval')
    VALID_SUBWORLDS = self.subworld_names()
    VALID_GENDERS = self.genders()
    protocol = self.__check_validity__(protocol, "protocol", VALID_PROTOCOLS, '')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, '')
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, '')
    gender = self.__check_validity__(gender, "gender", VALID_GENDERS, '')

    return self.clients(protocol, 'world', subworld, gender)

  def zclients(self, protocol=None, groups=None, subworld='onethird', gender=None):
    """Returns a set of Z-Norm clients for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the MOBIO protocols ('male', 'female').

    groups
      The groups to which the clients belong ('dev', 'eval').
      For the MOBIO database, this has no impact as the Z-Norm clients are coming from
      the 'world' set, and are hence the same for both the 'dev' and 'eval' sets.

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      Please note that 'onethird' is the default value.

    gender
      The gender to consider ('male', 'female')

    Returns: A list containing all the Z-norm clients belonging to the given group.
    """

    self.assert_validity()

    VALID_GROUPS = ('dev', 'eval')
    groups = self.__check_validity__(groups, 'group', VALID_GROUPS, '')

    return self.clients(protocol, 'world', subworld, gender)

  def models(self, protocol=None, groups=None, subworld=None, gender=None):
    """Returns a set of models for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the Mobio protocols ('male', 'female').

    groups
      The groups to which the subjects attached to the models belong ('dev', 'eval', 'world')
      Please note that world data are protocol/gender independent

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      In order to be considered, 'world' should be in groups and only one
      split should be specified.

    gender
      The gender to consider ('male', 'female')

    Returns: A list containing all the models belonging to the given group.
    """

    return self.clients(protocol, groups, subworld, gender)

  def tmodels(self, protocol=None, groups=None, subworld='onethird', gender=None):
    """Returns a set of T-Norm models for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the MOBIO protocols ('male', 'female').

    groups
      The groups to which the clients belong ('dev', 'eval').
      For the MOBIO database, this has no impact as the Z-Norm clients are coming from
      the 'world' set, and are hence the same for both the 'dev' and 'eval' sets.

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      Please note that 'onethird' is the default value.

    gender
      The gender to consider ('male', 'female')

    Returns: A list containing all the T-norm models belonging to the given group.
    """

    self.assert_validity()

    VALID_PROTOCOLS = self.protocol_names()
    VALID_GROUPS = ('dev', 'eval')
    VALID_SUBWORLDS = self.subworld_names()
    VALID_GENDERS = self.genders()
    protocol = self.__check_validity__(protocol, "protocol", VALID_PROTOCOLS, '')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, '')
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, '')
    gender = self.__check_validity__(gender, "gender", VALID_GENDERS, '')

    # List of the clients
    q = self.session.query(TModel).join(Client)
    if subworld:
      q = q.join(Subworld, Client.subworld).filter(Subworld.name.in_(subworld))
    if gender:
      q = q.filter(Client.gender.in_(gender))
    q = q.order_by(TModel.id)
    return list(q)


    return self.tclients(protocol, groups, subworld, gender)


  def get_client_id_from_model_id(self, model_id):
    """Returns the client_id attached to the given model_id

    Keyword Parameters:

    model_id
      The model_id to consider

    Returns: The client_id attached to the given model_id
    """
    return model_id

  def objects(self, protocol=None, purposes=None, model_ids=None,
      groups=None, classes=None, subworld=None, gender=None):
    """Returns a set of Files for the specific query by the user.

    Keyword Parameters:

    protocol
      One of the MOBIO protocols ('male', 'female').

    purposes
      The purposes required to be retrieved ('enrol', 'probe') or a tuple
      with several of them. If 'None' is given (this is the default), it is
      considered the same as a tuple with all possible values. This field is
      ignored for the data from the "world" group.

    model_ids
      Only retrieves the files for the provided list of model ids (claimed
      client id).  If 'None' is given (this is the default), no filter over
      the model_ids is performed.

    groups
      One of the groups ('dev', 'eval', 'world') or a tuple with several of them.
      If 'None' is given (this is the default), it is considered the same as a
      tuple with all possible values.

    classes
      The classes (types of accesses) to be retrieved ('client', 'impostor')
      or a tuple with several of them. If 'None' is given (this is the
      default), it is considered the same as a tuple with all possible values.

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      In order to be considered, "world" should be in groups and only one
      split should be specified.

    gender
      The gender to consider ('male', 'female')

    Returns: A set of Files with the given properties.
    """

    self.assert_validity()

    VALID_PROTOCOLS = self.protocol_names()
    VALID_PURPOSES = self.purposes()
    VALID_GROUPS = self.groups()
    VALID_CLASSES = ('client', 'impostor')
    VALID_SUBWORLDS = self.subworld_names()
    VALID_GENDERS = self.genders()

    protocol = self.__check_validity__(protocol, "protocol", VALID_PROTOCOLS, VALID_PROTOCOLS)
    purposes = self.__check_validity__(purposes, "purpose", VALID_PURPOSES, VALID_PURPOSES)
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, VALID_GROUPS)
    classes = self.__check_validity__(classes, "class", VALID_CLASSES, VALID_CLASSES)
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, "")
    gender = self.__check_validity__(gender, "gender", VALID_GENDERS, "")

    import collections
    if(model_ids is None):
      model_ids = ()
    elif not isinstance(model_ids, collections.Iterable):
      model_ids = (model_ids,)

    # Now query the database
    retval = []
    if 'world' in groups:
      q = self.session.query(File).join(Client).filter(Client.sgroup == 'world')
      if subworld:
        q = q.join(Subworld, File.subworld).filter(Subworld.name.in_(subworld))
      if gender:
        q = q.filter(Client.gender.in_(gender))
      if model_ids:
        q = q.filter(File.client_id.in_(model_ids))
      q = q.order_by(File.client_id, File.session_id, File.speech_type, File.shot_id, File.device)
      retval += list(q)

    if ('dev' in groups or 'eval' in groups):
      if('enrol' in purposes):
        q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocol_purposes).join(Protocol).\
              filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup.in_(groups), ProtocolPurpose.purpose == 'enrol'))
        if gender:
          q = q.filter(Client.gender.in_(gender))
        if model_ids:
          q = q.filter(Client.id.in_(model_ids))
        q = q.order_by(File.client_id, File.session_id, File.speech_type, File.shot_id, File.device)
        retval += list(q)

      if('probe' in purposes):
        if('client' in classes):
          q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocol_purposes).join(Protocol).\
                filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup.in_(groups), ProtocolPurpose.purpose == 'probe'))
          if gender:
            q = q.filter(Client.gender.in_(gender))
          if model_ids:
            q = q.filter(Client.id.in_(model_ids))
          q = q.order_by(File.client_id, File.session_id, File.speech_type, File.shot_id, File.device)
          retval += list(q)

        if('impostor' in classes):
          q = self.session.query(File).join(Client).join(ProtocolPurpose, File.protocol_purposes).join(Protocol).\
                filter(and_(Protocol.name.in_(protocol), ProtocolPurpose.sgroup.in_(groups), ProtocolPurpose.purpose == 'probe'))
          if gender:
            q = q.filter(Client.gender.in_(gender))
          if len(model_ids) == 1:
            q = q.filter(not_(File.client_id.in_(model_ids)))
          q = q.order_by(File.client_id, File.session_id, File.speech_type, File.shot_id, File.device)
          retval += list(q)

    return list(set(retval)) # To remove duplicates

  def tobjects(self, protocol=None, model_ids=None, groups=None, subworld='onethird', gender=None):
    """Returns a set of filenames for enroling T-norm models for score
       normalization.

    Keyword Parameters:

    protocol
      One of the MOBIO protocols ('male', 'female').

    model_ids
      Only retrieves the files for the provided list of model ids.
      If 'None' is given (this is the default), no filter over
      the model_ids is performed.

    groups
      The groups to which the clients belong ('dev', 'eval').
      For the MOBIO database, this has no impact as the Z-Norm clients are coming from
      the 'world' set, and are hence the same for both the 'dev' and 'eval' sets.

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      Please note that 'onethird' is the default value.

    gender
      The gender to consider ('male', 'female')

    Returns: A set of Files with the given properties.
    """

    self.assert_validity()

    VALID_PROTOCOLS = self.protocol_names()
    VALID_GROUPS = ('dev', 'eval')
    VALID_SUBWORLDS = self.subworld_names()
    VALID_GENDERS = self.genders()

    protocol = self.__check_validity__(protocol, "protocol", VALID_PROTOCOLS, VALID_PROTOCOLS)
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, '')
    subworld = self.__check_validity__(subworld, "subworld", VALID_SUBWORLDS, "")
    gender = self.__check_validity__(gender, "gender", VALID_GENDERS, "")

    if(model_ids is None):
      model_ids = ()
    elif isinstance(model_ids, (str,unicode)):
      model_ids = (model_ids,)

    # Now query the database
    retval = []
    q = self.session.query(File)
    if subworld:
      q = q.join(Subworld, File.subworld).filter(Subworld.name.in_(subworld))
    q = q.join(TModel, File.tmodels)
    if model_ids:
      q = q.filter(TModel.id.in_(model_ids))
    if gender:
      q = q.join(Client).filter(Client.gender.in_(gender))
    q = q.order_by(File.client_id, File.session_id, File.speech_type, File.shot_id, File.device)
    retval += list(q)
    return retval

  def zobjects(self, protocol=None, model_ids=None, groups=None, subworld='onethird', gender=None):
    """Returns a set of Files to perform Z-norm score normalization.

    Keyword Parameters:

    protocol
      One of the MOBIO protocols ('male', 'female').

    model_ids
      Only retrieves the files for the provided list of model ids (claimed
      client id).  If 'None' is given (this is the default), no filter over
      the model_ids is performed.

    groups
      One of the groups ('dev', 'eval', 'world') or a tuple with several of them.
      If 'None' is given (this is the default), it is considered the same as a
      tuple with all possible values.

    subworld
      Specify a split of the world data ('onethird', 'twothirds', 'twothirds-subsampled')
      Please note that 'onethird' is the default value.

    gender
      The gender to consider ('male', 'female')

    Returns: A set of Files with the given properties.
    """
    self.assert_validity()

    VALID_GROUPS = ('dev', 'eval')
    groups = self.__check_validity__(groups, "group", VALID_GROUPS, '')

    return self.objects(protocol, None, model_ids, 'world', None, subworld, gender)

  def protocol_names(self):
    """Returns all registered protocol names"""

    self.assert_validity()
    l = self.protocols()
    retval = [str(k.name) for k in l]
    return retval

  def protocols(self):
    """Returns all registered protocols"""

    self.assert_validity()
    return list(self.session.query(Protocol))

  def has_protocol(self, name):
    """Tells if a certain protocol is available"""

    self.assert_validity()
    return self.session.query(Protocol).filter(Protocol.name==name).count() != 0

  def protocol(self, name):
    """Returns the protocol object in the database given a certain name. Raises
    an error if that does not exist."""

    self.assert_validity()
    return self.session.query(Protocol).filter(Protocol.name==name).one()

  def protocol_purposes(self):
    """Returns all registered protocol purposes"""

    self.assert_validity()
    return list(self.session.query(ProtocolPurpose))

  def purposes(self):
    """Returns the list of allowed purposes"""

    return ProtocolPurpose.purpose_choices

  def paths(self, ids, prefix='', suffix=''):
    """Returns a full file paths considering particular file ids, a given
    directory and an extension

    Keyword Parameters:

    id
      The ids of the object in the database table "file". This object should be
      a python iterable (such as a tuple or list).

    prefix
      The bit of path to be prepended to the filename stem

    suffix
      The extension determines the suffix that will be appended to the filename
      stem.

    Returns a list (that may be empty) of the fully constructed paths given the
    file ids.
    """

    self.assert_validity()

    fobj = self.session.query(File).filter(File.id.in_(ids))
    retval = []
    for p in ids:
      retval.extend([k.make_path(prefix, suffix) for k in fobj if k.id == p])
    return retval

  def reverse(self, paths):
    """Reverses the lookup: from certain stems, returning file ids

    Keyword Parameters:

    paths
      The filename stems I'll query for. This object should be a python
      iterable (such as a tuple or list)

    Returns a list (that may be empty).
    """

    self.assert_validity()

    fobj = self.session.query(File).filter(File.path.in_(paths))
    for p in paths:
      retval.extend([k.id for k in fobj if k.path == p])
    return retval

