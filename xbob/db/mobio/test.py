#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
# Laurent El Shafey <laurent.el-shafey@idiap.ch>
#
# Copyright (C) 2011-2012 Idiap Research Institute, Martigny, Switzerland
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""A few checks at the MOBIO database.
"""

import os, sys
import unittest
from .query import Database

class MobioDatabaseTest(unittest.TestCase):
  """Performs various tests on the MOBIO database."""

  def test01_clients(self):

    db = Database()

    clients = db.clients()
    self.assertEqual(len(clients), 150) #337 clients overall
    # Number of clients in each set
    c_dev = db.clients(groups='dev')
    self.assertEqual(len(c_dev), 42)
    c_eval = db.clients(groups='eval')
    self.assertEqual(len(c_eval), 58)
    c_world = db.clients(groups='world')
    self.assertEqual(len(c_world), 50)
    # Check client ids
    self.assertTrue(db.has_client_id(204))
    self.assertFalse(db.has_client_id(395))
    # Check subworld
    self.assertEqual(len(db.clients(groups='world', subworld='onethird')), 16)
    self.assertEqual(len(db.clients(groups='world', subworld='twothirds')), 34)
    self.assertEqual(len(db.clients(groups='world', subworld='twothirds-subsampled')), 34)
    # Check files relationship
    c = db.client(204)
    len(c.files)
    # Check T-Norm and Z-Norm clients
    self.assertEqual(len(db.tclients()), 16)
    self.assertEqual(len(db.zclients()), 16)
    # Check T-Norm models
    self.assertEqual(len(db.tmodels()), 192)


  def test02_protocols(self):

    db = Database()

    self.assertEqual(len(db.protocols()), 2)
    self.assertEqual(len(db.protocol_names()), 2)
    self.assertTrue(db.has_protocol('male'))

    self.assertEqual(len(db.subworlds()), 3)
    self.assertEqual(len(db.subworld_names()), 3)
    self.assertTrue(db.has_subworld('onethird'))


  def test03_objects(self):

    db = Database()

    # Protocol female
    # World group
    self.assertEqual(len(db.objects(protocol='female', groups='world')), 9600)
    self.assertEqual(len(db.objects(protocol='female', groups='world', purposes='train')), 9600)
    self.assertEqual(len(db.objects(protocol='female', groups='world', gender='female')), 2496)
    self.assertEqual(len(db.objects(protocol='female', groups='world', purposes='train', model_ids=204)), 192)

    # Dev group
    self.assertEqual(len(db.objects(protocol='female', groups='dev')), 1980)
    self.assertEqual(len(db.objects(protocol='female', groups='dev', purposes='enrol')), 90)
    self.assertEqual(len(db.objects(protocol='female', groups='dev', purposes='probe')), 1890)
    self.assertEqual(len(db.objects(protocol='female', groups='dev', purposes='probe', classes='client')), 1890)
    self.assertEqual(len(db.objects(protocol='female', groups='dev', purposes='probe', classes='impostor')), 1890)
    self.assertEqual(len(db.objects(protocol='female', groups='dev', purposes='probe', classes='client', model_ids=118)), 105)
    self.assertEqual(len(db.objects(protocol='female', groups='dev', purposes='probe', classes='impostor', model_ids=118)), 1785)

    # Eval group
    self.assertEqual(len(db.objects(protocol='female', groups='eval')), 2200)
    self.assertEqual(len(db.objects(protocol='female', groups='eval', purposes='enrol')), 100)
    self.assertEqual(len(db.objects(protocol='female', groups='eval', purposes='probe')), 2100)
    self.assertEqual(len(db.objects(protocol='female', groups='eval', purposes='probe', classes='client')), 2100)
    self.assertEqual(len(db.objects(protocol='female', groups='eval', purposes='probe', classes='impostor')), 2100)
    self.assertEqual(len(db.objects(protocol='female', groups='eval', purposes='probe', classes='client', model_ids=7)), 105)
    self.assertEqual(len(db.objects(protocol='female', groups='eval', purposes='probe', classes='impostor', model_ids=7)), 1995)

    # Protocol male
    # World group
    self.assertEqual(len(db.objects(protocol='male', groups='world')), 9600)
    self.assertEqual(len(db.objects(protocol='male', groups='world', purposes='train')), 9600)
    self.assertEqual(len(db.objects(protocol='male', groups='world', gender='male')), 7104)
    self.assertEqual(len(db.objects(protocol='male', groups='world', purposes='train', model_ids=204)), 192)

    # Dev group
    self.assertEqual(len(db.objects(protocol='male', groups='dev')), 2640)
    self.assertEqual(len(db.objects(protocol='male', groups='dev', purposes='enrol')), 120)
    self.assertEqual(len(db.objects(protocol='male', groups='dev', purposes='probe')), 2520)
    self.assertEqual(len(db.objects(protocol='male', groups='dev', purposes='probe', classes='client')), 2520)
    self.assertEqual(len(db.objects(protocol='male', groups='dev', purposes='probe', classes='impostor')), 2520)
    self.assertEqual(len(db.objects(protocol='male', groups='dev', purposes='probe', classes='client', model_ids=103)), 105)
    self.assertEqual(len(db.objects(protocol='male', groups='dev', purposes='probe', classes='impostor', model_ids=103)), 2415)

    # Eval group
    self.assertEqual(len(db.objects(protocol='male', groups='eval')), 4180)
    self.assertEqual(len(db.objects(protocol='male', groups='eval', purposes='enrol')), 190)
    self.assertEqual(len(db.objects(protocol='male', groups='eval', purposes='probe')), 3990)
    self.assertEqual(len(db.objects(protocol='male', groups='eval', purposes='probe', classes='client')), 3990)
    self.assertEqual(len(db.objects(protocol='male', groups='eval', purposes='probe', classes='impostor')), 3990)
    self.assertEqual(len(db.objects(protocol='male', groups='eval', purposes='probe', classes='client', model_ids=1)), 105)
    self.assertEqual(len(db.objects(protocol='male', groups='eval', purposes='probe', classes='impostor', model_ids=1)), 3885)

    # T-Norm and Z-Norm files
    self.assertEqual(len(db.tobjects()), 960)
    self.assertEqual(len(db.tobjects(model_ids=('204_01',))), 5)
    self.assertEqual(len(db.zobjects()), 3072)
    self.assertEqual(len(db.zobjects(model_ids=(204,))), 192)


  def test04_driver_api(self):

    from bob.db.script.dbmanage import main
    self.assertEqual(main('mobio dumplist --self-test'.split()), 0)
    self.assertEqual(main('mobio dumplist --protocol=male --class=client --group=dev --purpose=enrol --client=115 --self-test'.split()), 0)
    self.assertEqual(main('mobio checkfiles --self-test'.split()), 0)
    self.assertEqual(main('mobio reverse m313/m313_01_p01_i0_0 --self-test'.split()), 0)
    self.assertEqual(main('mobio path 21132 --self-test'.split()), 0)


