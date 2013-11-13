#!/usr/bin/env python
# vim: set fileencoding=utf-8 :
# Laurent El Shafey <laurent.el-shafey@idiap.ch>
#
# Copyright (C) 2011-2013 Idiap Research Institute, Martigny, Switzerland
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
from nose.plugins.skip import SkipTest

class MobioDatabaseTest(unittest.TestCase):
  """Performs various tests on the MOBIO database."""

  def test01_clients(self):

    db = Database()

    clients = db.clients()
    self.assertEqual(len(clients), 150)
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
    assert len(c.files) == 213
    # Check T-Norm and Z-Norm clients
    self.assertEqual(len(db.tclients(protocol='mobile0-female')), 16)
    self.assertEqual(len(db.tclients(protocol='mobile0-male')), 16)
    self.assertEqual(len(db.tclients(protocol='mobile1-female')), 16)
    self.assertEqual(len(db.tclients(protocol='mobile1-male')), 16)
    self.assertEqual(len(db.tclients(protocol='laptop1-female')), 16)
    self.assertEqual(len(db.tclients(protocol='laptop1-male')), 16)
    self.assertEqual(len(db.tclients(protocol='laptop_mobile1-female')), 16)
    self.assertEqual(len(db.tclients(protocol='laptop_mobile1-male')), 16)
    self.assertEqual(len(db.tclients(protocol='male')), 16)
    self.assertEqual(len(db.tclients(protocol='female')), 16)
    self.assertEqual(len(db.zclients()), 16)
    self.assertEqual(len(db.zclients(protocol='mobile0-female')), 16)
    self.assertEqual(len(db.zclients(protocol='mobile0-male')), 16)
    self.assertEqual(len(db.zclients(protocol='mobile1-female')), 16)
    self.assertEqual(len(db.zclients(protocol='mobile1-male')), 16)
    self.assertEqual(len(db.zclients(protocol='laptop1-female')), 16)
    self.assertEqual(len(db.zclients(protocol='laptop1-male')), 16)
    self.assertEqual(len(db.zclients(protocol='laptop_mobile1-female')), 16)
    self.assertEqual(len(db.zclients(protocol='laptop_mobile1-male')), 16)
    self.assertEqual(len(db.zclients(protocol='male')), 16)
    self.assertEqual(len(db.zclients(protocol='female')), 16)
    # Check T-Norm models
    self.assertEqual(len(db.tmodels(protocol='mobile0-female')), 192)
    self.assertEqual(len(db.tmodels(protocol='mobile0-male')), 192)
    self.assertEqual(len(db.tmodels(protocol='mobile1-female')), 208)
    self.assertEqual(len(db.tmodels(protocol='mobile1-male')), 208)
    self.assertEqual(len(db.tmodels(protocol='laptop1-female')), 208)
    self.assertEqual(len(db.tmodels(protocol='laptop1-male')), 208)
    self.assertEqual(len(db.tmodels(protocol='laptop_mobile1-female')), 208)
    self.assertEqual(len(db.tmodels(protocol='laptop_mobile1-male')), 208)
    self.assertEqual(len(db.tmodels(protocol='male')), 192)
    self.assertEqual(len(db.tmodels(protocol='female')), 192)

  def test02_protocols(self):

    db = Database()

    self.assertEqual(len(db.protocols()), 8)
    self.assertEqual(len(db.protocol_names()), 8)
    self.assertTrue(db.has_protocol('mobile0-male'))
    self.assertTrue(db.has_protocol('mobile0-female'))
    self.assertTrue(db.has_protocol('mobile1-male'))
    self.assertTrue(db.has_protocol('mobile1-female'))
    self.assertTrue(db.has_protocol('laptop1-male'))
    self.assertTrue(db.has_protocol('laptop1-female'))
    self.assertTrue(db.has_protocol('laptop_mobile1-male'))
    self.assertTrue(db.has_protocol('laptop_mobile1-female'))
    self.assertTrue(db.has_protocol('male')) # alias to 'mobile0-male'
    self.assertTrue(db.has_protocol('female')) # alias 'mobile0-female'

    self.assertEqual(len(db.subworlds()), 3)
    self.assertEqual(len(db.subworld_names()), 3)
    self.assertTrue(db.has_subworld('onethird'))

  def test03_objects(self):

    db = Database()

    # Protocol mobile0-female
    # World group
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='world')), 9600)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='world', purposes='train')), 9600)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='world', gender='female')), 2496)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='world', purposes='train', model_ids=204)), 192)

    # Dev group
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev')), 1980)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev', purposes='enrol')), 90)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev', purposes='probe')), 1890)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev', purposes='probe', classes='client')), 1890)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev', purposes='probe', classes='impostor')), 1890)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev', purposes='probe', classes='client', model_ids=118)), 105)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='dev', purposes='probe', classes='impostor', model_ids=118)), 1785)

    # Eval group
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval')), 2200)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval', purposes='enrol')), 100)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval', purposes='probe')), 2100)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval', purposes='probe', classes='client')), 2100)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval', purposes='probe', classes='impostor')), 2100)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval', purposes='probe', classes='client', model_ids=7)), 105)
    self.assertEqual(len(db.objects(protocol='mobile0-female', groups='eval', purposes='probe', classes='impostor', model_ids=7)), 1995)

    # Protocol mobile0-male
    # World group
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='world')), 9600)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='world', purposes='train')), 9600)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='world', gender='male')), 7104)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='world', purposes='train', model_ids=204)), 192)

    # Dev group
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev')), 2640)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev', purposes='enrol')), 120)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev', purposes='probe')), 2520)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev', purposes='probe', classes='client')), 2520)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev', purposes='probe', classes='impostor')), 2520)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev', purposes='probe', classes='client', model_ids=103)), 105)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='dev', purposes='probe', classes='impostor', model_ids=103)), 2415)

    # Eval group
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval')), 4180)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval', purposes='enrol')), 190)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval', purposes='probe')), 3990)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval', purposes='probe', classes='client')), 3990)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval', purposes='probe', classes='impostor')), 3990)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval', purposes='probe', classes='client', model_ids=1)), 105)
    self.assertEqual(len(db.objects(protocol='mobile0-male', groups='eval', purposes='probe', classes='impostor', model_ids=1)), 3885)


    # Protocol mobile1-female
    # World group
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='world')), 10650)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='world', purposes='train')), 10650)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='world', gender='female')), 2769)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='world', purposes='train', model_ids=204)), 213)

    # Dev group
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev')), 1980)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev', purposes='enrol')), 90)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev', purposes='probe')), 1890)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev', purposes='probe', classes='client')), 1890)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev', purposes='probe', classes='impostor')), 1890)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev', purposes='probe', classes='client', model_ids=118)), 105)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='dev', purposes='probe', classes='impostor', model_ids=118)), 1785)

    # Eval group
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval')), 2200)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval', purposes='enrol')), 100)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval', purposes='probe')), 2100)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval', purposes='probe', classes='client')), 2100)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval', purposes='probe', classes='impostor')), 2100)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval', purposes='probe', classes='client', model_ids=7)), 105)
    self.assertEqual(len(db.objects(protocol='mobile1-female', groups='eval', purposes='probe', classes='impostor', model_ids=7)), 1995)

    # Protocol mobile1-male
    # World group
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='world')), 10650)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='world', purposes='train')), 10650)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='world', gender='male')), 7881)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='world', purposes='train', model_ids=204)), 213)

    # Dev group
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev')), 2640)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev', purposes='enrol')), 120)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev', purposes='probe')), 2520)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev', purposes='probe', classes='client')), 2520)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev', purposes='probe', classes='impostor')), 2520)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev', purposes='probe', classes='client', model_ids=103)), 105)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='dev', purposes='probe', classes='impostor', model_ids=103)), 2415)

    # Eval group
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval')), 4180)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval', purposes='enrol')), 190)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval', purposes='probe')), 3990)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval', purposes='probe', classes='client')), 3990)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval', purposes='probe', classes='impostor')), 3990)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval', purposes='probe', classes='client', model_ids=1)), 105)
    self.assertEqual(len(db.objects(protocol='mobile1-male', groups='eval', purposes='probe', classes='impostor', model_ids=1)), 3885)


    # Protocol laptop1-female
    # World group
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='world')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='world', purposes='train')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='world', gender='female')), 2769)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='world', purposes='train', model_ids=204)), 213)

    # Dev group
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev')), 1980)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev', purposes='enrol')), 90)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev', purposes='probe')), 1890)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev', purposes='probe', classes='client')), 1890)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev', purposes='probe', classes='impostor')), 1890)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev', purposes='probe', classes='client', model_ids=118)), 105)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='dev', purposes='probe', classes='impostor', model_ids=118)), 1785)

    # Eval group
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval')), 2200)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval', purposes='enrol')), 100)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval', purposes='probe')), 2100)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval', purposes='probe', classes='client')), 2100)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval', purposes='probe', classes='impostor')), 2100)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval', purposes='probe', classes='client', model_ids=7)), 105)
    self.assertEqual(len(db.objects(protocol='laptop1-female', groups='eval', purposes='probe', classes='impostor', model_ids=7)), 1995)

    # Protocol laptop1-male
    # World group
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='world')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='world', purposes='train')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='world', gender='male')), 7881)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='world', purposes='train', model_ids=204)), 213)

    # Dev group
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev')), 2640)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev', purposes='enrol')), 120)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev', purposes='probe')), 2520)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev', purposes='probe', classes='client')), 2520)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev', purposes='probe', classes='impostor')), 2520)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev', purposes='probe', classes='client', model_ids=103)), 105)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='dev', purposes='probe', classes='impostor', model_ids=103)), 2415)

    # Eval group
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval')), 4180)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval', purposes='enrol')), 190)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval', purposes='probe')), 3990)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval', purposes='probe', classes='client')), 3990)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval', purposes='probe', classes='impostor')), 3990)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval', purposes='probe', classes='client', model_ids=1)), 105)
    self.assertEqual(len(db.objects(protocol='laptop1-male', groups='eval', purposes='probe', classes='impostor', model_ids=1)), 3885)


    # Protocol laptop_mobile1-female
    # World group
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='world')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='world', purposes='train')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='world', gender='female')), 2769)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='world', purposes='train', model_ids=204)), 213)

    # Dev group
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev')), 2070)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev', purposes='enrol')), 180)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev', purposes='probe')), 1890)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev', purposes='probe', classes='client')), 1890)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev', purposes='probe', classes='impostor')), 1890)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev', purposes='probe', classes='client', model_ids=118)), 105)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='dev', purposes='probe', classes='impostor', model_ids=118)), 1785)

    # Eval group
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval')), 2300)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval', purposes='enrol')), 200)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval', purposes='probe')), 2100)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval', purposes='probe', classes='client')), 2100)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval', purposes='probe', classes='impostor')), 2100)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval', purposes='probe', classes='client', model_ids=7)), 105)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-female', groups='eval', purposes='probe', classes='impostor', model_ids=7)), 1995)

    # Protocol laptop_mobile1-male
    # World group
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='world')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='world', purposes='train')), 10650)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='world', gender='male')), 7881)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='world', purposes='train', model_ids=204)), 213)

    # Dev group
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev')), 2760)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev', purposes='enrol')), 240)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev', purposes='probe')), 2520)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev', purposes='probe', classes='client')), 2520)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev', purposes='probe', classes='impostor')), 2520)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev', purposes='probe', classes='client', model_ids=103)), 105)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='dev', purposes='probe', classes='impostor', model_ids=103)), 2415)

    # Eval group
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval')), 4370)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval', purposes='enrol')), 380)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval', purposes='probe')), 3990)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval', purposes='probe', classes='client')), 3990)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval', purposes='probe', classes='impostor')), 3990)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval', purposes='probe', classes='client', model_ids=1)), 105)
    self.assertEqual(len(db.objects(protocol='laptop_mobile1-male', groups='eval', purposes='probe', classes='impostor', model_ids=1)), 3885)


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
    # T-Norm
    self.assertEqual(len(db.tobjects(protocol='mobile0-female')), 3072)
    self.assertEqual(len(db.tobjects(protocol='mobile0-male')), 3072)
    self.assertEqual(len(db.tobjects(protocol='mobile1-female')), 3408)
    self.assertEqual(len(db.tobjects(protocol='mobile1-male')), 3408)
    self.assertEqual(len(db.tobjects(protocol='laptop1-female')), 3408)
    self.assertEqual(len(db.tobjects(protocol='laptop1-male')), 3408)
    self.assertEqual(len(db.tobjects(protocol='laptop_mobile1-female')), 3408)
    self.assertEqual(len(db.tobjects(protocol='laptop_mobile1-male')), 3408)
    self.assertEqual(len(db.tobjects(protocol='male', speech_type='p')), 960)
    self.assertEqual(len(db.tobjects(protocol='female', speech_type='p')), 960)
    self.assertEqual(len(db.tobjects(protocol='male',  speech_type='p', model_ids=('204_01_mobile',))), 5)

    # Z-Norm files
    self.assertEqual(len(db.zobjects()), 1920)
    self.assertEqual(len(db.zobjects(model_ids=(204,))), 120)
    self.assertEqual(len(db.zobjects(protocol='male', speech_type=['p','r','l','f'])), 3072)
    self.assertEqual(len(db.zobjects(protocol='male', speech_type=['p','r','l','f'], model_ids=(204,))), 192)


  def test04_annotations(self):
    # read some annotation files and test it's content
    dir = "/idiap/resource/database/mobio/IMAGE_ANNOTATIONS"
    if not os.path.exists(dir):
      raise SkipTest("The annotation directory '%d' is not available, annotations can't be tested.")
    db = Database(annotation_directory = dir)
    import random
    files = random.sample(db.all_files(), 1000)
    for file in files:
      annotations = db.annotations(file.id)
      self.assertTrue(annotations is not None)
      self.assertTrue('leye' in annotations)
      self.assertTrue('reye' in annotations)
      self.assertEqual(len(annotations['leye']), 2)
      self.assertEqual(len(annotations['reye']), 2)


  def test05_driver_api(self):

    from bob.db.script.dbmanage import main
    self.assertEqual(main('mobio dumplist --self-test'.split()), 0)
    self.assertEqual(main('mobio dumplist --protocol=mobile0-male --class=client --group=dev --purpose=enrol --client=115 --self-test'.split()), 0)
    self.assertEqual(main('mobio dumplist --protocol=male --class=client --group=dev --purpose=enrol --client=115 --self-test'.split()), 0)
    self.assertEqual(main('mobio checkfiles --self-test'.split()), 0)
    self.assertEqual(main('mobio reverse uoulu/m313/01_mobile/m313_01_p01_i0_0 --self-test'.split()), 0)
    self.assertEqual(main('mobio path 21132 --self-test'.split()), 0)

