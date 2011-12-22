import yaml

from juju.errors import IncompatibleVersion
from juju.lib.testing import TestCase
from juju.state.endpoint import RelationEndpoint
from juju.state.topology import (
    InternalTopology, InternalTopologyError, VERSION)


class InternalTopologyMapTest(TestCase):

    def setUp(self):
        self.topology = InternalTopology()

    def test_add_machine(self):
        """
        The topology map is stored as YAML at the moment, so it
        should be able to read it.
        """
        self.topology.add_machine("m-0")
        self.topology.add_machine("m-1")
        self.assertEquals(sorted(self.topology.get_machines()),
                          ["m-0", "m-1"])

    def test_add_duplicated_machine(self):
        """
        Adding a machine which is already registered should fail.
        """
        self.topology.add_machine("m-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.add_machine, "m-0")

    def test_has_machine(self):
        """
        Testing if a machine is registered should be possible.
        """
        self.assertFalse(self.topology.has_machine("m-0"))
        self.topology.add_machine("m-0")
        self.assertTrue(self.topology.has_machine("m-0"))
        self.assertFalse(self.topology.has_machine("m-1"))

    def test_get_machines(self):
        """
        get_machines() must return a list of machine ids
        previously registered.
        """
        self.assertEquals(self.topology.get_machines(), [])
        self.topology.add_machine("m-0")
        self.topology.add_machine("m-1")
        self.assertEquals(sorted(self.topology.get_machines()),
                          ["m-0", "m-1"])

    def test_remove_machine(self):
        """
        Removing machines should take them off the state.
        """
        self.topology.add_machine("m-0")
        self.topology.add_machine("m-1")

        # Add a non-assigned unit, to test that the logic of
        # checking for assigned units validates this.
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")

        self.topology.remove_machine("m-0")
        self.assertFalse(self.topology.has_machine("m-0"))
        self.assertTrue(self.topology.has_machine("m-1"))

    def test_remove_non_existent_machine(self):
        """
        Removing non-existing machines should raise an error.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.remove_machine, "m-0")

    def test_remove_machine_with_assigned_units(self):
        """
        A machine can't be removed when it has assigned units.
        """
        self.topology.add_machine("m-0")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.assign_service_unit_to_machine("s-0", "u-1", "m-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.remove_machine, "m-0")

    def test_machine_has_units(self):
        """Test various ways a machine might or might not be assigned."""
        self.topology.add_machine("m-0")
        self.topology.add_machine("m-1")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.assign_service_unit_to_machine("s-0", "u-1", "m-0")
        self.assertTrue(self.topology.machine_has_units("m-0"))
        self.assertFalse(self.topology.machine_has_units("m-1"))
        self.assertRaises(
            InternalTopologyError,
            self.topology.machine_has_units, "m-nonesuch")

    def test_add_service(self):
        """
        The topology map is stored as YAML at the moment, so it
        should be able to read it.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.assertEquals(sorted(self.topology.get_services()),
                          ["s-0", "s-1"])

    def test_add_duplicated_service(self):
        """
        Adding a service which is already registered should fail.
        """
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(InternalTopologyError,
                          self.topology.add_service, "s-0", "wp")

    def test_add_services_with_duplicated_names(self):
        """
        Adding a service which is already registered should fail.
        """
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(InternalTopologyError,
                          self.topology.add_service, "s-1", "wordpress")

    def test_has_service(self):
        """
        Testing if a service is registered should be possible.
        """
        self.assertFalse(self.topology.has_service("s-0"))
        self.topology.add_service("s-0", "wordpress")
        self.assertTrue(self.topology.has_service("s-0"))
        self.assertFalse(self.topology.has_service("s-1"))

    def test_find_service_with_name(self):
        """
        find_service_with_name() must return the service_id for
        the service with the given name, or None if no service
        is found with that name.
        """
        self.assertEquals(
            self.topology.find_service_with_name("wordpress"), None)
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.assertEquals(
            self.topology.find_service_with_name("wordpress"), "s-0")
        self.assertEquals(
            self.topology.find_service_with_name("mysql"), "s-1")

    def test_get_service_name(self):
        """
        get_service_name() should return the service name for the
        given service_id.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.assertEquals(
            self.topology.get_service_name("s-0"), "wordpress")
        self.assertEquals(
            self.topology.get_service_name("s-1"), "mysql")

    def test_get_service_name_with_non_existing_service(self):
        """
        get_service_name() should raise an error if the service
        does not exist.
        """
        # Without any state:
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_name, "s-0")

        self.topology.add_service("s-0", "wordpress")

        # With some state:
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_name, "s-1")

    def test_get_services(self):
        """
        Retrieving a list of available services must be possible.
        """
        self.assertEquals(self.topology.get_services(), [])
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.assertEquals(sorted(self.topology.get_services()),
                          ["s-0", "s-1"])

    def test_remove_service(self):
        """
        Removing a service should work properly, so that the service
        isn't available anymore after it happens (duh!).
        """
        self.topology.add_service("m-0", "wordpress")
        self.topology.add_service("m-1", "mysql")
        self.topology.remove_service("m-0")
        self.assertFalse(self.topology.has_service("m-0"))
        self.assertTrue(self.topology.has_service("m-1"))

    def test_remove_non_existent_service(self):
        """
        Attempting to remove a non-existing service should be an
        error.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.remove_service, "m-0")

    def test_add_service_unit(self):
        """
        add_service_unit() should register a new service unit for a
        given service, and should return a sequence number for the
        unit.  The sequence number increases monotonically for each
        service, and is helpful to provide nice unit names.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.assertEquals(self.topology.add_service_unit("s-0", "u-05"), 0)
        self.assertEquals(self.topology.add_service_unit("s-0", "u-12"), 1)
        self.assertEquals(self.topology.add_service_unit("s-1", "u-07"), 0)
        self.assertEquals(sorted(self.topology.get_service_units("s-0")),
                          ["u-05", "u-12"])
        self.assertEquals(self.topology.get_service_units("s-1"),
                          ["u-07"])

    def test_global_unique_service_names(self):
        """Service unit names are unique.

        Even if the underlying service is destroyed and a new
        service with the same name is created, we'll never
        get a duplicate service unit name.
        """
        self.topology.add_service("s-0", "wordpress")
        sequence = self.topology.add_service_unit("s-0", "u-0")
        self.assertEqual(sequence, 0)
        sequence = self.topology.add_service_unit("s-0", "u-1")
        self.assertEqual(sequence, 1)
        self.topology.remove_service("s-0")
        self.topology.add_service("s-0", "wordpress")
        sequence = self.topology.add_service_unit("s-0", "u-1")
        self.assertEqual(sequence, 2)
        self.assertEqual(
            self.topology.get_service_unit_name("s-0", "u-1"),
            "wordpress/2")

    def test_add_duplicated_service_unit(self):
        """
        Adding the same unit to the same service must not be
        possible.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.add_service_unit,
                          "s-0", "u-0")

    def test_add_service_unit_to_non_existing_service(self):
        """
        Adding a service unit requires the service to have been
        previously created.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.add_service_unit,
                          "s-0", "u-0")

    def test_add_service_unit_to_different_service(self):
        """
        Adding the same unit to two different services must not
        be possible.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.add_service_unit,
                          "s-1", "u-0")

    def test_get_service_units(self):
        """
        Getting units registered from a service should return a
        list of these.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.assertEquals(self.topology.get_service_units("s-0"), [])
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.add_service_unit("s-1", "u-2")
        self.assertEquals(sorted(self.topology.get_service_units("s-0")),
                          ["u-0", "u-1"])
        self.assertEquals(sorted(self.topology.get_service_units("s-1")),
                          ["u-2"])

    def test_get_service_units_with_non_existing_service(self):
        """
        Getting service units from a non-existing service should
        raise an error.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_units, "s-0")

    def test_has_service_units(self):
        """
        Testing if a service unit exists in a service should be
        possible.
        """
        self.topology.add_service("s-0", "wordpress")
        self.assertFalse(self.topology.has_service_unit("s-0", "u-0"))
        self.topology.add_service_unit("s-0", "u-0")
        self.assertTrue(self.topology.has_service_unit("s-0", "u-0"))
        self.assertFalse(self.topology.has_service_unit("s-0", "u-1"))

    def test_has_service_units_with_non_existing_service(self):
        """
        Testing if a service unit exists should only work if a
        sensible service was provided.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.has_service_unit, "s-1", "u-0")

    def test_get_service_unit_service(self):
        """
        The reverse operation is also feasible: given a service unit,
        return the service id for the service containing the unit.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.add_service_unit("s-1", "u-2")
        self.assertEquals(self.topology.get_service_unit_service("u-0"), "s-0")
        self.assertEquals(self.topology.get_service_unit_service("u-1"), "s-0")
        self.assertEquals(self.topology.get_service_unit_service("u-2"), "s-1")

    def test_get_unit_service_with_non_existing_unit(self):
        """
        If the unit provided to get_service_unit_service() doesn't exist,
        raise an error.
        """
        # Without any services.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_service, "u-1")

        # With a service without units.
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_service, "u-1")

        # With a service with a different unit.
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_service, "u-1")

    def test_get_service_unit_name(self):
        """
        Service units are named with the service name and the sequence
        number joined by a slash, such as wordpress/3.  This makes it
        convenient to use from higher layers.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.add_service_unit("s-1", "u-2")
        self.assertEquals(self.topology.get_service_unit_name("s-0", "u-0"),
                          "wordpress/0")
        self.assertEquals(self.topology.get_service_unit_name("s-0", "u-1"),
                          "wordpress/1")
        self.assertEquals(self.topology.get_service_unit_name("s-1", "u-2"),
                          "mysql/0")

    def test_get_service_unit_name_from_id(self):
        """
        Service units are named with the service name and the sequence
        number joined by a slash, such as wordpress/3.  This makes it
        convenient to use from higher layers. Those layers ocassionally
        need to resolve the id to a name. This is mostly a simple convience
        wrapper around get_service_unit_name
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")

        self.assertEqual(
            self.topology.get_service_unit_name_from_id("u-0"),
            "wordpress/0")
        self.assertEqual(
            self.topology.get_service_unit_name_from_id("u-1"),
            "wordpress/1")
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_name_from_id,
                          "u-2")

    def test_get_unit_service_id_from_name(self):
        """Retrieve the unit id from the user oriented unit name."""
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")

        self.assertEqual(
            "u-0",
            self.topology.get_service_unit_id_from_name("wordpress/0"))

        self.assertEqual(
            "u-1",
            self.topology.get_service_unit_id_from_name("wordpress/1"))

    def test_get_unit_service_with_non_existing_service_or_unit(self):
        """
        If the unit provided to get_service_unit_service() doesn't exist,
        raise an error.
        """
        # Without any services.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_name,
                          "s-0", "u-1")

        # With a service without units.
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_name,
                          "s-0", "u-1")

        # With a service with a different unit.
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_unit_name,
                          "s-0", "u-1")

    def test_remove_service_unit(self):
        """
        It should be possible to remove a service unit from an
        existing service.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "m-0")
        self.topology.add_service_unit("s-0", "m-1")
        self.topology.remove_service_unit("s-0", "m-0")
        self.assertFalse(self.topology.has_service_unit("s-0", "m-0"))
        self.assertTrue(self.topology.has_service_unit("s-0", "m-1"))

    def test_remove_non_existent_service_unit(self):
        """
        Attempting to remove a non-existing service unit or a unit
        in a non-existing service should raise a local error.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.remove_service_unit, "s-0", "m-0")
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(InternalTopologyError,
                          self.topology.remove_service_unit, "s-0", "m-0")

    def test_service_unit_sequencing(self):
        """
        Even if service units are unregistered, the sequence number
        should not be reused.
        """
        self.topology.add_service("s-0", "wordpress")
        self.assertEquals(self.topology.add_service_unit("s-0", "u-05"), 0)
        self.assertEquals(self.topology.add_service_unit("s-0", "u-12"), 1)
        self.topology.remove_service_unit("s-0", "u-05")
        self.topology.remove_service_unit("s-0", "u-12")
        self.assertEquals(self.topology.add_service_unit("s-0", "u-14"), 2)
        self.assertEquals(self.topology.add_service_unit("s-0", "u-17"), 3)

        self.assertEquals(
            self.topology.get_service_unit_sequence("s-0", "u-14"), 2)
        self.assertEquals(
            self.topology.get_service_unit_sequence("s-0", "u-17"), 3)
        self.assertRaises(
            InternalTopologyError,
            self.topology.get_service_unit_sequence, "s-0", "u-05")

    def test_find_service_unit_with_sequence(self):
        """
        Given a service name and a sequence number, the function
        find_service_unit_with_sequence() should return the unit_id,
        or None if the sequence number is not found.
        """
        self.topology.add_service("s-1", "mysql")
        self.topology.add_service_unit("s-1", "u-05")
        self.topology.add_service_unit("s-1", "u-12")
        self.assertEquals(
            self.topology.find_service_unit_with_sequence("s-1", 0),
            "u-05")
        self.assertEquals(
            self.topology.find_service_unit_with_sequence("s-1", 1),
            "u-12")
        self.assertEquals(
            self.topology.find_service_unit_with_sequence("s-1", 2),
            None)

    def test_find_service_unit_with_sequence_using_non_existing_service(self):
        """
        If the service_id provided to find_service_unit_with_sequence
        does not exist, an error should be raised.
        """
        self.assertRaises(
            InternalTopologyError,
            self.topology.find_service_unit_with_sequence, "s-0", 0)

    def test_assign_service_unit_to_machine(self):
        """
        Assigning a service unit to a machine should work.
        """
        self.topology.add_machine("m-0")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.assign_service_unit_to_machine("s-0", "u-0", "m-0")
        machine_id = self.topology.get_service_unit_machine("s-0", "u-0")
        self.assertEquals(machine_id, "m-0")

    def test_assign_service_unit_machine_with_non_existing_service(self):
        """
        If the service_id provided when assigning a unit to a machine
        doesn't exist, an error must be raised.
        """
        self.topology.add_machine("m-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_unit_to_machine,
                          "s-0", "u-0", "m-0")

    def test_assign_service_unit_machine_with_non_existing_service_unit(self):
        """
        If the unit_id provided when assigning a unit to a machine
        doesn't exist, an error must be raised.
        """
        self.topology.add_machine("m-0")
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_unit_to_machine,
                          "s-0", "u-0", "m-0")

    def test_assign_service_unit_machine_with_non_existing_machine(self):
        """
        If the machine_id provided when assigning a unit to a machine
        doesn't exist, an error must be raised.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_unit_to_machine,
                          "s-0", "u-0", "m-0")

    def test_assign_service_unit_machine_twice(self):
        """
        If the service unit was previously assigned to a machine_id,
        attempting to assign it again should raise an error, even if
        the machine_id is exactly the same.
        """
        self.topology.add_machine("m-0")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.assign_service_unit_to_machine("s-0", "u-0", "m-0")
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_unit_to_machine,
                          "s-0", "u-0", "m-0")

    def test_get_service_unit_machine(self):
        """
        get_service_unit_machine() should return the current
        machine the unit is assigned to, or None if it wasn't yet
        assigned to any machine.
        """
        self.topology.add_machine("m-0")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.assertEquals(
            self.topology.get_service_unit_machine("s-0", "u-0"),
            None)
        self.topology.assign_service_unit_to_machine("s-0", "u-0", "m-0")
        self.assertEquals(
            self.topology.get_service_unit_machine("s-0", "u-0"),
            "m-0")

    def test_get_service_unit_machine_with_non_existing_service(self):
        """
        If the service_id provided when attempting to retrieve
        a service unit's machine does not exist, an error must
        be raised.
        """
        self.assertRaises(
            InternalTopologyError,
            self.topology.get_service_unit_machine, "s-0", "u-0")

    def test_get_service_unit_machine_with_non_existing_service_unit(self):
        """
        If the unit_id provided when attempting to retrieve
        a service unit's machine does not exist, an error must
        be raised.
        """
        # Without any units:
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(
            InternalTopologyError,
            self.topology.get_service_unit_machine, "s-0", "u-0")

        # With a different unit in place:
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(
            InternalTopologyError,
            self.topology.get_service_unit_machine, "s-0", "u-1")

    def test_unassign_service_unit_from_machine(self):
        """
        It should be possible to unassign a service unit from a machine,
        as long as it has been previously assigned to some machine.
        """
        self.topology.add_machine("m-0")
        self.topology.add_machine("m-1")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.assign_service_unit_to_machine("s-0", "u-0", "m-0")
        self.topology.assign_service_unit_to_machine("s-0", "u-1", "m-1")
        self.topology.unassign_service_unit_from_machine("s-0", "u-0")
        self.assertEquals(
            self.topology.get_service_unit_machine("s-0", "u-0"),
            None)
        self.assertEquals(
            self.topology.get_service_unit_machine("s-0", "u-1"),
            "m-1")

    def test_unassign_service_unit_from_machine_when_not_assigned(self):
        """
        Can't unassign a unit from a machine if it wasn't previously
        assigned.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(
            InternalTopologyError,
            self.topology.unassign_service_unit_from_machine, "s-0", "u-0")

    def test_unassign_service_unit_with_non_existing_service(self):
        """
        If the service_id used when attempting to unassign the
        service unit from a machine does not exist, an error must
        be raised.
        """
        self.assertRaises(
            InternalTopologyError,
            self.topology.unassign_service_unit_from_machine, "s-0", "u-0")

    def test_unassign_service_unit_with_non_existing_unit(self):
        """
        If the unit_id used when attempting to unassign the
        service unit from a machine does not exist, an error must
        be raised.
        """
        # Without any units:
        self.topology.add_service("s-0", "wordpress")
        self.assertRaises(
            InternalTopologyError,
            self.topology.unassign_service_unit_from_machine, "s-0", "u-0")

        # Without a different unit in place:
        self.topology.add_service_unit("s-0", "u-0")
        self.assertRaises(
            InternalTopologyError,
            self.topology.unassign_service_unit_from_machine, "s-0", "u-1")

    def test_get_service_units_in_machine(self):
        """
        We must be able to get all service units in a given machine
        as well.
        """
        self.topology.add_machine("m-0")
        self.topology.add_machine("m-1")

        # Shouldn't break before services are added.
        self.assertEquals(self.topology.get_service_units_in_machine("m-0"),
                          [])

        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysql")

        # Shouldn't break before units are added either.
        self.assertEquals(self.topology.get_service_units_in_machine("m-0"),
                          [])

        self.topology.add_service_unit("s-0", "u-0")
        self.topology.add_service_unit("s-0", "u-1")
        self.topology.add_service_unit("s-1", "u-2")
        self.topology.add_service_unit("s-1", "u-3")

        # Shouldn't break with units which aren't assigned.
        self.topology.add_service_unit("s-1", "u-4")

        self.topology.assign_service_unit_to_machine("s-0", "u-0", "m-0")
        self.topology.assign_service_unit_to_machine("s-0", "u-1", "m-1")
        self.topology.assign_service_unit_to_machine("s-1", "u-2", "m-1")
        self.topology.assign_service_unit_to_machine("s-1", "u-3", "m-0")

        unit_ids0 = self.topology.get_service_units_in_machine("m-0")
        unit_ids1 = self.topology.get_service_units_in_machine("m-1")

        self.assertEquals(sorted(unit_ids0), ["u-0", "u-3"])
        self.assertEquals(sorted(unit_ids1), ["u-1", "u-2"])

    def test_get_service_units_in_machine_with_non_existing_machine(self):
        """
        If the machine passed to get_service_units_in_machine() doesn't
        exist, it should bail out gracefully.
        """
        # Shouldn't break before services are added.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_service_units_in_machine, "m-0")

    def test_dump_and_parse(self):
        """
        dump() and parse() are opposite operations which enable the
        state of a topology to be persisted as a string, and then
        loaded back.
        """
        empty_data = self.topology.dump()
        self.assertEquals(yaml.load(empty_data), {"version": VERSION})
        self.topology.add_machine("m-0")
        machine_data = self.topology.dump()
        self.topology.parse(empty_data)
        self.assertFalse(self.topology.has_machine("m-0"))
        self.topology.parse(machine_data)
        self.assertTrue(self.topology.has_machine("m-0"))

    def test_incompatible_version(self):
        """Verify `IncompatibleVersion` raised if using old topology."""
        empty_data = self.topology.dump()
        self.assertEquals(yaml.load(empty_data), {"version": VERSION})
        self.topology.add_machine("m-0")
        machine_data = self.topology.dump()
        self.topology.parse(machine_data)
        self.assertTrue(self.topology.has_machine("m-0"))

        # Pretend to bump the versioning by one
        actual_version = VERSION
        import juju
        self.patch(juju.state.topology, "VERSION", actual_version + 1)

        # With this change to juju.state.topology.VERSION, verify
        # topology ops will now raise an incompatibility exception
        ex = self.assertRaises(IncompatibleVersion,
                               self.topology.parse, machine_data)
        self.assertEqual(
            str(ex),
            "Incompatible juju protocol versions (found %d, want %d)" % (
                    actual_version, juju.state.topology.VERSION))

    def test_reset(self):
        """
        Resetting a topology should put it back in the state it
        was initialized with.
        """
        empty_data = self.topology.dump()
        self.topology.add_machine("m-0")
        self.topology.reset()
        self.assertEquals(self.topology.dump(), empty_data)
        self.assertEquals(self.topology._state["version"], VERSION)

    def test_has_relation(self):
        """Testing if a relation exists should be possible.
        """
        self.topology.add_service("s-0", "wordpress")
        self.assertFalse(self.topology.has_relation("r-1"))
        self.topology.add_relation("r-1", "type")
        self.assertTrue(self.topology.has_relation("r-1"))

    def test_add_relation(self):
        """Add a relation between the given service ids.
        """
        self.assertFalse(self.topology.has_relation("r-1"))

        # Verify add relation works correctly.
        self.topology.add_relation("r-1", "type")
        self.assertTrue(self.topology.has_relation("r-1"))

        # Attempting to add again raises an exception
        self.assertRaises(
            InternalTopologyError,
            self.topology.add_relation, "r-1", "type")

    def test_assign_service_to_relation(self):
        """A service can be associated to a relation.
        """
        # Both service and relation must be valid.
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_to_relation,
                          "r-1",
                          "s-0",
                          "name",
                          "role")
        self.topology.add_relation("r-1", "type")
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_to_relation,
                          "r-1",
                          "s-0",
                          "name",
                          "role")
        self.topology.add_service("s-0", "wordpress")

        # The relation can be assigned.
        self.assertFalse(self.topology.relation_has_service("r-1", "s-0"))
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        self.assertEqual(self.topology.get_relations_for_service("s-0"),
                         [("r-1", "type", {"name": "name", "role": "role"})])

        # Adding it again raises an error, even with a different name/role
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_to_relation,
                          "r-1",
                          "s-0",
                          "name2",
                          "role2")

        # Another service can't provide the same role within a relation.
        self.topology.add_service("s-1", "database")
        self.assertRaises(InternalTopologyError,
                          self.topology.assign_service_to_relation,
                          "r-1",
                          "s-1",
                          "name",
                          "role")

    def test_unassign_service_from_relation(self):
        """A service can be disassociated from a relation.
        """
        # Both service and relation must be valid.
        self.assertRaises(InternalTopologyError,
                          self.topology.unassign_service_from_relation,
                          "r-1",
                          "s-0")
        self.topology.add_relation("r-1", "type")
        self.assertRaises(InternalTopologyError,
                          self.topology.unassign_service_from_relation,
                          "r-1",
                          "s-0")
        self.topology.add_service("s-0", "wordpress")

        # If the service is not assigned to the relation, raises an error.
        self.assertRaises(InternalTopologyError,
                          self.topology.unassign_service_from_relation,
                          "r-1",
                          "s-0")

        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        self.assertEqual(self.topology.get_relations_for_service("s-0"),
                         [("r-1", "type", {"name": "name", "role": "role"})])

        self.topology.unassign_service_from_relation("r-1", "s-0")
        self.assertFalse(self.topology.get_relations_for_service("s-0"))

    def test_relation_has_service(self):
        """We can test to see if a service is associated to a relation."""
        self.assertFalse(self.topology.relation_has_service("r-1", "s-0"))
        self.topology.add_relation("r-1", "type")
        self.topology.add_service("s-0", "wordpress")
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        self.assertTrue(self.topology.relation_has_service("r-1", "s-0"))

    def test_get_relation_service(self):
        """We can fetch the setting of a service within a relation."""
        # Invalid relations cause an exception.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_relation_service,
                          "r-1",
                          "s-0")
        self.topology.add_relation("r-1", "rel-type")

        # Invalid services cause an exception.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_relation_service,
                          "r-1",
                          "s-0")
        self.topology.add_service("s-0", "wordpress")

        # Fetching info for services not assigned to a relation cause an error.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_relation_service,
                          "r-1",
                          "s-0")
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        relation_type, info = self.topology.get_relation_service("r-1", "s-0")

        self.assertEqual(info["name"], "name")
        self.assertEqual(info["role"], "role")
        self.assertEqual(relation_type, "rel-type")

    def test_get_relation_type(self):
        """The type of a relation can be instrospected.
        """
        self.assertRaises(InternalTopologyError,
                          self.topology.get_relation_type,
                          "r-1")

        self.topology.add_relation("r-1", "rel-type")
        self.assertEqual(self.topology.get_relation_type("r-1"),
                         "rel-type")

    def test_get_relations(self):
        names = self.topology.get_relations()
        self.assertEqual(names, [])

        self.topology.add_relation("r-1", "type")
        names = self.topology.get_relations()
        self.assertEqual(names, ["r-1"])

        self.topology.add_relation("r-2", "type")
        names = self.topology.get_relations()
        self.assertEqual(set(names), set(["r-1", "r-2"]))

    def test_get_services_for_relations(self):
        """The services for a given relation can be retrieved."""
        self.topology.add_relation("r-1", "type")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "database")
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        self.topology.assign_service_to_relation("r-1", "s-1", "name", "role2")
        self.assertEqual(
            self.topology.get_relation_services("r-1"),
            {'s-1': {'role': 'role2', 'name': 'name'},
             's-0': {'role': 'role', 'name': 'name'}})

    def test_get_relations_for_service(self):
        """The relations for a given service can be retrieved.
        """
        # Getting relations for unknown service raises a topologyerror.
        self.assertRaises(InternalTopologyError,
                          self.topology.get_relations_for_service,
                          "s-0")

        # A new service has no relations.
        self.topology.add_service("s-0", "wordpress")
        self.assertFalse(self.topology.get_relations_for_service("s-0"))

        # Add a relation and fetch it.
        self.topology.add_relation("r-1", "type")
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        self.topology.add_relation("r-2", "type")
        self.topology.assign_service_to_relation("r-2", "s-0", "name", "role")

        self.assertEqual(
            sorted(self.topology.get_relations_for_service("s-0")),
            [("r-1", "type", {"name": "name", "role": "role"}),
             ("r-2", "type", {"name": "name", "role": "role"})])

        self.topology.unassign_service_from_relation("r-2", "s-0")

    def test_remove_relation(self):
        """A relation can be removed.
        """
        # Attempting to remove unknown relation raises a topologyerror
        self.assertRaises(InternalTopologyError,
                          self.topology.remove_relation,
                          "r-1")

        # Adding a relation with associated service, and remove it.
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_relation("r-1", "type")
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")
        self.assertTrue(self.topology.has_relation("r-1"))
        self.topology.remove_relation("r-1")
        self.assertFalse(self.topology.has_relation("r-1"))

    def test_remove_service_with_relations(self):
        """
        Attempting to remove a service that's assigned to relations
        raises an InternalTopologyError.
        """
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_relation("r-1", "type")
        self.topology.assign_service_to_relation("r-1", "s-0", "name", "role")

        self.assertRaises(InternalTopologyError,
                          self.topology.remove_service,
                          "s-0")

    def test_has_relation_between_dyadic_endpoints(self):
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysqldb")
        self.topology.add_relation("r-0", "mysql")
        self.topology.assign_service_to_relation(
            "r-0", "s-0", "mysql", "client")
        self.topology.assign_service_to_relation(
            "r-0", "s-1", "db", "server")
        self.assertTrue(self.topology.has_relation_between_endpoints([
            mysql_ep, blog_ep]))
        self.assertTrue(self.topology.has_relation_between_endpoints([
            blog_ep, mysql_ep]))

    def test_has_relation_between_dyadic_endpoints_missing_assignment(self):
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysqldb")
        self.topology.add_relation("r-0", "mysql")
        self.topology.assign_service_to_relation(
            "r-0", "s-1", "db", "server")
        self.assertFalse(self.topology.has_relation_between_endpoints([
            mysql_ep, blog_ep]))
        self.assertFalse(self.topology.has_relation_between_endpoints([
            blog_ep, mysql_ep]))

    def test_has_relation_between_dyadic_endpoints_wrong_relation_name(self):
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "wrong-name", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysqldb")
        self.topology.add_relation("r-0", "mysql")
        self.topology.assign_service_to_relation(
            "r-0", "s-0", "mysql", "client")
        self.topology.assign_service_to_relation(
            "r-0", "s-1", "db", "server")
        self.assertFalse(self.topology.has_relation_between_endpoints([
            mysql_ep, blog_ep]))
        self.assertFalse(self.topology.has_relation_between_endpoints([
            blog_ep, mysql_ep]))

    def test_has_relation_between_monadic_endpoints(self):
        riak_ep = RelationEndpoint("riak", "riak", "riak", "peer")
        self.topology.add_service("s-0", "riak")
        self.topology.add_relation("r-0", "riak")
        self.topology.assign_service_to_relation("r-0", "s-0", "riak", "peer")
        self.assertTrue(self.topology.has_relation_between_endpoints(
            [riak_ep]))

    def test_get_relation_between_dyadic_endpoints(self):
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysqldb")
        self.topology.add_relation("r-0", "mysql")
        self.topology.assign_service_to_relation(
            "r-0", "s-0", "mysql", "client")
        self.topology.assign_service_to_relation(
            "r-0", "s-1", "db", "server")
        self.assertEqual(self.topology.get_relation_between_endpoints([
            mysql_ep, blog_ep]), "r-0")
        self.assertEqual(self.topology.get_relation_between_endpoints([
            blog_ep, mysql_ep]), "r-0")

    def test_get_relation_between_dyadic_endpoints_missing_assignment(self):
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "db", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysqldb")
        self.topology.add_relation("r-0", "mysql")
        self.topology.assign_service_to_relation(
            "r-0", "s-1", "db", "server")
        self.assertEqual(self.topology.get_relation_between_endpoints([
            mysql_ep, blog_ep]), None)
        self.assertEqual(self.topology.get_relation_between_endpoints([
            blog_ep, mysql_ep]), None)

    def test_get_relation_between_dyadic_endpoints_wrong_relation_name(self):
        mysql_ep = RelationEndpoint("mysqldb", "mysql", "wrong-name", "server")
        blog_ep = RelationEndpoint("wordpress", "mysql", "mysql", "client")
        self.topology.add_service("s-0", "wordpress")
        self.topology.add_service("s-1", "mysqldb")
        self.topology.add_relation("r-0", "mysql")
        self.topology.assign_service_to_relation(
            "r-0", "s-0", "mysql", "client")
        self.topology.assign_service_to_relation(
            "r-0", "s-1", "db", "server")
        self.assertEqual(self.topology.get_relation_between_endpoints([
            mysql_ep, blog_ep]), None)
        self.assertEqual(self.topology.get_relation_between_endpoints([
            blog_ep, mysql_ep]), None)

    def test_get_relation_between_monadic_endpoints(self):
        riak_ep = RelationEndpoint("riak", "riak", "riak", "peer")
        self.topology.add_service("s-0", "riak")
        self.topology.add_relation("r-0", "riak")
        self.topology.assign_service_to_relation("r-0", "s-0", "riak", "peer")
        self.assertEqual(self.topology.get_relation_between_endpoints(
            [riak_ep]), "r-0")
