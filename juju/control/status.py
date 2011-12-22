from fnmatch import fnmatch
import argparse
import functools
import json
import sys

from twisted.internet.defer import inlineCallbacks, returnValue
import yaml

from juju.errors import  ProviderError
from juju.environment.errors import EnvironmentsConfigError
from juju.state.errors import UnitRelationStateNotFound
from juju.state.charm import CharmStateManager
from juju.state.machine import MachineStateManager
from juju.state.service import ServiceStateManager
from juju.state.relation import RelationStateManager
from juju.unit.workflow import WorkflowStateClient


# a minimal registry for renderers
# maps from format name to callable
renderers = {}


def configure_subparser(subparsers):
    sub_parser = subparsers.add_parser(
        "status", help=status.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=command.__doc__)

    sub_parser.add_argument(
        "--environment", "-e",
        help="Environment to status.")

    sub_parser.add_argument("--output",
                            help="An optional filename to output "
                            "the result to",
                            type=argparse.FileType("w"),
                            default=sys.stdout)

    sub_parser.add_argument("--format",
                            help="Select an output format",
                            default="yaml"
                           )

    sub_parser.add_argument("scope",
                            nargs="*",
                            help="""scope of status request, service or unit"""
                                 """ must match at least one of these""")

    return sub_parser


def command(options):
    """Output status information about a deployment.

    This command will report on the runtime state of various system
    entities.

    $ juju status

    will return data on entire default deployment.

    $ juju status -e DEPLOYMENT2

    will return data on the DEPLOYMENT2 envionment.
    """
    environment = options.environments.get(options.environment)
    if environment is None and options.environment:
        raise EnvironmentsConfigError(
            "Invalid environment %r" % options.environment)
    elif environment is None:
        environment = options.environments.get_default()

    renderer = renderers.get(options.format)
    if renderer is None:
        formats = sorted(renderers.keys())
        formats = ", ".join(formats)

        raise SystemExit(
            "Unsupported render format %s (valid formats: %s)." % (
                options.format, formats))

    return status(environment,
                  options.scope,
                  renderer,
                  options.output,
                  options.log)


@inlineCallbacks
def status(environment, scope, renderer, output, log):
    """Display environment status information.
    """
    provider = environment.get_machine_provider()
    client = yield provider.connect()
    try:
        # Collect status information
        state = yield collect(scope, provider, client, log)
    finally:
        yield client.close()
    # Render
    renderer(state, output, environment)


def digest_scope(scope):
    """Parse scope used to filter status information.

    `scope`: a list of name specifiers. see collect()

    Returns a tuple of (service_filter, unit_filter). The values in
    either filter list will be passed as a glob to fnmatch
    """

    services = []
    units = []

    if scope is not None:
        for value in scope:
            if "/" in value:
                units.append(value)
            else:
                services.append(value)

    return (services, units)


@inlineCallbacks
def collect(scope, machine_provider, client, log):
    """Extract status information into nested dicts for rendering.

       `scope`: an optional list of name specifiers. Globbing based
       wildcards supported. Defaults to all units, services and
       relations.

       `machine_provider`: machine provider for the environment

       `client`: ZK client connection

       `log`: a Python stdlib logger.
    """
    service_manager = ServiceStateManager(client)
    relation_manager = RelationStateManager(client)
    machine_manager = MachineStateManager(client)
    charm_manager = CharmStateManager(client)

    service_data = {}
    machine_data = {}
    state = dict(services=service_data, machines=machine_data)

    seen_machines = set()
    filter_services, filter_units = digest_scope(scope)

    services = yield service_manager.get_all_service_states()
    for service in services:
        if len(filter_services):
            found = False
            for filter_service in filter_services:
                if fnmatch(service.service_name, filter_service):
                    found = True
                    break
            if not found:
                continue

        unit_data = {}
        relation_data = {}

        charm_id = yield service.get_charm_id()
        charm = yield charm_manager.get_charm_state(charm_id)

        service_data[service.service_name] = dict(units=unit_data,
                                                  charm=charm.id,
                                                  relations=relation_data)
        exposed = yield service.get_exposed_flag()
        if exposed:
            service_data[service.service_name].update(exposed=exposed)

        units = yield service.get_all_unit_states()
        unit_matched = False

        relations = yield relation_manager.get_relations_for_service(service)

        for unit in units:
            if len(filter_units):
                found = False
                for filter_unit in filter_units:
                    if fnmatch(unit.unit_name, filter_unit):
                        found = True
                        break
                if not found:
                    continue

            u = unit_data[unit.unit_name] = dict()
            machine_id = yield unit.get_assigned_machine_id()
            u["machine"] = machine_id
            unit_workflow_client = WorkflowStateClient(client, unit)
            unit_state = yield unit_workflow_client.get_state()
            if not unit_state:
                u["state"] = "pending"
            else:
                unit_connected = yield unit.has_agent()
                u["state"] = unit_state if unit_connected else "down"
            if exposed:
                open_ports = yield unit.get_open_ports()
                u["open-ports"] = ["{port}/{proto}".format(**port_info)
                                   for port_info in open_ports]

            u["public-address"] = yield unit.get_public_address()

            # indicate we should include information about this
            # machine later
            seen_machines.add(machine_id)
            unit_matched = True

            # collect info on each relation for the service unit
            relation_status = {}
            for relation in relations:
                try:
                    relation_unit = yield relation.get_unit_state(unit)
                except UnitRelationStateNotFound:
                    # This exception will occur when relations are
                    # established between services without service
                    # units, and therefore never have any
                    # corresponding service relation units. This
                    # scenario does not occur in actual deployments,
                    # but can happen in test circumstances. In
                    # particular, it will happen with a misconfigured
                    # provider, which exercises this codepath.
                    continue  # should not occur, but status should not fail
                relation_workflow_client = WorkflowStateClient(
                    client, relation_unit)
                relation_workflow_state = \
                    yield relation_workflow_client.get_state()
                relation_status[relation.relation_name] = dict(
                    state=relation_workflow_state)
            u["relations"] = relation_status

        # after filtering units check if any matched or remove the
        # service from the output
        if filter_units and not unit_matched:
            del service_data[service.service_name]
            continue

        for relation in relations:
            rel_services = yield relation.get_service_states()

            # A single related service implies a peer relation. More
            # imply a bi-directional provides/requires relationship.
            # In the later case we omit the local side of the relation
            # when reporting.
            if len(rel_services) > 1:
                # Filter out self from multi-service relations.
                rel_services = [
                    rsn for rsn in rel_services if rsn.service_name !=
                    service.service_name]

            if len(rel_services) > 1:
                raise ValueError("Unexpected relationship with more "
                                 "than 2 endpoints")

            rel_service = rel_services[0]
            relation_data[relation.relation_name] = rel_service.service_name

    machines = yield machine_manager.get_all_machine_states()
    for machine_state in machines:
        if (filter_services or filter_units) and \
                machine_state.id not in seen_machines:
            continue

        instance_id = yield machine_state.get_instance_id()
        m = {"instance-id": instance_id \
             if instance_id is not None else "pending"}
        if instance_id is not None:
            try:
                pm = yield machine_provider.get_machine(instance_id)
                m["dns-name"] = pm.dns_name
                m["instance-state"] = pm.state
                if (yield machine_state.has_agent()):
                    # if the agent's connected, we're fine
                    m["state"] = "running"
                else:
                    units = (yield machine_state.get_all_service_unit_states())
                    for unit in units:
                        unit_workflow_client = WorkflowStateClient(client, unit)
                        if (yield unit_workflow_client.get_state()):
                            # for unit to have a state, its agent must have
                            # run, which implies the machine agent must have
                            # been running correctly at some point in the past
                            m["state"] = "down"
                            break
                    else:
                        # otherwise we're probably just still waiting
                        m["state"] = "not-started"
            except ProviderError:
                # The provider doesn't have machine information
                log.error(
                    "Machine provider information missing: machine %s" % (
                        machine_state.id))

        machine_data[machine_state.id] = m

    returnValue(state)


def render_yaml(data, filelike, environment):
    # remove the root nodes empty name
    yaml.safe_dump(data, filelike, default_flow_style=False)

renderers["yaml"] = render_yaml


def jsonify(data, filelike, pretty=True, **kwargs):
    args = dict(skipkeys=True)
    args.update(kwargs)
    if pretty:
        args["sort_keys"] = True
        args["indent"] = 4
    return json.dump(data, filelike, **args)


def render_json(data, filelike, environment):
    jsonify(data, filelike)

renderers["json"] = render_json


# Supplement kwargs provided to pydot.Cluster/Edge/Node.
# The first key is used as the data type selector.
DEFAULT_STYLE = {
    "service_container": {
        "bgcolor": "#dedede",
        },
    "service": {
        "color": "#772953",
        "shape": "component",
        "style": "filled",
        "fontcolor": "#ffffff",
        },
    "unit": {
        "color": "#DD4814",
        "fontcolor": "#ffffff",
        "shape": "box",
        "style": "filled",
        },
    "relation": {
        "dir": "none"}
    }


def safe_dot_label(name):
    """Convert a name to a label safe for use in DOT.

    Works around an issue where service names like wiki-db will
    produce DOT items with names like cluster_wiki-db where the
    trailing '-' invalidates the name.
    """
    return name.replace("-", "_")


def render_dot(
    data, filelike, environment, format="dot", style=DEFAULT_STYLE):
    """Render a graphiz output of the status information.
    """
    try:
        import pydot
    except ImportError:
        raise SystemExit("You need to install the pydot "
                         "library to support DOT visualizations")

    dot = pydot.Dot(graph_name=environment.name)
    # first create a cluster for each service
    seen_relations = set()
    for service_name, service in data["services"].iteritems():

        cluster = pydot.Cluster(
            safe_dot_label(service_name),
            shape="component",
            label="%s service" % service_name,
            **style["service_container"])

        snode = pydot.Node(safe_dot_label(service_name),
                           label="<%s<br/>%s>" % (
                               service_name,
                               service["charm"]),
                           **style["service"])
        cluster.add_node(snode)

        for unit_name, unit in service["units"].iteritems():
            un = pydot.Node(safe_dot_label(unit_name),
                            label="<%s<br/><i>%s</i>>" % (
                                unit_name,
                                unit.get("public-address")),
                            **style["unit"])
            cluster.add_node(un)

            cluster.add_edge(pydot.Edge(snode, un))

        dot.add_subgraph(cluster)

        # now map the relationships
        for kind, relation in service["relations"].iteritems():
            src = safe_dot_label(relation)
            dest = safe_dot_label(service_name)
            descriptor = tuple(sorted((src, dest)))
            if descriptor not in seen_relations:
                seen_relations.add(descriptor)
                dot.add_edge(pydot.Edge(
                        src,
                        dest,
                        label=kind,
                        **style["relation"]
                    ))

    if format == 'dot':
        filelike.write(dot.to_string())
    else:
        filelike.write(dot.create(format=format))

renderers["dot"] = render_dot
renderers["svg"] = functools.partial(render_dot, format="svg")
renderers["png"] = functools.partial(render_dot, format="png")
