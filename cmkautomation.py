#!/usr/bin/python
import MySQLdb
import os
from subprocess import Popen, PIPE
import socket
import sys

SQL_HOST = "db01.mlp1.peakhosting.com"
SQL_USER = "admintool-read"
SQL_PASSWORD = "Y37ji68q9v"
SQL_DATABASE = "admintool"

CMK_CONF_PATH = "/etc/check_mk/conf.d/wato"
CMK_EXTRA_CONF_PATH = "/etc/check_mk/conf.d/extra"

# The top-level keys match the device type names from Admintool.
# The tags are arbitrary, but check_mk expects these particular ones.
DEVICE_TYPES = {'Router':               {'tags': ['router', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Distribution Switch':  {'tags': ['distributionswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'SAN Switch':           {'tags': ['sanswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Core Switch':          {'tags': ['coreswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Firewall':             {'tags': ['firewall', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'OOB Switch':           {'tags': ['oobswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Rack Switch':          {'tags': ['rackswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Access Switch':        {'tags': ['accessswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Terminal Server':      {'tags': ['terminalserver', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'SLB':                  {'tags': ['slb', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'VPN Appliance':        {'tags': ['vpnappliance', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Server':               {'tags': ['lom', 'ping', 'wato', 'lan', 'prod']},
                'Mgmt Server':          {'tags': ['mgmtserver', 'cmk-agent', 'tcp', 'wato', 'prod']},
                'PDU':                  {'tags': ['pdu', 'snmp-only', 'snmp', 'wato', 'prod']},
                }

# Temporarily keep DAL1 out of this list until it's been rebuilt
SITES = ['ams1', 'iad2', 'mlp1', 'dal2']

hostname, colo, domain, suffix = socket.gethostname().split('.')

def query_sql(device_type):
    # Most of the magic happens here. We do a lot of weird shit to get around
    # AdminTool's data consistency issues. Cleaning up AT is a long-term plan
    # and will make these queries way cleaner.

    # A generic query to get devices and return as a device name and colo
    # This query works for most device types
    generic_query = " \
    select lower(device.peakname), lower(colo.name) from device \
    join rack on rack.id = device.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    join device_type on device_type.id = device.type \
    where \
    device.monitor_device = 1 and \
    device_type.type = '%s' \
    " % device_type

    # Query to get LOM devices. Add another filter to only return
    # devices that match peakXXXX.
    lom_query = " \
    select lower(device.peakname), lower(colo.name) from device \
    join rack on rack.id = device.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    join device_type on device_type.id = device.type \
    where \
    device.monitor_device = 1 and \
    device_type.type = '%s' and device.peakname like 'peak____' \
    " % device_type

    # Query to get rack switches, but filter out all FEXes (since they're dumb)
    rack_switch_query = " \
    select lower(device.peakname), lower(colo.name) from device \
    join rack on rack.id = device.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    join device_type on device_type.id = device.type \
    join vmodel on vmodel.id = device.vmodelid \
    where \
    device.monitor_device = 1 and \
    device_type.type = '%s' and vmodel.name not like 'Cisco Nexus 2148T' \
    " % device_type

    # Query to get PDUs, since they live in a totally different table
    pdu_query = " \
    select lower(pdu.hostname), lower(colo.name) from pdu \
    join rack on rack.id = pdu.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    where pdu.monitor_device = 1
    "

    # Query to get VMs. VMs are assigned to a customer, so we filter
    # on the PWH customer ID.
    mgmtserver_vm_query = " \
    select vm_name from customer_vm \
    where customer_id = 1 \
    "

    # Query to get baremetal management servers.
    # A bit messy because of consistency issues in Admintool
    mgmtserver_baremetal_query = " \
    select lower(device.custname), lower(colo.name) from device \
    join rack on rack.id = device.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    join device_type on device_type.id = device.type \
    where \
    device.monitor_device = 1 \
    and device_type.type = 'Server' \
    and custid = 1 \
    and device.custname <> '' \
    and device.custname like '%.____' \
    and device.custname not like 'mon01.____'\
    "

    con = MySQLdb.Connection(SQL_HOST, SQL_USER, SQL_PASSWORD, SQL_DATABASE)
    cursor = con.cursor()
    if device_type == 'Server':
        cursor.execute(lom_query)
        result = cursor.fetchall()
        return result
    elif device_type == 'Rack Switch':
        cursor.execute(rack_switch_query)
        result = cursor.fetchall()
        return result
    elif device_type == "PDU":
        cursor.execute(pdu_query)
        result = cursor.fetchall()
        return result
    elif device_type == "Mgmt Server":
        mgmt_servers = []
        cursor.execute(mgmtserver_vm_query)
        mgmt_vm_list = cursor.fetchall()
        for entry in mgmt_vm_list:
            for vm in entry:
                hostname, colo, domain = vm.split('.', 2)
                mgmt_servers.append(('.'.join((hostname, colo)), colo))
        cursor.execute(mgmtserver_baremetal_query)
        mgmt_baremetal_list = cursor.fetchall()
        for bm_server in mgmt_baremetal_list:
            if bm_server[0].startswith('mon01'):
                pass
            else:
                mgmt_servers.append((bm_server[0], bm_server[1]))
        return mgmt_servers
    else:
        cursor.execute(generic_query)
        result = cursor.fetchall()
        return result


def format_device_list(device_list):
    """
    Take in the device list returned from the SQL query and append our domain
    """
    formatted_list = []
    for device_name, colo_name in device_list:
        formatted_device_name = device_name + '.peakhosting.com'
        formatted_list.append((formatted_device_name, colo_name))
    return formatted_list


def create_wato_folders():
    """
    Enumerate device types from the dict and then create a folder for each type
    """
    for device_type in DEVICE_TYPES.keys():
        if not os.path.exists(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0])):
            os.makedirs(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0]))


def create_wato_file(device_type):
    """
    Create the .wato file. We are writing the file directly.
    """
    with open(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0], '.wato'), 'w') as file:
        file.write("{'lock': 'Folders managed by System Engineering.', 'attributes': {'tag_device_type': '%s'}, 'num_hosts': %s, 'title': u'%s'}" % (DEVICE_TYPES[device_type]['tags'][0], len(format_device_list(query_sql(device_type))), device_type))


def create_hosts_file(device_type):
    """
    Create the hosts.mk file for each device type. We are writing the file directly.
    """
    with open(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0], 'hosts.mk'), 'w') as file:
        file.write('_lock = True\n')
        file.write('\n')
        file.write("all_hosts += [\n")
        for name, colo in format_device_list(query_sql(device_type)):
            if device_type == 'Server':
                name = name.replace('.peakhosting.com', '.lom.peakhosting.com')
            if device_type == 'PDU':
                name = name.replace('.peakwebhosting.com.peakhosting.com', '.peakhosting.com')
            tags = [tag for tag in DEVICE_TYPES[device_type]['tags']]
            file.write("  '" + name + "|" + '|'.join(tags) + "|site:" + colo + "|/' + FOLDER_PATH + '/',\n")
        file.write(']')
        file.write('\n')
        file.write('host_attributes.update({\n')
        for name, colo in format_device_list(query_sql(device_type)):
            file.write("  '%s': {'site': '%s' },\n" % (name, colo))
        file.write('})')


def communicate_cli(command):
    process = Popen(command, shell=True, stderr=PIPE, stdout=PIPE, universal_newlines=True)
    stdout, stderr = process.communicate()
    print stdout, stderr


def activate_local():
    """
    Activating the local master is easy. Ensure ownership, restart cmk."
    """
    print "Activating local master"
    command = "chown -R apache:nagios %s %s && /usr/bin/check_mk -O" % (CMK_CONF_PATH, CMK_EXTRA_CONF_PATH)
    communicate_cli(command)


def activate_remote():
    """
    These folders contain everything required for a distributed WATO sync.
    rsync them to each slave, preseving permissions/ownership.
    """
    USER = "cmk_automation"
    for site in SITES:
        print "Activating %s" % site
        cmd1 = "rsync -az /var/lib/check_mk/web/ root@%s.%s.peakhosting.com:/var/lib/check_mk/web/" % (SERVER_NAME, site)
        cmd2 = "rsync -az /etc/nagios/authconfigs/ root@%s.%s.peakhosting.com:/etc/nagios/authconfigs/" % (SERVER_NAME, site)
        cmd3 = "rsync -az --delete /etc/check_mk/conf.d/wato/ root@%s.%s.peakhosting.com:/etc/check_mk/conf.d/wato/" % (SERVER_NAME, site)
        cmd4 = "rsync -az /etc/check_mk/multisite.d/wato/ root@%s.%s.peakhosting.com:/etc/check_mk/multisite.d/wato/" % (SERVER_NAME, site)
        cmd5 = "rsync -az --delete /etc/check_mk/conf.d/extra/ root@%s.%s.peakhosting.com:/etc/check_mk/conf.d/extra/" % (SERVER_NAME, site)
        cmd6 = "ssh root@%s.%s.peakhosting.com \'/usr/bin/check_mk -O\'" % (SERVER_NAME, site)

        communicate_cli(cmd1)
        communicate_cli(cmd2)
        communicate_cli(cmd3)
        communicate_cli(cmd4)
        communicate_cli(cmd5)

if __name__ == "__main__":
    if hostname == "monitoring-qa":
        print "Found environment: QA"
        SERVER_NAME = "mon-qa"
    elif hostname == "monitoring":
        print "Found environment: Prod"
        SERVER_NAME = "mon01"
    else:
        print "Could not determine environment. Exiting."
        sys.exit()
    create_wato_folders()
    for device_type in DEVICE_TYPES.keys():
        create_wato_file(device_type)
        create_hosts_file(device_type)
    activate_local()
    activate_remote()
