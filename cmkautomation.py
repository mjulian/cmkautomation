#!/usr/bin/python
import MySQLdb
import os
from subprocess import Popen, PIPE
import socket
import sys

SQL_HOST = ''
SQL_USER = ''
SQL_PASSWORD = ''
SQL_DATABASE = ''

CMK_CONF_PATH = '/etc/check_mk/conf.d/wato'
CMK_EXTRA_CONF_PATH = '/etc/check_mk/conf.d/extra'

# The top-level keys match the device type names from Admintool.
# The tags are arbitrary, but check_mk expects these particular ones.
DEVICE_TYPES = {'Router':               {'tags': ['router', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Border Router':        {'tags': ['borderrouter', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Distribution Switch':  {'tags': ['distributionswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'SAN Switch':           {'tags': ['sanswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Core Switch':          {'tags': ['coreswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Core Router':          {'tags': ['corerouter', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Firewall':             {'tags': ['firewall', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'OOB Switch':           {'tags': ['oobswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Management Switch':    {'tags': ['mgmtswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Rack Switch':          {'tags': ['rackswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Access Switch':        {'tags': ['accessswitch', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Terminal Server':      {'tags': ['terminalserver', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'SLB':                  {'tags': ['slb', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'VPN Appliance':        {'tags': ['vpnappliance', 'snmp-only', 'snmp', 'wato', 'lan', 'prod']},
                'Mgmt Server':          {'tags': ['mgmtserver', 'cmk-agent', 'tcp', 'wato', 'prod']},
                }

SITES = ['nyc', 'lax', 'lon', 'sfo']

hostname, colo, domain, suffix = socket.gethostname().split('.')


def query_sql(device_type):
    # This function should return a tuple of device name and datacenter ID
    return result

def format_device_list(device_list):
    """
    Take in the device list returned from the SQL query and append our domain
    """
    formatted_list = []
    for device_name, colo_name in device_list:
        formatted_device_name = device_name + '.mydomain.com'
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
        file.write("{'lock': 'Folders managed by automated script.', 'attributes': {'tag_device_type': '%s'}, 'num_hosts': %s, 'title': u'%s'}" % (DEVICE_TYPES[device_type]['tags'][0], len(format_device_list(query_sql(device_type))), device_type))


def create_hosts_file(device_type):
    """
    Create the hosts.mk file for each device type. We are writing the file directly.
    """
    with open(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0], 'hosts.mk'), 'w') as file:
        file.write('_lock = True\n')
        file.write('\n')
        file.write("all_hosts += [\n")
        for name, colo in format_device_list(query_sql(device_type)):
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
    print 'Activating local master'
    command = 'chown -R apache:nagios %s %s && /usr/bin/check_mk -O' % (CMK_CONF_PATH, CMK_EXTRA_CONF_PATH)
    communicate_cli(command)


def activate_remote():
    """
    These folders contain everything required for a distributed WATO sync.
    rsync them to each slave, preseving permissions/ownership.
    """
    USER = 'cmk_automation'
    for site in SITES:
        print 'Syncing and restarting site:', site
        cmd1 = "rsync -az --rsync-path='sudo rsync' -e 'ssh -i /home/%s/.ssh/id_rsa' /var/lib/check_mk/web/ %s@%s.%s.mydomain.com:/var/lib/check_mk/web/" % (USER, USER, SERVER_NAME, site)
        cmd2 = "rsync -az --rsync-path='sudo rsync' -e 'ssh -i /home/%s/.ssh/id_rsa' --delete /etc/check_mk/conf.d/wato/ %s@%s.%s.mydomain.com:/etc/check_mk/conf.d/wato/" % (USER, USER, SERVER_NAME, site)
        cmd3 = "rsync -az --rsync-path='sudo rsync' -e 'ssh -i /home/%s/.ssh/id_rsa' /etc/check_mk/multisite.d/wato/ %s@%s.%s.mydomain.com:/etc/check_mk/multisite.d/wato/" % (USER, USER, SERVER_NAME, site)
        cmd4 = "rsync -az --rsync-path='sudo rsync' -e 'ssh -i /home/%s/.ssh/id_rsa' --delete /etc/check_mk/conf.d/extra/ %s@%s.%s.mydomain.com:/etc/check_mk/conf.d/extra/" % (USER, USER, SERVER_NAME, site)
        cmd5 = "ssh -t -i /home/%s/.ssh/id_rsa %s@%s.%s.mydomain.com \'sudo /usr/bin/check_mk -O\'" % (USER, USER, SERVER_NAME, site)

        communicate_cli(cmd1)
        communicate_cli(cmd2)
        communicate_cli(cmd3)
        communicate_cli(cmd4)
        communicate_cli(cmd5)

if __name__ == '__main__':
    if hostname == 'monitoring-qa':
        print 'Found environment: QA'
        SERVER_NAME = 'mon-qa'
    elif hostname == 'monitoring':
        print 'Found environment: Prod'
        SERVER_NAME = 'mon-prod'
    else:
        print 'Could not determine environment. Exiting.'
        sys.exit()
    create_wato_folders()
    for device_type in DEVICE_TYPES.keys():
        create_wato_file(device_type)
        create_hosts_file(device_type)
    activate_local()
    activate_remote()
