#!/usr/bin/python
import MySQLdb
import os

SQL_HOST = "db01.mlp1.peakhosting.com"
SQL_USER = "admintool-read"
SQL_PASSWORD = "Y37ji68q9v"
SQL_DATABASE = "admintool"

CMK_CONF_PATH = "/home/mjulian/cmkautomation/output"

DEVICE_TYPES = {'Router': {'tags': ['router', 'snmp', 'wato', 'lan', 'prod']},
                'Distribution Switch': {'tags': ['distributionswitch', 'snmp', 'wato', 'lan', 'prod']},
                'Core Switch': {'tags': ['coreswitch', 'snmp', 'wato', 'lan', 'prod']},
                'Firewall': {'tags': ['firewall', 'snmp', 'wato', 'lan', 'prod']},
                'OOB Switch': {'tags': ['oobswitch', 'snmp', 'wato', 'lan', 'prod']},
                'Rack Switch': {'tags': ['rackswitch', 'snmp', 'wato', 'lan', 'prod']},
                'Access Switch': {'tags': ['accessswitch', 'snmp', 'wato', 'lan', 'prod']},
                'Terminal Server': {'tags': ['terminalserver', 'snmp', 'wato', 'lan', 'prod']},
                'SLB': {'tags': ['slb', 'snmp', 'wato', 'lan', 'prod']},
                'VPN Appliance': {'tags': ['vpnappliance', 'snmp', 'wato', 'lan', 'prod']},
                'Server': {'tags': ['lom', 'ping', 'wato', 'lan', 'prod']},
                'PDU': {'tags': ['pdu', 'snmp', 'wato', 'lan', 'prod']},
                }


def query_sql(device_type):
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

    server_query = " \
    select lower(device.peakname), lower(colo.name) from device \
    join rack on rack.id = device.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    join device_type on device_type.id = device.type \
    where \
    device.monitor_device = 1 and \
    device_type.type = '%s' and device.peakname like 'peak____' \
    " % device_type

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

    pdu_query = " \
    select lower(pdu.hostname), lower(colo.name) from pdu \
    join rack on rack.id = pdu.rackid \
    join cage on cage.id = rack.cageid \
    join colo on colo.id = cage.coloid \
    "

    con = MySQLdb.Connection(SQL_HOST, SQL_USER, SQL_PASSWORD, SQL_DATABASE)
    cursor = con.cursor()
    if device_type == 'Server':
        cursor.execute(server_query)
    elif device_type == 'Rack Switch':
        cursor.execute(rack_switch_query)
    elif device_type == "PDU":
        cursor.execute(pdu_query)
    else:
        cursor.execute(generic_query)
    result = cursor.fetchall()
    return result


def format_device_list(device_list):
    formatted_list = []
    for device_name, colo_name in device_list:
        formatted_device_name = device_name + '.peakhosting.com'
        formatted_list.append((formatted_device_name, colo_name))
    return formatted_list


def create_wato_folders():
    for device_type in DEVICE_TYPES.keys():
        if not os.path.exists(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0])):
            os.makedirs(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0]))


def create_wato_file(device_type):
    with open(os.path.join(CMK_CONF_PATH, DEVICE_TYPES[device_type]['tags'][0], '.wato'), 'w') as file:
        file.write("{'lock': 'Folders managed by System Engineering.', 'attributes': {'tag_device_type': '%s'}, 'num_hosts': %s, 'title': u'%s'}" % (DEVICE_TYPES[device_type]['tags'][0], len(format_device_list(query_sql(device_type))), DEVICE_TYPES[device_type]['tags'][0]))


def create_hosts_file(device_type):
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
            file.write("  '%s': {},\n" % name)
        file.write('})')


if __name__ == "__main__":
    create_wato_folders()
    for device_type in DEVICE_TYPES.keys():
        create_wato_file(device_type)
        create_hosts_file(device_type)
