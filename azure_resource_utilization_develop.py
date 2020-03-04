# azure_under_utilized.py
#

# The purpose of this script is to fetch all the Instances that are currently running under the Azure TIES subscription.
# Get the Meter details from Azure.
# Get the resource utilization of each of the instance in the subscription.
# Generate a html file from the data that captured above.
# Provide that html file as input to jenkins html publisher post build step.

from optparse import OptionParser
import re
import subprocess
import json
import requests
# from AzureMetrics import get_cpu_usage

from operator import attrgetter
from datetime import datetime, timedelta
from collections import defaultdict
import datetime
from azure.monitor import MonitorClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource.resources import ResourceManagementClient
import datetime

html_file = ""
subscription_id = '############'

client_id = '########'
secret = '########'
tenant = '#########'
azure_mgmt_url = 'https://management.azure.com/subscriptions/'
min_date = "2017-05-23"
max_date = "2017-05-23"

credentials = ServicePrincipalCredentials(
    client_id='#####',
    secret='######',
    tenant='#######'
)

client = MonitorClient(
    credentials,
    subscription_id
)
resource_client = ResourceManagementClient(credentials, subscription_id)


def get_cpu_usage(resource_group_name, vm_name):
    # resource_group_name = 'JENKINS-RG'
    # vm_name = 'bitter-shadow-0494'

    metric_units = {'Percentage CPU': 'average', 'Network In': 'total', 'Network Out': 'total',
                    'Disk Read Bytes': 'total', 'Disk Write Bytes': 'total', 'Disk Read Operations/Sec': 'average',
                    'Disk Write Operations/Sec': 'average'}
    resource_id = (
        "subscriptions/{}/"
        "resourceGroups/{}/"
        "providers/Microsoft.Compute/virtualMachines/{}"
    ).format(subscription_id, resource_group_name, vm_name)

    supported_metrics = []
    for metric in client.metric_definitions.list(resource_id):
        # print("{}: id={}, unit={}".format(
        #       metric.name.localized_value,
        #       metric.name.value,
        #       metric.unit
        # ))
        supported_metrics.append(metric.name.value)

    end_time = datetime.datetime.now().date() + datetime.timedelta(days=1)
    start_time = end_time - datetime.timedelta(days=3)
    vm_metrics = defaultdict(dict)

    for metric in supported_metrics:
        filter = " and ".join([
            "name.value eq '{}'".format(metric),
            # "aggregationType eq 'Total'",
            "startTime eq {}".format(start_time),
            "endTime eq {}".format(end_time),
            "timeGrain eq duration'PT1H'"
        ])

        metrics_data = client.metrics.list(
            resource_id,
            filter=filter
        )

        total_units = 0
        average_cpu_usage = 0
        for item in metrics_data:
            # print("{} ({})".format(item.name.localized_value, item.unit.name))
            for data in item.data:
                # print("{}: {}".format(data.time_stamp, data.average))
                if data.__dict__[metric_units[metric]]:
                    average_cpu_usage = average_cpu_usage + data.__dict__[metric_units[metric]]
                    total_units = total_units + 1

        vm_metrics[metric] = 0
        if total_units > 0:
            vm_metrics[metric] = average_cpu_usage / total_units

    return vm_metrics


def get_access_token():
    try:
        url = 'https://login.microsoftonline.com/' + tenant + '/oauth2/token'
        data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret,
                "resource": "https://management.azure.com/"}
        response = requests.get(url, data=data)
        result = json.loads(response.text)
        access_token = result['access_token']
        return access_token
    except Exception as e:
        print "Failed"
        raise


known_owners = set()
unknown_owners = set()
args = None


class Owner:

    def __init__(self, name, email):
        # self.owner_id = owner_id
        self.name = name
        self.email = email


class Resource:
    def __init__(self, resource_id):
        self.quantity = 0
        self.cost = 0
        self.resource_id = resource_id
        self.name = ''
        self.resource_group = ''
        self.category = ''


class ResourceGroup:
    def __init__(self):
        self.network = defaultdict(dict)
        self.storage = defaultdict(dict)
        self.virtual_machine = defaultdict(dict)


class Instance:

    def __init__(self, instance=None):
        self.id = ""
        self.vmid = ""
        self.instance_type = ""
        self.name = ""
        self.owner_code = ""
        self.owner = Owner("", "")
        self.stack_id = ""
        self.region = ""
        self.underutilized = False
        self.public_ip = ""
        self.resource_group_name = ""
        self.sku = ""
        self.status = True
        self.create_date = ""
        self.cost = 0
        self.hours = 0
        self.average_cpu_usage = 0
        self.network_in = 0
        self.network_out = 0
        self.disks_read = 0
        self.disks_write = 0
        self.disks_read_operations = 0
        self.disks_write_operations = 0

        if instance != None:

            self.id = instance['id'].lower()
            self.name = instance['id'].split("/")[-1].lower()
            self.instance_type = instance['type']
            self.network_interfaces = instance['properties']['networkProfile']['networkInterfaces']
            self.vmid = instance['properties']['vmId']
            self.resource_group_name = instance['id'].split("/")[-5]
            self.sku = instance['properties']['hardwareProfile']['vmSize']
            self.region = instance['location']

            if 'tags' not in instance.keys():
                #    print "No tags for instance ", self.id
                return

            if 'tags' in instance.keys():
                if 'Owner' in instance['tags'].keys():
                    self.owner_code = instance['tags']['Owner'].split("@")[0]
                if 'CreateDate' in instance['tags'].keys():
                    created_date = instance['tags']['CreateDate'].split(" ")[0].split("/")
                    self.create_date = created_date[2] + '-' + created_date[0] + '-' + created_date[1]
                    global min_date
                    if self.create_date < min_date:
                        min_date = self.create_date


owner_dict = {
}


def get_running_instances():
    running_dict = defaultdict(dict)
    url = azure_mgmt_url + subscription_id + '/providers/Microsoft.Compute/virtualmachines?api-version=2016-04-30-preview'
    headers = {"Authorization": "Bearer " + get_access_token()}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    Vms_in_subscription = []
    while 'nextLink' in result.keys():
        Vms_in_subscription = Vms_in_subscription + result['value']
        url = result['nextLink']
        response = requests.get(url, headers=headers)
        result = json.loads(response.text)

    if 'value' in result.keys():
        Vms_in_subscription = Vms_in_subscription + result['value']

    access_token = get_access_token()
    print "Get Running status of each Instance"
    for instance in Vms_in_subscription:
        new_instance = Instance(instance)

        url = (azure_mgmt_url + subscription_id + '/resourceGroups/' + new_instance.resource_group_name +
               '/providers/Microsoft.Compute/virtualMachines/' + new_instance.name + '/InstanceView?api-version=2015-05-01-preview')
        headers = {"Authorization": "Bearer " + access_token}
        response = requests.get(url, headers=headers)
        result = json.loads(response.text)
        if 'statuses' in result.keys() and 'displayStatus' in result['statuses'][-1].keys():
            vm_status = result['statuses'][-1]['displayStatus']
            new_instance.status = vm_status

        running_dict[new_instance.region][new_instance.id] = new_instance

    return running_dict


def get_cost_info(azure_rate_card, running_dict):
    vm_utilization_details = []
    # start_date = min_date
    # print "Starting date is : "+str(start_date)
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')
    url = (azure_mgmt_url + subscription_id +
           '/providers/Microsoft.Commerce/UsageAggregates?api-version=2015-06-01-preview&reportedStartTime=' +
           start_date + 'T00%3a00%3a00%2b00%3a00&reportedEndTime=' + end_date +
           'T00%3a00%3a00%2b00%3a00&aggregationGranularity=Daily&showDetails=true')
    headers = {"Authorization": "Bearer " + get_access_token()}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    vm_util_data = defaultdict(list)

    while 'nextLink' in result.keys():
        vm_utilization_details = vm_utilization_details + result['value']
        url = result['nextLink']
        response = requests.get(url, headers=headers)
        result = json.loads(response.text)

    if 'value' in result.keys():
        vm_utilization_details = vm_utilization_details + result['value']

    for vm_data in vm_utilization_details:
        if 'meterCategory' in vm_data['properties'].keys():
            instance_data_obj = json.loads(vm_data['properties']['instanceData'])
            resource_id = instance_data_obj['Microsoft.Resources']['resourceUri']
            vm_util_data[resource_id.lower()].append(
                [vm_data['properties']['meterId'], vm_data['properties']['quantity']])

    for region in running_dict:
        for instance_id in running_dict[region].keys():
            for meter_quantity in vm_util_data[instance_id.lower()]:
                meter_id = meter_quantity[0]
                quantity = meter_quantity[1]
                running_dict[region][instance_id].hours = running_dict[region][instance_id].hours + quantity
                running_dict[region][instance_id].cost = running_dict[region][instance_id].cost + (
                        azure_rate_card[meter_id] * quantity)

    return running_dict


def get_azure_ratecard():
    azure_vm_rate_card = {}
    url = (azure_mgmt_url + subscription_id +
           "/providers/Microsoft.Commerce/RateCard?api-version=2015-06-01-preview&$filter=OfferDurableId "
           "eq 'MS-AZR-0017P' and Currency eq 'USD' and Locale eq 'en-US' and RegionInfo eq 'US'")
    headers = {"Authorization": "Bearer " + get_access_token()}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    for meter in result['Meters']:
        azure_vm_rate_card[meter['MeterId']] = meter['MeterRates'].values()[0]

    return azure_vm_rate_card


def match_owners(running_dict):
    global known_owners
    global unknown_owners

    for region in running_dict:
        for id in running_dict[region]:
            instance = running_dict[region][id]

            if instance.owner_code != "":
                if instance.owner_code in owner_dict:
                    known_owners.add(owner_dict[instance.owner_code])
                    instance.owner = owner_dict[instance.owner_code]
                else:
                    unknown_owners.add(instance.owner_code)
                    instance.owner = Owner(instance.owner_code, "")

    return


def create_table(fields, heading='Azure Resource Utilization:', referece=False, rg_name=''):
    global html_file
    if referece:
        html_file = (html_file + '<!DOCTYPE html><html><head><style>table, '
                                 'th, td {    border: 1px solid black;font-family: \'Calibri\'}</style></head><body><h2><a name="' + rg_name + '">' + heading + '</a></h2>')
    else:
        html_file = (html_file + '<!DOCTYPE html><html><head><style>table, '
                                 'th, td {    border: 1px solid black;font-family: \'Calibri\'}</style></head><body><h2>' + heading + '</h2>')
    html_file = html_file + '<table><tr>'
    for field in fields:
        html_file = html_file + '<th>' + str(field) + '</th>'
    html_file = html_file + '</tr>'


def add_row(keys, data):
    global html_file
    html_file = html_file + '<tr>'
    for key in keys:
        if key == 'Resource Group Name' and 'Resource Group Name' in data.keys():
            html_file = html_file + '<td><a href="#' + data[key] + '">' + str(data[key]) + '</a></td>'
        elif 'cost' in data.keys() and data['cost'] > 50:
            html_file = html_file + '<td><font color="red">' + str(data[key]) + '</font></td>'
        else:
            html_file = html_file + '<td>' + str(data[key]) + '</td>'
    html_file = html_file + '</tr>'


def close_table():
    global html_file
    html_file = html_file + '</table></body></html>'


def get_instance_owner_id(instance_id, access_token):
    start_date = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    end_time = datetime.datetime.now().strftime('%H:%M:%S')

    url = azure_mgmt_url + subscription_id + '/providers/microsoft.insights/eventtypes/management/values?api-version=2014-04-01&$filter=eventTimestamp ge \'' + start_date + 'T22:00:37Z\' and eventTimestamp le \'' + end_date + 'T' + end_time + 'Z\' and eventChannels eq \'Admin, Operation\' and resourceUri eq \'' + instance_id + '\''

    headers = {"Authorization": "Bearer " + access_token}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    owner = "UnknownOwner"
    if 'value' in result.keys() and len(result['value']) > 0:
        for each in result['value']:
            if 'caller' in each.keys() and each['caller'] != 'sa.nsgautomation@tdlabsazure.onmicrosoft.com':
                owner = each['caller']
    return owner


def display_results(running_dict, options):
    global owner_dict

    count_total = 0
    count_under = 0
    total_instance_cost = 0.0
    toemail = set()
    toemail.add("")

    format_str = "{:15} {:20} {:12} {:40} {:30} {:16} {:50} {:10} {:6}"

    if options.csv:
        format_str = "{},{},{},{},{},{},{},{},{}"

    fields = ["InstanceName", "InstanceSKU", "ResourceGroupName", "Owner", "Status", "TotalCost", "TotalHours",
              "Region", "VMID"]
    create_table(fields)

    instance_list = []

    for region in running_dict:
        for id in running_dict[region]:
            instance_list.append(running_dict[region][id])

    instance_list.sort(key=attrgetter("cost"), reverse=True)

    access_token = get_access_token()
    for instance in instance_list:
        count_total += 1

        if instance.owner.name == '':
            instance.owner.name = get_instance_owner_id(instance.id, access_token).split("@")[0]
        else:
            instance.owner.name = instance.owner.name.split("@")[0]

        keys = ['name', 'sku', 'rg_name', 'owner', 'status', 'cost', 'hours', 'region', 'vmid']
        values = {'name': instance.name, 'sku': instance.sku, 'rg_name': instance.resource_group_name,
                  'owner': instance.owner.name, 'status': instance.status, 'cost': instance.cost,
                  'hours': instance.hours, 'region': instance.region, 'vmid': instance.vmid}
        add_row(keys, values)

        count_under += 1
        total_instance_cost += instance.cost

        if instance.owner in known_owners:
            toemail.add(instance.owner.email)

    close_table()

    with open('/home/jenkins/azure_under_utilized.html', 'w') as f:
        f.write(html_file)


def display_detailed_result(running_dict, options):
    global owner_dict

    count_total = 0
    count_under = 0
    total_instance_cost = 0.0
    toemail = set()
    toemail.add("")

    format_str = "{:15} {:20} {:12} {:40} {:30} {:16} {:50} {:10} {:6} {:10} {:10} {:10} {:10} {:10} {:10} {:10}"

    if options.csv:
        format_str = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}"

    fields = ["InstanceName", "InstanceSKU", "ResourceGroupName", "Owner", "Status", "TotalCost", "TotalHours",
              "AverageCPU%", "NetworkIn(MB)", "NetworkOut(MB)", "DisksRead(MB)", "DisksWrite(MB)",
              "DiskReadOperations/Sec(Bytes)", "DiskWriteOperations/Sec(Bytes)", "Region", "VMID"]
    create_table(fields, heading='Azure Virtual Machines Utilization Report(<font color="blue">Past 7 days</font>):')

    instance_list = []

    for region in running_dict:
        for id in running_dict[region]:
            instance_list.append(running_dict[region][id])

    instance_list.sort(key=attrgetter("cost"), reverse=True)

    access_token = get_access_token()
    for instance in instance_list:
        count_total += 1

        if instance.owner.name == '':
            instance.owner.name = get_instance_owner_id(instance.id, access_token).split("@")[0]
        else:
            instance.owner.name = instance.owner.name.split("@")[0]

        keys = ['name', 'sku', 'rg_name', 'owner', 'status', 'cost', 'hours', 'avg_cpu', 'net_in', 'net_out',
                'disks_read', 'disks_write', 'disks_read_ops', 'disks_write_ops', 'region', 'vmid']
        values = {'region': instance.region, 'vmid': instance.vmid, 'sku': instance.sku, 'name': instance.name,
                  'rg_name': instance.resource_group_name, 'owner': instance.owner.name, 'status': instance.status,
                  'cost': instance.cost, 'hours': instance.hours, 'avg_cpu': instance.average_cpu_usage,
                  'net_in': instance.network_in, 'net_out': instance.network_out, 'disks_read': instance.disks_read,
                  'disks_write': instance.disks_write, 'disks_read_ops': instance.disks_read_operations,
                  'disks_write_ops': instance.disks_write_operations}
        add_row(keys, values)

        count_under += 1
        total_instance_cost += instance.cost

        if instance.owner in known_owners:
            toemail.add(instance.owner.email)

    close_table()

    with open('/home/jenkins/azure_under_utilized_detailed.html', 'w') as f:
        f.write(html_file)


def get_average_cpu_usage(running_dict):
    average_cpu_utilization = defaultdict(dict)
    convert_to_MB = 1024 * 1024
    for region in running_dict:
        for instance_id in running_dict[region].keys():
            resource_group_name = instance_id.split("/")[-5]
            instance_name = instance_id.split("/")[-1]
            vm_metrics = get_cpu_usage(resource_group_name, instance_name)
            running_dict[region][instance_id].average_cpu_usage = vm_metrics['Percentage CPU']
            running_dict[region][instance_id].network_in = vm_metrics['Network In'] / convert_to_MB
            running_dict[region][instance_id].network_out = vm_metrics['Network Out'] / convert_to_MB
            running_dict[region][instance_id].disks_read = vm_metrics['Disk Read Bytes'] / convert_to_MB
            running_dict[region][instance_id].disks_write = vm_metrics['Disk Write Bytes'] / convert_to_MB
            running_dict[region][instance_id].disks_read_operations = vm_metrics['Disk Read Operations/Sec']
            running_dict[region][instance_id].disks_write_operations = vm_metrics['Disk Write Operations/Sec']
    return running_dict


def get_resource_group_costing(azure_rate_card):
    resource_utilization_details = []
    # start_date = min_date
    # print "Starting date is : "+str(start_date)
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')
    url = (azure_mgmt_url + subscription_id +
           '/providers/Microsoft.Commerce/UsageAggregates?api-version=2015-06-01-preview&reportedStartTime=' +
           start_date + 'T00%3a00%3a00%2b00%3a00&reportedEndTime=' + end_date +
           'T00%3a00%3a00%2b00%3a00&aggregationGranularity=Daily&showDetails=true')
    headers = {"Authorization": "Bearer " + get_access_token()}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    resource_util_data = defaultdict(list)

    while 'nextLink' in result.keys():
        resource_utilization_details = resource_utilization_details + result['value']
        url = result['nextLink']
        response = requests.get(url, headers=headers)
        result = json.loads(response.text)

    if 'value' in result.keys():
        resource_utilization_details = resource_utilization_details + result['value']

    for resource_data in resource_utilization_details:
        if ('meterCategory' in resource_data['properties'].keys()):
            instance_data_obj = json.loads(resource_data['properties']['instanceData'])
            resource_id = instance_data_obj['Microsoft.Resources']['resourceUri']
            resource_util_data[resource_id.lower()].append(
                [resource_data['properties']['meterId'], resource_data['properties']['quantity'],
                 resource_data['properties']['meterCategory']])

    resource_util_dict = defaultdict(dict)
    for resource_id in resource_util_data.keys():
        if resource_id.lower() in resource_util_dict.keys():
            new_resource = resource_util_dict[resource_id.lower()]
        else:
            new_resource = Resource(resource_id.lower())
            resource_util_dict[resource_id.lower()] = new_resource
        new_resource.resource_group = new_resource.resource_id.split("/")[-5]
        new_resource.name = new_resource.resource_id.split("/")[-1]
        for meter_quantity in resource_util_data[resource_id.lower()]:
            meter_id = meter_quantity[0]
            quantity = meter_quantity[1]
            new_resource.category = meter_quantity[2]
            new_resource.quantity = new_resource.quantity + quantity
            new_resource.cost = new_resource.cost + (azure_rate_card[meter_id] * quantity)

    return resource_util_dict


def is_resource_group_alive(resource_group_name):
    try:
        resource_client.resource_groups.get(resource_group_name)
        return True
    except Exception as e:
        return False


def group_by_resource_group(live_data):
    fields = []
    for key in live_data.keys():
        fields.append(live_data[key].keys())
    fields = [i[0] for i in fields]
    fields = list(set(fields))
    fields.sort()
    table_fields = ['Resource Group Name']
    for field in fields:
        table_fields.append(field + ' Cost')
    table_fields.append('Total Cost')
    create_table(table_fields, 'Cost By Resource Group')

    for resource_group in live_data.keys():
        values = defaultdict()
        values['Resource Group Name'] = resource_group
        total_cost = 0
        for field in fields:
            field_cost = 0
            if field in live_data[resource_group].keys():
                for resource in live_data[resource_group][field]:
                    field_cost = field_cost + resource.cost
            else:
                field_cost = 0
            total_cost = total_cost + field_cost
            values[field + ' Cost'] = field_cost
        values['Total Cost'] = total_cost
        add_row(table_fields, values)
    close_table()
    with open('/home/jenkins/azure_under_utilized_detailed.html', 'w') as f:
        f.write(html_file)


def fill_rg_data(rg_data, fields, resource_group):
    create_table(fields,
                 heading='Billing Info for Resource Group : <font color="blue">' + str(resource_group) + '</font>',
                 referece=True, rg_name=str(resource_group))
    categories = rg_data.keys()
    categories.sort()
    keys = ['name', 'resource_group', 'category', 'quantity', 'cost', 'resource_id']
    for category in categories:
        for resource in rg_data[category]:
            values = defaultdict()
            for key in keys:
                values[key] = resource.__dict__[key]
            add_row(keys, values)
    close_table()
    with open('/home/jenkins/azure_under_utilized_detailed.html', 'w') as f:
        f.write(html_file)


def fill_resource_group_data(live_data):
    fields = ['Name', 'Resource_Group', 'Category', 'Quantity', 'Cost', 'ID']
    for resource_group in live_data.keys():
        fill_rg_data(live_data[resource_group], fields, resource_group)


def categorize_data(resources_data, running_dict, options):
    global html_file
    html_file = ""
    categorized_data = defaultdict()
    for resource in resources_data:
        if resource.resource_group not in categorized_data.keys():
            categorized_data[resource.resource_group] = defaultdict(list)
        categorized_data[resource.resource_group][resource.category].append(resource)

    live_data = dict(categorized_data)
    for resource_group in live_data.keys():
        if not is_resource_group_alive(resource_group):
            del live_data[resource_group]

    print "Preparing report for each Resource Group."
    group_by_resource_group(live_data)
    print "Preparing detailed report for each Virtual Machine"
    display_detailed_result(running_dict, options)
    fill_resource_group_data(live_data)


def main():
    global args

    parser = OptionParser(usage="usage: azure_under_utilize [options]",
                          version="azure_under_utilize 2.0")
    parser.add_option('-c', '--csv', action='store_true', default=False, dest="csv", help='output table in csv format')
    parser.add_option('-r', '--regions', action='store', nargs='+', dest="regions", default=['us-east-1', 'us-west-2'],
                      help='Azure regions to search (default of us-east-1 and us-west-2)')

    (options, args) = parser.parse_args()

    command = 'rm -rf azure_under_utilized.html azure_under_utilized_detailed.html'
    subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).wait()
    print "Get the Instance states and their details under current subscription."
    running_dict = get_running_instances()
    print "Fetched Instances details."

    print "Fetching CPU Utilization of each VM."
    running_dict = get_average_cpu_usage(running_dict)
    print "Fetched CPU Utilization details."

    print "Fetching Azure Rate Card"
    azure_rate_card = get_azure_ratecard()
    print "Fetched Azure Rate Card"

    print "Fetch Cost Information of all the current Instances"
    running_dict = get_cost_info(azure_rate_card, running_dict)
    # print "Generating HTML file for report generation"
    # display_results(running_dict, options)
    # print "Basic Report Generated"

    print "Fetching the resource utilization data of all the resources in the current subscription."
    resource_util_cost = get_resource_group_costing(azure_rate_card)
    # print resource_util_cost
    print "Categorizing data(group by RG)"
    categorize_data(resource_util_cost.values(), running_dict, options)
    print "Done"


if __name__ == "__main__":
    main()
