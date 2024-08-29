#!/usr/bin/python
# vim:fileencoding=utf-8:tabstop=8:shiftwidth=4:showtabline:expandtab:softtabstop=4:foldmethod=marker:autoindent

#from __future__ import print_function

#import sys
#import subprocess
import requests
from requests.auth import HTTPBasicAuth
#from requests.exceptions import HTTPError, ConnectionError
#import json
#import jsonpickle
#import os.path
#from jsonpath_ng import jsonpath
#from jsonpath_ng.ext import parse

from ansible.plugins.action import ActionBase
from ansible.module_utils.parsing.convert_bool import boolean
from ansible.module_utils.common.text.converters import to_native
from ansible.errors import AnsibleError, AnsibleAction, _AnsibleActionDone, AnsibleActionFail, AnsibleActionSkip
from ansible.utils.display import Display
from urllib.parse import urlparse
#import dns.resolver
#import copy

import yaml
#import tempfile

# {{{ some debug stuff
#
display = Display()
# use display to message the user like this:
#display.debug()
#display.display()
#display.v()
#display.vv()
#display.vvv()
# you get the idea...

# these are mainly just helpful for debugging
import dumper
import pprint

# a print function that prints to STDERR. Helpful for debugging.
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
# }}}

# magic stuff. If you find out what it does exactly, let me know.
ALWAYS_DELEGATE_FACT_PREFIXES = frozenset((
    'discovered_interpreter_',
))


# {{{ convenience functions for jsonpath
#
def _fetch_list(jp_filter, data):
    jsonpath_expr = parse(jp_filter)
    match = jsonpath_expr.find(data)
    return (match)

def fetch_list(jp_filter, data):
    match= _fetch_list(jp_filter, data)
    if match:
        result= [x.value for x in match]
        return (result)
    else:
        return {}

def fetch_value(jp_filter, data):
    match= _fetch_list(jp_filter, data)
    if match:
        return (match[0].value)
    else:
        return {}
# }}}

class DNSAPIConn:
    def __init__(self, api_url):
        self.api_url= api_url

    def get(self, zone= None):
        try:
            if zone == None:
                request_str= "/".join([self.api_url, "zones"])
            else:
                request_str= "/".join([self.api_url, "zones", zone])
                
            response= requests.get(request_str)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            raise AnsibleError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise AnsibleError(f"Connection failed: {conn_err}")
        except Exception as err:
            raise AnsibleError(f"Other error occurred: {err}")
        else:
            return response

    def post(self, zone, ip, name):
        try:
            response= requests.post(f"{self.api_url}/zones/{zone}/{ip}/{name}")
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            raise AnsibleError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise AnsibleError(f"Connection failed: {conn_err}")
        except Exception as err:
            raise AnsibleError(f"Other error occurred: {err}")
        else:
            return response


    def delete(self, zone, ip, name):
        try:
            response= requests.delete(f"{self.api_url}/zones/{zone}/{ip}/{name}")
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            raise AnsibleError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise AnsibleError(f"Connection failed: {conn_err}")
        except Exception as err:
            raise AnsibleError(f"Other error occurred: {err}")
        else:
            return response


class DNSResolver:
    def __init__(self, dns_server):
        self.dns_servers= [dns_server]

    def getIPs(self, record):
        r = dns.resolver.Resolver()
        r.nameservers= self.dns_servers
        answers = r.query(record, 'A')
        ret= []
        for a in answers:
            ret.append(a.to_text())
        return ret

def prune_ds(d):
    """recursively remove empty lists, empty dicts, or None elements from a dictionary"""

    def empty(x):
        return x is None or x == {} or x == []

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [v for v in (prune_ds(v) for v in d) if not empty(v)]
    else:
        return {k: v for k, v in ((k, prune_ds(v)) for k, v in d.items()) if not empty(v)}


class ActionModule(ActionBase):

    # plugin entry point
    def run(self, tmp=None, task_vars=None):

        if task_vars is None:
            task_vars = dict()
        result = super(ActionModule, self).run(tmp, task_vars)
        del tmp  # tmp no longer has any effect
        got= {}
        wanted= {}
        result['changed']= False

        # get module arguments
        api_url= str(self._task.args.get('api_url', None))
        zone= self._task.args.get('zone', None)
        record= self._task.args.get('record', None)
        value= self._task.args.get('value', None)
        state= self._task.args.get('state', "present")

        target_host= str(task_vars['inventory_hostname'])

        # prep stuff

        if state != "present" and state != "absent":
            raise AnsibleError(f"Unrecognized value in argument 'state': {state}")

        if state == "present" and zone is None:
            raise AnsibleError(f"Can't add records without a zone file given.")

        if type(value) is not list:
            value= [value]

        record= str(record)

        # read records from dnsmasq
        api= DNSAPIConn(api_url)
        res= api.get(zone)

        # handle calls with no zone specified
        if type(res.json()) is list:
            dnsmasq_zones= res.json()
        else:
            dnsmasq_zones=[zone]

        for zone in dnsmasq_zones:
            res= api.get(zone)
            zone= str(zone)

            # "turning around" dnsmasq's record
            a_records={}
            for ip, v in res.json().items():
                for item in v:
                    if item not in a_records:
                        a_records[item]=[ip]
                    else:
                        a_records[item].append(ip)

            if record in a_records:
                got[zone]= {record: a_records[record]}
            else:
                got[zone]= {record: {}}

            wanted_ips=[]
            for ip in value:
                wanted_ips.append(str(ip))

            # sorting got makes diff mode output nicer
            for k,v in got[zone].items():
                got[zone]= {k: list(sorted(set(v)))}

            #wanted= copy.deepcopy(got)
            if state == "present":
                wanted[zone]= {record: list(sorted(set(got[zone][record]) | set(wanted_ips)))}
            if state == "absent":
                wanted[zone]= {record: list(sorted(set(got[zone][record]) - set(wanted_ips)))}

	# prune empty fields, makes the diff look better

        if got != wanted:
            result['changed'] = True
            got_for_diff= prune_ds(got)
            wanted_for_diff= prune_ds(wanted)
            before= None
            after= None
            if got_for_diff:
                before= yaml.dump(got_for_diff, default_flow_style=False)
            if wanted_for_diff:
                after= yaml.dump(wanted_for_diff, default_flow_style=False)
            result['diff'] = dict(before= before, after= after)

        # if nothing needs changing or if we run in check mode we are done
        if self._task.check_mode or not result['changed']:
            return result


        # make some changes

        for zone in dnsmasq_zones:
            res= api.get(zone)
            zone= str(zone)
        
            # add record
            if state == "present":
                to_add= {}
                to_add[record]= list(sorted(set(wanted[zone][record]) - set(got[zone][record])))
                for ip in to_add[record]:
                    api.post(zone, ip, record)

            # remove record
            if state == "absent":
                to_remove= {}
                to_remove[record]= list(sorted(set(got[zone][record]) & set(wanted_ips)))
                for ip in to_remove[record]:
                    api.delete(zone, ip, record)


        ## If you need a local temp dir
        ##
        #local_tempdir= None
        #try:
        #    local_tempdir = tempfile.mkdtemp()
        #    
        #    # do stuff

        #except(Exception) as e:
        #    raise AnsibleError(to_native(e))

        #finally:
        #    os.system('rm -r ' + local_tempdir)

        # plugin exit point
        return result
