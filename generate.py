#!/usr/bin/env python
import multiprocessing.pool
# import bioblend
import tqdm
import json
import datetime
import sys
import random
import time
import json
import sys
import os
import collections
import requests
import argparse


def fetch_invocation(invocation_id, server, key):
    url = f'{server}/api/invocations/{invocation_id}'
    r = requests.get(url, headers={'x-api-key': f'{key}'})
    return r.json()

def check_step_status(step, server, key):
    url = f'{server}/api/invocations/any/steps/{step}'
    r = requests.get(url, headers={'x-api-key': f'{key}'})
    return r.json()


def check_job_id_status(job, server, key):
    url = f'{server}/api/jobs/{job}?full=True'
    r = requests.get(url, headers={'x-api-key': f'{key}'})
    return r.json()

def short_tool_id(tool_id):
    if '/' in tool_id:
        return '/'.join(tool_id.split('/')[-3:-1])
    else:
        return tool_id


def flatten(d):
    return {
        k: v
        for (k, v) in d.items()
        if not isinstance(v, dict) and not isinstance(v, list)
    }

def f2u(s, tz):
    d = datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f') 
    return (int(d.strftime('%s')) + (tz * 3600.0)) * 1000 * 1000


def collect(invocation_id, args, key):
    invocation = fetch_invocation(invocation_id, args.galaxy_server, key)
    invocation['step_details'] = []
    steps = sorted(invocation['steps'], key=lambda x: x['order_index'])

    def _status(job_id):
        return check_job_id_status(job_id, args.galaxy_server, key)

    # Collect data
    for step in tqdm.tqdm(steps, position=0):
        step_status = check_step_status(step['id'], args.galaxy_server, key)
        step['state'] = step_status
        step['jobs'] = []

        pool = multiprocessing.pool.ThreadPool(processes=12)
        job_ids = [job['id'] for job in step_status['jobs']]
        results = pool.map(_status, job_ids, chunksize=1)
        pool.close()
        step['jobs'] = results

        # for job in tqdm.tqdm(step_status['jobs'], position=1):
        #     job_info = check_job_id_status(job['id'], args.galaxy_server, key)
        #     step['jobs'].append(job_info)
        invocation['step_details'].append(step)

    # with open(f'cache-{invocation_id}.json', 'w') as handle:
    #     json.dump(invocation, handle)
    return invocation


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Monitor a galaxy workflow invocation.')
    parser.add_argument('galaxy_server', type=str, default='https://usegalaxy.eu')
    parser.add_argument('invocation_id', type=str)
    parser.add_argument('--timezone-offset', type=int, default=1, help='Server hours offset from UTC, as Galaxy does not properly expose a timezone on timestamps used in the UI, and instead shows you a naive datetime. E.g. 1 for a server in CET')
    parser.add_argument('--exclude-cached', action='store_true', help='Exclude jobs running from cache')
    parser.add_argument('--checkpoint', type=str, help='Optional location for storing a checkpoint which can be re-parsed later into a trace.')
    args = parser.parse_args()

    key = os.environ['GALAXY_API_KEY']
    if not key:
        raise Exception('GALAXY_API_KEY environment variable not set.')
    # gi = bioblend.galaxy.GalaxyInstance(url=args.galaxy_server, key=key)

    invocation_id = args.invocation_id

    trace = {
        "meta_user": "galaxy-workflow-monitor",
        "meta_cpu_count": "128",
        "otherData": {
            "version": "Galaxy Workflow Trace v0",
            "server": args.galaxy_server,
            "invocation": args.invocation_id,
        },
        'traceEvents': [],
    }

    if args.checkpoint and os.path.exists(args.checkpoint):
        print("Loading checkpoint")
        with open(args.checkpoint, 'r') as handle:
            data = json.load(handle)
    else:
        data = collect(invocation_id, args, key)
        if args.checkpoint:
            print("Storing checkpoint")
            with open(args.checkpoint, 'w') as handle:
                json.dump(data, handle)

    machine_names = []
    tool_ids = []

    print(data['create_time'])
    trace['traceEvents'].append({
        'pid': 1,
        'tid': 1,
        'ts': f2u(data['create_time'], args.timezone_offset),
        'ph': 'I',
        'name': 'Workflow Inovcation Creation',
        'args': {'id': data['id'], 'history_id': data['history_id']}
    })

    # Pre-collect machine names and tool IDs
    for step in data['step_details']:
        for job_info in step['jobs']:
            job_metrics = job_info['job_metrics']
            if len(job_metrics) == 0:
                continue

            hostname = [x for x in job_metrics if x['title'] == 'hostname'][0]['value'].replace('vgcnbwc-worker-', '').replace('.novalocal', '')
            if hostname not in machine_names:
                machine_names.append(hostname)

            tool_id = short_tool_id(job_info['tool_id'])
            if tool_id not in tool_ids:
                tool_ids.append(tool_id)


    # Fill out the instantaneous details (e.g. workflow invocation)
    for step in data['step_details']:
        trace['traceEvents'].append({
            'pid': 1,
            'tid': 2,
            'ts': f2u(step['update_time'], args.timezone_offset),
            'ph': 'I',
            'name': f'{step["workflow_step_label"] or "Workflow Step"} Updated',
            'args': flatten(step),
        })

        for job_info in step['jobs']:
            if args.exclude_cached and job_info['copied_from_job_id']:
                    continue

            job_metrics = job_info['job_metrics']
            if len(job_metrics) == 0:
                continue
            hostname = [x for x in job_metrics if x['title'] == 'hostname'][0]['value'].replace('vgcnbwc-worker-', '').replace('.novalocal', '')
            str_epoch = [x for x in job_metrics if x['name'] == 'start_epoch'][0]['raw_value']
            str_epoch = float(str_epoch)
            tool_id = short_tool_id(job_info['tool_id'])
            # trace['traceEvents'].append({
            #     'pid': 3,
            #     'tid': 1 + tool_ids.index(tool_id),
            #     'ts': f2u(job_info['create_time']),
            #     'dur': f2u(job_info['update_time']) - f2u(job_info['create_time']),
            #     'ph': 'X',
            #     'name': f"Job {short_tool_id(job_info['tool_id'])} Updated",
            #     'args': flatten(job_info),
            # })


    # Add traces for the actual invocation
    for step in data['step_details']:
        for job_info in step['jobs']:
            if args.exclude_cached and job_info['copied_from_job_id']:
                    continue

            job_metrics = job_info['job_metrics']
            if len(job_metrics) == 0:
                continue
            hostname = [x for x in job_metrics if x['title'] == 'hostname'][0]['value'].replace('vgcnbwc-worker-', '').replace('.novalocal', '')
            str_epoch = [x for x in job_metrics if x['name'] == 'start_epoch'][0]['raw_value']
            str_epoch = float(str_epoch)
            end_epoch = [x for x in job_metrics if x['name'] == 'end_epoch'][0]['raw_value']
            end_epoch = float(end_epoch)
            memory = [x for x in job_metrics if x['name'] == 'memory.max_usage_in_bytes'][0]['raw_value']
            tool_id = short_tool_id(job_info['tool_id'])

            if hostname not in machine_names:
                machine_names.append(hostname)

            # Better presentation
            job_info['job_metrics'] = {
                x['title']: x['value']
                for x in job_info['job_metrics']
            }
            if 'dependencies' in job_info:
                del job_info['dependencies']

            trace['traceEvents'].append({
                'pid': 2,
                # 'pid': machine_names.index(hostname),
                'tid': 1 + machine_names.index(hostname),
                'ts': str_epoch * 1000 * 1000,
                'dur': (end_epoch - str_epoch) * 1000 * 1000,
                'ph': 'X',
                'name': tool_id,
                'args': job_info
            })

    print(f"Trace saved to trace-{invocation_id}.json")
    with open(f'trace-{invocation_id}.json', 'w') as handle:
        json.dump(trace, handle)
