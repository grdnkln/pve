#!/usr/bin/env python3

# taken from:
# https://forum.proxmox.com/threads/how-to-get-the-exactly-backup-size-in-proxmox-backup.93901/
# by "masgo" 2023-03-27

import os
import sys
import argparse
import json

format_json = False
all = False
datastore_path = "/mnt/datastore"


def scan_vmid(datastore, vmid, all):
    if not vmid:
        raise Exception("vmid is blank")

    scan_path = os.path.join(datastore, vmid)

    # Get all .img.fidx file paths for the chosen VM
    filearray = []
    for root, dirs, files in os.walk(scan_path, topdown=False):
        for name in files:
            if name.endswith(".img.fidx"):
                filearray.append(os.path.join(root, name))

    # sort to obtain filepath sort
    filearray.sort()

    if len(filearray) > 0:
        if format_json:
            snapshots = {}
        else:
            print("-" * 86)
            print(f"| {'snapshot'.ljust(20)} | {'filename'.ljust(18)} | {'new chunks'.ljust(19)} | {'new chunks size'.ljust(16)} |")
            print("-" * 86)
        chunkarray = set()

        totals = {"new_chunks": 0, "new_chunks_bytes": 0}

        for filepath in filearray:
            snapshot = os.path.basename(os.path.dirname(filepath))
            filename = os.path.basename(filepath)
            # remove .img.fidx since it's just redundant (duplicated, always there, always the same)?
            filename = filename[0:len(filename)-len(".img.fidx")]

#            print("DEBUG: snapshot = %s" % snapshot)
            with open(filepath, "rb") as f:
                f.seek(4096)
                data = f.read()
                hex_data = data.hex()
                file_chunks = []
                new_unique_chunks = set()
                for i in range(0, len(hex_data), 64):
                    file_chunk = hex_data[i:i+64]
                    if file_chunk not in chunkarray :
                        new_unique_chunks.add(file_chunk)
                    chunkarray.add(file_chunk)
                if new_unique_chunks:
                    new_chunks = len(new_unique_chunks)
                elif all:
                    new_chunks = 0
                else:
                    continue

                if format_json:
                    if snapshot in snapshots:
                        images = snapshots[snapshot]
                    else:
                        images = []
                        snapshots[snapshot] = images
                    images += [{"filename": filename, "new_chunks": new_chunks, "new_chunks_bytes": new_chunks * 4194304}]
                else:
                    print(f"| {snapshot.ljust(20)} | {filename.ljust(18)} | {new_chunks:>12} chunks | {new_chunks * 4:>12.2f} MiB |")

                totals["new_chunks"] += new_chunks
                totals["new_chunks_bytes"] += new_chunks * 4194304
        if format_json:
            return {"snapshots": snapshots, "totals": totals}
        else:
            print("-" * 86)
            print(f"| {'TOTAL'.ljust(20)} | {''.ljust(18)} | {totals['new_chunks']:>12} chunks | {totals['new_chunks'] * 4:>12.2f} MiB |")
            print("-" * 86)

    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Estimate space used by vm images.")
    parser.add_argument('datastore', metavar='datastore', type=str,
                    help="datastore to look in, also takes full path to alternative location")
    parser.add_argument('vmids', metavar='vmids', type=str, nargs='*',
                    help='vmid(s) to scan for backups')
    parser.add_argument('-j', '--json', action='store_const',
                    const=True, default=False,
                    help='enable json output')
    parser.add_argument('-a', '--all', action='store_const',
                    const=True, default=False,
                    help='show snapshots and images that have no new chunks')

    args = parser.parse_args()

    format_json = args.json
    all = args.all

    if os.path.isabs(args.datastore):
        datastore_path = args.datastore
    else:
        datastore_path = os.path.join(datastore_path, args.datastore)
    datastore_path = os.path.join(datastore_path,"vm")
    if len(args.vmids) == 0:
        vmids_list = []
        for f in sorted(os.listdir(datastore_path)):
            fp = os.path.join(datastore_path, f)
            if os.path.isdir(fp):
                vmids_list += [f]
    else:
        vmids_list = args.vmids

    vmids = []
    for vmid in vmids_list:
        if not format_json:
            print("vmid = %s" % vmid)
        result = scan_vmid(datastore_path, vmid, all)

        if format_json:
            vmids += [{"vmid": vmid, "totals": result["totals"], "snapshots": result["snapshots"]}]
    if format_json:
        print(json.dumps(vmids))

