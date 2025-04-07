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

chunk_size = 4194304

column_names = ["snapshot", "filename", "new chunks", "new chunks size"]
column_widths = [20,25,19,16]
table_width = sum(column_widths) + len(column_widths)*3 + 1

def sizeof_fmt(num, unit=None):
    units = ("Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB")
    if unit:
        i = units.index(unit)
        num /= 1024.0 ** i
        if i == 0:
            return f"{int(num):3d} {units[i]}"
        else:
            return f"{num:3.2f} {units[i]}"
    for i in range(0, len(units)):
        if abs(num) < 1024.0:
            if i == 0:
                return f"{int(num):3d} {units[i]}"
            else:
                return f"{num:3.2f} {units[i]}"
        num /= 1024.0
    return f"{num:.2f} YiB"

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
            print("-" * table_width)
            print(f"| {column_names[0].ljust(column_widths[0])} | {column_names[1].ljust(column_widths[1])} | {column_names[2].ljust(column_widths[2])} | {column_names[3].ljust(column_widths[3])} |")
            print("-" * table_width)
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
                    images += [{"filename": filename, "new_chunks": new_chunks, "new_chunks_bytes": new_chunks * chunk_size}]
                else:
                    print(f"| {snapshot.ljust(column_widths[0])} | {filename.ljust(column_widths[1])} | {(str(new_chunks) + ' chunks').rjust(column_widths[2])} | {sizeof_fmt(new_chunks * chunk_size, args.units).rjust(column_widths[3])} |")

                totals["new_chunks"] += new_chunks
                totals["new_chunks_bytes"] += new_chunks * chunk_size
        if format_json:
            return {"snapshots": snapshots, "totals": totals}
        else:
            print("-" * table_width)
            print(f"| {'TOTAL'.ljust(column_widths[0])} | {''.ljust(column_widths[1])} | {(str(totals['new_chunks']) + ' chunks').rjust(column_widths[2])} | {sizeof_fmt(totals['new_chunks_bytes'], args.units).rjust(column_widths[3])} |")
            print("-" * table_width)

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
    parser.add_argument('-u', '--units', type=str, default=None,
                        choices=["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"],
                        help='Specify the units for size output')
    
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

