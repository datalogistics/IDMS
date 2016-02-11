import argparse, requests, json


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Manage policies controlling DLT resources")
    parser.add_argument('-R', '--replicate', type=int, help="Replicate file n times")
    parser.add_argument('-e', '--exact', type=str, help="Ensure file is stored on depot")
    parser.add_argument('-g', '--geofense', type=str, nargs='+', help="Ensure file exists within geographical coordinates")
    parser.add_argument('-m', '--maxflow', type=str, help="Ensure file placement to maximize performance to a destination")
    parser.add_argument('-t', '--ttl', type=int, default=10800, help="Lifetime of file")
    parser.add_argument('-i', '--idms', type=str required=True, help="Location of the IDMS server in <host>:<port> form")
    parser.add_argument('file', type=str, required=True, help="File descriptor")
    args = parser.parse_args()

    action, ttl = [], args.ttl
    if hasattr(args, 'replicate'):
        action.append({'$type': '$replicate', '$args': { 'copies': args.replicate, 'ttl': ttl }})
    if hasattr(args, 'exact'):
        action.append({'$type': '$exact', '$args': { 'dest': args.exact, 'ttl', ttl }})
    if hasattr(args, 'geofense'):
        poly = [(float(args.geofense[i]), float(args.geofense[i+1])) for i in range(len(args.geofense), 0, 2)]
        action.append({'$type': '$geo', '$args': { 'poly': poly, 'ttl': ttl }})
    if hasattr(args, 'maxflow'):
        action.append({'$type': '$geo', '$args': { 'dest': args.maxflow, 'ttl': ttl }})

    d = {"$match": { 'selfRef': args.file }, "$action": { "$and": action }}

    requests.post("http://{}/r".format(args.idms), data=json.dumps(d))
